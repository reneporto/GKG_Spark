#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 23 19:11:46 2022

Script com função auxiliares para analise e download dos ficheiros via S3.

@author: rene
"""

import os
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import pandas as pd
import os.path as op


def get_objects_name(s3_resource, bucket_name, prefix="", name_word = "", test=0):
    list_filename = []
    list_size = []
    my_bucket = s3_resource.Bucket(bucket_name)
    count = 0
    for s3_object in my_bucket.objects.filter(Prefix=prefix):
        filename = s3_object.key
        length = s3_object.size
        if name_word == "" or name_word in filename: 
            list_filename.append(filename) 
            list_size.append(length)
        if test == 1:
            count += 1
        if count >= 100:
            break
    names = pd.DataFrame({"filename":list_filename, "size_b":list_size})
    return names


def download_objects(s3_resource, bucket_name, prefix, name_word = "", aux=0):
    my_bucket = s3_resource.Bucket(bucket_name)
    objects = my_bucket.objects.filter(Prefix=prefix)
    for obj in objects:
        path, filename = os.path.split(obj.key)
        complete_filename = path.replace('/', '|') + '|' + filename
        if name_word == "" or name_word in filename:
            my_bucket.download_file(obj.key, complete_filename)


def download_list_objects(s3_resource, bucket_name, prefix, files, path):
    my_bucket = s3_resource.Bucket(bucket_name)
    count=0
    for each in files:
        if os.path.exists(each):
            my_bucket.download_file(str(prefix) + str(each), 
                                    str(path) + str(each))
            count+=1
            print(count)
        else:
            print(f"Already downloaded: {each}")
        
