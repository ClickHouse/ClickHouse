#!/usr/bin/env bash

# Exclude from libmysqlclient.a those object files that contain symbols conflicting with symbols from the libraries we use.

LIB=$1
OUT=$2

ZLIB_OBJS_REGEX="(my_new.cc.o|adler32.c.o|compress.c.o|crc32.c.o|deflate.c.o|gzio.c.o|infback.c.o|inffast.c.o|inflate.c.o|inftrees.c.o|trees.c.o|uncompr.c.o|zutil.c.o|my_sha2.cc.o)"

mkdir -p tmp
cd tmp

ar x $LIB
ar t $LIB | egrep --word-regex -v $ZLIB_OBJS_REGEX | xargs ar rcs $OUT

cd ..
