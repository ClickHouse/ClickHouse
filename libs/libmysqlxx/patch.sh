#!/usr/bin/env bash

# Удаляем из библиотеки объектный файл с лишними символами, которые конфликтуют с имеющимися при статической сборке.

LIB=$1
OUT=$2

mkdir -p tmp
cd tmp

ar x $LIB
ar t $LIB | grep -v 'my_new.cc.o' | xargs ar rcs $OUT

cd ..
