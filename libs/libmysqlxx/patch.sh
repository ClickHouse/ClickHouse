#!/bin/bash

# Удаляем из библиотеки объектный файл с лишними символами, которые конфликтуют с имеющимися при статической сборке.

LIB=$(find /usr/lib/ -name 'libmysqlclient.a')

mkdir -p tmp
cd tmp

ar x $LIB
ar t $LIB | grep -v 'my_new.cc.o' | xargs ar rcs ../$(basename $LIB)

cd ..
