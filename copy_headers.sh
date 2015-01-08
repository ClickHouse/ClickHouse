#!/bin/bash -e

# Этот скрипт собирает все заголовочные файлы, нужные для компиляции некоторого translation unit-а
#  и копирует их с сохранением путей в директорию DST.
# Это затем может быть использовано, чтобы скомпилировать translation unit на другом сервере,
#  используя ровно такой же набор заголовочных файлов.

PROJECT_PATH=${1:-..}
DST=${2:-$PROJECT_PATH/headers};

for i in $(/usr/bin/c++ -M -xc++ -std=gnu++1y -Wall -Werror -march=native -O3 -g -shared -fPIC -rdynamic \
	$(cat $PROJECT_PATH/src/CMakeLists.txt | grep include_directories | grep -v METRICA_BINARY_DIR | sed -e "s!\${METRICA_SOURCE_DIR}!$PROJECT_PATH/src!; s!include_directories (!-I !; s!)!!;" | tr '\n' ' ') \
	$PROJECT_PATH/src/dbms/include/DB/Interpreters/SpecializedAggregator.h |
	tr -d '\\' |
	grep -v '.o:' |
	ssed -R -e 's/^.+\.cpp / /');
do
	mkdir -p $DST/$(echo $i | ssed -R -e 's/\/[^/]*$/\//');
	cp $i $DST/$i;
#	echo $(readlink -f $i);
done
