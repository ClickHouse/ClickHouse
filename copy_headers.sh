#!/bin/bash -e

# Этот скрипт собирает все заголовочные файлы, нужные для компиляции некоторого translation unit-а
#  и копирует их с сохранением путей в директорию DST.
# Это затем может быть использовано, чтобы скомпилировать translation unit на другом сервере,
#  используя ровно такой же набор заголовочных файлов.
#
# Требуется clang, желательно наиболее свежий (trunk).
#
# Используется при сборке пакетов.
# Заголовочные файлы записываются в пакет clickhouse-server-base, в директорию /usr/share/clickhouse/headers.
#
# Если вы хотите установить их самостоятельно, без сборки пакета,
#  чтобы clickhouse-server видел их там, где ожидается, выполните:
#
# sudo ./copy_headers.sh . /usr/share/clickhouse/headers/

SOURCE_PATH=${1:-.}
DST=${2:-$SOURCE_PATH/../headers}
BUILD_PATH=${3:-$SOURCE_PATH/build}

PATH="/usr/local/bin:/usr/local/sbin:/usr/bin:$PATH"

if [[ -z $CLANG ]]; then
    CLANG="clang"
fi

START_HEADERS=$(echo \
    $BUILD_PATH/dbms/src/Common/config_version.h \
    $SOURCE_PATH/dbms/src/Interpreters/SpecializedAggregator.h \
    $SOURCE_PATH/dbms/src/AggregateFunctions/AggregateFunction*.h)

# Опция -mcx16 для того, чтобы выбиралось больше заголовочных файлов (с запасом).
# The latter options are the same that are added while building packages.

GCC_ROOT=`$CLANG -v 2>&1 | grep "Selected GCC installation"| sed -n -e 's/^.*: //p'`

for src_file in $(echo | $CLANG -M -xc++ -std=c++1z -Wall -Werror -msse4 -mcx16 -mpopcnt -O3 -g -fPIC -fstack-protector -D_FORTIFY_SOURCE=2 \
    -I $GCC_ROOT/include \
    -I $GCC_ROOT/include-fixed \
    $(cat "$BUILD_PATH/include_directories.txt") \
    $(echo $START_HEADERS | sed -r -e 's/[^ ]+/-include \0/g') \
    - |
    tr -d '\\' |
    sed -r -e 's/^-\.o://');
do
    dst_file=$src_file;
    dst_file=$(echo $dst_file | sed -r -e 's/build\///')    # for simplicity reasons, will put generated headers near the rest.
    mkdir -p "$DST/$(echo $dst_file | sed -r -e 's/\/[^/]*$/\//')";
    cp "$src_file" "$DST/$dst_file";
done


# Копируем больше заголовочных файлов с интринсиками, так как на серверах, куда будут устанавливаться
#  заголовочные файлы, будет использоваться опция -march=native.

for src_file in $(ls -1 $($CLANG -v -xc++ - <<<'' 2>&1 | grep '^ /' | grep 'include' | grep -E '/lib/clang/|/include/clang/')/*.h | grep -vE 'arm|altivec|Intrin');
do
    mkdir -p "$DST/$(echo $src_file | sed -r -e 's/\/[^/]*$/\//')";
    cp "$src_file" "$DST/$src_file";
done

# Even more platform-specific headers
for src_file in $(ls -1 $SOURCE_PATH/contrib/boost/libs/smart_ptr/include/boost/smart_ptr/detail/*);
do
    mkdir -p "$DST/$(echo $src_file | sed -r -e 's/\/[^/]*$/\//')";
    cp "$src_file" "$DST/$src_file";
done

for src_file in $(ls -1 $SOURCE_PATH/contrib/boost/boost/smart_ptr/detail/*);
do
    mkdir -p "$DST/$(echo $src_file | sed -r -e 's/\/[^/]*$/\//')";
    cp "$src_file" "$DST/$src_file";
done
