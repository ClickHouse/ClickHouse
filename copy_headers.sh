#!/bin/bash -e

#echo "Args: $*"; env | sort
#set -x

# Этот скрипт собирает все заголовочные файлы, нужные для компиляции некоторого translation unit-а
#  и копирует их с сохранением путей в директорию DST.
# Это затем может быть использовано, чтобы скомпилировать translation unit на другом сервере,
#  используя ровно такой же набор заголовочных файлов.
#
# Требуется clang, желательно наиболее свежий (trunk).
#
# Используется при сборке пакетов.
# Заголовочные файлы записываются в пакет clickhouse-common, в директорию /usr/share/clickhouse/headers.
#
# Если вы хотите установить их самостоятельно, без сборки пакета,
#  чтобы clickhouse-server видел их там, где ожидается, выполните:
#
# sudo ./copy_headers.sh . /usr/share/clickhouse/headers/

SOURCE_PATH=${1:-.}
DST=${2:-$SOURCE_PATH/../headers}
BUILD_PATH=${BUILD_PATH=${3:-$SOURCE_PATH/build}}

PATH="/usr/local/bin:/usr/local/sbin:/usr/bin:$PATH"

if [[ -z $CLANG ]]; then
    CLANG="clang"
fi

START_HEADERS=$(echo \
    $BUILD_PATH/dbms/src/Common/config_version.h \
    $SOURCE_PATH/dbms/src/Interpreters/SpecializedAggregator.h \
    $SOURCE_PATH/dbms/src/AggregateFunctions/AggregateFunction*.h)

for header in $START_HEADERS; do
    START_HEADERS_INCLUDE+="-include $header "
done

# Опция -mcx16 для того, чтобы выбиралось больше заголовочных файлов (с запасом).
# The latter options are the same that are added while building packages.

# TODO: Does not work on macos:
GCC_ROOT=`$CLANG -v 2>&1 | grep "Selected GCC installation"| sed -n -e 's/^.*: //p'`

for src_file in $(echo | $CLANG -M -xc++ -std=c++1z -Wall -Werror -msse4 -mcx16 -mpopcnt -O3 -g -fPIC -fstack-protector -D_FORTIFY_SOURCE=2 \
    -I $GCC_ROOT/include \
    -I $GCC_ROOT/include-fixed \
    $(cat "$BUILD_PATH/include_directories.txt") \
    $START_HEADERS_INCLUDE \
    - |
    tr -d '\\' |
    sed -E -e 's/^-\.o://');
do
    dst_file=$src_file;
    [ -n $BUILD_PATH ] && dst_file=$(echo $dst_file | sed -E -e "s!^$BUILD_PATH!!")
    [ -n $DESTDIR ] && dst_file=$(echo $dst_file | sed -E -e "s!^$DESTDIR!!")
    dst_file=$(echo $dst_file | sed -E -e 's/build\///')    # for simplicity reasons, will put generated headers near the rest.
    mkdir -p "$DST/$(echo $dst_file | sed -E -e 's/\/[^/]*$/\//')";
    cp "$src_file" "$DST/$dst_file";
done


# Копируем больше заголовочных файлов с интринсиками, так как на серверах, куда будут устанавливаться
#  заголовочные файлы, будет использоваться опция -march=native.

for src_file in $(ls -1 $($CLANG -v -xc++ - <<<'' 2>&1 | grep '^ /' | grep 'include' | grep -E '/lib/clang/|/include/clang/')/*.h | grep -vE 'arm|altivec|Intrin');
do
    dst_file=$src_file;
    [ -n $BUILD_PATH ] && dst_file=$(echo $dst_file | sed -E -e "s!^$BUILD_PATH!!")
    [ -n $DESTDIR ] && dst_file=$(echo $dst_file | sed -E -e "s!^$DESTDIR!!")
    mkdir -p "$DST/$(echo $dst_file | sed -E -e 's/\/[^/]*$/\//')";
    cp "$src_file" "$DST/$dst_file";
done

# Even more platform-specific headers
for src_file in $(ls -1 $SOURCE_PATH/contrib/boost/libs/smart_ptr/include/boost/smart_ptr/detail/*);
do
    dst_file=$src_file;
    [ -n $BUILD_PATH ] && dst_file=$(echo $dst_file | sed -E -e "s!^$BUILD_PATH!!")
    [ -n $DESTDIR ] && dst_file=$(echo $dst_file | sed -E -e "s!^$DESTDIR!!")
    mkdir -p "$DST/$(echo $dst_file | sed -E -e 's/\/[^/]*$/\//')";
    cp "$src_file" "$DST/$dst_file";
done

for src_file in $(ls -1 $SOURCE_PATH/contrib/boost/boost/smart_ptr/detail/*);
do
    dst_file=$src_file;
    [ -n $BUILD_PATH ] && dst_file=$(echo $dst_file | sed -E -e "s!^$BUILD_PATH!!")
    [ -n $DESTDIR ] && dst_file=$(echo $dst_file | sed -E -e "s!^$DESTDIR!!")
    mkdir -p "$DST/$(echo $dst_file | sed -E -e 's/\/[^/]*$/\//')";
    cp "$src_file" "$DST/$dst_file";
done
