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
DST=${2:-$SOURCE_PATH/../headers};

PATH="/usr/local/bin:/usr/local/sbin:/usr/bin:$PATH"

# Опция -mcx16 для того, чтобы выбиралось больше заголовочных файлов (с запасом).

for src_file in $($CLANG -M -xc++ -std=gnu++1z -Wall -Werror -msse4 -mcx16 -mpopcnt -O3 -g -fPIC \
    $(cat "$SOURCE_PATH/build/include_directories.txt") \
    "$SOURCE_PATH/dbms/src/Interpreters/SpecializedAggregator.h" |
    tr -d '\\' |
    sed -r -e 's/^\w+\.o://');
do
    dst_file=$src_file;
    mkdir -p "$DST/$(echo $dst_file | sed -r -e 's/\/[^/]*$/\//')";
    cp "$src_file" "$DST/$dst_file";
done


# Копируем больше заголовочных файлов с интринсиками, так как на серверах, куда будут устанавливаться
#  заголовочные файлы, будет использоваться опция -march=native.

for i in $(ls -1 $($CLANG -v -xc++ - <<<'' 2>&1 | grep '^ /' | grep 'include' | grep '/lib/clang/')/*.h | grep -vE 'arm|altivec|Intrin');
do
    cp "$i" "$DST/$i";
done
