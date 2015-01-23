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


for i in $(clang -M -xc++ -std=gnu++1y -Wall -Werror -msse4 -mpopcnt -O3 -g -fPIC \
	$(cat "$SOURCE_PATH/CMakeLists.txt" | grep include_directories | grep -v METRICA_BINARY_DIR | sed -e "s!\${METRICA_SOURCE_DIR}!$SOURCE_PATH!; s!include_directories (!-I !; s!)!!;" | tr '\n' ' ') \
	"$SOURCE_PATH/dbms/include/DB/Interpreters/SpecializedAggregator.h" |
	tr -d '\\' |
	grep -v '.o:' |
	sed -r -e 's/^.+\.cpp / /');
do
	mkdir -p "$DST/$(echo $i | sed -r -e 's/\/[^/]*$/\//')";
	cp "$i" "$DST/$i";
done


# Копируем больше заголовочных файлов с интринсиками, так как на серверах, куда будут устанавливаться
#  заголовочные файлы, будет использоваться опция -march=native.

for i in $(ls -1 $(clang -v -xc++ - <<<'' 2>&1 | grep '^ /' | grep 'include' | grep '/lib/clang/')/*.h | grep -vE 'arm|altivec|Intrin');
do
	cp "$i" "$DST/$i";
done
