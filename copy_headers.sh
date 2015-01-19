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

# Указано -mavx2, чтобы было найдено больше заголовочных файлов с интринсиками
# - с запасом, для последующего использования -march=native на сервере.

for i in $(clang -M -xc++ -std=gnu++1y -Wall -Werror -mavx2 -O3 -g -fPIC \
	$(cat "$SOURCE_PATH/CMakeLists.txt" | grep include_directories | grep -v METRICA_BINARY_DIR | sed -e "s!\${METRICA_SOURCE_DIR}!$SOURCE_PATH!; s!include_directories (!-I !; s!)!!;" | tr '\n' ' ') \
	"$SOURCE_PATH/dbms/include/DB/Interpreters/SpecializedAggregator.h" |
	tr -d '\\' |
	grep -v '.o:' |
	sed -r -e 's/^.+\.cpp / /');
do
	mkdir -p "$DST/$(echo $i | sed -r -e 's/\/[^/]*$/\//')";
	cp "$i" "$DST/$i";
done
