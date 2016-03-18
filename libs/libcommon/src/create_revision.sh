#!/bin/bash

# отрезает суффикс от тегов мобильной метрики, чтобы привести к числу
# фильтрует теги, не являющиеся релизными тегами
function tag_filter
{
    sed 's/-mobmet//g' | grep -E "^[0-9]{5,8}$"
}

if [[ $# -ne 1 ]] && [[ $# -ne 2 ]]; then
	echo "usage: create_revision.sh out_file_path [--use_dbms_tcp_protocol_version]"
	exit 1
fi

out_file=$1
dir=$(dirname $out_file)

use_dbms_tcp_protocol_version="$2"

mkdir -p $dir

if [ "$use_dbms_tcp_protocol_version" == "--use_dbms_tcp_protocol_version" ];
then

	echo "
#include \"DB/Core/Defines.h\"
#ifndef REVISION
#define REVISION DBMS_TCP_PROTOCOL_VERSION
#endif
" > $out_file

else
	# GIT
	git fetch --tags;
	# берем последний тэг из текущего коммита
	revision=$(git tag --points-at HEAD 2> /dev/null | tag_filter | tail -1)
	# или ближайший тэг если в данном комите нет тэгов
	if [[ "$revision" = "" ]]; then
		revision=$( ( git describe --tags) | cut -d "-" -f 1 | tag_filter )
	fi
	
	if [[ "$revision" == "" ]]; then
	    # в крайнем случае выбирем любую версию как версию демона
	    # нужно для stash или неполноценной копии репозитория
	    revision="77777"
	fi

	echo "
#ifndef REVISION
#define REVISION $revision
#endif
" > $out_file

fi
