#!/bin/bash

source "$(dirname "$0")/get_revision_lib.sh"

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
	# берем последний тэг из текущего коммита
	revision=$(get_revision)

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
