#!/bin/bash

set -e

if [[ $# -ne 1 ]]; then
	echo usage: create_revision.sh out_file_path
	exit 1
fi

out_file=$1
dir=$(dirname $out_file)

mkdir -p $dir
echo "#ifndef REVISION" > $out_file
echo -n "#define REVISION " >> $out_file

# для stash выставляем жестко ревизию
if [ -z "$(git config --get remote.origin.url | grep github)" ];
then
	revision=53694
else
	# GIT
	git fetch --tags;
	# берем последний тэг из текущего коммита
	revision=$(git tag --points-at HEAD 2> /dev/null | tail -1)
	# или ближайший тэг если в данном комите нет тэгов
	if [[ "$revision" = "" ]]; then
		revision=$( ( git describe --tags) | cut -d "-" -f 1 )
	fi
fi

echo $revision >> $out_file
echo "#endif" >> $out_file
