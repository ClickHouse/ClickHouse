#!/bin/bash
mkdir -p "${CMAKE_CURRENT_BINARY_DIR}/src"
echo "#ifndef REVISION" > "${CMAKE_CURRENT_BINARY_DIR}/src/revision.h"
echo -n "#define REVISION " >> "${CMAKE_CURRENT_BINARY_DIR}/src/revision.h"

cd "${CMAKE_CURRENT_SOURCE_DIR}";

# GIT
git fetch --tags;

# берем последний тэг из текущего коммита
revision=$(git tag --points-at HEAD 2> /dev/null | tail -1)

# или ближайший тэг если в данном комите нет тэгов
if [[ "$revision" = "" ]]; then
	revision=$( ( git describe --tags || echo 1 ) | cut -d "-" -f 1 )
fi

is_it_github=$( git config --get remote.origin.url | grep 'github' )
if [[ "$is_it_github" = "" ]]; then
	revision=53694
fi

echo $revision >> "${CMAKE_CURRENT_BINARY_DIR}/src/revision.h";

echo "#endif" >> "${CMAKE_CURRENT_BINARY_DIR}/src/revision.h"
