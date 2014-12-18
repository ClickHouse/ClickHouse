#!/bin/bash
mkdir -p ${CMAKE_CURRENT_BINARY_DIR}/src
echo "#ifndef REVISION" > ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h
echo -n "#define REVISION " >> ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h

cd ${CMAKE_CURRENT_SOURCE_DIR};

if (git rev-parse --is-inside-work-tree &> /dev/null)
then
	# GIT
	git fetch --tags;

	# берем последний тэг из текущего коммита
	revision=$(git tag --points-at HEAD 2> /dev/null | tail -1)

	# или ближайший тэг если в данном комите нет тэгов
	if [[ "$revision" = "" ]]; then
		revision=$( ( git describe --tags || echo 1 ) | cut -d "-" -f 1 )
	fi
	echo $revision >> ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h;
else
	#SVN
	echo && (LC_ALL=C svn info ${PROJECT_SOURCE_DIR}/ 2>/dev/null || echo Revision 1) | grep Revision | cut -d " " -f 2 >> ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h;
fi

echo "#endif" >> ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h
