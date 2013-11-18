#!/bin/bash
mkdir -p ${CMAKE_CURRENT_BINARY_DIR}/src
echo "#ifndef SVN_REVISION" > ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h
echo -n "#define SVN_REVISION " >> ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h

cd ${CMAKE_CURRENT_SOURCE_DIR};
if [ $(git rev-parse --is-inside-work-tree 2> /dev/null ) ];
then
	# git svn
	echo && (LC_ALL=C git svn log --oneline -1 2> /dev/null || echo 'r1 |') | cut -d '|' -f 1 | sed s/r// >> ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h ;
else
	# svn
	echo && (LC_ALL=C svn info ${PROJECT_SOURCE_DIR}/ 2>/dev/null || echo Revision 1) | grep Revision | cut -d " " -f 2 >> ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h;
fi
echo "#endif" >> ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h
