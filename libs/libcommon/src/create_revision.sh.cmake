#!/bin/bash
mkdir -p ${CMAKE_CURRENT_BINARY_DIR}/src
echo "#ifndef SVN_REVISION" > ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h
echo -n "#define SVN_REVISION " >> ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h

cd ${CMAKE_CURRENT_SOURCE_DIR};

# svn
echo && (LC_ALL=C svn info ${PROJECT_SOURCE_DIR}/ 2>/dev/null || echo Revision 1) | grep Revision | cut -d " " -f 2 >> ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h;

echo "#endif" >> ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h
