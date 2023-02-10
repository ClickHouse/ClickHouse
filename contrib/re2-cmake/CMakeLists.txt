# Copyright 2015 The RE2 Authors.  All Rights Reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

# This file was edited for ClickHouse

string(FIND ${CMAKE_CURRENT_BINARY_DIR} " " _have_space)
if(_have_space GREATER 0)
    message(FATAL_ERROR "Using spaces in build path [${CMAKE_CURRENT_BINARY_DIR}] highly not recommended. Library re2st will be disabled.")
endif()

set(SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/re2")

set(RE2_SOURCES
    ${SRC_DIR}/re2/bitstate.cc
    ${SRC_DIR}/re2/compile.cc
    ${SRC_DIR}/re2/dfa.cc
    ${SRC_DIR}/re2/filtered_re2.cc
    ${SRC_DIR}/re2/mimics_pcre.cc
    ${SRC_DIR}/re2/nfa.cc
    ${SRC_DIR}/re2/onepass.cc
    ${SRC_DIR}/re2/parse.cc
    ${SRC_DIR}/re2/perl_groups.cc
    ${SRC_DIR}/re2/prefilter.cc
    ${SRC_DIR}/re2/prefilter_tree.cc
    ${SRC_DIR}/re2/prog.cc
    ${SRC_DIR}/re2/re2.cc
    ${SRC_DIR}/re2/regexp.cc
    ${SRC_DIR}/re2/set.cc
    ${SRC_DIR}/re2/simplify.cc
    ${SRC_DIR}/re2/stringpiece.cc
    ${SRC_DIR}/re2/tostring.cc
    ${SRC_DIR}/re2/unicode_casefold.cc
    ${SRC_DIR}/re2/unicode_groups.cc
    ${SRC_DIR}/util/rune.cc
    ${SRC_DIR}/util/strutil.cc
)
add_library(re2 ${RE2_SOURCES})
target_include_directories(re2 PUBLIC "${SRC_DIR}")

# Building re2 which is thread-safe and re2_st which is not.
# re2 changes its state during matching of regular expression, e.g. creates temporary DFA.
# It uses RWLock to process the same regular expression object from different threads.
# In order to avoid redundant locks in some cases, we use not thread-safe version of the library (re2_st).

add_library(re2_st ${RE2_SOURCES})
target_compile_definitions (re2_st PRIVATE NDEBUG NO_THREADS re2=re2_st)
target_include_directories (re2_st PRIVATE .)
target_include_directories (re2_st SYSTEM PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories (re2_st SYSTEM BEFORE PUBLIC ${SRC_DIR})

file (MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/re2_st)
foreach (FILENAME filtered_re2.h re2.h set.h stringpiece.h)
    add_custom_command (OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/re2_st/${FILENAME}"
        COMMAND ${CMAKE_COMMAND} -DSOURCE_FILENAME="${SRC_DIR}/re2/${FILENAME}"
            -DTARGET_FILENAME="${CMAKE_CURRENT_BINARY_DIR}/re2_st/${FILENAME}"
            -P "${CMAKE_CURRENT_SOURCE_DIR}/re2_transform.cmake"
        COMMENT "Creating ${FILENAME} for re2_st library.")
    add_custom_target (transform_${FILENAME} DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/re2_st/${FILENAME}")
    add_dependencies (re2_st transform_${FILENAME})
endforeach ()

file (MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/util)
foreach (FILENAME mutex.h)
    add_custom_command (OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/util/${FILENAME}"
        COMMAND ${CMAKE_COMMAND} -DSOURCE_FILENAME="${SRC_DIR}/util/${FILENAME}"
            -DTARGET_FILENAME="${CMAKE_CURRENT_BINARY_DIR}/util/${FILENAME}"
            -P "${CMAKE_CURRENT_SOURCE_DIR}/re2_transform.cmake"
        COMMENT "Creating ${FILENAME} for re2_st library.")
    add_custom_target (transform_${FILENAME} DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/util/${FILENAME}")
    add_dependencies (re2_st transform_${FILENAME})
endforeach ()

# NOTE: you should not change name of library here, since it is used to generate required header (see above)
add_library(ch_contrib::re2 ALIAS re2)
add_library(ch_contrib::re2_st ALIAS re2_st)
