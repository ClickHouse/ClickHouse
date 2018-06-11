# 2006-2008 (c) Viva64.com Team
# 2008-2018 (c) OOO "Program Verification Systems"
#
# Version 7
#
# Source: https://github.com/viva64/pvs-studio-cmake-examples/blob/master/PVS-Studio.cmake
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

cmake_minimum_required(VERSION 2.8.12)
cmake_policy(SET CMP0054 NEW)
cmake_policy(SET CMP0045 OLD)

if(__PVS_STUDIO_INCLUDED)
    return()
endif()
set(__PVS_STUDIO_INCLUDED TRUE)

function (pvs_studio_relative_path VAR ROOT FILEPATH)
    set("${VAR}" "${FILEPATH}" PARENT_SCOPE)
    if ("${FILEPATH}" MATCHES "^/.*$")
        file(RELATIVE_PATH RPATH "${ROOT}" "${FILEPATH}")
        if (NOT "${RPATH}" MATCHES "^\\.\\..*$")
            set("${VAR}" "${RPATH}" PARENT_SCOPE)
        endif ()
    endif ()
endfunction ()

function (pvs_studio_join_path VAR DIR1 DIR2)
    if ("${DIR2}" MATCHES "^(/|~).*$" OR "${DIR1}" STREQUAL "")
        set("${VAR}" "${DIR2}" PARENT_SCOPE)
    else ()
        set("${VAR}" "${DIR1}/${DIR2}" PARENT_SCOPE)
    endif ()
endfunction ()

macro (pvs_studio_append_flags_from_property CXX C DIR PREFIX)
    if (NOT "${PROPERTY}" STREQUAL "NOTFOUND" AND NOT "${PROPERTY}" STREQUAL "PROPERTY-NOTFOUND")
        foreach (PROP ${PROPERTY})
            pvs_studio_join_path(PROP "${DIR}" "${PROP}")

            if (APPLE AND "${PREFIX}" STREQUAL "-I" AND IS_ABSOLUTE "${PROP}" AND "${PROP}" MATCHES "\\.framework$")
                get_filename_component(FRAMEWORK "${PROP}" DIRECTORY)
                list(APPEND "${CXX}" "-iframework")
                list(APPEND "${CXX}" "${FRAMEWORK}")
                list(APPEND "${C}" "-iframework")
                list(APPEND "${C}" "${FRAMEWORK}")
                if (PVS_STUDIO_DEBUG)
                    message("PVS-Studio: framework: ${FRAMEWORK}")
                endif ()
            elseif ("${PROP}" MATCHES "^\\$<.*>")
                if (PVS_STUDIO_DEBUG)
                    message("PVS-Studio: warning: ignored ${PREFIX}${PROP}")
                endif ()
            elseif (NOT "${PROP}" STREQUAL "")
                list(APPEND "${CXX}" "${PREFIX}${PROP}")
                list(APPEND "${C}" "${PREFIX}${PROP}")
                if (PVS_STUDIO_DEBUG)
                    message("PVS-Studio: compile flag: ${PREFIX}${PROP}")
                endif ()
            endif()
        endforeach ()
    endif ()
endmacro ()

macro (pvs_studio_append_standard_flag FLAGS STANDARD)
    if ("${STANDARD}" MATCHES "^(99|11|14|17)$")
        if ("${PVS_STUDIO_PREPROCESSOR}" MATCHES "gcc|clang")
            list(APPEND "${FLAGS}" "-std=c++${STANDARD}")
        endif ()
    endif ()
endmacro ()

function (pvs_studio_set_directory_flags DIRECTORY CXX C)
    set(CXX_FLAGS "${${CXX}}")
    set(C_FLAGS "${${C}}")

    get_directory_property(PROPERTY DIRECTORY "${DIRECTORY}" INCLUDE_DIRECTORIES)
    pvs_studio_append_flags_from_property(CXX_FLAGS C_FLAGS "${DIRECTORY}" "-I")

    get_directory_property(PROPERTY DIRECTORY "${DIRECTORY}" COMPILE_DEFINITIONS)
    pvs_studio_append_flags_from_property(CXX_FLAGS C_FLAGS "" "-D")

    set("${CXX}" "${CXX_FLAGS}" PARENT_SCOPE)
    set("${C}" "${C_FLAGS}" PARENT_SCOPE)
endfunction ()

function (pvs_studio_get_target_dependencies TARGET RES_VAR)
    set(RES)
    get_target_property(LIBS "${TARGET}" LINK_LIBRARIES)
    foreach (LIB IN LISTS LIBS)
        if (TARGET "${LIB}")
            list(FIND RES "${LIB}" INDEX)
            if ("${INDEX}" LESS 0)
                list(APPEND RES "${LIB}")
                get_target_property(LIB_LIBS "${LIB}" INTERFACE_LINK_LIBRARIES)
                if (LIB_LIBS)
                    list(APPEND RES "${LIB_LIBS}")
                    list(REMOVE_DUPLICATES RES)
                endif()
            endif()
        endif()
    endforeach()

    set("${RES_VAR}" "${RES}" PARENT_SCOPE)
endfunction()

function (pvs_studio_set_target_flags TARGET CXX C)
    set(CXX_FLAGS "${${CXX}}")
    set(C_FLAGS "${${C}}")

    get_target_property(PROPERTY "${TARGET}" INCLUDE_DIRECTORIES)
    pvs_studio_append_flags_from_property(CXX_FLAGS C_FLAGS "${DIRECTORY}" "-I")

    get_target_property(PROPERTY "${TARGET}" COMPILE_DEFINITIONS)
    pvs_studio_append_flags_from_property(CXX_FLAGS C_FLAGS "" "-D")

    get_target_property(PROPERTY "${TARGET}" CXX_STANDARD)
    pvs_studio_append_standard_flag(CXX_FLAGS "${PROPERTY}")

    pvs_studio_get_target_dependencies("${TARGET}" LIBS)
    foreach (LIB IN LISTS LIBS)
        get_target_property(PROPERTY "${LIB}" INTERFACE_INCLUDE_DIRECTORIES)
        pvs_studio_append_flags_from_property(CXX_FLAGS C_FLAGS "${DIRECTORY}" "-I")
        get_target_property(PROPERTY "${LIB}" INTERFACE_SYSTEM_INCLUDE_DIRECTORIES)
        pvs_studio_append_flags_from_property(CXX_FLAGS C_FLAGS "${DIRECTORY}" "-I")
        get_target_property(PROPERTY "${LIB}" INTERFACE_COMPILE_DEFINITIONS)
        pvs_studio_append_flags_from_property(CXX_FLAGS C_FLAGS "" "-D")
    endforeach ()

    set("${CXX}" "${CXX_FLAGS}" PARENT_SCOPE)
    set("${C}" "${C_FLAGS}" PARENT_SCOPE)
endfunction ()

function (pvs_studio_set_source_file_flags SOURCE)
    set(LANGUAGE "")

    string(TOLOWER "${SOURCE}" SOURCE_LOWER)
    if ("${LANGUAGE}" STREQUAL "" AND "${SOURCE_LOWER}" MATCHES "^.*\\.(c|cpp|cc|cx|cxx|cp|c\\+\\+)$")
        if ("${SOURCE}" MATCHES "^.*\\.c$")
            set(LANGUAGE C)
        else ()
            set(LANGUAGE CXX)
        endif ()
    endif ()

    if ("${LANGUAGE}" STREQUAL "C")
        set(CL_PARAMS ${PVS_STUDIO_C_FLAGS} ${PVS_STUDIO_TARGET_C_FLAGS} -DPVS_STUDIO)
    elseif ("${LANGUAGE}" STREQUAL "CXX")
        set(CL_PARAMS ${PVS_STUDIO_CXX_FLAGS} ${PVS_STUDIO_TARGET_CXX_FLAGS} -DPVS_STUDIO)
    endif ()

    set(PVS_STUDIO_LANGUAGE "${LANGUAGE}" PARENT_SCOPE)
    set(PVS_STUDIO_CL_PARAMS "${CL_PARAMS}" PARENT_SCOPE)
endfunction ()

function (pvs_studio_analyze_file SOURCE SOURCE_DIR BINARY_DIR)
    set(PLOGS ${PVS_STUDIO_PLOGS})
    pvs_studio_set_source_file_flags("${SOURCE}")

    get_filename_component(SOURCE "${SOURCE}" REALPATH)
    pvs_studio_relative_path(SOURCE_RELATIVE "${SOURCE_DIR}" "${SOURCE}")
    pvs_studio_join_path(SOURCE "${SOURCE_DIR}" "${SOURCE}")

    set(LOG "${BINARY_DIR}/PVS-Studio/${SOURCE_RELATIVE}.plog")
    get_filename_component(LOG "${LOG}" REALPATH)
    get_filename_component(PARENT_DIR "${LOG}" DIRECTORY)

    message("Will analyze file ${SOURCE}")

    if (EXISTS "${SOURCE}" AND NOT TARGET "${LOG}" AND NOT "${PVS_STUDIO_LANGUAGE}" STREQUAL "")
        add_custom_command(OUTPUT "${LOG}"
                           COMMAND mkdir -p "${PARENT_DIR}"
                           COMMAND rm -f "${LOG}"
                           COMMAND "${PVS_STUDIO_BIN}" analyze
                                                       --output-file "${LOG}"
                                                       --source-file "${SOURCE}"
                                                       ${PVS_STUDIO_ARGS}
                                                       --cl-params ${PVS_STUDIO_CL_PARAMS} "${SOURCE}"
                           WORKING_DIRECTORY "${BINARY_DIR}"
                           DEPENDS "${SOURCE}" "${PVS_STUDIO_CONFIG}"
                           IMPLICIT_DEPENDS "${PVS_STUDIO_LANGUAGE}" "${SOURCE}"
                           VERBATIM
                           COMMENT "Analyzing ${PVS_STUDIO_LANGUAGE} file ${SOURCE_RELATIVE}")
        list(APPEND PLOGS "${LOG}")
    endif ()
    set(PVS_STUDIO_PLOGS "${PLOGS}" PARENT_SCOPE)
endfunction ()

function (pvs_studio_analyze_target TARGET DIR)
    set(PVS_STUDIO_PLOGS "${PVS_STUDIO_PLOGS}")
    set(PVS_STUDIO_TARGET_CXX_FLAGS "")
    set(PVS_STUDIO_TARGET_C_FLAGS "")

    get_target_property(PROPERTY "${TARGET}" SOURCES)
    pvs_studio_relative_path(BINARY_DIR "${CMAKE_SOURCE_DIR}" "${DIR}")
    if ("${BINARY_DIR}" MATCHES "^/.*$")
        pvs_studio_join_path(BINARY_DIR "${CMAKE_BINARY_DIR}" "PVS-Studio/__${BINARY_DIR}")
    else ()
        pvs_studio_join_path(BINARY_DIR "${CMAKE_BINARY_DIR}" "${BINARY_DIR}")
    endif ()

    file(MAKE_DIRECTORY "${BINARY_DIR}")

    pvs_studio_set_directory_flags("${DIR}" PVS_STUDIO_TARGET_CXX_FLAGS PVS_STUDIO_TARGET_C_FLAGS)
    pvs_studio_set_target_flags("${TARGET}" PVS_STUDIO_TARGET_CXX_FLAGS PVS_STUDIO_TARGET_C_FLAGS)

    if (NOT "${PROPERTY}" STREQUAL "NOTFOUND" AND NOT "${PROPERTY}" STREQUAL "PROPERTY-NOTFOUND")
        foreach (SOURCE ${PROPERTY})
            pvs_studio_join_path(SOURCE "${DIR}" "${SOURCE}")
            pvs_studio_analyze_file("${SOURCE}" "${DIR}" "${BINARY_DIR}")
        endforeach ()
    endif ()

    set(PVS_STUDIO_PLOGS "${PVS_STUDIO_PLOGS}" PARENT_SCOPE)
endfunction ()

set(PVS_STUDIO_RECURSIVE_TARGETS)
set(PVS_STUDIO_RECURSIVE_TARGETS_NEW)

macro(pvs_studio_get_recursive_targets TARGET)
    get_target_property(libs "${TARGET}" LINK_LIBRARIES)
    foreach (lib IN LISTS libs)
        list(FIND PVS_STUDIO_RECURSIVE_TARGETS "${lib}" index)
        if (TARGET "${lib}" AND "${index}" STREQUAL -1)
            get_target_property(target_type "${lib}" TYPE)
            if (NOT "${target_type}" STREQUAL "INTERFACE_LIBRARY")
                list(APPEND PVS_STUDIO_RECURSIVE_TARGETS "${lib}")
                list(APPEND PVS_STUDIO_RECURSIVE_TARGETS_NEW "${lib}")
                pvs_studio_get_recursive_targets("${lib}")
            endif ()
        endif ()
    endforeach ()
endmacro()

option(PVS_STUDIO_DISABLE OFF "Disable PVS-Studio targets")
option(PVS_STUDIO_DEBUG OFF "Add debug info")

# pvs_studio_add_target
# Target options:
# ALL                           add this target to default build (default: off)
# TARGET target                 name of analysis target (default: pvs)
# ANALYZE targets...            targets to analyze
# RECURSIVE                     analyze target's dependencies (requires CMake 3.5+)
#
# Output options:
# OUTPUT                        prints report to stdout
# LOG path                      path to report (default: ${CMAKE_CURRENT_BINARY_DIR}/PVS-Studio.log)
# FORMAT format                 format of report
# MODE mode                     analyzers/levels filter (default: GA:1,2)
#
# Analyzer options:
# PLATFORM name                 linux32/linux64 (default: linux64)
# PREPROCESSOR name             preprocessor type: gcc/clang (default: auto detected)
# LICENSE path                  path to PVS-Studio.lic (default: ~/.config/PVS-Studio/PVS-Studio.lic)
# CONFIG path                   path to PVS-Studio.cfg
# CFG_TEXT text                 embedded PVS-Studio.cfg
#
# Misc options:
# DEPENDS targets..             additional target dependencies
# SOURCES path...               list of source files to analyze
# BIN path                      path to pvs-studio-analyzer (default: pvs-studio-analyzer)
# CONVERTER path                path to plog-converter (default: plog-converter)
# C_FLAGS flags...              additional C_FLAGS
# CXX_FLAGS flags...            additional CXX_FLAGS
# ARGS args...                  additional pvs-studio-analyzer flags
function (pvs_studio_add_target)
    if (WIN32)
        return()
    endif ()

    macro (default VAR VALUE)
        if ("${${VAR}}" STREQUAL "")
            set("${VAR}" "${VALUE}")
        endif ()
    endmacro ()

    set(PVS_STUDIO_SUPPORTED_PREPROCESSORS "gcc|clang")
    if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
        set(DEFAULT_PREPROCESSOR "clang")
    else ()
        set(DEFAULT_PREPROCESSOR "gcc")
    endif ()

    set(OPTIONAL OUTPUT ALL RECURSIVE)
    set(SINGLE LICENSE CONFIG TARGET LOG FORMAT BIN CONVERTER PLATFORM PREPROCESSOR CFG_TEXT)
    set(MULTI SOURCES C_FLAGS CXX_FLAGS ARGS DEPENDS ANALYZE MODE)
    cmake_parse_arguments(PVS_STUDIO "${OPTIONAL}" "${SINGLE}" "${MULTI}" ${ARGN})

    if ("${PVS_STUDIO_CFG}" STREQUAL "" OR NOT "${PVS_STUDIO_CFG_TEXT}" STREQUAL "")
        set(PVS_STUDIO_EMPTY_CONFIG ON)
    else ()
        set(PVS_STUDIO_EMPTY_CONFIG OFF)
    endif ()

    default(PVS_STUDIO_CFG_TEXT "analysis-mode=31")
    default(PVS_STUDIO_CONFIG "${CMAKE_BINARY_DIR}/PVS-Studio.cfg")
    default(PVS_STUDIO_C_FLAGS "")
    default(PVS_STUDIO_CXX_FLAGS "")
    default(PVS_STUDIO_TARGET "pvs")
    default(PVS_STUDIO_LOG "PVS-Studio.log")
    default(PVS_STUDIO_BIN "pvs-studio-analyzer")
    default(PVS_STUDIO_CONVERTER "plog-converter")
    default(PVS_STUDIO_MODE "GA:1,2")
    default(PVS_STUDIO_PREPROCESSOR "${DEFAULT_PREPROCESSOR}")
    default(PVS_STUDIO_PLATFORM "linux64")

    string(REPLACE ";" "/" PVS_STUDIO_MODE "${PVS_STUDIO_MODE}")

    if (PVS_STUDIO_EMPTY_CONFIG)
        set(PVS_STUDIO_CONFIG_COMMAND echo "${PVS_STUDIO_CFG_TEXT}" > "${PVS_STUDIO_CONFIG}")
    else ()
        set(PVS_STUDIO_CONFIG_COMMAND touch "${PVS_STUDIO_CONFIG}")
    endif ()

    add_custom_command(OUTPUT "${PVS_STUDIO_CONFIG}"
                       COMMAND ${PVS_STUDIO_CONFIG_COMMAND}
                       WORKING_DIRECTORY "${BINARY_DIR}"
                       COMMENT "Generating PVS-Studio.cfg")

    pvs_studio_append_standard_flag(PVS_STUDIO_CXX_FLAGS "${CMAKE_CXX_STANDARD}")
    pvs_studio_set_directory_flags("${CMAKE_CURRENT_SOURCE_DIR}" PVS_STUDIO_CXX_FLAGS PVS_STUDIO_C_FLAGS)

    if (NOT "${PVS_STUDIO_LICENSE}" STREQUAL "")
        pvs_studio_join_path(PVS_STUDIO_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}" "${PVS_STUDIO_LICENSE}")
        list(APPEND PVS_STUDIO_ARGS --lic-file "${PVS_STUDIO_LICENSE}")
    endif ()
    list(APPEND PVS_STUDIO_ARGS --cfg "${PVS_STUDIO_CONFIG}"
                                --platform "${PVS_STUDIO_PLATFORM}"
                                --preprocessor "${PVS_STUDIO_PREPROCESSOR}")

    set(PVS_STUDIO_PLOGS "")

    set(PVS_STUDIO_RECURSIVE_TARGETS_NEW)
    if (${PVS_STUDIO_RECURSIVE})
        foreach (TARGET IN LISTS PVS_STUDIO_ANALYZE)
            list(APPEND PVS_STUDIO_RECURSIVE_TARGETS_NEW "${TARGET}")
            pvs_studio_get_recursive_targets("${TARGET}")
        endforeach ()
        set(PVS_STUDIO_ANALYZE "${PVS_STUDIO_RECURSIVE_TARGETS_NEW}")
    endif ()

    message("${PVS_STUDIO_ANALYZE}")

    foreach (TARGET ${PVS_STUDIO_ANALYZE})
        set(DIR "${CMAKE_CURRENT_SOURCE_DIR}")
#         string(FIND "${TARGET}" ":" DELIM)
#
#         if ("${DELIM}" GREATER "-1")
#             # target name has colon. Example: OpenSSL::SSL
#
#             message("${TARGET}, ${CMAKE_CURRENT_SOURCE_DIR}")
#
#             math(EXPR DELIMI "${DELIM}+1")
#             # set DIR to suffix
#             string(SUBSTRING "${TARGET}" "${DELIMI}" "-1" DIR)
#             # set TARGET to prefix
#             string(SUBSTRING "${TARGET}" "0" "${DELIM}" TARGET)
#             # set DIR to current source dir + suffix
#             pvs_studio_join_path(DIR "${CMAKE_CURRENT_SOURCE_DIR}" "${DIR}")
#         else ()
          get_target_property(TARGET_SOURCE_DIR "${TARGET}" SOURCE_DIR)
          if (EXISTS "${TARGET_SOURCE_DIR}")
            set(DIR "${TARGET_SOURCE_DIR}")
          endif ()
#         endif ()

        message("${DIR}")
        if (NOT "${DIR}" MATCHES "contrib")
            pvs_studio_analyze_target("${TARGET}" "${DIR}")
            list(APPEND PVS_STUDIO_DEPENDS "${TARGET}")
        endif ()
    endforeach ()

    set(PVS_STUDIO_TARGET_CXX_FLAGS "")
    set(PVS_STUDIO_TARGET_C_FLAGS "")

    foreach (SOURCE ${PVS_STUDIO_SOURCES})
        pvs_studio_analyze_file("${SOURCE}" "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}")
    endforeach ()

    pvs_studio_relative_path(LOG_RELATIVE "${CMAKE_BINARY_DIR}" "${PVS_STUDIO_LOG}")
    if (PVS_STUDIO_PLOGS)
        set(COMMANDS COMMAND cat ${PVS_STUDIO_PLOGS} > "${PVS_STUDIO_LOG}")
        set(COMMENT "Generating ${LOG_RELATIVE}")
        if (NOT "${PVS_STUDIO_FORMAT}" STREQUAL "" OR PVS_STUDIO_OUTPUT)
            if ("${PVS_STUDIO_FORMAT}" STREQUAL "")
                set(PVS_STUDIO_FORMAT "errorfile")
            endif ()
            list(APPEND COMMANDS
                 COMMAND mv "${PVS_STUDIO_LOG}" "${PVS_STUDIO_LOG}.pvs.raw"
                 COMMAND "${PVS_STUDIO_CONVERTER}" -t "${PVS_STUDIO_FORMAT}" "${PVS_STUDIO_LOG}.pvs.raw" -o "${PVS_STUDIO_LOG}" -a "${PVS_STUDIO_MODE}"
                 COMMAND rm -f "${PVS_STUDIO_LOG}.pvs.raw")
        endif ()
    else ()
        set(COMMANDS COMMAND touch "${PVS_STUDIO_LOG}")
        set(COMMENT "Generating ${LOG_RELATIVE}: no sources found")
    endif ()

    add_custom_command(OUTPUT "${PVS_STUDIO_LOG}"
                       ${COMMANDS}
                       COMMENT "${COMMENT}"
                       DEPENDS ${PVS_STUDIO_PLOGS}
                       WORKING_DIRECTORY "${CMAKE_BINARY_DIR}")

    if (PVS_STUDIO_ALL)
        set(ALL "ALL")
    else ()
        set(ALL "")
    endif ()

    if (PVS_STUDIO_OUTPUT)
        set(COMMANDS COMMAND cat "${PVS_STUDIO_LOG}" 1>&2)
    else ()
        set(COMMANDS "")
    endif ()

    add_custom_target("${PVS_STUDIO_TARGET}" ${ALL} ${COMMANDS} WORKING_DIRECTORY "${CMAKE_BINARY_DIR}" DEPENDS ${PVS_STUDIO_DEPENDS} "${PVS_STUDIO_LOG}")
endfunction ()
