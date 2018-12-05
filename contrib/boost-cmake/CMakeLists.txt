# Supported contrib/boost source variants:
# 1. Default - Minimized vrsion from release archive : https://github.com/ClickHouse-Extras/boost
# 2. Release archive unpacked to contrib/boost
# 3. Full boost https://github.com/boostorg/boost

# if boostorg/boost connected as submodule: Update all boost internal submodules to tag:
# git submodule foreach "git fetch --all && git checkout boost-1.66.0 || true"

#
# Important boost patch: 094c18b
#

include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

set (LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/boost)

if (NOT MSVC)
    add_definitions(-Wno-unused-variable -Wno-deprecated-declarations)
endif ()

add_headers_and_sources(boost_program_options ${LIBRARY_DIR}/libs/program_options/src)
add_library(boost_program_options_internal ${LINK_MODE} ${boost_program_options_sources})
add_headers_and_sources(boost_filesystem ${LIBRARY_DIR}/libs/filesystem/src)
add_library(boost_filesystem_internal ${LINK_MODE} ${boost_filesystem_sources})
add_headers_and_sources(boost_system ${LIBRARY_DIR}/libs/system/src)
add_library(boost_system_internal ${LINK_MODE} ${boost_system_sources})
add_headers_and_sources(boost_regex ${LIBRARY_DIR}/libs/regex/src)
add_library(boost_regex_internal ${LINK_MODE} ${boost_regex_sources})

if (0)
add_library(boost_program_options_internal ${LINK_MODE}
${LIBRARY_DIR}/libs/program_options/src/cmdline.cpp
${LIBRARY_DIR}/libs/program_options/src/config_file.cpp
${LIBRARY_DIR}/libs/program_options/src/convert.cpp
${LIBRARY_DIR}/libs/program_options/src/options_description.cpp
${LIBRARY_DIR}/libs/program_options/src/parsers.cpp
${LIBRARY_DIR}/libs/program_options/src/positional_options.cpp
${LIBRARY_DIR}/libs/program_options/src/split.cpp
${LIBRARY_DIR}/libs/program_options/src/utf8_codecvt_facet.cpp
${LIBRARY_DIR}/libs/program_options/src/value_semantic.cpp
${LIBRARY_DIR}/libs/program_options/src/variables_map.cpp
${LIBRARY_DIR}/libs/program_options/src/winmain.cpp)

add_library(boost_filesystem_internal ${LINK_MODE}
${LIBRARY_DIR}/libs/filesystem/src/codecvt_error_category.cpp
${LIBRARY_DIR}/libs/filesystem/src/operations.cpp
${LIBRARY_DIR}/libs/filesystem/src/path.cpp
${LIBRARY_DIR}/libs/filesystem/src/path_traits.cpp
${LIBRARY_DIR}/libs/filesystem/src/portability.cpp
${LIBRARY_DIR}/libs/filesystem/src/unique_path.cpp
${LIBRARY_DIR}/libs/filesystem/src/utf8_codecvt_facet.cpp
${LIBRARY_DIR}/libs/filesystem/src/windows_file_codecvt.cpp)

add_library(boost_system_internal ${LINK_MODE}
${LIBRARY_DIR}/libs/system/src/error_code.cpp)
endif ()


target_link_libraries (boost_filesystem_internal PRIVATE boost_system_internal)

target_include_directories (boost_program_options_internal SYSTEM BEFORE PUBLIC ${Boost_INCLUDE_DIRS})
target_include_directories (boost_filesystem_internal SYSTEM BEFORE PUBLIC ${Boost_INCLUDE_DIRS})
target_include_directories (boost_system_internal SYSTEM BEFORE PUBLIC ${Boost_INCLUDE_DIRS})
target_include_directories (boost_regex_internal SYSTEM BEFORE PUBLIC ${Boost_INCLUDE_DIRS})

target_compile_definitions (boost_program_options_internal PUBLIC BOOST_SYSTEM_NO_DEPRECATED)
target_compile_definitions (boost_filesystem_internal PUBLIC BOOST_SYSTEM_NO_DEPRECATED)
target_compile_definitions (boost_system_internal PUBLIC BOOST_SYSTEM_NO_DEPRECATED)
target_compile_definitions (boost_regex_internal PUBLIC BOOST_SYSTEM_NO_DEPRECATED)
