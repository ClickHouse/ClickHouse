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

set(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/boost)

if(NOT MSVC)
    add_definitions(-Wno-unused-variable -Wno-deprecated-declarations)
endif()

macro(add_boost_lib lib_name)
    add_headers_and_sources(boost_${lib_name} ${LIBRARY_DIR}/libs/${lib_name}/src)
    add_library(boost_${lib_name}_internal ${boost_${lib_name}_sources})
    target_include_directories(boost_${lib_name}_internal SYSTEM BEFORE PUBLIC ${Boost_INCLUDE_DIRS})
    target_compile_definitions(boost_${lib_name}_internal PUBLIC BOOST_SYSTEM_NO_DEPRECATED)
endmacro()

add_boost_lib(system)

add_boost_lib(program_options)

add_boost_lib(filesystem)
target_link_libraries(boost_filesystem_internal PRIVATE boost_system_internal)

#add_boost_lib(random)

if (USE_INTERNAL_PARQUET_LIBRARY)
    add_boost_lib(regex)
endif()
