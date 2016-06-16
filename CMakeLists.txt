project (ClickHouse)
cmake_minimum_required(VERSION 2.6)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    # require at least gcc 5
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5)
        message(FATAL_ERROR "GCC version must be at least 5! For example, if GCC 5 is available under gcc-5, g++-5 names, do the following: export CC=gcc-5 CXX=g++-5; rm -rf CMakeCache.txt CMakeFiles; and re run cmake.")
    endif()
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    # require at least clang 3.8
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 3.8)
        message(FATAL_ERROR "Clang version must be at least 3.8!")
    endif()
else()
    message(WARNING "You are using an unsupported compiler! Compilation has only been tested with Clang 3.8+ and GCC 5+.")
endif()

# отключаем варнинг о том, что в каждой директории должен быть CMakeLists.txt
cmake_policy(SET CMP0014 OLD)

cmake_policy(SET CMP0012 NEW) # Don't dereference TRUE and FALSE

IF(NOT CMAKE_BUILD_TYPE)
	message(STATUS "CMAKE_BUILD_TYPE is not set, set to default = RELWITHDEBINFO")
	SET(CMAKE_BUILD_TYPE            "RELWITHDEBINFO")
ENDIF()
MESSAGE( STATUS "CMAKE_BUILD_TYPE: " ${CMAKE_BUILD_TYPE} )

set(CMAKE_CONFIGURATION_TYPES "RelWithDebInfo;Debug;Release;MinSizeRel" CACHE STRING "" FORCE)

IF (CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64.*|AARCH64.*)")
	SET(AARCH64 1)
ENDIF()

IF (NOT AARCH64)
	SET(MACHINE_FLAGS "-msse4 -mpopcnt")
ENDIF()

SET(CMAKE_BUILD_COLOR_MAKEFILE          ON)
SET(CMAKE_CXX_FLAGS                     "-std=gnu++1y -Wall -Werror -Wnon-virtual-dtor ${MACHINE_FLAGS}")
SET(CMAKE_CXX_FLAGS_RELEASE             "-O3 -DNDEBUG")
SET(CMAKE_CXX_FLAGS_RELWITHDEBINFO      "-O3 -g")
SET(CMAKE_C_FLAGS                       "-Wall -Werror ${MACHINE_FLAGS}")
SET(CMAKE_C_FLAGS_RELEASE               "-O3 -DNDEBUG")
SET(CMAKE_C_FLAGS_RELWITHDEBINFO        "-O3 -g")
SET(CMAKE_EXE_LINKER_FLAGS              "-static-libgcc -static-libstdc++")

# cmake -DCMAKE_BUILD_TYPE=Debug ..
SET(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")

# Флаги для test coverage
SET(TEST_COVERAGE TRUE CACHE BOOL "Add compliler flags for test coverage")
IF (TEST_COVERAGE)
SET(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -fprofile-arcs -ftest-coverage -fPIC -DIS_DEBUG")
ENDIF(TEST_COVERAGE)

# Собирать тесты?
IF (NOT DEFINED TESTS)
	MESSAGE(STATUS "Tests are enabled")
	SET(TESTS YES)
ENDIF()

# тесты запускать с помощью "make check"
IF(TESTS)
	INCLUDE(add.test.cmake)
ENDIF(TESTS)

# Префикс для установки
SET(CMAKE_INSTALL_PREFIX /usr)

# Директория для файлов, специфичных для Яндекса
SET(CLICKHOUSE_PRIVATE_DIR ${ClickHouse_SOURCE_DIR}/private/)

include_directories (${ClickHouse_SOURCE_DIR}/contrib/libcityhash/include/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/liblz4/include/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libdivide/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libdouble-conversion/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libcpuid/include/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libzstd/include/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libfarmhash/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libmetrohash/src)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libsparsehash/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libre2/)
include_directories (${ClickHouse_BINARY_DIR}/contrib/libre2/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libboost-threadpool/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libpoco/Foundation/include/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libpoco/Util/include/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libpoco/Net/include/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libpoco/NetSSL_OpenSSL/include/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libpoco/Data/include/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libpoco/Data/MySQL/include/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libpoco/Data/ODBC/include/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libpoco/Data/SQLite/include/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libpoco/Crypto/include/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libpoco/XML/include/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libpoco/JSON/include/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libpoco/MongoDB/include/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libpoco/Zip/include/)

include_directories (${ClickHouse_SOURCE_DIR}/libs/libcommon/include/)
include_directories (${ClickHouse_SOURCE_DIR}/libs/libdaemon/include/)
include_directories (${ClickHouse_SOURCE_DIR}/libs/libpocoext/include/)
include_directories (${ClickHouse_SOURCE_DIR}/libs/libmysqlxx/include/)
include_directories (${ClickHouse_SOURCE_DIR}/libs/libzkutil/include/)

include_directories (${ClickHouse_SOURCE_DIR}/dbms/include)

include_directories (/usr/local/include/glib-2.0/)
include_directories (/usr/local/lib/glib-2.0/include/)
include_directories (/usr/include/glib-2.0/)
include_directories (/usr/lib64/glib-2.0/include/)

IF (AARCH64)
    include_directories (/usr/lib/aarch64-linux-gnu/glib-2.0/include/)
ELSE()
    include_directories (/usr/lib/x86_64-linux-gnu/glib-2.0/include/)
ENDIF()

include_directories (/usr/local/include/)

link_directories (/usr/local/lib)

add_subdirectory (contrib)
add_subdirectory (libs)
add_subdirectory (utils)
add_subdirectory (dbms)
add_subdirectory (private)
