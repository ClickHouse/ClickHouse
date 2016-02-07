SET(CMAKE_C_COMPILER "gcc-5")
SET(CMAKE_CXX_COMPILER "g++-5")

project (METRICA)
cmake_minimum_required(VERSION 2.6)

# отключаем варнинг о том, что в каждой директории должен быть CMakeLists.txt
CMAKE_POLICY(SET CMP0014 OLD)

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
SET(CMAKE_CXX_FLAGS                     "-std=gnu++1y -Wall -Werror -Wnon-virtual-dtor ${MACHINE_FLAGS} -static-libgcc -static-libstdc++")
SET(CMAKE_CXX_FLAGS_RELEASE             "-O3 -DNDEBUG")
SET(CMAKE_CXX_FLAGS_RELWITHDEBINFO      "-O3 -g" )
SET(CMAKE_C_FLAGS                       "-Wall -Werror ${MACHINE_FLAGS} -static-libgcc")
SET(CMAKE_C_FLAGS_RELEASE               "-O3 -DNDEBUG")
SET(CMAKE_C_FLAGS_RELWITHDEBINFO        "-O3 -g"      )

# собираем с отслеживанием покрытия в Debug режиме
# cmake -DCMAKE_BUILD_TYPE=Debug ..
SET(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -fprofile-arcs -ftest-coverage -fPIC -DIS_DEBUG")

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

include_directories (${METRICA_SOURCE_DIR}/contrib/libcityhash/)
include_directories (${METRICA_SOURCE_DIR}/contrib/liblz4/include/)
include_directories (${METRICA_SOURCE_DIR}/contrib/libdivide/)
include_directories (${METRICA_SOURCE_DIR}/contrib/libvectorclass/)
include_directories (${METRICA_SOURCE_DIR}/contrib/libdouble-conversion/)
include_directories (${METRICA_SOURCE_DIR}/contrib/libcpuid/include/)
include_directories (${METRICA_SOURCE_DIR}/contrib/libzstd/include/)
include_directories (${METRICA_SOURCE_DIR}/contrib/libfarmhash/)
include_directories (${METRICA_SOURCE_DIR}/contrib/libmetrohash/src)
include_directories (${METRICA_SOURCE_DIR}/contrib/libpoco/Foundation/include/)
include_directories (${METRICA_SOURCE_DIR}/contrib/libpoco/Util/include/)
include_directories (${METRICA_SOURCE_DIR}/contrib/libpoco/Net/include/)
include_directories (${METRICA_SOURCE_DIR}/contrib/libpoco/NetSSL_OpenSSL/include/)
include_directories (${METRICA_SOURCE_DIR}/contrib/libpoco/Data/include/)
include_directories (${METRICA_SOURCE_DIR}/contrib/libpoco/Data/MySQL/include/)
include_directories (${METRICA_SOURCE_DIR}/contrib/libpoco/Data/ODBC/include/)
include_directories (${METRICA_SOURCE_DIR}/contrib/libpoco/Data/SQLite/include/)
include_directories (${METRICA_SOURCE_DIR}/contrib/libpoco/Crypto/include/)
include_directories (${METRICA_SOURCE_DIR}/contrib/libpoco/XML/include/)
include_directories (${METRICA_SOURCE_DIR}/contrib/libpoco/JSON/include/)
include_directories (${METRICA_SOURCE_DIR}/contrib/libpoco/MongoDB/include/)
include_directories (${METRICA_SOURCE_DIR}/contrib/libpoco/Zip/include/)

include_directories (${METRICA_SOURCE_DIR}/libs/libcommon/include/)
include_directories (${METRICA_SOURCE_DIR}/libs/libdaemon/include/)
include_directories (${METRICA_SOURCE_DIR}/libs/libpocoext/include/)
include_directories (${METRICA_BINARY_DIR}/libs/libpocoext/charsets/include/)
include_directories (${METRICA_SOURCE_DIR}/libs/libmysqlxx/include/)
include_directories (${METRICA_SOURCE_DIR}/libs/libzkutil/include/)

include_directories (${METRICA_SOURCE_DIR}/tools/)
include_directories (${METRICA_SOURCE_DIR}/dbms/include)
include_directories (${METRICA_SOURCE_DIR})
include_directories (${METRICA_BINARY_DIR})

include_directories (/usr/local/include/glib-2.0/)
include_directories (/usr/local/lib/glib-2.0/include/)
include_directories (/usr/include/glib-2.0/)

IF (AARCH64)
    include_directories (/usr/lib/aarch64-linux-gnu/glib-2.0/include/)
ELSE()
    include_directories (/usr/lib/x86_64-linux-gnu/glib-2.0/include/)
ENDIF()

include_directories (/usr/local/include/)

link_directories (${METRICA_SOURCE_DIR}/contrib/libcityhash)
link_directories (${METRICA_SOURCE_DIR}/contrib/liblz4)
link_directories (${METRICA_SOURCE_DIR}/contrib/libvectorclass)
link_directories (${METRICA_SOURCE_DIR}/contrib/libdouble-conversion)
link_directories (${METRICA_SOURCE_DIR}/contrib/libcpuid)
link_directories (${METRICA_SOURCE_DIR}/contrib/libzstd)
link_directories (${METRICA_SOURCE_DIR}/contrib/libpoco/lib)

link_directories (${METRICA_BINARY_DIR}/libs/libcommon)
link_directories (${METRICA_BINARY_DIR}/libs/libdaemon)
link_directories (${METRICA_BINARY_DIR}/libs/libpocoext)
link_directories (${METRICA_BINARY_DIR}/libs/libmysqlxx)

link_directories (${METRICA_BINARY_DIR}/dbms)
link_directories (/usr/local/lib)

add_subdirectory (contrib)
add_subdirectory (libs)
add_subdirectory (utils)
add_subdirectory (dbms)
