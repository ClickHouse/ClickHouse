project (ClickHouse)
cmake_minimum_required(VERSION 2.6)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    # require at least gcc 5
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5)
        message(FATAL_ERROR "GCC version must be at least 5! For example, if GCC 5 is available under gcc-5, g++-5 names, do the following: export CC=gcc-5 CXX=g++-5; rm -rf CMakeCache.txt CMakeFiles; and re run cmake or ./release.")
    endif()
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    # require at least clang 3.8
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 3.8)
        message(FATAL_ERROR "Clang version must be at least 3.8!")
    endif()
else()
    message(WARNING "You are using an unsupported compiler! Compilation has only been tested with Clang 3.8+ and GCC 5+.")
endif()

if (APPLE)
	set(APPLE_EXTRA_CXX_FLAG "-Dexp10=__exp10") # Also needed for libc++
endif()

# отключаем варнинг о том, что в каждой директории должен быть CMakeLists.txt
cmake_policy(SET CMP0014 OLD)

cmake_policy(SET CMP0012 NEW) # Don't dereference TRUE and FALSE

IF(NOT CMAKE_BUILD_TYPE)
	message(STATUS "CMAKE_BUILD_TYPE is not set, set to default = RELWITHDEBINFO")
	SET(CMAKE_BUILD_TYPE            "RELWITHDEBINFO")
ENDIF()
MESSAGE( STATUS "CMAKE_BUILD_TYPE: " ${CMAKE_BUILD_TYPE} )

# ASan - build type with address sanitizer
# UBSan - build type with undefined behaviour sanitizer
# TSan is not supported due to false positive errors in libstdc++ and necessity to rebuild libstdc++ with TSan
set(CMAKE_CONFIGURATION_TYPES "RelWithDebInfo;Debug;Release;MinSizeRel;ASan;UBSan" CACHE STRING "" FORCE)


IF (CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64.*|AARCH64.*)")
	SET(AARCH64 1)
ENDIF()

IF (NOT AARCH64)
	SET(MACHINE_FLAGS "-msse4 -mpopcnt")
ENDIF()

SET(COMMON_WARNING_FLAGS "-Wall -Werror")
SET(CXX_WARNING_FLAGS "-Wnon-virtual-dtor -Wold-style-cast")


set (GLIBC_COMPATIBILITY FALSE CACHE BOOL "Set to TRUE to enable compatibility with older glibc libraries")

if ($ENV{GLIBC_COMPATIBILITY})
	set (GLIBC_COMPATIBILITY TRUE)
endif()

if (GLIBC_COMPATIBILITY)
	SET(GLIBC_COMPATIBILITY_COMPILE_FLAGS   "-include ${ClickHouse_SOURCE_DIR}/libs/libcommon/include/common/glibc_compatibility.h")
	SET(GLIBC_COMPATIBILITY_LINK_FLAGS      "-Wl,--wrap=memcpy")
endif()

if (DISABLE_CXX11_ABI)
    SET(CXX11_ABI "-D_GLIBCXX_USE_CXX11_ABI=0")
endif()

SET(CMAKE_BUILD_COLOR_MAKEFILE          ON)
SET(CMAKE_CXX_FLAGS                     "-std=gnu++1y ${APPLE_EXTRA_CXX_FLAG} -fno-omit-frame-pointer ${COMMON_WARNING_FLAGS} ${CXX_WARNING_FLAGS} ${MACHINE_FLAGS} ${GLIBC_COMPATIBILITY_COMPILE_FLAGS} ${CXX11_ABI}")
SET(CMAKE_CXX_FLAGS_RELEASE             "-O3 -DNDEBUG")
SET(CMAKE_CXX_FLAGS_RELWITHDEBINFO      "-O3 -g")
SET(CMAKE_CXX_FLAGS_DEBUG               "-O0 -g3 -ggdb3 -fno-inline")

SET(CMAKE_C_FLAGS                       "-fno-omit-frame-pointer ${COMMON_WARNING_FLAGS} ${MACHINE_FLAGS} ${GLIBC_COMPATIBILITY_COMPILE_FLAGS} ${CXX11_ABI}")
SET(CMAKE_C_FLAGS_RELEASE               "-O3 -DNDEBUG")
SET(CMAKE_C_FLAGS_RELWITHDEBINFO        "-O3 -g")
SET(CMAKE_C_FLAGS_DEBUG                 "-O0 -g3 -ggdb3 -fno-inline")

if (NOT APPLE)
	SET(CMAKE_EXE_LINKER_FLAGS              "-static-libgcc -static-libstdc++ ${GLIBC_COMPATIBILITY_LINK_FLAGS} ${CXX11_ABI}")
endif()

# -fuse-ld=gold - fix linkage for gcc-5.4, gcc-6.1 
# see more in http://stackoverflow.com/questions/37603238/fsanitize-not-using-gold-linker-in-gcc-6-1
SET(CMAKE_CXX_FLAGS_ASAN                "-O3 -g -fsanitize=address -fno-omit-frame-pointer -fuse-ld=gold ${CXX11_ABI}")
SET(CMAKE_CXX_FLAGS_UBSAN               "-O3 -g -fsanitize=undefined -fno-omit-frame-pointer ${CXX11_ABI}")
SET(CMAKE_C_FLAGS_ASAN                  "-O3 -g -fsanitize=address -fno-omit-frame-pointer -fuse-ld=gold ${CXX11_ABI}")
SET(CMAKE_C_FLAGS_UBSAN                 "-O3 -g -fsanitize=undefined -fno-omit-frame-pointer ${CXX11_ABI}")

# Флаги для test coverage
IF (TEST_COVERAGE)
    SET(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -fprofile-arcs -ftest-coverage -fPIC -DIS_DEBUG ${CXX11_ABI}")
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
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libzookeeper/include/)
include_directories (${ClickHouse_SOURCE_DIR}/contrib/libtcmalloc/include/)
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
include_directories (${ClickHouse_BINARY_DIR}/libs/libcommon/include/)
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

# External libraries

# 1. openssl
include_directories ("/usr/local/opt/openssl/include")
set (OPENSSL_HINTS "/usr/local/opt/openssl/lib")
find_library (LIBSSL libssl.a HINTS ${OPENSSL_HINTS})
find_library (LIBCRYPTO libcrypto.a HINTS ${OPENSSL_HINTS})
set (OPENSSL_LIBS ${LIBSSL} ${LIBCRYPTO})

# 2. icu4c
include_directories ("/usr/local/opt/icu4c/include")
set (ICU_HINTS "/usr/local/opt/icu4c/lib")
find_library (ICUI18N libicui18n.a HINTS ${ICU_HINTS})
find_library (ICUUC libicuuc.a HINTS ${ICU_HINTS})
find_library (ICUDATA libicudata.a HINTS ${ICU_HINTS})
set (ICU_LIBS ${ICUI18N} ${ICUUC} ${ICUDATA})

# Directory for Yandex specific files
SET(CLICKHOUSE_PRIVATE_DIR ${ClickHouse_SOURCE_DIR}/private/)

add_subdirectory (contrib)
add_subdirectory (libs)
add_subdirectory (utils)
add_subdirectory (dbms)

IF (EXISTS ${CLICKHOUSE_PRIVATE_DIR})
	add_subdirectory (private)
ENDIF()

message(STATUS "C_FLAGS:  =${CMAKE_C_FLAGS}")
message(STATUS "CXX_FLAGS:=${CMAKE_CXX_FLAGS}")
