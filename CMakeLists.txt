project (ClickHouse)
cmake_minimum_required (VERSION 2.6)

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
	# Require at least gcc 5
	if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5)
		message (FATAL_ERROR "GCC version must be at least 5! For example, if GCC 5 is available under gcc-5, g++-5 names, do the following: export CC=gcc-5 CXX=g++-5; rm -rf CMakeCache.txt CMakeFiles; and re run cmake or ./release.")
	endif ()
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
	# Require at least clang 3.8
	if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 3.8)
		message (FATAL_ERROR "Clang version must be at least 3.8!")
	endif ()
else ()
	message (WARNING "You are using an unsupported compiler! Compilation has only been tested with Clang 3.8+ and GCC 5+.")
endif ()

if (APPLE)
	set (APPLE_EXTRA_CXX_FLAG "-Dexp10=__exp10") # Also needed for libc++
endif ()

cmake_policy(SET CMP0014 OLD) # Ignore warning about CMakeLists.txt in each directory
cmake_policy(SET CMP0012 NEW) # Don't dereference TRUE and FALSE

if (NOT CMAKE_BUILD_TYPE)
	message (STATUS "CMAKE_BUILD_TYPE is not set, set to default = RELWITHDEBINFO")
	set (CMAKE_BUILD_TYPE "RELWITHDEBINFO")
endif ()
message (STATUS "CMAKE_BUILD_TYPE: " ${CMAKE_BUILD_TYPE} )

# ASan - build type with address sanitizer
# UBSan - build type with undefined behaviour sanitizer
# TSan is not supported due to false positive errors in libstdc++ and necessity to rebuild libstdc++ with TSan
set (CMAKE_CONFIGURATION_TYPES "RelWithDebInfo;Debug;Release;MinSizeRel;ASan;UBSan" CACHE STRING "" FORCE)


if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64.*|AARCH64.*)")
	set (AARCH64 1)
endif ()

if (NOT AARCH64)
	set (MACHINE_FLAGS "-msse4 -mpopcnt")
endif ()

set (COMMON_WARNING_FLAGS "-Wall -Werror")
set (CXX_WARNING_FLAGS "-Wnon-virtual-dtor -Wold-style-cast")

set (ENABLE_CXX11_ABI FALSE CACHE BOOL "Enables C++11 ABI")
set (TEST_COVERAGE FALSE CACHE BOOL "Enables flags for test coverage")
set (ENABLE_TESTS TRUE CACHE BOOL "Enables tests")

set (USE_STATIC_LIBRARIES TRUE CACHE BOOL "Set to FALSE to use shared libraries")
if (NOT $ENV{USE_STATIC_LIBRARIES})
	set (USE_STATIC_LIBRARIES FALSE)
endif ()

set (GLIBC_COMPATIBILITY FALSE CACHE BOOL "Set to TRUE to enable compatibility with older glibc libraries")
if ($ENV{GLIBC_COMPATIBILITY})
	set (GLIBC_COMPATIBILITY TRUE)
endif ()

set (ENABLE_MONGODB TRUE CACHE BOOL "Set to TRUE to enable MongoDB support as source for external dictionaries")
if (NOT $ENV{ENABLE_MONGODB})
	set (ENABLE_MONGODB FALSE)
endif ()

set (ENABLE_LIBTCMALLOC TRUE CACHE BOOL "Set to TRUE to enable libtcmalloc.")
if (NOT $ENV{ENABLE_LIBTCMALLOC})
	set (ENABLE_LIBTCMALLOC FALSE)
endif ()

set (DEBUG_LIBTCMALLOC FALSE CACHE BOOL "Set to TRUE to use debug version of libtcmalloc.")
if ($ENV{DEBUG_LIBTCMALLOC})
	set (ENABLE_LIBTCMALLOC TRUE)
endif ()

if (GLIBC_COMPATIBILITY)
	set (GLIBC_COMPATIBILITY_COMPILE_FLAGS   "-include ${ClickHouse_SOURCE_DIR}/libs/libcommon/include/common/glibc_compatibility.h")
	set (GLIBC_COMPATIBILITY_LINK_FLAGS      "-Wl,--wrap=memcpy")
endif ()

if (NOT ENABLE_CXX11_ABI)
	set (CXX11_ABI "-D_GLIBCXX_USE_CXX11_ABI=0")
endif ()

set (CMAKE_BUILD_COLOR_MAKEFILE          ON)
set (CMAKE_CXX_FLAGS                     "-std=gnu++1y ${APPLE_EXTRA_CXX_FLAG} -fno-omit-frame-pointer ${COMMON_WARNING_FLAGS} ${CXX_WARNING_FLAGS} ${MACHINE_FLAGS} ${GLIBC_COMPATIBILITY_COMPILE_FLAGS} ${CXX11_ABI}")
set (CMAKE_CXX_FLAGS_RELEASE             "-O3 -DNDEBUG")
set (CMAKE_CXX_FLAGS_RELWITHDEBINFO      "-O3 -g")
set (CMAKE_CXX_FLAGS_DEBUG               "-O0 -g3 -ggdb3 -fno-inline")

set (CMAKE_C_FLAGS                       "-fno-omit-frame-pointer ${COMMON_WARNING_FLAGS} ${MACHINE_FLAGS} ${GLIBC_COMPATIBILITY_COMPILE_FLAGS} ${CXX11_ABI}")
set (CMAKE_C_FLAGS_RELEASE               "-O3 -DNDEBUG")
set (CMAKE_C_FLAGS_RELWITHDEBINFO        "-O3 -g")
set (CMAKE_C_FLAGS_DEBUG                 "-O0 -g3 -ggdb3 -fno-inline")

if (NOT APPLE)
	set (CMAKE_EXE_LINKER_FLAGS              "-static-libgcc -static-libstdc++ ${GLIBC_COMPATIBILITY_LINK_FLAGS} ${CXX11_ABI}")
endif()

# -fuse-ld=gold - fix linkage for gcc-5.4, gcc-6.1 
# See more in http://stackoverflow.com/questions/37603238/fsanitize-not-using-gold-linker-in-gcc-6-1
set (CMAKE_CXX_FLAGS_ASAN                "-O3 -g -fsanitize=address -fno-omit-frame-pointer -fuse-ld=gold ${CXX11_ABI}")
set (CMAKE_CXX_FLAGS_UBSAN               "-O3 -g -fsanitize=undefined -fno-omit-frame-pointer ${CXX11_ABI}")
set (CMAKE_C_FLAGS_ASAN                  "-O3 -g -fsanitize=address -fno-omit-frame-pointer -fuse-ld=gold ${CXX11_ABI}")
set (CMAKE_C_FLAGS_UBSAN                 "-O3 -g -fsanitize=undefined -fno-omit-frame-pointer ${CXX11_ABI}")

# Flags for test coverage
if (TEST_COVERAGE)
	set (CMAKE_CXX_FLAGS_DEBUG "-O0 -g -fprofile-arcs -ftest-coverage -fPIC -DIS_DEBUG ${CXX11_ABI}")
endif (TEST_COVERAGE)

# Run tests with "make check"
if (ENABLE_TESTS)
	message (STATUS "Tests are enabled")
	include (add.test.cmake)
endif (ENABLE_TESTS)

# Installation prefix
set (CMAKE_INSTALL_PREFIX /usr)

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

if (AARCH64)
	include_directories (/usr/lib/aarch64-linux-gnu/glib-2.0/include/)
else ()
	include_directories (/usr/lib/x86_64-linux-gnu/glib-2.0/include/)
endif ()

include_directories (/usr/local/include/)

link_directories (/usr/local/lib)

# External libraries

# 1. openssl
include_directories ("/usr/local/opt/openssl/include")
set (OPENSSL_HINTS "/usr/local/opt/openssl/lib")
if (USE_STATIC_LIBRARIES)
	find_library (LIBSSL libssl.a HINTS ${OPENSSL_HINTS})
	find_library (LIBCRYPTO libcrypto.a HINTS ${OPENSSL_HINTS})
else ()
	find_library (LIBSSL ssl HINTS ${OPENSSL_HINTS})
	find_library (LIBCRYPTO crypto HINTS ${OPENSSL_HINTS})
endif ()
set (OPENSSL_LIBS ${LIBSSL} ${LIBCRYPTO})

# 2. icu4c
include_directories ("/usr/local/opt/icu4c/include")
set (ICU_HINTS "/usr/local/opt/icu4c/lib")
if (USE_STATIC_LIBRARIES)
	find_library (ICUI18N libicui18n.a HINTS ${ICU_HINTS})
	find_library (ICUUC libicuuc.a HINTS ${ICU_HINTS})
	find_library (ICUDATA libicudata.a HINTS ${ICU_HINTS})
else ()
	find_library (ICUI18N icui18n HINTS ${ICU_HINTS})
	find_library (ICUUC icuuc HINTS ${ICU_HINTS})
	find_library (ICUDATA icudata HINTS ${ICU_HINTS})
endif ()
set (ICU_LIBS ${ICUI18N} ${ICUUC} ${ICUDATA})

# 3. boost
set (BOOST_HINTS "/usr/local/opt/boost/lib")
if (USE_STATIC_LIBRARIES)
	find_library (BOOST_PROGRAM_OPTIONS_LIB libboost_program_options.a HINTS ${BOOST_HINTS})
	find_library (BOOST_SYSTEM_LIB libboost_system.a HINTS ${BOOST_HINTS})
	find_library (BOOST_FILESYSTEM_LIB libboost_filesystem.a HINTS ${BOOST_HINTS})
	find_library (BOOST_REGEX_LIB libboost_regex.a HINTS ${BOOST_HINTS})
	find_library (BOOST_THREAD_LIB libboost_thread.a HINTS ${BOOST_HINTS})
else ()
	find_library (BOOST_PROGRAM_OPTIONS_LIB boost_program_options HINTS ${BOOST_HINTS})
	find_library (BOOST_SYSTEM_LIB boost_system HINTS ${BOOST_HINTS})
	find_library (BOOST_FILESYSTEM_LIB boost_filesystem HINTS ${BOOST_HINTS})
	find_library (BOOST_REGEX_LIB boost_regex HINTS ${BOOST_HINTS})
	find_library (BOOST_THREAD_LIB boost_thread HINTS ${BOOST_HINTS})
endif ()

# 4. ltdl
set (LTDL_HINTS "/usr/local/opt/libtool/lib")
if (USE_STATIC_LIBRARIES)
	find_library (LTDL_LIB libltdl.a HINTS ${LTDL_HINTS})
else ()
	find_library (LTDL_LIB ltdl HINTS ${LTDL_HINTS})
endif ()

# Directory for Yandex specific files
set (CLICKHOUSE_PRIVATE_DIR ${ClickHouse_SOURCE_DIR}/private/)

add_subdirectory (contrib)
add_subdirectory (libs)
add_subdirectory (utils)
add_subdirectory (dbms)

if (EXISTS ${CLICKHOUSE_PRIVATE_DIR})
	add_subdirectory (private)
endif ()

message (STATUS "C_FLAGS:  =${CMAKE_C_FLAGS}")
message (STATUS "CXX_FLAGS:=${CMAKE_CXX_FLAGS}")
