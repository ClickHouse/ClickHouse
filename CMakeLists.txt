project (ClickHouse)
cmake_minimum_required (VERSION 2.8)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${ClickHouse_SOURCE_DIR}/cmake/Modules/")

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
	# Require at least gcc 5
	if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5 AND NOT CMAKE_VERSION VERSION_LESS 2.8.9)
		message (FATAL_ERROR "GCC version must be at least 5! For example, if GCC 5 is available under gcc-5, g++-5 names, do the following: export CC=gcc-5 CXX=g++-5; rm -rf CMakeCache.txt CMakeFiles; and re run cmake or ./release.")
	endif ()
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
	# Require at least clang 3.8
	if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 3.8)
		message (FATAL_ERROR "Clang version must be at least 3.8!")
	endif ()
else ()
	message (WARNING "You are using an unsupported compiler! Compilation has only been tested with Clang 3.8+ and GCC 5+.")
endif ()

if (CMAKE_SYSTEM MATCHES "FreeBSD")
	set (PLATFORM_EXTRA_CXX_FLAG "-DCLOCK_MONOTONIC_COARSE=CLOCK_MONOTONIC_FAST")
endif ()

cmake_policy (SET CMP0014 OLD) # Ignore warning about CMakeLists.txt in each directory
cmake_policy (SET CMP0012 NEW) # Don't dereference TRUE and FALSE

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

set (COMMON_WARNING_FLAGS "-Wall")	# -Werror is also added inside directories with our own code.
set (CXX_WARNING_FLAGS "-Wnon-virtual-dtor")

set (CXX11_ABI "ENABLE" CACHE STRING "Use C++11 ABI: DEFAULT, ENABLE, DISABLE")
option (TEST_COVERAGE "Enables flags for test coverage" OFF)
option (ENABLE_TESTS "Enables tests" ON)

option (USE_STATIC_LIBRARIES "Set to FALSE to use shared libraries" ON)
if (USE_STATIC_LIBRARIES)
	list(REVERSE CMAKE_FIND_LIBRARY_SUFFIXES)
endif ()

option (GLIBC_COMPATIBILITY "Set to TRUE to enable compatibility with older glibc libraries. Note that it is not compatible with ASan." OFF)

if (GLIBC_COMPATIBILITY)
	set (GLIBC_COMPATIBILITY_COMPILE_FLAGS   "-include ${ClickHouse_SOURCE_DIR}/libs/libcommon/include/common/glibc_compatibility.h")
	set (GLIBC_COMPATIBILITY_LINK_FLAGS      "-Wl,--wrap=memcpy")
endif ()

if (CXX11_ABI STREQUAL ENABLE)
	set (CXX11_ABI_FLAGS "-D_GLIBCXX_USE_CXX11_ABI=1")
elseif (CXX11_ABI STREQUAL DISABLE)
	set (CXX11_ABI_FLAGS "-D_GLIBCXX_USE_CXX11_ABI=0")
else ()
	set (CXX11_ABI_FLAGS "")
endif ()

set (COMPILER_FLAGS                       "${COMPILER_FLAGS} ${CXX11_ABI_FLAGS}")

option (PIPE "-pipe compiler option [less /tmp usage, more ram usage]" ON)
if (PIPE)
	set (COMPILER_FLAGS                   "${COMPILER_FLAGS} -pipe")
endif ()

include (cmake/test_cpu.cmake)

option (ARCHNATIVE "Enable -march=native compiler flag" OFF)
if (ARCHNATIVE)
	set (COMPILER_FLAGS                   "${COMPILER_FLAGS} -march=native")
endif ()

set (CMAKE_BUILD_COLOR_MAKEFILE          ON)
set (CMAKE_CXX_FLAGS                     "${CMAKE_CXX_FLAGS} ${COMPILER_FLAGS} -std=gnu++1y ${PLATFORM_EXTRA_CXX_FLAG} -fno-omit-frame-pointer ${COMMON_WARNING_FLAGS} ${CXX_WARNING_FLAGS} ${GLIBC_COMPATIBILITY_COMPILE_FLAGS}")
#set (CMAKE_CXX_FLAGS_RELEASE             "${CMAKE_CXX_FLAGS_RELEASE}")
set (CMAKE_CXX_FLAGS_RELWITHDEBINFO      "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O3")
set (CMAKE_CXX_FLAGS_DEBUG               "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g3 -ggdb3 -fno-inline")

set (CMAKE_C_FLAGS                       "${CMAKE_C_FLAGS} ${COMPILER_FLAGS} -fno-omit-frame-pointer ${COMMON_WARNING_FLAGS} ${GLIBC_COMPATIBILITY_COMPILE_FLAGS}")
#set (CMAKE_C_FLAGS_RELEASE               "${CMAKE_C_FLAGS_RELEASE}")
set (CMAKE_C_FLAGS_RELWITHDEBINFO        "${CMAKE_C_FLAGS_RELWITHDEBINFO} -O3")
set (CMAKE_C_FLAGS_DEBUG                 "${CMAKE_C_FLAGS_DEBUG} -O0 -g3 -ggdb3 -fno-inline")

if (NOT APPLE AND NOT (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND CMAKE_SYSTEM MATCHES "FreeBSD"))
	set (CMAKE_EXE_LINKER_FLAGS          "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++")
endif ()

if (NOT APPLE)
	set (CMAKE_EXE_LINKER_FLAGS          "${CMAKE_EXE_LINKER_FLAGS} ${GLIBC_COMPATIBILITY_LINK_FLAGS} ${CXX11_ABI_FLAGS}")
endif ()

include (cmake/test_compiler.cmake)
if (USE_STATIC_LIBRARIES AND HAVE_NO_PIE)
	set (CMAKE_CXX_FLAGS                 "${CMAKE_CXX_FLAGS} ${FLAG_NO_PIE}")
	set (CMAKE_C_FLAGS                   "${CMAKE_C_FLAGS} ${FLAG_NO_PIE}")
endif ()

if (NOT USE_STATIC_LIBRARIES)
	set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endif ()

set (SAN_FLAGS "-O3 -g -fno-omit-frame-pointer")
set (CMAKE_CXX_FLAGS_ASAN                "${CMAKE_CXX_FLAGS_ASAN}  ${SAN_FLAGS} -fsanitize=address")
set (CMAKE_C_FLAGS_ASAN                  "${CMAKE_C_FLAGS_ASAN}    ${SAN_FLAGS} -fsanitize=address")
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
	# -fuse-ld=gold - fix linkage for gcc-5.4, gcc-6.1
	# See more in http://stackoverflow.com/questions/37603238/fsanitize-not-using-gold-linker-in-gcc-6-1
	set (CMAKE_CXX_FLAGS_ASAN            "${CMAKE_CXX_FLAGS_ASAN} -fuse-ld=gold")
	set (CMAKE_C_FLAGS_ASAN              "${CMAKE_C_FLAGS_ASAN}   -fuse-ld=gold")
endif ()
set (CMAKE_CXX_FLAGS_UBSAN               "${CMAKE_CXX_FLAGS_UBSAN} ${SAN_FLAGS} -fsanitize=undefined")
set (CMAKE_C_FLAGS_UBSAN                 "${CMAKE_C_FLAGS_UBSAN}   ${SAN_FLAGS} -fsanitize=undefined")
set (CMAKE_CXX_FLAGS_MSAN                "${CMAKE_CXX_FLAGS_MSAN}  ${SAN_FLAGS} -fsanitize=memory")
set (CMAKE_C_FLAGS_MSAN                  "${CMAKE_C_FLAGS_MSAN}    ${SAN_FLAGS} -fsanitize=memory")
set (CMAKE_CXX_FLAGS_TSAN                "${CMAKE_CXX_FLAGS_TSAN}  ${SAN_FLAGS} -fsanitize=thread")
set (CMAKE_C_FLAGS_TSAN                  "${CMAKE_C_FLAGS_TSAN}    ${SAN_FLAGS} -fsanitize=thread")

# Flags for test coverage
if (TEST_COVERAGE)
	set (CMAKE_CXX_FLAGS_DEBUG "${COMPILER_FLAGS} -O0 -g -fprofile-arcs -ftest-coverage -fPIC -DIS_DEBUG")
endif (TEST_COVERAGE)

# Run tests with "make check"
if (ENABLE_TESTS)
	message (STATUS "Tests are enabled")
	if (TARGET check)
		message (STATUS "Target check already exists")
	else ()
		include (cmake/add_check.cmake)
	endif ()
endif (ENABLE_TESTS)

# Installation prefix
if (NOT CMAKE_SYSTEM MATCHES "FreeBSD")
	set (CMAKE_INSTALL_PREFIX /usr)
	set (CLICKHOUSE_ETC_DIR /etc)
else ()
	set (CLICKHOUSE_ETC_DIR ${CMAKE_INSTALL_PREFIX}/etc)
endif ()


option (UNBUNDLED "Try find all libraries in system (if fail - use bundled from contrib/)" OFF)
if (UNBUNDLED)
	set(NOT_UNBUNDLED 0)
else ()
	set(NOT_UNBUNDLED 1)
endif ()

message (STATUS "Building for: ${CMAKE_SYSTEM} ${CMAKE_SYSTEM_PROCESSOR} ${CMAKE_LIBRARY_ARCHITECTURE} ; USE_STATIC_LIBRARIES=${USE_STATIC_LIBRARIES} UNBUNDLED=${UNBUNDLED}")

include (cmake/find_openssl.cmake)
include (cmake/find_icu4c.cmake)
include (cmake/find_boost.cmake)
# zlib before poco
include (cmake/find_zlib.cmake)
include (cmake/find_zstd.cmake)
include (cmake/find_poco.cmake)
include (cmake/find_lz4.cmake)
include (cmake/find_sparsehash.cmake)
include (cmake/find_libtool.cmake)
include (cmake/find_rt.cmake)
include (cmake/find_readline_edit.cmake)
include (cmake/find_zookeeper.cmake)
include (cmake/find_double-conversion.cmake)
include (cmake/find_re2.cmake)
include (libs/libcommon/cmake/find_gperftools.cmake)
include (libs/libcommon/cmake/find_jemalloc.cmake)

set (FULL_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${CMAKE_BUILD_TYPE}}")
set (FULL_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${CMAKE_BUILD_TYPE}}")
message (STATUS "C_FLAGS    = ${FULL_C_FLAGS}")
message (STATUS "CXX_FLAGS  = ${FULL_CXX_FLAGS}")
message (STATUS "LINK_FLAGS = ${CMAKE_EXE_LINKER_FLAGS}")

# Directory for Yandex specific files
set (CLICKHOUSE_PRIVATE_DIR ${ClickHouse_SOURCE_DIR}/private/)
if (EXISTS ${CLICKHOUSE_PRIVATE_DIR})
	add_subdirectory (${CLICKHOUSE_PRIVATE_DIR})
endif ()

add_subdirectory (contrib)
add_subdirectory (libs)
add_subdirectory (utils)
add_subdirectory (dbms)

include (cmake/print_include_directories.cmake)
