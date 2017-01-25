project (ClickHouse)
cmake_minimum_required (VERSION 2.6)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

message(STATUS "Building for: ${CMAKE_SYSTEM} ${CMAKE_SYSTEM_PROCESSOR} ${CMAKE_LIBRARY_ARCHITECTURE}")

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

if (APPLE)
	set (PLATFORM_EXTRA_CXX_FLAG "-Dexp10=__exp10") # Also needed for libc++
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

if (NOT AARCH64)
	set (MACHINE_FLAGS "-msse4 -mpopcnt")
endif ()

set (COMMON_WARNING_FLAGS "-Wall")	# -Werror is also added inside directories with our own code.
set (CXX_WARNING_FLAGS "-Wnon-virtual-dtor")

set (CXX11_ABI "ENABLE" CACHE STRING "Use C++11 ABI: DEFAULT, ENABLE, DISABLE")
option (TEST_COVERAGE "Enables flags for test coverage" OFF)
option (ENABLE_TESTS "Enables tests" ON)

option (USE_STATIC_LIBRARIES "Set to FALSE to use shared libraries" ON)
if (NOT $ENV{USE_STATIC_LIBRARIES})
	set (USE_STATIC_LIBRARIES FALSE)
endif ()

set (USE_INTERNAL_BOOST_LIBRARY TRUE CACHE BOOL "Set to FALSE to use system boost library instead of bundled")
if (NOT $ENV{USE_INTERNAL_BOOST_LIBRARY})
	set (USE_INTERNAL_BOOST_LIBRARY FALSE)
endif ()

if (USE_INTERNAL_BOOST_LIBRARY)
	add_definitions(-DBOOST_SYSTEM_NO_DEPRECATED)
endif ()

option (USE_INTERNAL_POCO_LIBRARY "Set to FALSE to use system poco library instead of bundled" ON)
option (USE_INTERNAL_GPERFTOOLS_LIBRARY "Set to FALSE to use system gperftools (tcmalloc) library instead of bundled" ON)


option (ENABLE_LIBTCMALLOC "Set to TRUE to enable libtcmalloc." ON)
if (NOT $ENV{ENABLE_LIBTCMALLOC})
	set (ENABLE_LIBTCMALLOC FALSE)
endif ()

option (DEBUG_LIBTCMALLOC "Set to TRUE to use debug version of libtcmalloc." OFF)
if ($ENV{DEBUG_LIBTCMALLOC})
	set (DEBUG_LIBTCMALLOC TRUE)
endif ()

option (GLIBC_COMPATIBILITY "Set to TRUE to enable compatibility with older glibc libraries. Note that it is not compatible with ASan." OFF)
if ($ENV{GLIBC_COMPATIBILITY})
	set (GLIBC_COMPATIBILITY TRUE)
endif ()

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


set (CMAKE_BUILD_COLOR_MAKEFILE          ON)
set (CMAKE_CXX_FLAGS                     "${CMAKE_CXX_FLAGS} ${COMPILER_FLAGS} -std=gnu++1y ${PLATFORM_EXTRA_CXX_FLAG} -fno-omit-frame-pointer ${COMMON_WARNING_FLAGS} ${CXX_WARNING_FLAGS} ${MACHINE_FLAGS} ${GLIBC_COMPATIBILITY_COMPILE_FLAGS}")
#set (CMAKE_CXX_FLAGS_RELEASE             "${CMAKE_CXX_FLAGS_RELEASE}")
set (CMAKE_CXX_FLAGS_RELWITHDEBINFO      "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O3")
set (CMAKE_CXX_FLAGS_DEBUG               "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g3 -ggdb3 -fno-inline")

set (CMAKE_C_FLAGS                       "${CMAKE_C_FLAGS} ${COMPILER_FLAGS} -fno-omit-frame-pointer ${COMMON_WARNING_FLAGS} ${MACHINE_FLAGS} ${GLIBC_COMPATIBILITY_COMPILE_FLAGS}")
#set (CMAKE_C_FLAGS_RELEASE               "${CMAKE_C_FLAGS_RELEASE}")
set (CMAKE_C_FLAGS_RELWITHDEBINFO        "${CMAKE_C_FLAGS_RELWITHDEBINFO} -O3")
set (CMAKE_C_FLAGS_DEBUG                 "${CMAKE_C_FLAGS_DEBUG} -O0 -g3 -ggdb3 -fno-inline")

if (NOT APPLE AND NOT (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND CMAKE_SYSTEM MATCHES "FreeBSD"))
	set (CMAKE_EXE_LINKER_FLAGS          "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++")
endif ()

if (NOT APPLE)
	set (CMAKE_EXE_LINKER_FLAGS          "${CMAKE_EXE_LINKER_FLAGS} ${GLIBC_COMPATIBILITY_LINK_FLAGS} ${CXX11_ABI_FLAGS}")
endif ()

if (USE_STATIC_LIBRARIES AND NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
	set (CMAKE_CXX_FLAGS                 "${CMAKE_CXX_FLAGS} -no-pie")
	set (CMAKE_C_FLAGS                   "${CMAKE_C_FLAGS} -no-pie")
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
	include (cmake/add_check.cmake)
endif (ENABLE_TESTS)

# Installation prefix
if (NOT CMAKE_SYSTEM MATCHES "FreeBSD")
	set (CMAKE_INSTALL_PREFIX /usr)
	set (CLICKHOUSE_ETC_DIR /etc)
else ()
	set (CLICKHOUSE_ETC_DIR ${CMAKE_INSTALL_PREFIX}/etc)
endif ()

include (cmake/find_openssl.cmake)
include (cmake/find_icu4c.cmake)
include (cmake/find_boost.cmake)
include (cmake/find_poco.cmake)
include (cmake/find_libtool.cmake)
include (cmake/find_rt.cmake)
include (cmake/find_readline_edit.cmake)

if (ENABLE_LIBTCMALLOC)
    include (cmake/find_gperftools.cmake)
endif ()

set(FULL_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${CMAKE_BUILD_TYPE}}")
set(FULL_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${CMAKE_BUILD_TYPE}}")
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
