foreach(policy
        CMP0023
        CMP0048 # CMake 3.0
        CMP0074 # CMake 3.12
        CMP0077
        CMP0079
    )
    if(POLICY ${policy})
        cmake_policy(SET ${policy} NEW)
    endif()
endforeach()

project(ClickHouse)
cmake_minimum_required(VERSION 3.3)

# Ignore export() since we don't use it,
# but it gets broken with a global targets via link_libraries()
macro (export)
endmacro ()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/")
set(CMAKE_EXPORT_COMPILE_COMMANDS 1) # Write compile_commands.json
set(CMAKE_LINK_DEPENDS_NO_SHARED 1) # Do not relink all depended targets on .so
set(CMAKE_CONFIGURATION_TYPES "RelWithDebInfo;Debug;Release;MinSizeRel" CACHE STRING "" FORCE)
set(CMAKE_DEBUG_POSTFIX "d" CACHE STRING "Generate debug library name with a postfix.")    # To be consistent with CMakeLists from contrib libs.

include (cmake/arch.cmake)

option(ENABLE_IPO "Enable inter-procedural optimization (aka LTO)" OFF) # need cmake 3.9+
if(ENABLE_IPO)
    cmake_policy(SET CMP0069 NEW)
    include(CheckIPOSupported)
    check_ipo_supported(RESULT IPO_SUPPORTED OUTPUT IPO_NOT_SUPPORTED)
    if(IPO_SUPPORTED)
        message(STATUS "IPO/LTO is supported, enabling")
        set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
    else()
        message(STATUS "IPO/LTO is not supported: <${IPO_NOT_SUPPORTED}>")
    endif()
else()
    message(STATUS "IPO/LTO not enabled.")
endif()

if (COMPILER_GCC)
    # Require minimum version of gcc
    set (GCC_MINIMUM_VERSION 8)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${GCC_MINIMUM_VERSION} AND NOT CMAKE_VERSION VERSION_LESS 2.8.9)
        message (FATAL_ERROR "GCC version must be at least ${GCC_MINIMUM_VERSION}. For example, if GCC ${GCC_MINIMUM_VERSION} is available under gcc-${GCC_MINIMUM_VERSION}, g++-${GCC_MINIMUM_VERSION} names, do the following: export CC=gcc-${GCC_MINIMUM_VERSION} CXX=g++-${GCC_MINIMUM_VERSION}; rm -rf CMakeCache.txt CMakeFiles; and re run cmake or ./release.")
    endif ()
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # Require minimum version of clang
    set (CLANG_MINIMUM_VERSION 7)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${CLANG_MINIMUM_VERSION})
        message (FATAL_ERROR "Clang version must be at least ${CLANG_MINIMUM_VERSION}.")
    endif ()
else ()
    message (WARNING "You are using an unsupported compiler. Compilation has only been tested with Clang 6+ and GCC 7+.")
endif ()

# Check that submodules are present only if source was downloaded with git
if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.git" AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/boost/boost")
    message (FATAL_ERROR "Submodules are not initialized. Run\n\tgit submodule update --init --recursive")
endif ()

include (cmake/find_ccache.cmake)

if (NOT CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL "None")
    message (STATUS "CMAKE_BUILD_TYPE is not set, set to default = RELWITHDEBINFO")
    set (CMAKE_BUILD_TYPE "RELWITHDEBINFO")
endif ()
string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_UC)
message (STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")


option (USE_STATIC_LIBRARIES "Set to FALSE to use shared libraries" ON)
option (MAKE_STATIC_LIBRARIES "Set to FALSE to make shared libraries" ${USE_STATIC_LIBRARIES})
if (NOT MAKE_STATIC_LIBRARIES)
    option (SPLIT_SHARED_LIBRARIES "DEV ONLY. Keep all internal libs as separate .so for faster linking" OFF)
    option (CLICKHOUSE_SPLIT_BINARY "Make several binaries instead one bundled (clickhouse-server, clickhouse-client, ... )" OFF)
endif ()

if (MAKE_STATIC_LIBRARIES AND SPLIT_SHARED_LIBRARIES)
    message(FATAL_ERROR "Defining SPLIT_SHARED_LIBRARIES=1 without MAKE_STATIC_LIBRARIES=0 has no effect.")
endif()

if (NOT MAKE_STATIC_LIBRARIES AND SPLIT_SHARED_LIBRARIES)
    set(BUILD_SHARED_LIBS 1 CACHE INTERNAL "")
endif ()

if (USE_STATIC_LIBRARIES)
    list(REVERSE CMAKE_FIND_LIBRARY_SUFFIXES)
endif ()

include (cmake/sanitize.cmake)


if (CMAKE_GENERATOR STREQUAL "Ninja")
    # Turn on colored output. https://github.com/ninja-build/ninja/wiki/FAQ
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-color=always")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fdiagnostics-color=always")
endif ()

if (NOT MSVC)
    set (COMMON_WARNING_FLAGS "${COMMON_WARNING_FLAGS} -Wall")    # -Werror is also added inside directories with our own code.
endif ()

if (COMPILER_GCC OR COMPILER_CLANG)
    set (CXX_WARNING_FLAGS "${CXX_WARNING_FLAGS} -Wnon-virtual-dtor")
endif ()

if (COMPILER_GCC AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "8.3.0")
    # Warnings in protobuf generating
    set (CXX_WARNING_FLAGS "${CXX_WARNING_FLAGS} -Wno-array-bounds")
endif ()

if (COMPILER_CLANG)
    # clang: warning: argument unused during compilation: '-specs=/usr/share/dpkg/no-pie-compile.specs' [-Wunused-command-line-argument]
    set (COMMON_WARNING_FLAGS "${COMMON_WARNING_FLAGS} -Wno-unused-command-line-argument")
    # generate ranges for fast "addr2line" search
    if (NOT CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE")
        set(COMPILER_FLAGS "${COMPILER_FLAGS} -gdwarf-aranges")
    endif ()
endif ()

option (ENABLE_TESTS "Enables tests" ON)

if (CMAKE_SYSTEM_PROCESSOR MATCHES "amd64|x86_64")
    option (USE_INTERNAL_MEMCPY "Use internal implementation of 'memcpy' function instead of provided by libc. Only for x86_64." ON)

    if (OS_LINUX AND NOT UNBUNDLED AND MAKE_STATIC_LIBRARIES AND NOT SPLIT_SHARED_LIBRARIES AND CMAKE_VERSION VERSION_GREATER "3.9.0")
        option (GLIBC_COMPATIBILITY "Set to TRUE to enable compatibility with older glibc libraries. Only for x86_64, Linux. Implies USE_INTERNAL_MEMCPY." ON)
    endif ()

    if (NOT CMAKE_VERSION VERSION_GREATER "3.9.0")
        message (WARNING "CMake version must be greater than 3.9.0 for production builds.")
    endif ()
endif ()

string(REGEX MATCH "-?[0-9]+(.[0-9]+)?$" COMPILER_POSTFIX ${CMAKE_CXX_COMPILER})

find_program (LLD_PATH NAMES "lld${COMPILER_POSTFIX}" "lld")
find_program (GOLD_PATH NAMES "gold")

if (COMPILER_CLANG AND LLD_PATH AND NOT LINKER_NAME)
    set (LINKER_NAME "lld")
elseif (GOLD_PATH)
    set (LINKER_NAME "gold")
endif ()

if (LINKER_NAME)
    message(STATUS "Using linker: ${LINKER_NAME} (selected from: LLD_PATH=${LLD_PATH}; GOLD_PATH=${GOLD_PATH}; COMPILER_POSTFIX=${COMPILER_POSTFIX})")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=${LINKER_NAME}")
endif ()

cmake_host_system_information(RESULT AVAILABLE_PHYSICAL_MEMORY QUERY AVAILABLE_PHYSICAL_MEMORY) # Not available under freebsd
if(NOT AVAILABLE_PHYSICAL_MEMORY OR AVAILABLE_PHYSICAL_MEMORY GREATER 8000)
    option(COMPILER_PIPE "-pipe compiler option [less /tmp usage, more ram usage]" ON)
endif()
if(COMPILER_PIPE)
    set(COMPILER_FLAGS "${COMPILER_FLAGS} -pipe")
else()
    message(STATUS "Disabling compiler -pipe option (have only ${AVAILABLE_PHYSICAL_MEMORY} mb of memory)")
endif()

if(NOT DISABLE_CPU_OPTIMIZE)
    include(cmake/test_cpu.cmake)
endif()

if(NOT COMPILER_CLANG) # clang: error: the clang compiler does not support '-march=native'
    option(ARCH_NATIVE "Enable -march=native compiler flag" ${ARCH_ARM})
endif()

if (ARCH_NATIVE)
    set (COMPILER_FLAGS "${COMPILER_FLAGS} -march=native")
endif ()

if (CMAKE_VERSION VERSION_LESS "3.8.0")
    if (NOT MSVC)
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
    endif ()
else ()
    set (CMAKE_CXX_STANDARD 17)
    set (CMAKE_CXX_EXTENSIONS 0) # https://cmake.org/cmake/help/latest/prop_tgt/CXX_EXTENSIONS.html#prop_tgt:CXX_EXTENSIONS
    set (CMAKE_CXX_STANDARD_REQUIRED ON)
    set (CXX_FLAGS_INTERNAL_COMPILER "-std=c++17")
endif ()

if (COMPILER_GCC OR COMPILER_CLANG)
    # Enable C++14 sized global deallocation functions. It should be enabled by setting -std=c++14 but I'm not sure.
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsized-deallocation")
endif ()

option(WITH_COVERAGE "Build with coverage." 0)
if(WITH_COVERAGE AND COMPILER_CLANG)
   set(COMPILER_FLAGS "${COMPILER_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
   # If we want to disable coverage for specific translation units
   set(WITHOUT_COVERAGE "-fno-profile-instr-generate -fno-coverage-mapping")
endif()
if(WITH_COVERAGE AND COMPILER_GCC)
   set(COMPILER_FLAGS "${COMPILER_FLAGS} -fprofile-arcs -ftest-coverage")
   set(COVERAGE_OPTION "-lgcov")
   set(WITHOUT_COVERAGE "-fno-profile-arcs -fno-test-coverage")
endif()

set (CMAKE_BUILD_COLOR_MAKEFILE          ON)
set (CMAKE_CXX_FLAGS                     "${CMAKE_CXX_FLAGS} ${COMPILER_FLAGS} ${PLATFORM_EXTRA_CXX_FLAG} -fno-omit-frame-pointer ${COMMON_WARNING_FLAGS} ${CXX_WARNING_FLAGS}")
set (CMAKE_CXX_FLAGS_RELWITHDEBINFO      "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O3 ${CMAKE_CXX_FLAGS_ADD}")
set (CMAKE_CXX_FLAGS_DEBUG               "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g3 -ggdb3 -fno-inline ${CMAKE_CXX_FLAGS_ADD}")

set (CMAKE_C_FLAGS                       "${CMAKE_C_FLAGS} ${COMPILER_FLAGS} -fno-omit-frame-pointer ${COMMON_WARNING_FLAGS} ${CMAKE_C_FLAGS_ADD}")
set (CMAKE_C_FLAGS_RELWITHDEBINFO        "${CMAKE_C_FLAGS_RELWITHDEBINFO} -O3 ${CMAKE_C_FLAGS_ADD}")
set (CMAKE_C_FLAGS_DEBUG                 "${CMAKE_C_FLAGS_DEBUG} -O0 -g3 -ggdb3 -fno-inline ${CMAKE_C_FLAGS_ADD}")


option (UNBUNDLED "Try find all libraries in system. We recommend to avoid this mode for production builds, because we cannot guarantee exact versions and variants of libraries your system has installed. This mode exists for enthusiastic developers who search for trouble. Also it is useful for maintainers of OS packages." OFF)
if (UNBUNDLED)
    set(NOT_UNBUNDLED 0)
else ()
    set(NOT_UNBUNDLED 1)
endif ()

# Using system libs can cause lot of warnings in includes.
if (UNBUNDLED OR NOT (OS_LINUX OR APPLE) OR ARCH_32)
    option (NO_WERROR "Disable -Werror compiler option" ON)
endif ()

# Make this extra-checks for correct library dependencies.
if (NOT SANITIZE)
    set (CMAKE_EXE_LINKER_FLAGS    "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-undefined")
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--no-undefined")
endif ()

include(cmake/dbms_glob_sources.cmake)
include(cmake/default_libs.cmake)

######################################
### Add targets below this comment ###
######################################

string (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_UC)
set (CMAKE_POSTFIX_VARIABLE "CMAKE_${CMAKE_BUILD_TYPE_UC}_POSTFIX")

if (NOT MAKE_STATIC_LIBRARIES)
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endif ()

# Using "include-what-you-use" tool.
option (USE_INCLUDE_WHAT_YOU_USE "Use 'include-what-you-use' tool" OFF)
if (USE_INCLUDE_WHAT_YOU_USE)
    find_program(IWYU_PATH NAMES include-what-you-use iwyu)
    if (NOT IWYU_PATH)
        message(FATAL_ERROR "Could not find the program include-what-you-use")
    endif()
    if (${CMAKE_VERSION} VERSION_LESS "3.3.0")
        message(FATAL_ERROR "include-what-you-use requires CMake version at least 3.3.")
    endif()
endif ()

# Using clang-tidy static analyzer http://mariobadr.com/using-clang-tidy-with-cmake-36.html https://cmake.org/cmake/help/v3.6/prop_tgt/LANG_CLANG_TIDY.html
option (ENABLE_CLANG_TIDY "Use 'clang-tidy' static analyzer" OFF)
if (ENABLE_CLANG_TIDY)
    if (${CMAKE_VERSION} VERSION_LESS "3.6.0")
        message(FATAL_ERROR "clang-tidy requires CMake version at least 3.6.")
    endif()
    find_program (CLANG_TIDY_EXE NAMES "clang-tidy" DOC "Path to clang-tidy executable")
    if (NOT CLANG_TIDY_EXE)
        set (USE_CLANG_TIDY 0)
        message (STATUS "clang-tidy not found.")
    else ()
        set (USE_CLANG_TIDY 1)
        message (STATUS "clang-tidy found: ${CLANG_TIDY_EXE}")
        set (DO_CLANG_TIDY "${CLANG_TIDY_EXE}" "-checks=*,-clang-analyzer-alpha.*")
        # You can enable it within a directory by: set (CMAKE_CXX_CLANG_TIDY "${DO_CLANG_TIDY}")
    endif ()
endif ()

if (ENABLE_TESTS)
    message (STATUS "Tests are enabled")
endif ()
enable_testing() # Enable for tests without binary

# when installing to /usr - place configs to /etc but for /usr/local place to /usr/local/etc
if (CMAKE_INSTALL_PREFIX STREQUAL "/usr")
    set (CLICKHOUSE_ETC_DIR "/etc")
else ()
    set (CLICKHOUSE_ETC_DIR "${CMAKE_INSTALL_PREFIX}/etc")
endif ()

option (UNBUNDLED "Try find all libraries in system. We recommend to avoid this mode for production builds, because we cannot guarantee exact versions and variants of libraries your system has installed. This mode exists for enthusiastic developers who search for trouble. Also it is useful for maintainers of OS packages." OFF)
if (UNBUNDLED)
    set(NOT_UNBUNDLED 0)
else ()
    set(NOT_UNBUNDLED 1)
endif ()

# Using system libs can cause lot of warnings in includes.
if (UNBUNDLED OR NOT (OS_LINUX OR APPLE) OR ARCH_32)
    option (NO_WERROR "Disable -Werror compiler option" ON)
endif ()

message (STATUS "Building for: ${CMAKE_SYSTEM} ${CMAKE_SYSTEM_PROCESSOR} ${CMAKE_LIBRARY_ARCHITECTURE} ; USE_STATIC_LIBRARIES=${USE_STATIC_LIBRARIES} MAKE_STATIC_LIBRARIES=${MAKE_STATIC_LIBRARIES} SPLIT_SHARED=${SPLIT_SHARED_LIBRARIES} UNBUNDLED=${UNBUNDLED} CCACHE=${CCACHE_FOUND} ${CCACHE_VERSION}")

include(GNUInstallDirs)
include (cmake/find_contrib_lib.cmake)
include (cmake/lib_name.cmake)

find_contrib_lib(double-conversion) # Must be before parquet
include (cmake/find_ssl.cmake)
include (cmake/find_icu.cmake)
include (cmake/find_boost.cmake)
include (cmake/find_zlib.cmake)
include (cmake/find_zstd.cmake)
include (cmake/find_ltdl.cmake) # for odbc
include (cmake/find_termcap.cmake)
include (cmake/find_odbc.cmake)
# openssl, zlib, odbc before poco
include (cmake/find_poco.cmake)
include (cmake/find_lz4.cmake)
include (cmake/find_xxhash.cmake)
include (cmake/find_sparsehash.cmake)
include (cmake/find_rt.cmake)
include (cmake/find_execinfo.cmake)
include (cmake/find_readline_edit.cmake)
include (cmake/find_re2.cmake)
include (cmake/find_libgsasl.cmake)
include (cmake/find_rdkafka.cmake)
include (cmake/find_capnp.cmake)
include (cmake/find_llvm.cmake)
include (cmake/find_h3.cmake)
include (cmake/find_cpuid.cmake) # Freebsd, bundled
if (NOT USE_CPUID)
    include (cmake/find_cpuinfo.cmake) # Debian
endif()
include (cmake/find_libxml2.cmake)
include (cmake/find_brotli.cmake)
include (cmake/find_protobuf.cmake)
include (cmake/find_pdqsort.cmake)
include (cmake/find_hdfs3.cmake) # uses protobuf
include (cmake/find_consistent-hashing.cmake)
include (cmake/find_base64.cmake)
include (cmake/find_parquet.cmake)
include (cmake/find_hyperscan.cmake)
include (cmake/find_simdjson.cmake)
include (cmake/find_rapidjson.cmake)
include (cmake/find_fastops.cmake)
#include (cmake/find_orc.cmake)

find_contrib_lib(cityhash)
find_contrib_lib(farmhash)
find_contrib_lib(metrohash)
find_contrib_lib(btrie)

if (ENABLE_TESTS)
    include (cmake/find_gtest.cmake)
endif ()

# Need to process before "contrib" dir:
include (libs/libcommon/cmake/find_gperftools.cmake)
include (libs/libcommon/cmake/find_jemalloc.cmake)
include (libs/libcommon/cmake/find_cctz.cmake)
include (libs/libmysqlxx/cmake/find_mysqlclient.cmake)

include (cmake/print_flags.cmake)

install (EXPORT global DESTINATION cmake)

add_subdirectory (contrib EXCLUDE_FROM_ALL)
add_subdirectory (libs)
add_subdirectory (utils)
add_subdirectory (dbms)

include (cmake/print_include_directories.cmake)
