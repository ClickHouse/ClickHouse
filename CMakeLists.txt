project (ClickHouse)
cmake_minimum_required (VERSION 2.8)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${ClickHouse_SOURCE_DIR}/cmake/Modules/")

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # Require at least gcc 7
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7 AND NOT CMAKE_VERSION VERSION_LESS 2.8.9)
        message (FATAL_ERROR "GCC version must be at least 7. For example, if GCC 7 is available under gcc-7, g++-7 names, do the following: export CC=gcc-7 CXX=g++-7; rm -rf CMakeCache.txt CMakeFiles; and re run cmake or ./release.")
    endif ()
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # Require at least clang 5
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5)
        message (FATAL_ERROR "Clang version must be at least 5.")
    endif ()
else ()
    message (WARNING "You are using an unsupported compiler. Compilation has only been tested with Clang 5+ and GCC 7+.")
endif ()

# Check that submodules are present only if source was downloaded with git
if (EXISTS "${ClickHouse_SOURCE_DIR}/.git" AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/boost/boost")
    message (FATAL_ERROR "Submodules are not initialized. Run\n\tgit submodule update --init --recursive")
endif ()

# Write compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

include (cmake/find_ccache.cmake)

if (NOT CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL "None")
    message (STATUS "CMAKE_BUILD_TYPE is not set, set to default = RELWITHDEBINFO")
    set (CMAKE_BUILD_TYPE "RELWITHDEBINFO")
endif ()
string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_UC)
message (STATUS "CMAKE_BUILD_TYPE: " ${CMAKE_BUILD_TYPE} )

# ASan - build type with address sanitizer
# UBSan - build type with undefined behaviour sanitizer
# TSan is not supported due to false positive errors in libstdc++ and necessity to rebuild libstdc++ with TSan
set (CMAKE_CONFIGURATION_TYPES "RelWithDebInfo;Debug;Release;MinSizeRel;ASan;UBSan" CACHE STRING "" FORCE)

include (cmake/arch.cmake)

if (CMAKE_GENERATOR STREQUAL "Ninja")
    # Turn on colored output. https://github.com/ninja-build/ninja/wiki/FAQ
    set (COMPILER_FLAGS "${COMPILER_FLAGS} -fdiagnostics-color=always")
endif ()

if (NOT MSVC)
    set (COMMON_WARNING_FLAGS "${COMMON_WARNING_FLAGS} -Wall")    # -Werror is also added inside directories with our own code.
endif ()

if (COMPILER_GCC OR COMPILER_CLANG)
    set (CXX_WARNING_FLAGS "${CXX_WARNING_FLAGS} -Wnon-virtual-dtor")
endif ()

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # clang: warning: argument unused during compilation: '-stdlib=libc++'
    # clang: warning: argument unused during compilation: '-specs=/usr/share/dpkg/no-pie-compile.specs' [-Wunused-command-line-argument]
    set (COMMON_WARNING_FLAGS "${COMMON_WARNING_FLAGS} -Wno-unused-command-line-argument")
endif ()

if (ARCH_LINUX)
    set (CXX11_ABI "ENABLE" CACHE STRING "Use C++11 ABI: DEFAULT, ENABLE, DISABLE")
endif ()

option (TEST_COVERAGE "Enables flags for test coverage" OFF)
option (ENABLE_TESTS "Enables tests" ${NOT_MSVC})

option (USE_STATIC_LIBRARIES "Set to FALSE to use shared libraries" ON)
option (MAKE_STATIC_LIBRARIES "Set to FALSE to make shared libraries" ${USE_STATIC_LIBRARIES})
if (NOT MAKE_STATIC_LIBRARIES)
    option (SPLIT_SHARED_LIBRARIES "DEV ONLY. Keep all internal libs as separate .so for faster linking" OFF)
    option (CLICKHOUSE_SPLIT_BINARY "Make several binaries instead one bundled (clickhouse-server, clickhouse-client, ... )" OFF)
endif ()

if (SPLIT_SHARED_LIBRARIES)
    set (SPLIT_SHARED SHARED)
endif ()

if (USE_STATIC_LIBRARIES)
    list(REVERSE CMAKE_FIND_LIBRARY_SUFFIXES)
endif ()

if (CMAKE_LIBRARY_ARCHITECTURE MATCHES "amd64.*|x86_64.*|AMD64.*")
    option (USE_INTERNAL_MEMCPY "Use internal implementation of 'memcpy' function instead of provided by libc. Only for x86_64." ON)

    if (ARCH_LINUX)
        option (GLIBC_COMPATIBILITY "Set to TRUE to enable compatibility with older glibc libraries. Only for x86_64, Linux. Implies USE_INTERNAL_MEMCPY." ON)
    endif()
endif ()

if (GLIBC_COMPATIBILITY)
    set (USE_INTERNAL_MEMCPY ON)
endif ()

if (CXX11_ABI STREQUAL ENABLE)
    set (CXX11_ABI_FLAGS "-D_GLIBCXX_USE_CXX11_ABI=1")
elseif (CXX11_ABI STREQUAL DISABLE)
    set (CXX11_ABI_FLAGS "-D_GLIBCXX_USE_CXX11_ABI=0")
else ()
    set (CXX11_ABI_FLAGS "")
endif ()

set (COMPILER_FLAGS "${COMPILER_FLAGS} ${CXX11_ABI_FLAGS}")

string(REGEX MATCH "-?[0-9]+(.[0-9]+)?$" COMPILER_POSTFIX ${CMAKE_CXX_COMPILER})

find_program (LLD_PATH NAMES "lld${COMPILER_POSTFIX}" "lld")
find_program (GOLD_PATH NAMES "gold")

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND LLD_PATH AND NOT LINKER_NAME)
    set (LINKER_NAME "lld")
elseif (GOLD_PATH)
    set (LINKER_NAME "gold")
endif ()

if (LINKER_NAME)
    message(STATUS "Using linker: ${LINKER_NAME} (selected from: LLD_PATH=${LLD_PATH}; GOLD_PATH=${GOLD_PATH}; COMPILER_POSTFIX=${COMPILER_POSTFIX})")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=${LINKER_NAME}")
endif ()

option (PIPE "-pipe compiler option [less /tmp usage, more ram usage]" ON)
if (PIPE)
    set (COMPILER_FLAGS                  "${COMPILER_FLAGS} -pipe")
endif ()

include (cmake/test_cpu.cmake)

option (ARCHNATIVE "Enable -march=native compiler flag" OFF)
if (ARCHNATIVE)
    set (COMPILER_FLAGS                  "${COMPILER_FLAGS} -march=native")
endif ()

# Special options for better optimized code with clang
#if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
#    set (CMAKE_CXX_FLAGS_RELWITHDEBINFO  "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -Wno-unused-command-line-argument -mllvm -inline-threshold=10000")
#endif ()

if (CMAKE_VERSION VERSION_LESS "3.8.0")
    if (NOT MSVC)
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z")
    endif ()
else ()
    set (CMAKE_CXX_STANDARD 17)
    set (CMAKE_CXX_EXTENSIONS 0) # https://cmake.org/cmake/help/latest/prop_tgt/CXX_EXTENSIONS.html#prop_tgt:CXX_EXTENSIONS
    set (CMAKE_CXX_STANDARD_REQUIRED ON)
    set (CXX_FLAGS_INTERNAL_COMPILER "-std=c++1z")
endif ()

set (CMAKE_BUILD_COLOR_MAKEFILE          ON)
set (CMAKE_CXX_FLAGS                     "${CMAKE_CXX_FLAGS} ${COMPILER_FLAGS} ${PLATFORM_EXTRA_CXX_FLAG} -fno-omit-frame-pointer ${COMMON_WARNING_FLAGS} ${CXX_WARNING_FLAGS} ${GLIBC_COMPATIBILITY_COMPILE_FLAGS}")
#set (CMAKE_CXX_FLAGS_RELEASE             "${CMAKE_CXX_FLAGS_RELEASE} ${CMAKE_CXX_FLAGS_ADD}")
set (CMAKE_CXX_FLAGS_RELWITHDEBINFO      "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O3 ${CMAKE_CXX_FLAGS_ADD}")
set (CMAKE_CXX_FLAGS_DEBUG               "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g3 -ggdb3 -fno-inline ${CMAKE_CXX_FLAGS_ADD}")

set (CMAKE_C_FLAGS                       "${CMAKE_C_FLAGS} ${COMPILER_FLAGS} -fno-omit-frame-pointer ${COMMON_WARNING_FLAGS} ${GLIBC_COMPATIBILITY_COMPILE_FLAGS} ${CMAKE_C_FLAGS_ADD}")
#set (CMAKE_C_FLAGS_RELEASE               "${CMAKE_C_FLAGS_RELEASE} ${CMAKE_C_FLAGS_ADD}")
set (CMAKE_C_FLAGS_RELWITHDEBINFO        "${CMAKE_C_FLAGS_RELWITHDEBINFO} -O3 ${CMAKE_C_FLAGS_ADD}")
set (CMAKE_C_FLAGS_DEBUG                 "${CMAKE_C_FLAGS_DEBUG} -O0 -g3 -ggdb3 -fno-inline ${CMAKE_C_FLAGS_ADD}")

if (MAKE_STATIC_LIBRARIES AND NOT APPLE AND NOT (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND ARCH_FREEBSD))
    set (CMAKE_EXE_LINKER_FLAGS          "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++")
endif ()

set(THREADS_PREFER_PTHREAD_FLAG ON)

include (cmake/test_compiler.cmake)

if (ARCH_LINUX AND CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set (CMAKE_EXE_LINKER_FLAGS          "${CMAKE_EXE_LINKER_FLAGS} ${GLIBC_COMPATIBILITY_LINK_FLAGS} ${CXX11_ABI_FLAGS}")

    option (USE_LIBCXX "Use libc++ and libc++abi instead of libstdc++ (only make sense on Linux with Clang)" ${HAVE_LIBCXX})
    set (LIBCXX_PATH "" CACHE STRING "Use custom path for libc++. It should be used for MSan.")

    if (USE_LIBCXX)
        set (CMAKE_CXX_FLAGS             "${CMAKE_CXX_FLAGS} -stdlib=libc++") # Ok for clang6, for older can cause 'not used option' warning
        set (CMAKE_CXX_FLAGS_DEBUG       "${CMAKE_CXX_FLAGS_DEBUG} -D_LIBCPP_DEBUG=0") # More checks in debug build.
        if (MAKE_STATIC_LIBRARIES)
            link_libraries (-Wl,-Bstatic -stdlib=libc++ c++ c++abi -Wl,-Bdynamic)
        else ()
            link_libraries (-stdlib=libc++ c++ c++abi)
        endif ()

        if (LIBCXX_PATH)
#            include_directories (BEFORE SYSTEM "${LIBCXX_PATH}/include" "${LIBCXX_PATH}/include/c++/v1")
            link_directories ("${LIBCXX_PATH}/lib")
        endif ()
    endif ()
endif ()

if (USE_STATIC_LIBRARIES AND HAVE_NO_PIE)
    set (CMAKE_CXX_FLAGS                 "${CMAKE_CXX_FLAGS} ${FLAG_NO_PIE}")
    set (CMAKE_C_FLAGS                   "${CMAKE_C_FLAGS} ${FLAG_NO_PIE}")
endif ()

if (NOT MAKE_STATIC_LIBRARIES)
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endif ()

include (cmake/sanitize.cmake)

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

# Flags for test coverage
if (TEST_COVERAGE)
    set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fprofile-arcs -ftest-coverage -DIS_DEBUG")
endif (TEST_COVERAGE)

if (ENABLE_TESTS)
    message (STATUS "Tests are enabled")
    enable_testing()
endif ()

# when installing to /usr - place configs to /etc but for /usr/local place to /usr/local/etc
if (CMAKE_INSTALL_PREFIX STREQUAL "/usr")
    set (CLICKHOUSE_ETC_DIR "/etc")
else ()
    set (CLICKHOUSE_ETC_DIR "${CMAKE_INSTALL_PREFIX}/etc")
endif ()

option (UNBUNDLED "Try find all libraries in system (if fail - use bundled from contrib/)" OFF)
if (UNBUNDLED)
    set(NOT_UNBUNDLED 0)
else ()
    set(NOT_UNBUNDLED 1)
endif ()
# Using system libs can cause lot of warnings in includes.
if (UNBUNDLED OR NOT (ARCH_LINUX OR APPLE) OR ARCH_32)
    option (NO_WERROR "Disable -Werror compiler option" ON)
endif ()

message (STATUS "Building for: ${CMAKE_SYSTEM} ${CMAKE_SYSTEM_PROCESSOR} ${CMAKE_LIBRARY_ARCHITECTURE} ; USE_STATIC_LIBRARIES=${USE_STATIC_LIBRARIES} MAKE_STATIC_LIBRARIES=${MAKE_STATIC_LIBRARIES} UNBUNDLED=${UNBUNDLED} CCACHE=${CCACHE_FOUND} ${CCACHE_VERSION}")

include(GNUInstallDirs)

include (cmake/find_ssl.cmake)
if (NOT OPENSSL_FOUND)
    message (FATAL_ERROR "Need openssl for build. debian tip: sudo apt install libssl-dev")
endif ()

include (cmake/lib_name.cmake)
include (cmake/find_icu4c.cmake)
include (cmake/find_boost.cmake)
# openssl, zlib before poco
include (cmake/find_zlib.cmake)
include (cmake/find_zstd.cmake)
include (cmake/find_ltdl.cmake) # for odbc
include (cmake/find_termcap.cmake)
if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/contrib/poco/cmake/FindODBC.cmake)
    include (${CMAKE_CURRENT_SOURCE_DIR}/contrib/poco/cmake/FindODBC.cmake) # for poco
else ()
    include (cmake/find_odbc.cmake)
endif ()
message (STATUS "Using odbc: ${ODBC_INCLUDE_DIRECTORIES} : ${ODBC_LIBRARIES}")
include (cmake/find_poco.cmake)
include (cmake/find_lz4.cmake)
include (cmake/find_sparsehash.cmake)
include (cmake/find_rt.cmake)
include (cmake/find_execinfo.cmake)
include (cmake/find_readline_edit.cmake)
include (cmake/find_re2.cmake)
include (cmake/find_rdkafka.cmake)
include (cmake/find_capnp.cmake)
include (cmake/find_llvm.cmake)
include (cmake/find_cpuid.cmake)

include (cmake/find_contrib_lib.cmake)
find_contrib_lib(cityhash)
find_contrib_lib(farmhash)
find_contrib_lib(metrohash)
find_contrib_lib(btrie)
find_contrib_lib(double-conversion)

# Need to process before "contrib" dir:
include (libs/libcommon/cmake/find_gperftools.cmake)
include (libs/libcommon/cmake/find_jemalloc.cmake)
include (libs/libcommon/cmake/find_cctz.cmake)
include (libs/libmysqlxx/cmake/find_mysqlclient.cmake)
include (libs/libdaemon/cmake/find_unwind.cmake)

include (cmake/print_flags.cmake)

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
