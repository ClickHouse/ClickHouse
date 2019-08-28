project(ClickHouse)
cmake_minimum_required(VERSION 3.3)

foreach(policy
    CMP0023
    CMP0074 # CMake 3.12
    )
  if(POLICY ${policy})
    cmake_policy(SET ${policy} NEW)
  endif()
endforeach()

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

if (GLIBC_COMPATIBILITY)
    set (USE_INTERNAL_MEMCPY ON)
else ()
    message (WARNING "Option GLIBC_COMPATIBILITY must be turned on for production builds.")
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

# Special options for better optimized code with clang
#if (COMPILER_CLANG)
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
#set (CMAKE_CXX_FLAGS_RELEASE             "${CMAKE_CXX_FLAGS_RELEASE} ${CMAKE_CXX_FLAGS_ADD}")
set (CMAKE_CXX_FLAGS_RELWITHDEBINFO      "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O3 ${CMAKE_CXX_FLAGS_ADD}")
set (CMAKE_CXX_FLAGS_DEBUG               "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g3 -ggdb3 -fno-inline ${CMAKE_CXX_FLAGS_ADD}")

set (CMAKE_C_FLAGS                       "${CMAKE_C_FLAGS} ${COMPILER_FLAGS} -fno-omit-frame-pointer ${COMMON_WARNING_FLAGS} ${CMAKE_C_FLAGS_ADD}")
#set (CMAKE_C_FLAGS_RELEASE               "${CMAKE_C_FLAGS_RELEASE} ${CMAKE_C_FLAGS_ADD}")
set (CMAKE_C_FLAGS_RELWITHDEBINFO        "${CMAKE_C_FLAGS_RELWITHDEBINFO} -O3 ${CMAKE_C_FLAGS_ADD}")
set (CMAKE_C_FLAGS_DEBUG                 "${CMAKE_C_FLAGS_DEBUG} -O0 -g3 -ggdb3 -fno-inline ${CMAKE_C_FLAGS_ADD}")

# Uses MAKE_STATIC_LIBRARIES


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


set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package (Threads)

include (cmake/find_cxx.cmake)

include (cmake/test_compiler.cmake)

if (OS_LINUX AND COMPILER_CLANG AND USE_STATIC_LIBRARIES)
    option (USE_LIBCXX "Use libc++ and libc++abi instead of libstdc++ (only make sense on Linux)" ${HAVE_LIBCXX})

    if (USE_LIBCXX)
        set (CMAKE_CXX_FLAGS_DEBUG       "${CMAKE_CXX_FLAGS_DEBUG} -D_LIBCPP_DEBUG=0") # More checks in debug build.
    endif ()
endif ()

if (USE_LIBCXX)
    set (STATIC_STDLIB_FLAGS "")
else ()
    set (STATIC_STDLIB_FLAGS "-static-libgcc -static-libstdc++")
endif ()

if (MAKE_STATIC_LIBRARIES AND NOT APPLE AND NOT (COMPILER_CLANG AND OS_FREEBSD))
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${STATIC_STDLIB_FLAGS}")

    # Along with executables, we also build example of shared library for "library dictionary source"; and it also should be self-contained.
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${STATIC_STDLIB_FLAGS}")
endif ()

if (USE_STATIC_LIBRARIES AND HAVE_NO_PIE)
    set (CMAKE_CXX_FLAGS                 "${CMAKE_CXX_FLAGS} ${FLAG_NO_PIE}")
    set (CMAKE_C_FLAGS                   "${CMAKE_C_FLAGS} ${FLAG_NO_PIE}")
endif ()

# Make this extra-checks for correct library dependencies.
if (NOT SANITIZE)
    set (CMAKE_EXE_LINKER_FLAGS    "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-undefined")
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--no-undefined")
endif ()

include (cmake/find_unwind.cmake)

if (USE_INTERNAL_UNWIND_LIBRARY)
    option (USE_INTERNAL_UNWIND_LIBRARY_FOR_EXCEPTION_HANDLING "Use internal unwind library for exception handling" ${USE_STATIC_LIBRARIES})
endif ()


# Set standard, system and compiler libraries explicitly.
# This is intended for more control of what we are linking.

string (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_UC)
set (CMAKE_POSTFIX_VARIABLE "CMAKE_${CMAKE_BUILD_TYPE_UC}_POSTFIX")

set (DEFAULT_LIBS "")
if (OS_LINUX AND NOT UNBUNDLED AND (GLIBC_COMPATIBILITY OR USE_INTERNAL_UNWIND_LIBRARY_FOR_EXCEPTION_HANDLING OR USE_LIBCXX))
    # Note: this probably has no effect, but I'm not an expert in CMake.
    set (CMAKE_C_IMPLICIT_LINK_LIBRARIES "")
    set (CMAKE_CXX_IMPLICIT_LINK_LIBRARIES "")

    # Disable default linked libraries.
    set (DEFAULT_LIBS "-nodefaultlibs")

    # We need builtins from Clang's RT even without libcxx - for ubsan+int128. See https://bugs.llvm.org/show_bug.cgi?id=16404
    set (BUILTINS_LIB_PATH "")
    if (COMPILER_CLANG)
        execute_process (COMMAND ${CMAKE_CXX_COMPILER} --print-file-name=libclang_rt.builtins-${CMAKE_SYSTEM_PROCESSOR}.a OUTPUT_VARIABLE BUILTINS_LIB_PATH OUTPUT_STRIP_TRAILING_WHITESPACE)
    else ()
        set (BUILTINS_LIB_PATH "-lgcc")
    endif ()

    string (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_UC)
    set (CMAKE_POSTFIX_VARIABLE "CMAKE_${CMAKE_BUILD_TYPE_UC}_POSTFIX")

    # Add C++ libraries.
    #
    # This consist of:
    # - C++ standard library (like implementation of std::string);
    # - C++ ABI implementation (functions for exceptions like __cxa_throw, RTTI, etc);
    # - functions for internal implementation of exception handling (stack unwinding based on DWARF info; TODO replace with bundled libunwind);
    # - compiler builtins (example: functions for implementation of __int128 operations);
    #
    # There are two variants of C++ library: libc++ (from LLVM compiler infrastructure) and libstdc++ (from GCC).

    if (USE_INTERNAL_UNWIND_LIBRARY_FOR_EXCEPTION_HANDLING)
        if (USE_STATIC_LIBRARIES)
            set (EXCEPTION_HANDLING_LIBRARY "${ClickHouse_BINARY_DIR}/contrib/libunwind-cmake/libunwind_static${${CMAKE_POSTFIX_VARIABLE}}.a")
        else ()
            set (EXCEPTION_HANDLING_LIBRARY "${ClickHouse_BINARY_DIR}/contrib/libunwind-cmake/libunwind_shared${${CMAKE_POSTFIX_VARIABLE}}.so")
        endif ()
    else ()
        set (EXCEPTION_HANDLING_LIBRARY "-lgcc_eh")
    endif ()

    message (STATUS "Using exception handling library: ${EXCEPTION_HANDLING_LIBRARY}")

    if (USE_LIBCXX)
        if (USE_INTERNAL_LIBCXX_LIBRARY)
            set (LIBCXX_LIBS "${ClickHouse_BINARY_DIR}/contrib/libcxx-cmake/libcxx_static${${CMAKE_POSTFIX_VARIABLE}}.a ${ClickHouse_BINARY_DIR}/contrib/libcxxabi-cmake/libcxxabi_static${${CMAKE_POSTFIX_VARIABLE}}.a")
        else ()
            set (LIBCXX_LIBS "-lc++ -lc++abi -lc++fs")
        endif ()

        set (DEFAULT_LIBS "${DEFAULT_LIBS} -Wl,-Bstatic ${LIBCXX_LIBS} ${EXCEPTION_HANDLING_LIBRARY} ${BUILTINS_LIB_PATH} -Wl,-Bdynamic")
    else ()
        set (DEFAULT_LIBS "${DEFAULT_LIBS} -Wl,-Bstatic -lstdc++ -lstdc++fs ${EXCEPTION_HANDLING_LIBRARY} ${COVERAGE_OPTION} ${BUILTINS_LIB_PATH} -Wl,-Bdynamic")
    endif ()

    # Linking with GLIBC prevents portability of binaries to older systems.
    # We overcome this behaviour by statically linking with our own implementation of all new symbols (that don't exist in older Libc or have infamous "symbol versioning").
    # The order of linking is important: 'glibc-compatibility' must be before libc but after all other libraries.
    if (GLIBC_COMPATIBILITY)
        message (STATUS "Some symbols from glibc will be replaced for compatibility")

        string (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_UC)
        set (CMAKE_POSTFIX_VARIABLE "CMAKE_${CMAKE_BUILD_TYPE_UC}_POSTFIX")

        # FIXME: glibc-compatibility may be non-static in some builds!
        set (DEFAULT_LIBS "${DEFAULT_LIBS} ${ClickHouse_BINARY_DIR}/libs/libglibc-compatibility/libglibc-compatibility${${CMAKE_POSTFIX_VARIABLE}}.a")
    endif ()

    # Add Libc. GLIBC is actually a collection of interdependent libraries.
    set (DEFAULT_LIBS "${DEFAULT_LIBS} -lrt -ldl -lpthread -lm -lc")

    # Note: we'd rather use Musl libc library, but it's little bit more difficult to use.

    message(STATUS "Default libraries: ${DEFAULT_LIBS}")
endif ()

if (NOT GLIBC_COMPATIBILITY)
    set (M_LIBRARY m)
endif ()

if (DEFAULT_LIBS)
    # Add default libs to all targets as the last dependency.
    set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
    set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})
endif ()

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

if (USE_LIBCXX)
    set (HAVE_LIBCXX 1)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
endif()

if (USE_LIBCXX AND USE_INTERNAL_LIBCXX_LIBRARY)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -nostdinc++ -isystem ${LIBCXX_INCLUDE_DIR} -isystem ${LIBCXXABI_INCLUDE_DIR}")
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
include (cmake/find_orc.cmake)

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

add_subdirectory (contrib EXCLUDE_FROM_ALL)
add_subdirectory (libs)
add_subdirectory (utils)
add_subdirectory (dbms)

include (cmake/print_include_directories.cmake)

if (GLIBC_COMPATIBILITY OR USE_INTERNAL_UNWIND_LIBRARY_FOR_EXCEPTION_HANDLING)
    # FIXME: actually glibc-compatibility should always be built first,
    #        because it's unconditionally linked via $DEFAULT_LIBS,
    #        and these looks like the first places that get linked.
    function (add_default_dependencies target_name)
        if (TARGET ${target_name})
            if (GLIBC_COMPATIBILITY)
                add_dependencies(${target_name} glibc-compatibility)
            endif ()

            if (USE_LIBCXX AND USE_INTERNAL_LIBCXX_LIBRARY)
                add_dependencies(${target_name} cxx_static cxxabi_static)
            endif ()

            if (USE_INTERNAL_UNWIND_LIBRARY_FOR_EXCEPTION_HANDLING)
                add_dependencies(${target_name} unwind_static)
            endif ()
        endif ()
    endfunction ()

    add_default_dependencies(ltdl)
    add_default_dependencies(zlibstatic)
    add_default_dependencies(jemalloc)
    add_default_dependencies(memcpy)
    add_default_dependencies(Foundation)
    add_default_dependencies(common)
    add_default_dependencies(gtest)
    add_default_dependencies(lz4)
    add_default_dependencies(zstd)
    add_default_dependencies(snappy)
    add_default_dependencies(arrow)
    add_default_dependencies(protoc)
    add_default_dependencies(thrift_static)
    add_default_dependencies(cityhash)
    add_default_dependencies(farmhash)
    add_default_dependencies(murmurhash)
    add_default_dependencies(metrohash)
    add_default_dependencies(metrohash128)
    add_default_dependencies(consistent-hashing)
    add_default_dependencies(double-conversion)
    add_default_dependencies(cctz)
    add_default_dependencies(kj)
    add_default_dependencies(simdjson)
    add_default_dependencies(apple_rt)
    add_default_dependencies(h3)
    add_default_dependencies(re2)
    add_default_dependencies(re2_st)
    add_default_dependencies(hs_compile_shared)
    add_default_dependencies(hs_exec_shared)
    add_default_dependencies(hs_shared)
    add_default_dependencies(widechar_width)
    add_default_dependencies(string_utils)
    add_default_dependencies(consistent-hashing-sumbur)
    add_default_dependencies(boost_program_options_internal)
    add_default_dependencies(boost_system_internal)
    add_default_dependencies(boost_regex_internal)
    add_default_dependencies(roaring)
    add_default_dependencies(btrie)
    add_default_dependencies(cpuid)
    add_default_dependencies(mysqlclient)
    add_default_dependencies(zlib)
    add_default_dependencies(thrift)
    add_default_dependencies(brotli)
    add_default_dependencies(libprotobuf)
    add_default_dependencies(base64)
    add_default_dependencies(readpassphrase)
    add_default_dependencies(unwind_static)
    add_default_dependencies(fastops)
endif ()
