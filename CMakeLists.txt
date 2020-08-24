cmake_minimum_required(VERSION 3.3)

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

# set default policy
foreach(default_policy_var_name
        # make option() honor normal variables for BUILD_SHARED_LIBS:
        # - re2
        # - snappy
        CMAKE_POLICY_DEFAULT_CMP0077
        # Google Test from sources uses too old cmake, 2.6.x, and CMP0022 should
        # set, to avoid using deprecated LINK_INTERFACE_LIBRARIES(_<CONFIG>)? over
        # INTERFACE_LINK_LIBRARIES.
        CMAKE_POLICY_DEFAULT_CMP0022
    )
    set(${default_policy_var_name} NEW)
endforeach()

project(ClickHouse)

include (cmake/arch.cmake)
include (cmake/target.cmake)
include (cmake/tools.cmake)
include (cmake/analysis.cmake)

# Ignore export() since we don't use it,
# but it gets broken with a global targets via link_libraries()
macro (export)
endmacro ()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/")
set(CMAKE_EXPORT_COMPILE_COMMANDS 1) # Write compile_commands.json
set(CMAKE_LINK_DEPENDS_NO_SHARED 1) # Do not relink all depended targets on .so
set(CMAKE_CONFIGURATION_TYPES "RelWithDebInfo;Debug;Release;MinSizeRel" CACHE STRING "" FORCE)
set(CMAKE_DEBUG_POSTFIX "d" CACHE STRING "Generate debug library name with a postfix.")    # To be consistent with CMakeLists from contrib libs.

# Enable the ability to organize targets into hierarchies of "folders" for capable GUI-based IDEs.
# For more info see https://cmake.org/cmake/help/latest/prop_gbl/USE_FOLDERS.html
set_property(GLOBAL PROPERTY USE_FOLDERS ON)

option(ENABLE_IPO "Enable full link time optimization (it's usually impractical; see also ENABLE_THINLTO)" OFF) # need cmake 3.9+
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

# Check that submodules are present only if source was downloaded with git
if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.git" AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/boost/boost")
    message (FATAL_ERROR "Submodules are not initialized. Run\n\tgit submodule update --init --recursive")
endif ()

include (cmake/find/ccache.cmake)

if (NOT CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL "None")
    set (CMAKE_BUILD_TYPE "RelWithDebInfo")
    message (STATUS "CMAKE_BUILD_TYPE is not set, set to default = ${CMAKE_BUILD_TYPE}")
endif ()
message (STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

string (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_UC)

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

option (ENABLE_FUZZING "Enables fuzzing instrumentation" OFF)

if (ENABLE_FUZZING)
    message (STATUS "Fuzzing instrumentation enabled")
    set (WITH_COVERAGE ON)
    set (FUZZER "libfuzzer")
endif()

include (cmake/fuzzer.cmake)
include (cmake/sanitize.cmake)

if (CMAKE_GENERATOR STREQUAL "Ninja" AND NOT DISABLE_COLORED_BUILD)
    # Turn on colored output. https://github.com/ninja-build/ninja/wiki/FAQ
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-color=always")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fdiagnostics-color=always")
endif ()

include (cmake/add_warning.cmake)

if (NOT MSVC)
    set (COMMON_WARNING_FLAGS "${COMMON_WARNING_FLAGS} -Wall")    # -Werror and many more is also added inside cmake/warnings.cmake
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

if (OS_LINUX AND NOT UNBUNDLED AND MAKE_STATIC_LIBRARIES AND NOT SPLIT_SHARED_LIBRARIES AND CMAKE_VERSION VERSION_GREATER "3.9.0")
    option (GLIBC_COMPATIBILITY "Set to TRUE to enable compatibility with older glibc libraries. Only for x86_64, Linux. Implies ENABLE_FASTMEMCPY." ON)
endif ()

if (NOT CMAKE_VERSION VERSION_GREATER "3.9.0")
    message (WARNING "CMake version must be greater than 3.9.0 for production builds.")
endif ()

# Make sure the final executable has symbols exported
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -rdynamic")

option (ADD_GDB_INDEX_FOR_GOLD "Set to add .gdb-index to resulting binaries for gold linker. NOOP if lld is used." 0)
if (NOT CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE")
    if (LINKER_NAME STREQUAL "lld")
        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--gdb-index")
        set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--gdb-index")
        message (STATUS "Adding .gdb-index via --gdb-index linker option.")
    # we use another tool for gdb-index, because gold linker removes section .debug_aranges, which used inside clickhouse stacktraces
    # http://sourceware-org.1504.n7.nabble.com/gold-No-debug-aranges-section-when-linking-with-gdb-index-td540965.html#a556932
    elseif (LINKER_NAME STREQUAL "gold" AND ADD_GDB_INDEX_FOR_GOLD)
        find_program (GDB_ADD_INDEX_EXE NAMES "gdb-add-index" DOC "Path to gdb-add-index executable")
        if (NOT GDB_ADD_INDEX_EXE)
            set (USE_GDB_ADD_INDEX 0)
            message (WARNING "Cannot add gdb index to binaries, because gold linker is used, but gdb-add-index executable not found.")
        else()
            set (USE_GDB_ADD_INDEX 1)
            message (STATUS "gdb-add-index found: ${GDB_ADD_INDEX_EXE}")
        endif()
    endif ()
endif()

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
    include(cmake/cpu_features.cmake)
endif()

if(NOT COMPILER_CLANG) # clang: error: the clang compiler does not support '-march=native'
    option(ARCH_NATIVE "Enable -march=native compiler flag" ${ARCH_ARM})
endif()

if (ARCH_NATIVE)
    set (COMPILER_FLAGS "${COMPILER_FLAGS} -march=native")
endif ()

# cmake < 3.12 doesn't supoprt 20. We'll set CMAKE_CXX_FLAGS for now
# set (CMAKE_CXX_STANDARD 20)
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++2a")
set (CMAKE_CXX_EXTENSIONS 0) # https://cmake.org/cmake/help/latest/prop_tgt/CXX_EXTENSIONS.html#prop_tgt:CXX_EXTENSIONS
set (CMAKE_CXX_STANDARD_REQUIRED ON)

if (COMPILER_GCC OR COMPILER_CLANG)
    # Enable C++14 sized global deallocation functions. It should be enabled by setting -std=c++14 but I'm not sure.
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsized-deallocation")
endif ()

option(WITH_COVERAGE "Build with coverage." 0)

if (WITH_COVERAGE AND COMPILER_CLANG)
    set(COMPILER_FLAGS "${COMPILER_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
    # If we want to disable coverage for specific translation units
    set(WITHOUT_COVERAGE "-fno-profile-instr-generate -fno-coverage-mapping")
endif()

if (WITH_COVERAGE AND COMPILER_GCC)
    set(COMPILER_FLAGS "${COMPILER_FLAGS} -fprofile-arcs -ftest-coverage")
    set(COVERAGE_OPTION "-lgcov")
    set(WITHOUT_COVERAGE "-fno-profile-arcs -fno-test-coverage")
endif()

set (CMAKE_BUILD_COLOR_MAKEFILE          ON)
set (CMAKE_CXX_FLAGS                     "${CMAKE_CXX_FLAGS} ${COMPILER_FLAGS} ${PLATFORM_EXTRA_CXX_FLAG} ${COMMON_WARNING_FLAGS} ${CXX_WARNING_FLAGS}")
set (CMAKE_CXX_FLAGS_RELWITHDEBINFO      "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O3 ${CMAKE_CXX_FLAGS_ADD}")
set (CMAKE_CXX_FLAGS_DEBUG               "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g3 -ggdb3 -fno-inline ${CMAKE_CXX_FLAGS_ADD}")

set (CMAKE_C_FLAGS                       "${CMAKE_C_FLAGS} ${COMPILER_FLAGS} ${COMMON_WARNING_FLAGS} ${CMAKE_C_FLAGS_ADD}")
set (CMAKE_C_FLAGS_RELWITHDEBINFO        "${CMAKE_C_FLAGS_RELWITHDEBINFO} -O3 ${CMAKE_C_FLAGS_ADD}")
set (CMAKE_C_FLAGS_DEBUG                 "${CMAKE_C_FLAGS_DEBUG} -O0 -g3 -ggdb3 -fno-inline ${CMAKE_C_FLAGS_ADD}")

if (COMPILER_CLANG)
    if (OS_DARWIN)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-U,_inside_main")
    endif()

    # Display absolute paths in error messages. Otherwise KDevelop fails to navigate to correct file and opens a new file instead.
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-absolute-paths")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fdiagnostics-absolute-paths")

    option(ENABLE_THINLTO "Enable Thin LTO. Only applicable for clang. It's also suppressed when building with tests or sanitizers." ON)

    # We cannot afford to use LTO when compiling unitests, and it's not enough
    # to only supply -fno-lto at the final linking stage. So we disable it
    # completely.
    if (ENABLE_THINLTO AND NOT ENABLE_TESTS AND NOT SANITIZE)
        # Link time optimization
        set (CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO} -flto=thin")
        set (CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -flto=thin")
        set (CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO "${CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO} -flto=thin")
    endif ()

    # Always prefer llvm tools when using clang. For instance, we cannot use GNU ar when llvm LTO is enabled

    find_program (LLVM_AR_PATH NAMES "llvm-ar" "llvm-ar-10" "llvm-ar-9" "llvm-ar-8")
    if (LLVM_AR_PATH)
        message(STATUS "Using llvm-ar: ${LLVM_AR_PATH}.")
        set (CMAKE_AR ${LLVM_AR_PATH})
    else ()
        message(WARNING "Cannot find llvm-ar. System ar will be used instead. It does not work with ThinLTO.")
    endif ()

    find_program (LLVM_RANLIB_PATH NAMES "llvm-ranlib" "llvm-ranlib-10" "llvm-ranlib-9" "llvm-ranlib-8")
    if (LLVM_RANLIB_PATH)
        message(STATUS "Using llvm-ranlib: ${LLVM_RANLIB_PATH}.")
        set (CMAKE_RANLIB ${LLVM_RANLIB_PATH})
    else ()
        message(WARNING "Cannot find llvm-ranlib. System ranlib will be used instead. It does not work with ThinLTO.")
    endif ()
endif ()

option (ENABLE_LIBRARIES "Enable all libraries (Global default switch)" ON)

option (UNBUNDLED "Try find all libraries in system. We recommend to avoid this mode for production builds, because we cannot guarantee exact versions and variants of libraries your system has installed. This mode exists for enthusiastic developers who search for trouble. Also it is useful for maintainers of OS packages." OFF)
if (UNBUNDLED)
    set(NOT_UNBUNDLED 0)
else ()
    set(NOT_UNBUNDLED 1)
endif ()

if (UNBUNDLED OR NOT (OS_LINUX OR OS_DARWIN))
    # Using system libs can cause a lot of warnings in includes (on macro expansion).
    option (WERROR "Enable -Werror compiler option" OFF)
else ()
    option (WERROR "Enable -Werror compiler option" ON)
endif ()

if (WERROR)
    add_warning(error)
endif ()

# Make this extra-checks for correct library dependencies.
if (OS_LINUX AND NOT SANITIZE)
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-undefined")
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--no-undefined")
endif ()

include(cmake/dbms_glob_sources.cmake)

if (OS_LINUX OR OS_ANDROID)
    include(cmake/linux/default_libs.cmake)
elseif (OS_DARWIN)
    include(cmake/darwin/default_libs.cmake)
elseif (OS_FREEBSD)
    include(cmake/freebsd/default_libs.cmake)
endif ()

######################################
### Add targets below this comment ###
######################################

set (CMAKE_POSTFIX_VARIABLE "CMAKE_${CMAKE_BUILD_TYPE_UC}_POSTFIX")

if (MAKE_STATIC_LIBRARIES)
    set (CMAKE_POSITION_INDEPENDENT_CODE OFF)
    if (OS_LINUX AND NOT ARCH_ARM)
        # Slightly more efficient code can be generated
        # It's disabled for ARM because otherwise ClickHouse cannot run on Android.
        set (CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fno-pie")
        set (CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO} -fno-pie")
        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-no-pie")
    endif ()
else ()
    set (CMAKE_POSITION_INDEPENDENT_CODE ON)
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

message (STATUS "Building for: ${CMAKE_SYSTEM} ${CMAKE_SYSTEM_PROCESSOR} ${CMAKE_LIBRARY_ARCHITECTURE} ; USE_STATIC_LIBRARIES=${USE_STATIC_LIBRARIES} MAKE_STATIC_LIBRARIES=${MAKE_STATIC_LIBRARIES} SPLIT_SHARED=${SPLIT_SHARED_LIBRARIES} UNBUNDLED=${UNBUNDLED} CCACHE=${CCACHE_FOUND} ${CCACHE_VERSION}")

include (GNUInstallDirs)
include (cmake/contrib_finder.cmake)

find_contrib_lib(double-conversion) # Must be before parquet
include (cmake/find/ssl.cmake)
include (cmake/find/ldap.cmake) # after ssl
include (cmake/find/icu.cmake)
include (cmake/find/zlib.cmake)
include (cmake/find/zstd.cmake)
include (cmake/find/ltdl.cmake) # for odbc
include (cmake/find/termcap.cmake)
# openssl, zlib before poco
include (cmake/find/sparsehash.cmake)
include (cmake/find/re2.cmake)
include (cmake/find/libgsasl.cmake)
include (cmake/find/rdkafka.cmake)
include (cmake/find/amqpcpp.cmake)
include (cmake/find/capnp.cmake)
include (cmake/find/llvm.cmake)
include (cmake/find/opencl.cmake)
include (cmake/find/h3.cmake)
include (cmake/find/libxml2.cmake)
include (cmake/find/brotli.cmake)
include (cmake/find/protobuf.cmake)
include (cmake/find/grpc.cmake)
include (cmake/find/pdqsort.cmake)
include (cmake/find/hdfs3.cmake) # uses protobuf
include (cmake/find/s3.cmake)
include (cmake/find/base64.cmake)
include (cmake/find/parquet.cmake)
include (cmake/find/simdjson.cmake)
include (cmake/find/rapidjson.cmake)
include (cmake/find/fastops.cmake)
include (cmake/find/orc.cmake)
include (cmake/find/avro.cmake)
include (cmake/find/msgpack.cmake)
include (cmake/find/cassandra.cmake)
include (cmake/find/sentry.cmake)
include (cmake/find/stats.cmake)

find_contrib_lib(cityhash)
find_contrib_lib(farmhash)
find_contrib_lib(btrie)

if (ENABLE_TESTS)
    include (cmake/find/gtest.cmake)
endif ()

# Need to process before "contrib" dir:
include (cmake/find/mysqlclient.cmake)

# When testing for memory leaks with Valgrind, don't link tcmalloc or jemalloc.

if (USE_OPENCL)
    if (OS_DARWIN)
        set(OPENCL_LINKER_FLAGS "-framework OpenCL")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OPENCL_LINKER_FLAGS}")
    endif ()
endif ()

include (cmake/print_flags.cmake)

if (TARGET global-group)
    install (EXPORT global DESTINATION cmake)
endif ()

add_subdirectory (contrib EXCLUDE_FROM_ALL)

if (NOT ENABLE_JEMALLOC)
    message (WARNING "Non default allocator is disabled. This is not recommended for production builds.")
endif ()

macro (add_executable target)
    # invoke built-in add_executable
    # explicitly acquire and interpose malloc symbols by clickhouse_malloc
    _add_executable (${ARGV} $<TARGET_OBJECTS:clickhouse_malloc>)
    get_target_property (type ${target} TYPE)
    if (${type} STREQUAL EXECUTABLE)
        # operator::new/delete for executables (MemoryTracker stuff)
        target_link_libraries (${target} PRIVATE clickhouse_new_delete ${MALLOC_LIBRARIES})
    endif()
endmacro()

set(ConfigIncludePath ${CMAKE_CURRENT_BINARY_DIR}/includes/configs CACHE INTERNAL "Path to generated configuration files.")
include_directories(${ConfigIncludePath})

# Add as many warnings as possible for our own code.
include (cmake/warnings.cmake)

add_subdirectory (base)
add_subdirectory (programs)
add_subdirectory (src)
add_subdirectory (tests)
add_subdirectory (utils)

include (cmake/print_include_directories.cmake)

include (cmake/sanitize_target_link_libraries.cmake)
