cmake_minimum_required(VERSION 3.20)

project(ClickHouse LANGUAGES C CXX ASM)

# If turned off: e.g. when ENABLE_FOO is ON, but FOO tool was not found, the CMake will continue.
option(FAIL_ON_UNSUPPORTED_OPTIONS_COMBINATION
   "Stop/Fail CMake configuration if some ENABLE_XXX option is defined (either ON or OFF)
   but is not possible to satisfy" ON)

if(FAIL_ON_UNSUPPORTED_OPTIONS_COMBINATION)
    set(RECONFIGURE_MESSAGE_LEVEL FATAL_ERROR)
else()
    set(RECONFIGURE_MESSAGE_LEVEL WARNING)
endif()

include (cmake/arch.cmake)
include (cmake/target.cmake)
include (cmake/tools.cmake)
include (cmake/ccache.cmake)
include (cmake/clang_tidy.cmake)
include (cmake/git.cmake)
include (cmake/utils.cmake)

# This is needed to set up the CMAKE_INSTALL_BINDIR variable.
include (GNUInstallDirs)

# Ignore export() since we don't use it,
# but it gets broken with global targets via link_libraries()
macro (export)
endmacro ()

set(CMAKE_EXPORT_COMPILE_COMMANDS 1) # Write compile_commands.json
set(CMAKE_LINK_DEPENDS_NO_SHARED 1) # Do not relink all depended targets on .so
set(CMAKE_CONFIGURATION_TYPES "RelWithDebInfo;Debug;Release;MinSizeRel" CACHE STRING "" FORCE)
set(CMAKE_DEBUG_POSTFIX "d" CACHE STRING "Generate debug library name with a postfix.")    # To be consistent with CMakeLists from contrib libs.

# Enable the ability to organize targets into hierarchies of "folders" for capable GUI-based IDEs.
# For more info see https://cmake.org/cmake/help/latest/prop_gbl/USE_FOLDERS.html
set_property(GLOBAL PROPERTY USE_FOLDERS ON)

# Check that submodules are present
if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/sysroot/README.md")
    message (FATAL_ERROR "Submodules are not initialized. Run\n\tgit submodule update --init")
endif ()

# Take care to add prlimit in command line before ccache, or else ccache thinks that
# prlimit is compiler, and clang++ is its input file, and refuses to work  with
# multiple inputs, e.g in ccache log:
# [2021-03-31T18:06:32.655327 36900] Command line: /usr/bin/ccache prlimit --as=10000000000 --data=5000000000 --cpu=600 /usr/bin/clang++-11 - ...... std=gnu++2a -MD -MT src/CMakeFiles/dbms.dir/Storages/MergeTree/IMergeTreeDataPart.cpp.o -MF src/CMakeFiles/dbms.dir/Storages/MergeTree/IMergeTreeDataPart.cpp.o.d -o src/CMakeFiles/dbms.dir/Storages/MergeTree/IMergeTreeDataPart.cpp.o -c ../src/Storages/MergeTree/IMergeTreeDataPart.cpp
#
# [2021-03-31T18:06:32.656704 36900] Multiple input files: /usr/bin/clang++-11 and ../src/Storages/MergeTree/IMergeTreeDataPart.cpp
#
# Another way would be to use --ccache-skip option before clang++-11 to make
# ccache ignore it.
option(ENABLE_CHECK_HEAVY_BUILDS "Don't allow C++ translation units to compile too long or to take too much memory while compiling." OFF)
if (ENABLE_CHECK_HEAVY_BUILDS)
    # set DATA (since RSS does not work since 2.6.x+) to 5G
    set (RLIMIT_DATA 5000000000)
    # set VIRT (RLIMIT_AS) to 10G (DATA*2)
    set (RLIMIT_AS 10000000000)
    # set CPU time limit to 1000 seconds
    set (RLIMIT_CPU 1000)

    # Sanitizers are too heavy. Some architectures too.
    if (SANITIZE OR SANITIZE_COVERAGE OR WITH_COVERAGE OR ARCH_RISCV64 OR ARCH_LOONGARCH64)
        # Twice as large
        set (RLIMIT_DATA 10000000000)
        set (RLIMIT_AS 20000000000)
    endif()

    # For some files currently building RISCV64/LOONGARCH64 might be too slow.
    # TODO: Improve compilation times per file
    if (ARCH_RISCV64 OR ARCH_LOONGARCH64)
        set (RLIMIT_CPU 1800)
    endif()

    set (CMAKE_CXX_COMPILER_LAUNCHER prlimit --as=${RLIMIT_AS} --data=${RLIMIT_DATA} --cpu=${RLIMIT_CPU} ${CMAKE_CXX_COMPILER_LAUNCHER})
endif ()

if (NOT CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL "None")
    set (CMAKE_BUILD_TYPE "RelWithDebInfo")
    message (STATUS "CMAKE_BUILD_TYPE is not set, set to default = ${CMAKE_BUILD_TYPE}")
endif ()
message (STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

string (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_UC)

list(REVERSE CMAKE_FIND_LIBRARY_SUFFIXES)

option (ENABLE_FUZZING "Fuzzy testing using libfuzzer" OFF)
option (ENABLE_FUZZER_TEST "Build testing fuzzers in order to test libFuzzer functionality" OFF)

if (ENABLE_FUZZING)
    # Also set WITH_COVERAGE=1 for better fuzzing process
    # By default this is disabled, because fuzzers are built in CI with the clickhouse itself.
    # And we don't want to enable coverage for it.
    message (STATUS "Fuzzing instrumentation enabled")
    set (FUZZER "libfuzzer")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -nostdlib++")
    set (ENABLE_CLICKHOUSE_ODBC_BRIDGE OFF)
    set (ENABLE_LIBRARIES 0)
    set (ENABLE_SSL 1)
    set (ENABLE_EXAMPLES 0)
    set (ENABLE_UTILS 0)
    set (ENABLE_THINLTO 0)
    set (ENABLE_TCMALLOC 0)
    set (ENABLE_JEMALLOC 0)
    set (ENABLE_CHECK_HEAVY_BUILDS 1)
    set (GLIBC_COMPATIBILITY OFF)
    set (ENABLE_BENCHMARKS 0)

    # For codegen_select_fuzzer
    set (ENABLE_PROTOBUF 1)

    add_compile_definitions(FUZZING_MODE=1)
endif()

# Global libraries
# See:
# - default_libs.cmake
# - sanitize.cmake
add_library(global-libs INTERFACE)

include (cmake/sanitize.cmake)

include (cmake/xray_instrumentation.cmake)

option(ENABLE_COLORED_BUILD "Enable colors in compiler output" ON)

set (CMAKE_COLOR_MAKEFILE ${ENABLE_COLORED_BUILD}) # works only for the makefile generator

if (ENABLE_COLORED_BUILD AND CMAKE_GENERATOR STREQUAL "Ninja")
    # Turn on colored output. https://github.com/ninja-build/ninja/wiki/FAQ
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-color=always")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fdiagnostics-color=always")
    # ... such manually setting of flags can be removed once CMake supports a variable to
    # activate colors in *all* build systems: https://gitlab.kitware.com/cmake/cmake/-/issues/15502
    # --> available since CMake 3.24: https://stackoverflow.com/a/73349744
endif ()

include (cmake/check_flags.cmake)
include (cmake/add_warning.cmake)

# generate ranges for fast "addr2line" search
if (NOT CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE")
    # NOTE: that clang has a bug because of it does not emit .debug_aranges
    # with ThinLTO, so custom ld.lld wrapper is shipped in docker images.
    set(COMPILER_FLAGS "${COMPILER_FLAGS} -gdwarf-aranges")
endif ()

# See https://blog.llvm.org/posts/2021-04-05-constructor-homing-for-debug-info/
if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG" OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Xclang -fuse-ctor-homing")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Xclang -fuse-ctor-homing")
endif()

no_warning(enum-constexpr-conversion) # breaks Protobuf in clang-16

option(ENABLE_TESTS "Provide unit_test_dbms target with Google.Test unit tests" ON)
option(ENABLE_EXAMPLES "Build all example programs in 'examples' subdirectories" OFF)
option(ENABLE_BENCHMARKS "Build all benchmark programs in 'benchmarks' subdirectories" OFF)

if (OS_LINUX AND (ARCH_AMD64 OR ARCH_AARCH64) AND NOT USE_MUSL)
    # Only for Linux, x86_64 or aarch64.
    option(GLIBC_COMPATIBILITY "Enable compatibility with older glibc libraries." ON)
elseif(GLIBC_COMPATIBILITY)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Glibc compatibility cannot be enabled in current configuration")
endif ()

if (OS_LINUX)
    # We should not export dynamic symbols, because:
    # - The main clickhouse binary does not use dlopen,
    #   and whatever is poisoning it by LD_PRELOAD should not link to our symbols.
    # - The clickhouse-odbc-bridge and clickhouse-library-bridge binaries
    #   should not expose their symbols to ODBC drivers and libraries.
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-export-dynamic -Wl,--gc-sections")
endif ()

if (OS_DARWIN)
    # The `-all_load` flag forces loading of all symbols from all libraries,
    # and leads to multiply-defined symbols. This flag allows force loading
    # from a _specific_ library, which is what we need.
    set(WHOLE_ARCHIVE -force_load)
    # The `-noall_load` flag is the default and now obsolete.
    set(NO_WHOLE_ARCHIVE "-undefined,error") # Effectively, a no-op. Here to avoid empty "-Wl, " sequence to be generated in the command line.
else ()
    set(WHOLE_ARCHIVE --whole-archive)
    set(NO_WHOLE_ARCHIVE --no-whole-archive)
endif ()

if (NOT (SANITIZE_COVERAGE OR WITH_COVERAGE)
    AND (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE"
        OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO"
        OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL"))
    set (OMIT_HEAVY_DEBUG_SYMBOLS_DEFAULT ON)
else()
    set (OMIT_HEAVY_DEBUG_SYMBOLS_DEFAULT OFF)
endif()
# Provides faster linking and lower binary size.
# Tradeoff is the inability to debug some source files with e.g. gdb
# (empty stack frames and no local variables)."
option(OMIT_HEAVY_DEBUG_SYMBOLS
    "Do not generate debugger info for heavy modules (ClickHouse functions and dictionaries, some contrib)"
    ${OMIT_HEAVY_DEBUG_SYMBOLS_DEFAULT})

option(BUILD_STANDALONE_KEEPER "Build keeper as small standalone binary" OFF)
if (NOT BUILD_STANDALONE_KEEPER)
    option(CREATE_KEEPER_SYMLINK "Create symlink for clickhouse-keeper to main server binary" ON)
else ()
    option(CREATE_KEEPER_SYMLINK "Create symlink for clickhouse-keeper to main server binary" OFF)
endif ()

# Create BuildID when using lld. For other linkers it is created by default.
# (NOTE: LINKER_NAME can be either path or name, and in different variants)
if (LINKER_NAME MATCHES "lld")
    # SHA1 is not cryptographically secure but it is the best what lld is offering.
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--build-id=sha1")
endif ()

# Add a section with the hash of the compiled machine code for integrity checks.
# Only for official builds, because adding a section can be time consuming (rewrite of several GB).
# And cross compiled binaries are not supported (since you cannot execute clickhouse hash-binary)
if (CLICKHOUSE_OFFICIAL_BUILD AND (NOT CMAKE_TOOLCHAIN_FILE OR CMAKE_TOOLCHAIN_FILE MATCHES "linux/toolchain-x86_64.cmake$"))
    message(STATUS "Official build: A checksum hash will be added to the clickhouse executable")
    set (USE_BINARY_HASH 1 CACHE STRING "Calculate binary hash and store it in the separate section")
else ()
    message(STATUS "No official build: A checksum hash will not be added to the clickhouse executable")
endif ()

# Optionally split binaries and debug symbols.
option(SPLIT_DEBUG_SYMBOLS "Split binaries and debug symbols" OFF)
if (SPLIT_DEBUG_SYMBOLS)
    message(STATUS "Will split binaries and debug symbols")
    set(SPLITTED_DEBUG_SYMBOLS_DIR "stripped" CACHE STRING "A separate directory for stripped information")
endif()

cmake_host_system_information(RESULT AVAILABLE_PHYSICAL_MEMORY QUERY AVAILABLE_PHYSICAL_MEMORY) # Not available under freebsd


if(NOT AVAILABLE_PHYSICAL_MEMORY OR AVAILABLE_PHYSICAL_MEMORY GREATER 8000)
    # Less `/tmp` usage, more RAM usage.
    option(COMPILER_PIPE "-pipe compiler option" ON)
endif()

if(COMPILER_PIPE)
    set(COMPILER_FLAGS "${COMPILER_FLAGS} -pipe")
else()
    message(STATUS "Disabling compiler -pipe option (have only ${AVAILABLE_PHYSICAL_MEMORY} mb of memory)")
endif()

include(cmake/cpu_features.cmake)


# Query Profiler doesn't work on MacOS for several reasons
# - PHDR cache is not available
# - We use native functionality to get stacktraces which is not async signal safe
# and thus we don't need to generate asynchronous unwind tables
if (NOT OS_DARWIN)
    # Asynchronous unwind tables are needed for Query Profiler.
    # They are already by default on some platforms but possibly not on all platforms.
    # Enable it explicitly.
    set (COMPILER_FLAGS "${COMPILER_FLAGS} -fasynchronous-unwind-tables")
endif()

# Reproducible builds.
if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    set (ENABLE_BUILD_PATH_MAPPING_DEFAULT OFF)
else ()
    set (ENABLE_BUILD_PATH_MAPPING_DEFAULT ON)
endif ()

option (ENABLE_BUILD_PATH_MAPPING "Enable remapping of file source paths in debug info, predefined preprocessor macros, and __builtin_FILE(). It's used to generate reproducible builds. See https://reproducible-builds.org/docs/build-path" ${ENABLE_BUILD_PATH_MAPPING_DEFAULT})

if (ENABLE_BUILD_PATH_MAPPING)
    set (COMPILER_FLAGS "${COMPILER_FLAGS} -ffile-prefix-map=${PROJECT_SOURCE_DIR}=.")
    set (CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} -ffile-prefix-map=${PROJECT_SOURCE_DIR}=.")
endif ()

option (ENABLE_BUILD_PROFILING "Enable profiling of build time" OFF)
if (ENABLE_BUILD_PROFILING)
    set (COMPILER_FLAGS "${COMPILER_FLAGS} -ftime-trace")

    if (LINKER_NAME MATCHES "lld")
        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--time-trace")
        set (CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -Wl,--time-trace")
    endif ()
endif ()

set (CMAKE_CXX_STANDARD 23)
set (CMAKE_CXX_EXTENSIONS OFF)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

set (CMAKE_C_STANDARD 11)
set (CMAKE_C_EXTENSIONS ON) # required by most contribs written in C
set (CMAKE_C_STANDARD_REQUIRED ON)

# Enable C++14 sized global deallocation functions. It should be enabled by setting -std=c++14 but I'm not sure.
# See https://reviews.llvm.org/D112921
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsized-deallocation")

# falign-functions=32 prevents from random performance regressions with the code change. Thus, providing more stable
# benchmarks.
set(COMPILER_FLAGS "${COMPILER_FLAGS} -falign-functions=32")

if (ARCH_AMD64)
    # align branches within a 32-Byte boundary to avoid the potential performance loss when code layout change,
    # which makes benchmark results more stable.
    set(BRANCHES_WITHIN_32B_BOUNDARIES "-mbranches-within-32B-boundaries")
    set(COMPILER_FLAGS "${COMPILER_FLAGS} ${BRANCHES_WITHIN_32B_BOUNDARIES}")
endif()

# Disable floating-point expression contraction in order to get consistent floating point calculation results across platforms
set (COMPILER_FLAGS "${COMPILER_FLAGS} -ffp-contract=off")

set (DEBUG_INFO_FLAGS "-g")

# Disable omit frame pointer compiler optimization using -fno-omit-frame-pointer
option(DISABLE_OMIT_FRAME_POINTER "Disable omit frame pointer compiler optimization" OFF)

if (DISABLE_OMIT_FRAME_POINTER)
    set (CMAKE_CXX_FLAGS_ADD "${CMAKE_CXX_FLAGS_ADD} -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer")
    set (CMAKE_C_FLAGS_ADD "${CMAKE_C_FLAGS_ADD} -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer")
    set (CMAKE_ASM_FLAGS_ADD "${CMAKE_ASM_FLAGS_ADD} -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer")
endif()

# Before you start hating your debugger because it refuses to show variables ('<optimized out>'), try building with -DDEBUG_O_LEVEL="0"
# https://stackoverflow.com/questions/63386189/whats-the-difference-between-a-compilers-o0-option-and-og-option/63386263#63386263
set(DEBUG_O_LEVEL "g" CACHE STRING "The -Ox level used for debug builds")

set (CMAKE_CXX_FLAGS                     "${CMAKE_CXX_FLAGS} ${COMPILER_FLAGS} ${CMAKE_CXX_FLAGS_ADD}")
set (CMAKE_CXX_FLAGS_RELWITHDEBINFO      "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O3 ${DEBUG_INFO_FLAGS} ${CMAKE_CXX_FLAGS_ADD}")
set (CMAKE_CXX_FLAGS_DEBUG               "${CMAKE_CXX_FLAGS_DEBUG} -O${DEBUG_O_LEVEL} ${DEBUG_INFO_FLAGS} ${CMAKE_CXX_FLAGS_ADD}")

set (CMAKE_C_FLAGS                       "${CMAKE_C_FLAGS} ${COMPILER_FLAGS} ${CMAKE_C_FLAGS_ADD}")
set (CMAKE_C_FLAGS_RELWITHDEBINFO        "${CMAKE_C_FLAGS_RELWITHDEBINFO} -O3 ${DEBUG_INFO_FLAGS} ${CMAKE_C_FLAGS_ADD}")
set (CMAKE_C_FLAGS_DEBUG                 "${CMAKE_C_FLAGS_DEBUG} -O${DEBUG_O_LEVEL} ${DEBUG_INFO_FLAGS} ${CMAKE_C_FLAGS_ADD}")

set (CMAKE_ASM_FLAGS                     "${CMAKE_ASM_FLAGS} ${COMPILER_FLAGS} ${CMAKE_ASM_FLAGS_ADD}")
set (CMAKE_ASM_FLAGS_RELWITHDEBINFO      "${CMAKE_ASM_FLAGS_RELWITHDEBINFO} -O3 ${DEBUG_INFO_FLAGS} ${CMAKE_ASM_FLAGS_ADD}")
set (CMAKE_ASM_FLAGS_DEBUG               "${CMAKE_ASM_FLAGS_DEBUG} -O${DEBUG_O_LEVEL} ${DEBUG_INFO_FLAGS} ${CMAKE_ASM_FLAGS_ADD}")

if (OS_DARWIN)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-U,_inside_main")
endif()

# Display absolute paths in error messages. Otherwise KDevelop fails to navigate to correct file and opens a new file instead.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-absolute-paths")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fdiagnostics-absolute-paths")

if (NOT ENABLE_TESTS AND NOT SANITIZE AND NOT SANITIZE_COVERAGE AND OS_LINUX)
    # https://clang.llvm.org/docs/ThinLTO.html
    # Applies to clang and linux only.
    # Disabled when building with tests or sanitizers.
    option(ENABLE_THINLTO "Clang-specific link time optimization" ON)
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fstrict-vtable-pointers")

# We cannot afford to use LTO when compiling unit tests, and it's not enough
# to only supply -fno-lto at the final linking stage. So we disable it
# completely.
if (ENABLE_THINLTO AND NOT ENABLE_TESTS AND NOT SANITIZE)
    # Link time optimization
    set (CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO} -flto=thin -fwhole-program-vtables")
    set (CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -flto=thin -fwhole-program-vtables")
    set (CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO "${CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO} -flto=thin -fwhole-program-vtables")
elseif (ENABLE_THINLTO)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Cannot enable ThinLTO")
endif ()

# Turns on all external libs like s3, kafka, ODBC, ...
option(ENABLE_LIBRARIES "Enable all external libraries by default" ON)

# Increase stack size on Musl. We need big stack for our recursive-descend parser.
if (USE_MUSL)
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-z,stack-size=2097152")
endif ()

include(cmake/dbms_glob_sources.cmake)

add_library(global-group INTERFACE)
if (OS_LINUX OR OS_ANDROID)
    include(cmake/linux/default_libs.cmake)
elseif (OS_DARWIN)
    include(cmake/darwin/default_libs.cmake)
elseif (OS_FREEBSD)
    include(cmake/freebsd/default_libs.cmake)
else()
    link_libraries(global-group)
endif ()

option (ENABLE_GWP_ASAN "Enable Gwp-Asan" ON)
# We use mmap for allocations more heavily in debug builds,
# but GWP-ASan also wants to use mmap frequently,
# and due to a large number of memory mappings,
# it does not work together well.
if ((NOT OS_LINUX AND NOT OS_ANDROID) OR (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG") OR SANITIZE)
    set(ENABLE_GWP_ASAN OFF)
endif ()

option (ENABLE_LIBFIU "Enable libfiu" ON)

option(WERROR "Enable -Werror compiler option" ON)

if (WERROR)
    # Don't pollute CMAKE_CXX_FLAGS with -Werror as it will break some CMake checks.
    # Instead, adopt modern cmake usage requirement.
    # TODO: Set CMAKE_COMPILE_WARNING_AS_ERROR (cmake 3.24)
    target_compile_options(global-group INTERFACE "-Werror")
endif ()

# Make this extra-checks for correct library dependencies.
if (OS_LINUX AND NOT SANITIZE)
    target_link_options(global-group INTERFACE "LINKER:--no-undefined")
endif ()

######################################
### Add targets below this comment ###
######################################

set (CMAKE_POSTFIX_VARIABLE "CMAKE_${CMAKE_BUILD_TYPE_UC}_POSTFIX")

if (NOT SANITIZE)
    set (CMAKE_POSITION_INDEPENDENT_CODE OFF)
endif()

if (NOT OS_ANDROID AND OS_LINUX AND NOT ARCH_S390X AND NOT SANITIZE)
    # Using '-no-pie' builds executables with fixed addresses, resulting in slightly more efficient code
    # and keeping binary addresses constant even with ASLR enabled.
    # Disabled on Android as it requires PIE: https://source.android.com/docs/security/enhancements#android-5
    # Disabled on IBM S390X due to build issues with 'no-pie'
    # Disabled with sanitizers to avoid issues with maximum relocation size: https://github.com/ClickHouse/ClickHouse/pull/49145
    set (CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fno-pie")
    set (CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO} -fno-pie")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -no-pie -Wl,-no-pie")
else ()
    message (WARNING "ClickHouse is built as PIE, system.trace_log will contain invalid addresses after server restart.")
endif ()

if (ENABLE_TESTS)
    message (STATUS "Unit tests are enabled")
else()
    message(STATUS "Unit tests are disabled")
endif ()

enable_testing() # Enable for tests without binary

if (ARCH_S390X)
    set(ENABLE_OPENSSL_DYNAMIC_DEFAULT ON)
else ()
    set(ENABLE_OPENSSL_DYNAMIC_DEFAULT OFF)
endif ()
option(ENABLE_OPENSSL_DYNAMIC "This option removes SSL from ClickHouse and will link to the OpenSSL version supplied by OS." ${ENABLE_OPENSSL_DYNAMIC_DEFAULT})

# when installing to /usr - place configs to /etc but for /usr/local place to /usr/local/etc
if (CMAKE_INSTALL_PREFIX STREQUAL "/usr")
    set (CLICKHOUSE_ETC_DIR "/etc")
else ()
    set (CLICKHOUSE_ETC_DIR "${CMAKE_INSTALL_PREFIX}/etc")
endif ()

message (STATUS "Building for: ${CMAKE_SYSTEM} ${CMAKE_SYSTEM_PROCESSOR} ${CMAKE_LIBRARY_ARCHITECTURE}")

add_subdirectory (contrib EXCLUDE_FROM_ALL)

if (NOT ENABLE_JEMALLOC)
    message (WARNING "Non default allocator is disabled. This is not recommended for production builds.")
endif ()

macro (clickhouse_add_executable target)
    # invoke built-in add_executable
    # explicitly acquire and interpose malloc symbols by clickhouse_malloc
    # if GLIBC_COMPATIBILITY is ON and ENABLE_THINLTO is on than provide memcpy symbol explicitly to neutrialize thinlto's libcall generation.
    if (ARCH_AMD64 AND GLIBC_COMPATIBILITY AND ENABLE_THINLTO)
        add_executable (${ARGV} $<TARGET_OBJECTS:clickhouse_malloc> $<TARGET_OBJECTS:memcpy>)
    else ()
        add_executable (${ARGV} $<TARGET_OBJECTS:clickhouse_malloc>)
    endif ()

    get_target_property (type ${target} TYPE)
    if (${type} STREQUAL EXECUTABLE)
        # disabled for TSAN and gcc since libtsan.a provides overrides too
        if (TARGET clickhouse_new_delete)
            # operator::new/delete for executables (MemoryTracker stuff)
            target_link_libraries (${target} PRIVATE clickhouse_new_delete)
        endif()

        # In case of static jemalloc, because zone_register() is located in zone.c and
        # is never used outside (it is declared as constructor) it is omitted
        # by the linker, and so jemalloc will not be registered as system
        # allocator under osx [1], and clickhouse will SIGSEGV.
        #
        #   [1]: https://github.com/jemalloc/jemalloc/issues/708
        #
        # About symbol name:
        # - _zone_register not zone_register due to Mach-O binary format,
        # - _je_zone_register due to JEMALLOC_PRIVATE_NAMESPACE=je_ under OS X.
        # - but jemalloc-cmake does not run private_namespace.sh
        #   so symbol name should be _zone_register
        if (ENABLE_JEMALLOC AND OS_DARWIN)
            set_property(TARGET ${target} APPEND PROPERTY LINK_OPTIONS -u_zone_register)
        endif()
    endif()
endmacro()

# With cross-compiling, all targets are built for the target platform which usually different from the host
# platform. This is problematic if a build artifact X (e.g. a file or an executable) is generated by running
# another executable Y previously produced in the build. This is solved by compiling and running Y for/on
# the host platform. Add target to the list:
#    add_native_target(<target> ...)
set_property (GLOBAL PROPERTY NATIVE_BUILD_TARGETS)
function (add_native_target)
    set_property (GLOBAL APPEND PROPERTY NATIVE_BUILD_TARGETS ${ARGV})
endfunction (add_native_target)

set(CONFIG_INCLUDE_PATH ${CMAKE_CURRENT_BINARY_DIR}/includes/configs CACHE INTERNAL "Path to generated configuration files.")
include_directories(${CONFIG_INCLUDE_PATH})

# Add as many warnings as possible for our own code.
include (cmake/warnings.cmake)
include (cmake/print_flags.cmake)

if (ENABLE_RUST)
    add_subdirectory (rust)

    # With LTO Rust adds few symbols with global visiblity, the most common is
    # rust_eh_personality. And this leads to linking errors because multiple
    # Rust libraries contains the same symbol.
    #
    # If it was shared library, that we could use version script for linker to
    # hide this symbols, but libraries are static.
    #
    # we could in theory compile everything to one library but this will be a
    # mess
    #
    # But this should be OK since CI has lots of other builds that are done
    # without LTO and it will find multiple definitions if there will be any.
    #
    # More information about this behaviour in Rust can be found here
    # - https://github.com/rust-lang/rust/issues/44322
    # - https://alanwu.space/post/symbol-hygiene/
    if (ENABLE_THINLTO)
        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--allow-multiple-definition")
    endif()
endif()

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO"
    AND NOT SANITIZE AND NOT SANITIZE_COVERAGE AND NOT ENABLE_FUZZING
    AND OMIT_HEAVY_DEBUG_SYMBOLS AND OS_LINUX AND (ARCH_AMD64 OR ARCH_AARCH64))
    set(CHECK_LARGE_OBJECT_SIZES_DEFAULT ON)
else ()
    set(CHECK_LARGE_OBJECT_SIZES_DEFAULT OFF)
endif ()
option(CHECK_LARGE_OBJECT_SIZES "Check that there are no large object files after build." ${CHECK_LARGE_OBJECT_SIZES_DEFAULT})

add_subdirectory (base)
add_subdirectory (src)
add_subdirectory (programs)
add_subdirectory (utils)

if (FUZZER)
    # Bundle fuzzers target
    add_custom_target(fuzzers)
    # Instrument all targets fuzzer and link with libfuzzer
    get_all_targets(all_targets)
    foreach(target ${all_targets})
        if (NOT(target STREQUAL "_fuzzer" OR target STREQUAL "_fuzzer_no_main"))
            get_target_property(target_type ${target} TYPE)
            if (NOT(target_type STREQUAL "INTERFACE_LIBRARY" OR target_type STREQUAL "UTILITY"))
                target_compile_options(${target} PRIVATE "-fsanitize=fuzzer-no-link")
            endif()
            if (target_type STREQUAL "EXECUTABLE" AND target MATCHES ".+_fuzzer")
                message(STATUS "${target} instrumented with fuzzer")
                target_link_libraries(${target} PUBLIC ch_contrib::fuzzer)
                # Add to fuzzers bundle
                add_dependencies(fuzzers ${target})
                get_target_filename(${target} target_bin_name)
                get_target_property(target_bin_dir ${target} BINARY_DIR)
                add_custom_command(TARGET fuzzers POST_BUILD COMMAND mv "${target_bin_dir}/${target_bin_name}" "${CMAKE_CURRENT_BINARY_DIR}/programs/" VERBATIM)
            endif()
            if (target STREQUAL "clickhouse")
                message(STATUS "${target} instrumented with fuzzer")
                target_link_libraries(${target} PUBLIC ch_contrib::fuzzer_no_main)
                # Add to fuzzers bundle
                add_dependencies(fuzzers ${target})
            endif()
        endif()
    endforeach()
    add_custom_command(TARGET fuzzers POST_BUILD COMMAND SRC=${CMAKE_SOURCE_DIR} BIN=${CMAKE_BINARY_DIR} OUT=${CMAKE_BINARY_DIR}/programs ${CMAKE_SOURCE_DIR}/tests/fuzz/build.sh VERBATIM)
endif()

include (cmake/sanitize_targets.cmake)

# Build native targets if necessary
get_property(NATIVE_BUILD_TARGETS GLOBAL PROPERTY NATIVE_BUILD_TARGETS)
if (NATIVE_BUILD_TARGETS
    AND NOT(
        CMAKE_HOST_SYSTEM_NAME STREQUAL CMAKE_SYSTEM_NAME
        AND CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL CMAKE_SYSTEM_PROCESSOR
    )
)
    message (STATUS "Building native targets...")

    set (NATIVE_BUILD_DIR "${PROJECT_BINARY_DIR}/native")

    execute_process(
        COMMAND ${CMAKE_COMMAND} -E make_directory "${NATIVE_BUILD_DIR}"
        COMMAND_ECHO STDOUT
        COMMAND_ERROR_IS_FATAL ANY
    )

    execute_process(
        COMMAND ${CMAKE_COMMAND}
            "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
            "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
            "-DCOMPILER_CACHE=${COMPILER_CACHE}"
            # Avoid overriding .cargo/config.toml with native toolchain.
            "-DENABLE_RUST=OFF"
            "-DENABLE_CLICKHOUSE_SELF_EXTRACTING=${ENABLE_CLICKHOUSE_SELF_EXTRACTING}"
        ${PROJECT_SOURCE_DIR}
        WORKING_DIRECTORY "${NATIVE_BUILD_DIR}"
        COMMAND_ECHO STDOUT
        COMMAND_ERROR_IS_FATAL ANY
    )

    execute_process(
        COMMAND ${CMAKE_COMMAND} --build "${NATIVE_BUILD_DIR}" --target ${NATIVE_BUILD_TARGETS}
        COMMAND_ECHO STDOUT
        COMMAND_ERROR_IS_FATAL ANY
    )
endif ()
