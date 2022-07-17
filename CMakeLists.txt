cmake_minimum_required(VERSION 3.14)

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
include (cmake/git_status.cmake)

# Ignore export() since we don't use it,
# but it gets broken with a global targets via link_libraries()
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
    # set VIRT (RLIMIT_AS) to 10G (DATA*10)
    set (RLIMIT_AS 10000000000)
    # set CPU time limit to 1000 seconds
    set (RLIMIT_CPU 1000)

    # gcc10/gcc10/clang -fsanitize=memory is too heavy
    if (SANITIZE STREQUAL "memory" OR COMPILER_GCC)
       set (RLIMIT_DATA 10000000000) # 10G
    endif()

    set (CMAKE_CXX_COMPILER_LAUNCHER prlimit --as=${RLIMIT_AS} --data=${RLIMIT_DATA} --cpu=${RLIMIT_CPU} ${CMAKE_CXX_COMPILER_LAUNCHER})
endif ()

if (NOT CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL "None")
    set (CMAKE_BUILD_TYPE "RelWithDebInfo")
    message (STATUS "CMAKE_BUILD_TYPE is not set, set to default = ${CMAKE_BUILD_TYPE}")
endif ()
message (STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

string (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_UC)

option(USE_STATIC_LIBRARIES "Disable to use shared libraries" ON)
# DEVELOPER ONLY.
# Faster linking if turned on.
option(SPLIT_SHARED_LIBRARIES "Keep all internal libraries as separate .so files" OFF)
option(CLICKHOUSE_SPLIT_BINARY "Make several binaries (clickhouse-server, clickhouse-client etc.) instead of one bundled" OFF)

if (USE_STATIC_LIBRARIES AND SPLIT_SHARED_LIBRARIES)
    message(FATAL_ERROR "Defining SPLIT_SHARED_LIBRARIES=1 without USE_STATIC_LIBRARIES=0 has no effect.")
endif()

if (NOT USE_STATIC_LIBRARIES AND SPLIT_SHARED_LIBRARIES)
    set(BUILD_SHARED_LIBS 1 CACHE INTERNAL "")
endif ()

if (USE_STATIC_LIBRARIES)
    list(REVERSE CMAKE_FIND_LIBRARY_SUFFIXES)
endif ()

option (ENABLE_FUZZING "Fuzzy testing using libfuzzer" OFF)

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
    set (USE_UNWIND ON)
    set (ENABLE_EMBEDDED_COMPILER 0)
    set (ENABLE_EXAMPLES 0)
    set (ENABLE_UTILS 0)
    set (ENABLE_THINLTO 0)
    set (ENABLE_TCMALLOC 0)
    set (ENABLE_JEMALLOC 0)
    set (ENABLE_CHECK_HEAVY_BUILDS 1)
    set (GLIBC_COMPATIBILITY OFF)

    # For codegen_select_fuzzer
    set (ENABLE_PROTOBUF 1)
endif()

# Global libraries
# See:
# - default_libs.cmake
# - sanitize.cmake
add_library(global-libs INTERFACE)

include (cmake/fuzzer.cmake)
include (cmake/sanitize.cmake)

option(ENABLE_COLORED_BUILD "Enable colors in compiler output" ON)

set (CMAKE_COLOR_MAKEFILE ${ENABLE_COLORED_BUILD}) # works only for the makefile generator

if (ENABLE_COLORED_BUILD AND CMAKE_GENERATOR STREQUAL "Ninja")
    # Turn on colored output. https://github.com/ninja-build/ninja/wiki/FAQ
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-color=always")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fdiagnostics-color=always")
    # ... such manually setting of flags can be removed once CMake supports a variable to
    # activate colors in *all* build systems: https://gitlab.kitware.com/cmake/cmake/-/issues/15502
endif ()

include (cmake/check_flags.cmake)
include (cmake/add_warning.cmake)

if (COMPILER_CLANG)
    # generate ranges for fast "addr2line" search
    if (NOT CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE")
        set(COMPILER_FLAGS "${COMPILER_FLAGS} -gdwarf-aranges")
    endif ()

    if (HAS_USE_CTOR_HOMING)
        # For more info see https://blog.llvm.org/posts/2021-04-05-constructor-homing-for-debug-info/
        if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG" OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO")
            set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Xclang -fuse-ctor-homing")
            set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Xclang -fuse-ctor-homing")
        endif()
    endif()
endif ()

# If compiler has support for -Wreserved-identifier. It is difficult to detect by clang version,
# because there are two different branches of clang: clang and AppleClang.
# (AppleClang is not supported by ClickHouse, but some developers have misfortune to use it).
if (HAS_RESERVED_IDENTIFIER)
    add_compile_definitions (HAS_RESERVED_IDENTIFIER)
endif ()

# If turned `ON`, assumes the user has either the system GTest library or the bundled one.
option(ENABLE_TESTS "Provide unit_test_dbms target with Google.Test unit tests" ON)
option(ENABLE_EXAMPLES "Build all example programs in 'examples' subdirectories" OFF)

if (OS_LINUX AND (ARCH_AMD64 OR ARCH_AARCH64) AND USE_STATIC_LIBRARIES AND NOT SPLIT_SHARED_LIBRARIES AND NOT USE_MUSL)
    # Only for Linux, x86_64 or aarch64.
    option(GLIBC_COMPATIBILITY "Enable compatibility with older glibc libraries." ON)
elseif(GLIBC_COMPATIBILITY)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Glibc compatibility cannot be enabled in current configuration")
endif ()

# Make sure the final executable has symbols exported
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -rdynamic")

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

option(ENABLE_CURL_BUILD "Enable curl, azure, sentry build on by default except MacOS." ON)
if (OS_DARWIN)
    # Disable the curl, azure, senry build on MacOS
    set (ENABLE_CURL_BUILD OFF)
endif ()

# Ignored if `lld` is used
option(ADD_GDB_INDEX_FOR_GOLD "Add .gdb-index to resulting binaries for gold linker.")

if (NOT CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE")
    # Can be lld or ld-lld.
    if (LINKER_NAME MATCHES "lld$")
        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--gdb-index")
        set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--gdb-index")
        message (STATUS "Adding .gdb-index via --gdb-index linker option.")
    # we use another tool for gdb-index, because gold linker removes section .debug_aranges, which used inside clickhouse stacktraces
    # http://sourceware-org.1504.n7.nabble.com/gold-No-debug-aranges-section-when-linking-with-gdb-index-td540965.html#a556932
    elseif (LINKER_NAME MATCHES "gold$" AND ADD_GDB_INDEX_FOR_GOLD)
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

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE"
    OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO"
    OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
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

if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    set(USE_DEBUG_HELPERS ON)
endif()
option(USE_DEBUG_HELPERS "Enable debug helpers" ${USE_DEBUG_HELPERS})

option(BUILD_STANDALONE_KEEPER "Build keeper as small standalone binary" OFF)
if (NOT BUILD_STANDALONE_KEEPER)
    option(CREATE_KEEPER_SYMLINK "Create symlink for clickhouse-keeper to main server binary" ON)
else ()
    option(CREATE_KEEPER_SYMLINK "Create symlink for clickhouse-keeper to main server binary" OFF)
endif ()

# Create BuildID when using lld. For other linkers it is created by default.
if (LINKER_NAME MATCHES "lld$")
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

# Asynchronous unwind tables are needed for Query Profiler.
# They are already by default on some platforms but possibly not on all platforms.
# Enable it explicitly.
set (COMPILER_FLAGS "${COMPILER_FLAGS} -fasynchronous-unwind-tables")

# Reproducible builds.
if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    set (ENABLE_BUILD_PATH_MAPPING_DEFAULT OFF)
else ()
    set (ENABLE_BUILD_PATH_MAPPING_DEFAULT ON)
endif ()

option (ENABLE_BUILD_PATH_MAPPING "Enable remapping of file source paths in debug info, predefined preprocessor macros, and __builtin_FILE(). It's used to generate reproducible builds. See https://reproducible-builds.org/docs/build-path" ${ENABLE_BUILD_PATH_MAPPING_DEFAULT})

if (ENABLE_BUILD_PATH_MAPPING)
    set (COMPILER_FLAGS "${COMPILER_FLAGS} -ffile-prefix-map=${CMAKE_SOURCE_DIR}=.")
    set (CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} -ffile-prefix-map=${CMAKE_SOURCE_DIR}=.")
endif ()

option (ENABLE_BUILD_PROFILING "Enable profiling of build time" OFF)
if (ENABLE_BUILD_PROFILING)
     if (COMPILER_CLANG)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} -ftime-trace")
     else ()
        message (${RECONFIGURE_MESSAGE_LEVEL} "Build profiling is only available with CLang")
     endif ()
endif ()

set (CMAKE_CXX_STANDARD 20)
set (CMAKE_CXX_EXTENSIONS ON) # Same as gnu++2a (ON) vs c++2a (OFF): https://cmake.org/cmake/help/latest/prop_tgt/CXX_EXTENSIONS.html
set (CMAKE_CXX_STANDARD_REQUIRED ON)

set (CMAKE_C_STANDARD 11)
set (CMAKE_C_EXTENSIONS ON)
set (CMAKE_C_STANDARD_REQUIRED ON)

if (COMPILER_GCC OR COMPILER_CLANG)
    # Enable C++14 sized global deallocation functions. It should be enabled by setting -std=c++14 but I'm not sure.
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsized-deallocation")
endif ()

# falign-functions=32 prevents from random performance regressions with the code change. Thus, providing more stable
# benchmarks.
if (COMPILER_GCC OR COMPILER_CLANG)
    set(COMPILER_FLAGS "${COMPILER_FLAGS} -falign-functions=32")
endif ()

if (ARCH_AMD64)
    # align branches within a 32-Byte boundary to avoid the potential performance loss when code layout change,
    # which makes benchmark results more stable.
    set(BRANCHES_WITHIN_32B_BOUNDARIES "-mbranches-within-32B-boundaries")
    if (COMPILER_GCC)
        # gcc is in assembler, need to add "-Wa," prefix
        set(BRANCHES_WITHIN_32B_BOUNDARIES "-Wa,${BRANCHES_WITHIN_32B_BOUNDARIES}")
    endif()

    include(CheckCXXCompilerFlag)
    check_cxx_compiler_flag("${BRANCHES_WITHIN_32B_BOUNDARIES}" HAS_BRANCHES_WITHIN_32B_BOUNDARIES)
    if (HAS_BRANCHES_WITHIN_32B_BOUNDARIES)
        set(COMPILER_FLAGS "${COMPILER_FLAGS} ${BRANCHES_WITHIN_32B_BOUNDARIES}")
    endif()
endif()

if (COMPILER_GCC)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcoroutines")
endif ()

# Compiler-specific coverage flags e.g. -fcoverage-mapping for gcc
option(WITH_COVERAGE "Profile the resulting binary/binaries" OFF)

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

set (COMPILER_FLAGS "${COMPILER_FLAGS}")

# Our built-in unwinder only supports DWARF version up to 4.
set (DEBUG_INFO_FLAGS "-g -gdwarf-4")

set (CMAKE_CXX_FLAGS                     "${CMAKE_CXX_FLAGS} ${COMPILER_FLAGS}")
set (CMAKE_CXX_FLAGS_RELWITHDEBINFO      "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O3 ${DEBUG_INFO_FLAGS} ${CMAKE_CXX_FLAGS_ADD}")
set (CMAKE_CXX_FLAGS_DEBUG               "${CMAKE_CXX_FLAGS_DEBUG} -O0 ${DEBUG_INFO_FLAGS} -fno-inline ${CMAKE_CXX_FLAGS_ADD}")

set (CMAKE_C_FLAGS                       "${CMAKE_C_FLAGS} ${COMPILER_FLAGS} ${CMAKE_C_FLAGS_ADD}")
set (CMAKE_C_FLAGS_RELWITHDEBINFO        "${CMAKE_C_FLAGS_RELWITHDEBINFO} -O3 ${DEBUG_INFO_FLAGS} ${CMAKE_C_FLAGS_ADD}")
set (CMAKE_C_FLAGS_DEBUG                 "${CMAKE_C_FLAGS_DEBUG} -O0 ${DEBUG_INFO_FLAGS} -fno-inline ${CMAKE_C_FLAGS_ADD}")

set (CMAKE_ASM_FLAGS                     "${CMAKE_ASM_FLAGS} ${COMPILER_FLAGS} ${CMAKE_ASM_FLAGS_ADD}")
set (CMAKE_ASM_FLAGS_RELWITHDEBINFO      "${CMAKE_ASM_FLAGS_RELWITHDEBINFO} -O3 ${DEBUG_INFO_FLAGS} ${CMAKE_ASM_FLAGS_ADD}")
set (CMAKE_ASM_FLAGS_DEBUG               "${CMAKE_ASM_FLAGS_DEBUG} -O0 ${DEBUG_INFO_FLAGS} -fno-inline ${CMAKE_ASM_FLAGS_ADD}")

if (COMPILER_CLANG)
    if (OS_DARWIN)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-U,_inside_main")
    endif()

    # Display absolute paths in error messages. Otherwise KDevelop fails to navigate to correct file and opens a new file instead.
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-absolute-paths")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fdiagnostics-absolute-paths")

    if (NOT ENABLE_TESTS AND NOT SANITIZE)
        # https://clang.llvm.org/docs/ThinLTO.html
        # Applies to clang only.
        # Disabled when building with tests or sanitizers.
        option(ENABLE_THINLTO "Clang-specific link time optimization" ON)
    endif()

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fstrict-vtable-pointers")

    # Set new experimental pass manager, it's a performance, build time and binary size win.
    # Can be removed after https://reviews.llvm.org/D66490 merged and released to at least two versions of clang.
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fexperimental-new-pass-manager")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fexperimental-new-pass-manager")

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

elseif (ENABLE_THINLTO)
    message (${RECONFIGURE_MESSAGE_LEVEL} "ThinLTO is only available with Clang")
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
endif ()
link_libraries(global-group)

if (NOT (OS_LINUX OR OS_DARWIN))
    # Using system libs can cause a lot of warnings in includes (on macro expansion).
    option(WERROR "Enable -Werror compiler option" OFF)
else ()
    option(WERROR "Enable -Werror compiler option" ON)
endif ()

if (WERROR)
    # Don't pollute CMAKE_CXX_FLAGS with -Werror as it will break some CMake checks.
    # Instead, adopt modern cmake usage requirement.
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

if (USE_STATIC_LIBRARIES)
    set (CMAKE_POSITION_INDEPENDENT_CODE OFF)
    if (OS_LINUX AND NOT ARCH_AARCH64)
        # Slightly more efficient code can be generated
        # It's disabled for ARM because otherwise ClickHouse cannot run on Android.
        set (CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fno-pie")
        set (CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO} -fno-pie")
        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -no-pie -Wl,-no-pie")
    endif ()
else ()
    set (CMAKE_POSITION_INDEPENDENT_CODE ON)
    # This is required for clang on Arch linux, that uses PIE by default.
    # See enable-SSP-and-PIE-by-default.patch [1].
    #
    #   [1]: https://github.com/archlinux/svntogit-packages/blob/6e681aa860e65ad46a1387081482eb875c2200f2/trunk/enable-SSP-and-PIE-by-default.patch
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -no-pie")
endif ()

if (ENABLE_TESTS)
    message (STATUS "Unit tests are enabled")
else()
    message(STATUS "Unit tests are disabled")
endif ()

enable_testing() # Enable for tests without binary

# when installing to /usr - place configs to /etc but for /usr/local place to /usr/local/etc
if (CMAKE_INSTALL_PREFIX STREQUAL "/usr")
    set (CLICKHOUSE_ETC_DIR "/etc")
else ()
    set (CLICKHOUSE_ETC_DIR "${CMAKE_INSTALL_PREFIX}/etc")
endif ()

message (STATUS
    "Building for: ${CMAKE_SYSTEM} ${CMAKE_SYSTEM_PROCESSOR} ${CMAKE_LIBRARY_ARCHITECTURE} ;
    USE_STATIC_LIBRARIES=${USE_STATIC_LIBRARIES}
    SPLIT_SHARED=${SPLIT_SHARED_LIBRARIES}")

include (GNUInstallDirs)

# When testing for memory leaks with Valgrind, don't link tcmalloc or jemalloc.

if (TARGET global-group)
    install (EXPORT global DESTINATION cmake)
endif ()

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
        if (ENABLE_JEMALLOC AND USE_STATIC_LIBRARIES AND OS_DARWIN)
            set_property(TARGET ${target} APPEND PROPERTY LINK_OPTIONS -u_zone_register)
        endif()
    endif()
endmacro()

set(ConfigIncludePath ${CMAKE_CURRENT_BINARY_DIR}/includes/configs CACHE INTERNAL "Path to generated configuration files.")
include_directories(${ConfigIncludePath})

# Add as many warnings as possible for our own code.
include (cmake/warnings.cmake)
include (cmake/print_flags.cmake)

add_subdirectory (base)
add_subdirectory (src)
add_subdirectory (programs)
add_subdirectory (tests)
add_subdirectory (utils)

include (cmake/sanitize_target_link_libraries.cmake)
