# The memory allocator: a C++ reimplementation of jemalloc (the ClickHouse fork) exposing the same `je_*` C ABI.
#
# Works inside the ClickHouse tree and standalone (for development and testing):
#   cmake -S base/allocator -B build_alloc -G Ninja -DALLOCATOR_LG_PAGE=16 -DALLOCATOR_DEBUG=1

if (CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    cmake_minimum_required (VERSION 3.25)
    project (allocator C CXX)

    set (CMAKE_CXX_STANDARD 23)
    set (CMAKE_CXX_STANDARD_REQUIRED ON)
    set (CMAKE_CXX_EXTENSIONS OFF)
    set (CMAKE_C_STANDARD 11)

    set (ALLOCATOR_STANDALONE 1)
    set (ALLOCATOR_LG_PAGE "" CACHE STRING "log2 of the allocator page size (required on Linux aarch64)")
    set (ALLOCATOR_MALLOC_CONF "percpu_arena:percpu,oversize_threshold:67108864,muzzy_decay_ms:0,dirty_decay_ms:5000,prof:true,prof_active:true,prof_thread_active_init:false,background_thread:true,lg_extent_max_active_fit:6"
        CACHE STRING "Compiled-in configuration string")
    option (ALLOCATOR_DEBUG "Enable internal assertions" OFF)
    option (ALLOCATOR_UAF_DETECTION "Compile in use-after-free detection" OFF)
    option (ALLOCATOR_MUSL "Building against musl" OFF)
    option (ALLOCATOR_BUILD_TESTS "Build tests" ON)
    set (ALLOCATOR_REFERENCE_JEMALLOC "" CACHE FILEPATH "Reference lib_jemalloc.a for differential tests")
    set (ALLOCATOR_REFERENCE_INCLUDE "" CACHE STRING "Include directories of the reference jemalloc build (internal defs)")
    # The heap profiler takes backtraces with `unw_backtrace` (like jemalloc with JEMALLOC_PROF_LIBUNWIND), so a
    # libunwind is required: by default ClickHouse's LLVM libunwind from a ClickHouse build tree.
    set (ALLOCATOR_LIBUNWIND "" CACHE FILEPATH "libunwind library providing unw_backtrace")

    if (NOT CMAKE_BUILD_TYPE)
        set (CMAKE_BUILD_TYPE RelWithDebInfo)
    endif ()
    if (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
        # Match the optimization level jemalloc is built with in ClickHouse.
        set (CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -g")
        set (CMAKE_C_FLAGS_RELWITHDEBINFO "-O3 -g")
    endif ()
endif ()

set (ALLOCATOR_SOURCES_DIR "${CMAKE_CURRENT_SOURCE_DIR}")

file (GLOB ALLOCATOR_SOURCES CONFIGURE_DEPENDS "${ALLOCATOR_SOURCES_DIR}/*.cpp")
file (GLOB ALLOCATOR_HEADERS CONFIGURE_DEPENDS "${ALLOCATOR_SOURCES_DIR}/*.h")

if (NOT CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    list (REMOVE_ITEM ALLOCATOR_SOURCES "${ALLOCATOR_SOURCES_DIR}/Zone.cpp")
endif ()

# Everything except the C API, so that internal components can be tested together with the reference jemalloc
# (which exports the same `je_*` symbols).
set (ALLOCATOR_API_SOURCES "${ALLOCATOR_SOURCES_DIR}/Api.cpp")
set (ALLOCATOR_CORE_SOURCES ${ALLOCATOR_SOURCES})
list (REMOVE_ITEM ALLOCATOR_CORE_SOURCES ${ALLOCATOR_API_SOURCES})

function (allocator_configure_target target)
    # The include root is `base/`, so the headers are included as <allocator/X.h>.
    target_include_directories (${target} PUBLIC "${ALLOCATOR_SOURCES_DIR}/..")
    target_include_directories (${target} SYSTEM PUBLIC "${ALLOCATOR_SOURCES_DIR}/include")

    target_compile_options (${target} PRIVATE -fno-exceptions -fno-rtti -fvisibility=hidden -fvisibility-inlines-hidden)
    # Coverage callbacks may allocate, which must not recurse into the allocator.
    if (WITHOUT_COVERAGE_FLAGS_LIST)
        target_compile_options (${target} PRIVATE ${WITHOUT_COVERAGE_FLAGS_LIST})
    endif ()

    if (ALLOCATOR_LG_PAGE)
        target_compile_definitions (${target} PUBLIC ALLOCATOR_LG_PAGE=${ALLOCATOR_LG_PAGE})
    endif ()
    if (ALLOCATOR_DEBUG)
        target_compile_definitions (${target} PUBLIC ALLOCATOR_DEBUG=1)
    endif ()
    if (ALLOCATOR_UAF_DETECTION)
        target_compile_definitions (${target} PUBLIC ALLOCATOR_UAF_DETECTION=1)
    endif ()
    if (ALLOCATOR_MUSL)
        target_compile_definitions (${target} PUBLIC ALLOCATOR_MUSL=1)
    endif ()
    target_compile_definitions (${target} PUBLIC "ALLOCATOR_MALLOC_CONF=\"${ALLOCATOR_MALLOC_CONF}\"")
endfunction ()

if (NOT ALLOCATOR_STANDALONE)
    # Inside ClickHouse: a single library with everything. The variables ALLOCATOR_LG_PAGE, ALLOCATOR_MALLOC_CONF,
    # ALLOCATOR_UAF_DETECTION, ALLOCATOR_MUSL are set by contrib/jemalloc-cmake.
    add_library (_allocator STATIC ${ALLOCATOR_SOURCES})
    allocator_configure_target (_allocator)
    target_link_libraries (_allocator PRIVATE unwind)
    return ()
endif ()

add_library (_allocator_core STATIC ${ALLOCATOR_CORE_SOURCES})
allocator_configure_target (_allocator_core)
find_package (Threads REQUIRED)
target_link_libraries (_allocator_core PUBLIC Threads::Threads ${CMAKE_DL_LIBS})

# `unw_backtrace` for heap profiling (jemalloc-cmake: `target_link_libraries (_jemalloc PRIVATE unwind)`).
# By default, the LLVM libunwind of a ClickHouse build tree next to the sources.
if (NOT ALLOCATOR_LIBUNWIND)
    set (ALLOCATOR_LIBUNWIND "${ALLOCATOR_SOURCES_DIR}/../../build/contrib/libunwind-cmake/libunwind.a" CACHE FILEPATH "libunwind library providing unw_backtrace" FORCE)
endif ()
if (NOT EXISTS "${ALLOCATOR_LIBUNWIND}")
    message (FATAL_ERROR "libunwind is not found at ${ALLOCATOR_LIBUNWIND}: set ALLOCATOR_LIBUNWIND")
endif ()
target_link_libraries (_allocator_core PUBLIC "${ALLOCATOR_LIBUNWIND}")

if (EXISTS "${ALLOCATOR_API_SOURCES}")
    add_library (_allocator STATIC ${ALLOCATOR_API_SOURCES})
    allocator_configure_target (_allocator)
    target_link_libraries (_allocator PUBLIC _allocator_core)
endif ()

if (ALLOCATOR_STANDALONE AND ALLOCATOR_BUILD_TESTS)
    enable_testing ()
    add_subdirectory (tests)
endif ()
