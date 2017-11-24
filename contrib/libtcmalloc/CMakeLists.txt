
message (STATUS "Building: tcmalloc_minimal_internal")

add_library (tcmalloc_minimal_internal
./src/malloc_hook.cc
./src/base/spinlock_internal.cc
./src/base/spinlock.cc
./src/base/dynamic_annotations.c
./src/base/linuxthreads.cc
./src/base/elf_mem_image.cc
./src/base/vdso_support.cc
./src/base/sysinfo.cc
./src/base/low_level_alloc.cc
./src/base/thread_lister.c
./src/base/logging.cc
./src/base/atomicops-internals-x86.cc
./src/memfs_malloc.cc
./src/tcmalloc.cc
./src/malloc_extension.cc
./src/thread_cache.cc
./src/symbolize.cc
./src/page_heap.cc
./src/maybe_threads.cc
./src/central_freelist.cc
./src/static_vars.cc
./src/sampler.cc
./src/internal_logging.cc
./src/system-alloc.cc
./src/span.cc
./src/common.cc
./src/stacktrace.cc
./src/stack_trace_table.cc
./src/heap-checker.cc
./src/heap-checker-bcad.cc
./src/heap-profile-table.cc
./src/raw_printer.cc
./src/memory_region_map.cc
)


target_compile_options (tcmalloc_minimal_internal
    PRIVATE
    -DNO_TCMALLOC_SAMPLES
    -DNDEBUG
    -DNO_FRAME_POINTER
    -Wwrite-strings
    -Wno-sign-compare
    -Wno-unused-result
    -Wno-deprecated-declarations
    -Wno-unused-function
    -Wno-unused-private-field

    PUBLIC
    -fno-builtin-malloc
    -fno-builtin-free
    -fno-builtin-realloc
    -fno-builtin-calloc
    -fno-builtin-cfree
    -fno-builtin-memalign
    -fno-builtin-posix_memalign
    -fno-builtin-valloc
    -fno-builtin-pvalloc
)

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 3.9.1)
    target_compile_options(tcmalloc_minimal_internal PUBLIC -Wno-dynamic-exception-spec )
endif ()

if (CMAKE_SYSTEM MATCHES "FreeBSD" AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(tcmalloc_minimal_internal PUBLIC -Wno-unused-but-set-variable)
endif ()

if (CMAKE_SYSTEM MATCHES "FreeBSD")
    target_compile_definitions(tcmalloc_minimal_internal PUBLIC _GNU_SOURCE)
endif ()

target_include_directories (tcmalloc_minimal_internal PUBLIC include)
target_include_directories (tcmalloc_minimal_internal PRIVATE src)

find_package (Threads)
target_link_libraries (tcmalloc_minimal_internal ${CMAKE_THREAD_LIBS_INIT})
