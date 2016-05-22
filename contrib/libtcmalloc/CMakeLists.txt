add_definitions(
    -DNO_TCMALLOC_SAMPLES -DNO_TCMALLOC_SAMPLES
    -DNDEBUG -DNO_FRAME_POINTER
    -Wwrite-strings -Wno-sign-compare -Wno-unused-result
    -fno-builtin-malloc -fno-builtin-free -fno-builtin-realloc -fno-builtin-calloc -fno-builtin-cfree
    -fno-builtin-memalign -fno-builtin-posix_memalign -fno-builtin-valloc -fno-builtin-pvalloc
)

include_directories (include)

add_library (tcmalloc_minimal
./include/malloc_hook.cc
./include/base/spinlock_internal.cc
./include/base/spinlock.cc
./include/base/dynamic_annotations.c
./include/base/linuxthreads.cc
./include/base/elf_mem_image.cc
./include/base/vdso_support.cc
./include/base/sysinfo.cc
./include/base/low_level_alloc.cc
./include/base/thread_lister.c
./include/base/logging.cc
./include/base/atomicops-internals-x86.cc
./include/memfs_malloc.cc
./include/tcmalloc.cc
./include/malloc_extension.cc
./include/thread_cache.cc
./include/symbolize.cc
./include/page_heap.cc
./include/maybe_threads.cc
./include/central_freelist.cc
./include/static_vars.cc
./include/sampler.cc
./include/internal_logging.cc
./include/system-alloc.cc
./include/span.cc
./include/common.cc
./include/debugallocation.cc
./include/stacktrace.cc
./include/stack_trace_table.cc
)
