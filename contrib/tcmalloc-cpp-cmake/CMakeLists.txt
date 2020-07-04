set (ENABLE_TCMALLOC_CPP_DEFAULT OFF)
if (${ENABLE_LIBRARIES} AND NOT ${ENABLE_JEMALLOC})
    set (ENABLE_TCMALLOC_CPP_DEFAULT ON)
endif()
option (ENABLE_TCMALLOC_CPP "Enable tcmalloc allocator" ${ENABLE_LIBRARIES})

if ((SANITIZE AND NOT SANITIZE STREQUAL "undefined") OR NOT OS_LINUX OR NOT ARCH_AMD64)
    set (ENABLE_TCMALLOC_CPP OFF)
    message (STATUS "tcmalloc-cpp is disabled implicitly: it doesn't work with ${SANITIZE} sanitizer on non-linux or non x86-64.")
endif ()

if (${ENABLE_TCMALLOC_CPP})
    # There is no abseil in distro repos anyway
    option (USE_INTERNAL_TCMALLOC_CPP "Use internal tcmalloc library" ON)

    # It has asm bits
    enable_language (ASM)

    add_subdirectory (../abseil-cpp-cmake ${CMAKE_CURRENT_BINARY_DIR}/abseil-cpp-cmake)

    set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/tcmalloc-cpp")

    set (SRCS
        # tcmalloc/BUILD::common_srcs (without headers):
        ${LIBRARY_DIR}/tcmalloc/arena.cc
        ${LIBRARY_DIR}/tcmalloc/central_freelist.cc
        ${LIBRARY_DIR}/tcmalloc/common.cc
        ${LIBRARY_DIR}/tcmalloc/cpu_cache.cc
        ${LIBRARY_DIR}/tcmalloc/experimental_56_size_class.cc
        ${LIBRARY_DIR}/tcmalloc/experimental_4k_size_class.cc
        ${LIBRARY_DIR}/tcmalloc/guarded_page_allocator.cc
        ${LIBRARY_DIR}/tcmalloc/huge_address_map.cc
        ${LIBRARY_DIR}/tcmalloc/huge_allocator.cc
        ${LIBRARY_DIR}/tcmalloc/huge_cache.cc
        ${LIBRARY_DIR}/tcmalloc/huge_page_aware_allocator.cc
        ${LIBRARY_DIR}/tcmalloc/page_allocator.cc
        ${LIBRARY_DIR}/tcmalloc/page_allocator_interface.cc
        ${LIBRARY_DIR}/tcmalloc/page_heap.cc
        ${LIBRARY_DIR}/tcmalloc/pagemap.cc
        ${LIBRARY_DIR}/tcmalloc/parameters.cc
        ${LIBRARY_DIR}/tcmalloc/peak_heap_tracker.cc
        ${LIBRARY_DIR}/tcmalloc/sampler.cc
        ${LIBRARY_DIR}/tcmalloc/size_classes.cc
        ${LIBRARY_DIR}/tcmalloc/span.cc
        ${LIBRARY_DIR}/tcmalloc/stack_trace_table.cc
        ${LIBRARY_DIR}/tcmalloc/static_vars.cc
        ${LIBRARY_DIR}/tcmalloc/stats.cc
        ${LIBRARY_DIR}/tcmalloc/system-alloc.cc
        ${LIBRARY_DIR}/tcmalloc/thread_cache.cc
        ${LIBRARY_DIR}/tcmalloc/transfer_cache.cc

        # :experiment
        ${LIBRARY_DIR}/tcmalloc/experiment.cc
        # /internal
        ${LIBRARY_DIR}/tcmalloc/internal/logging.cc
        ${LIBRARY_DIR}/tcmalloc/internal/environment.cc
        ${LIBRARY_DIR}/tcmalloc/internal/memory_stats.cc
        ${LIBRARY_DIR}/tcmalloc/internal/proc_maps.cc
        ${LIBRARY_DIR}/tcmalloc/internal/util.cc
        # /internal:percpu
        ${LIBRARY_DIR}/tcmalloc/internal/percpu.cc
        ${LIBRARY_DIR}/tcmalloc/internal/percpu_rseq_asm.S
        ${LIBRARY_DIR}/tcmalloc/internal/percpu_rseq_unsupported.cc

        # Overrides
        ${LIBRARY_DIR}/tcmalloc/tcmalloc.cc
        ${LIBRARY_DIR}/tcmalloc/internal/mincore.cc
        ${LIBRARY_DIR}/tcmalloc/malloc_extension.cc
        ${LIBRARY_DIR}/tcmalloc/runtime_size_classes.cc
    )

    add_library (tcmalloc-cpp ${SRCS})
    # use PUBLIC linkage to allow target that is linked with tcmalloc-cpp use
    # tcmalloc functions.
    target_link_libraries (tcmalloc-cpp PUBLIC
        # abseil
        absl_algorithm
        absl_container
        absl_debugging
        absl_function_ref
        absl_hash
        absl_memory
        absl_random_distributions
        absl_random_seed_sequences
        absl_strings
        absl_str_format
        absl_synchronization
        absl_time
        absl_optional
        absl_span
        absl_base

        ## for tests only
        # absl_flags
    )

    set (DEFINITIONS)
    # for RSEQ (restartable sequences) support
    # define that we have __thread support
    list (APPEND DEFINITIONS -D_GLIBCXX_HAVE_TLS)
    # increase number of size classes and maximum allocation size that can be
    # satisfied by front-end (i.e. lock-free).
    list (APPEND DEFINITIONS -DTCMALLOC_4M_MAX_SIZE)

    # XXX: disable new/delete overrides
    #
    # This define TCMALLOC_NO_NEW_DELETE_OVERRIDES is only in the patched
    # version of tcmalloc, in upstream version both new/delete and C allocation
    # API will be overidden, but new/delete should not be overwritten due to
    # they are overwritten in new_delete.cpp
    list (APPEND DEFINITIONS -DTCMALLOC_NO_NEW_DELETE_OVERRIDES)

    target_include_directories (tcmalloc-cpp PUBLIC ${LIBRARY_DIR})

    # jemalloc compatibility
    list (APPEND DEFINITIONS -DUSE_JEMALLOC=0)
    list (APPEND DEFINITIONS -DUSE_TCMALLOC_CPP=1)

    target_compile_definitions (tcmalloc-cpp PUBLIC ${DEFINITIONS})

    target_link_libraries (allocator INTERFACE tcmalloc-cpp)

    message (STATUS "Using tcmalloc-cpp")
endif()
