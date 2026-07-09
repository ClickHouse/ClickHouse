if ((SANITIZE AND NOT SANITIZE STREQUAL "undefined") OR NOT (
    ((OS_LINUX OR OS_FREEBSD) AND (ARCH_AMD64 OR ARCH_AARCH64 OR ARCH_PPC64LE OR ARCH_RISCV64 OR ARCH_S390X OR ARCH_E2K)) OR
    (OS_DARWIN AND (CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO" OR CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG"))
))
    if (ENABLE_JEMALLOC)
        message (${RECONFIGURE_MESSAGE_LEVEL}
                 "jemalloc is disabled implicitly: it doesn't work with sanitizers (except UBSan) and can only be used with x86_64, aarch64, or ppc64le Linux or FreeBSD builds and RelWithDebInfo macOS builds. Use -DENABLE_JEMALLOC=0")
    endif ()
    set (ENABLE_JEMALLOC OFF)
else ()
    option (ENABLE_JEMALLOC "Enable jemalloc allocator" ${ENABLE_LIBRARIES})
endif ()

if (NOT ENABLE_JEMALLOC)
    message (STATUS "Not using jemalloc")
    return()
endif ()

# jemalloc runtime config (malloc_conf). Options common to all platforms:
#   muzzy_decay_ms:0           Release muzzy pages to the OS immediately (we tune via dirty decay only).
#   dirty_decay_ms:5000        Purge dirty pages 5s after they go idle, off the allocation path.
#   background_thread:true     Purge on jemalloc's own threads rather than from application allocations,
#                              keeping RSS bounded without eager (per-free) purging.
#   lg_extent_max_active_fit:8 Reuse an extent for requests up to 2^8 (256x) smaller; default 6 (64x).
#                              Lets ClickBench Q35's 256 coalesced 16KB sub-hashtables be split and
#                              reused instead of faulting via MADV_DONTNEED.
#                              https://github.com/ClickHouse/ClickHouse/pull/80245
#                              https://github.com/jemalloc/jemalloc/pull/2842
# Linux and macOS:
#   prof:true,prof_active:true,prof_thread_active_init:false
#                              Compile in and globally arm sampling but leave it off per-thread, so it can
#                              be switched on at runtime (per-thread or globally). Profiling is compiled in
#                              (JEMALLOC_PROF=1) and linked against libunwind on every platform below, so
#                              arming it at runtime is what makes the allocation profiler usable. Left off
#                              on FreeBSD, which has no perf coverage to validate the change.
#   percpu_arena:percpu        Cap arenas at the CPU count. Otherwise each thread keeps its own arena
#                              (up to 4*CPU) and holds that memory while idle, bloating RSS. Needs a
#                              current-CPU query: sched_getcpu on Linux (a glibc extension). macOS has no
#                              sched_getcpu, so jemalloc reads the CPU number from TPIDR_EL0 (arm64) or
#                              sidt (x86) like Apple's _os_cpu_number, verified to spread across all CPUs
#                              on Apple Silicon. https://github.com/jemalloc/jemalloc/pull/2953
#                              FreeBSD has no working query and keeps it off.
#
# Linux-only:
#   oversize_threshold:N       Route large allocations (>= N) to jemalloc's dedicated oversize arena,
#                              isolating big transient buffers (e.g. large hash tables) from the per-CPU
#                              arenas. 64MiB on Linux: fewer page faults on hashtable-heavy queries and
#                              ~2x less retained virtual address space; explicit arenas are not affected.
#                              https://github.com/ClickHouse/ClickHouse/pull/103958
#                              0 disables the arena. Left disabled on non-Linux: the win relies on THP
#                              madvise on the oversize arena (JEMALLOC_HAVE_MADVISE_HUGE, absent on macOS)
#                              and is unvalidated there.
if (OS_LINUX)
    set (JEMALLOC_CONFIG_MALLOC_CONF "percpu_arena:percpu,oversize_threshold:67108864,muzzy_decay_ms:0,dirty_decay_ms:5000,prof:true,prof_active:true,prof_thread_active_init:false,background_thread:true,lg_extent_max_active_fit:8")
elseif (OS_DARWIN)
    # background_thread is unavailable in upstream jemalloc on Darwin; we enable it via the Darwin config
    # headers (include_darwin_*/jemalloc/internal/jemalloc_internal_defs.h.in), where the full rationale
    # lives (it requires switching malloc_mutex_t off os_unfair_lock and the nstime clock off Mach uptime).
    # Previously this used dirty_decay_ms:0 (eager per-free purging) to bound RSS, under the wrong
    # assumption that background_thread was unavailable; see
    # https://github.com/ClickHouse/ClickHouse/issues/108429
    # percpu_arena relies on malloc_getcpu, which reads the CPU number via TPIDR_EL0/sidt on macOS (see above).
    set (JEMALLOC_CONFIG_MALLOC_CONF "percpu_arena:percpu,oversize_threshold:0,muzzy_decay_ms:0,dirty_decay_ms:5000,prof:true,prof_active:true,prof_thread_active_init:false,background_thread:true,lg_extent_max_active_fit:8")
else()
    # FreeBSD and any other non-Linux target. background_thread is supported out of the box.
    set (JEMALLOC_CONFIG_MALLOC_CONF "oversize_threshold:0,muzzy_decay_ms:0,dirty_decay_ms:5000,background_thread:true,lg_extent_max_active_fit:8")
endif()
# CACHE variable is empty to allow changing defaults without the necessity
# to purge cache
set (JEMALLOC_CONFIG_MALLOC_CONF_OVERRIDE "" CACHE STRING "Change default configuration string of JEMalloc" )
if (JEMALLOC_CONFIG_MALLOC_CONF_OVERRIDE)
    set (JEMALLOC_CONFIG_MALLOC_CONF "${JEMALLOC_CONFIG_MALLOC_CONF_OVERRIDE}")
endif()
message (STATUS "jemalloc malloc_conf: ${JEMALLOC_CONFIG_MALLOC_CONF}")

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/jemalloc")

set (SRCS
    "${LIBRARY_DIR}/src/arena.c"
    "${LIBRARY_DIR}/src/background_thread.c"
    "${LIBRARY_DIR}/src/base.c"
    "${LIBRARY_DIR}/src/bin.c"
    "${LIBRARY_DIR}/src/bin_info.c"
    "${LIBRARY_DIR}/src/bitmap.c"
    "${LIBRARY_DIR}/src/buf_writer.c"
    "${LIBRARY_DIR}/src/cache_bin.c"
    "${LIBRARY_DIR}/src/ckh.c"
    "${LIBRARY_DIR}/src/conf.c"
    "${LIBRARY_DIR}/src/counter.c"
    "${LIBRARY_DIR}/src/ctl.c"
    "${LIBRARY_DIR}/src/decay.c"
    "${LIBRARY_DIR}/src/div.c"
    "${LIBRARY_DIR}/src/ecache.c"
    "${LIBRARY_DIR}/src/edata.c"
    "${LIBRARY_DIR}/src/edata_cache.c"
    "${LIBRARY_DIR}/src/ehooks.c"
    "${LIBRARY_DIR}/src/emap.c"
    "${LIBRARY_DIR}/src/eset.c"
    "${LIBRARY_DIR}/src/exp_grow.c"
    "${LIBRARY_DIR}/src/extent.c"
    "${LIBRARY_DIR}/src/extent_dss.c"
    "${LIBRARY_DIR}/src/extent_mmap.c"
    "${LIBRARY_DIR}/src/fxp.c"
    "${LIBRARY_DIR}/src/hook.c"
    "${LIBRARY_DIR}/src/hpa.c"
    "${LIBRARY_DIR}/src/hpa_central.c"
    "${LIBRARY_DIR}/src/hpa_hooks.c"
    "${LIBRARY_DIR}/src/hpa_utils.c"
    "${LIBRARY_DIR}/src/hpdata.c"
    "${LIBRARY_DIR}/src/inspect.c"
    "${LIBRARY_DIR}/src/jemalloc.c"
    "${LIBRARY_DIR}/src/large.c"
    "${LIBRARY_DIR}/src/log.c"
    "${LIBRARY_DIR}/src/malloc_io.c"
    "${LIBRARY_DIR}/src/mutex.c"
    "${LIBRARY_DIR}/src/nstime.c"
    "${LIBRARY_DIR}/src/pa.c"
    "${LIBRARY_DIR}/src/pac.c"
    "${LIBRARY_DIR}/src/pa_extra.c"
    "${LIBRARY_DIR}/src/pages.c"
    "${LIBRARY_DIR}/src/peak_event.c"
    "${LIBRARY_DIR}/src/prof.c"
    "${LIBRARY_DIR}/src/prof_data.c"
    "${LIBRARY_DIR}/src/prof_log.c"
    "${LIBRARY_DIR}/src/prof_recent.c"
    "${LIBRARY_DIR}/src/prof_stats.c"
    "${LIBRARY_DIR}/src/prof_stack_range.c"
    "${LIBRARY_DIR}/src/prof_sys.c"
    "${LIBRARY_DIR}/src/psset.c"
    "${LIBRARY_DIR}/src/rtree.c"
    "${LIBRARY_DIR}/src/safety_check.c"
    "${LIBRARY_DIR}/src/san_bump.c"
    "${LIBRARY_DIR}/src/san.c"
    "${LIBRARY_DIR}/src/sc.c"
    "${LIBRARY_DIR}/src/sec.c"
    "${LIBRARY_DIR}/src/stats.c"
    "${LIBRARY_DIR}/src/sz.c"
    "${LIBRARY_DIR}/src/tcache.c"
    "${LIBRARY_DIR}/src/test_hooks.c"
    "${LIBRARY_DIR}/src/thread_event.c"
    "${LIBRARY_DIR}/src/thread_event_registry.c"
    "${LIBRARY_DIR}/src/ticker.c"
    "${LIBRARY_DIR}/src/tsd.c"
    "${LIBRARY_DIR}/src/util.c"
    "${LIBRARY_DIR}/src/witness.c"
)
if (OS_DARWIN)
    list(APPEND SRCS "${LIBRARY_DIR}/src/zone.c")
endif ()

add_library(_jemalloc ${SRCS})

# First include jemalloc-cmake files, to override anything that jemalloc has.
# (for example if you were trying to build jemalloc inside contrib/jemalloc you
# will have some files that may be out of date)
target_include_directories(_jemalloc SYSTEM PUBLIC include)
target_include_directories(_jemalloc PRIVATE "${LIBRARY_DIR}/include")

set (JEMALLOC_INCLUDE_PREFIX)
# OS_
if (OS_LINUX)
    set (JEMALLOC_INCLUDE_PREFIX "include_linux")
elseif (OS_FREEBSD)
    set (JEMALLOC_INCLUDE_PREFIX "include_freebsd")
elseif (OS_DARWIN)
    set (JEMALLOC_INCLUDE_PREFIX "include_darwin")
else ()
    message (FATAL_ERROR "internal jemalloc: This OS is not supported")
endif ()
# ARCH_
if (ARCH_AMD64)
    if (USE_MUSL)
        set(JEMALLOC_INCLUDE_PREFIX "${JEMALLOC_INCLUDE_PREFIX}_x86_64_musl")
    else()
        set(JEMALLOC_INCLUDE_PREFIX "${JEMALLOC_INCLUDE_PREFIX}_x86_64")
    endif()
elseif (ARCH_AARCH64)
    set(JEMALLOC_INCLUDE_PREFIX "${JEMALLOC_INCLUDE_PREFIX}_aarch64")
elseif (ARCH_PPC64LE)
    set(JEMALLOC_INCLUDE_PREFIX "${JEMALLOC_INCLUDE_PREFIX}_ppc64le")
elseif (ARCH_RISCV64)
    set(JEMALLOC_INCLUDE_PREFIX "${JEMALLOC_INCLUDE_PREFIX}_riscv64")
elseif (ARCH_S390X)
    set(JEMALLOC_INCLUDE_PREFIX "${JEMALLOC_INCLUDE_PREFIX}_s390x")
elseif (ARCH_E2K)
    set(JEMALLOC_INCLUDE_PREFIX "${JEMALLOC_INCLUDE_PREFIX}_e2k")
else ()
    message (FATAL_ERROR "internal jemalloc: This arch is not supported")
endif ()

configure_file(${JEMALLOC_INCLUDE_PREFIX}/jemalloc/internal/jemalloc_internal_defs.h.in
    ${JEMALLOC_INCLUDE_PREFIX}/jemalloc/internal/jemalloc_internal_defs.h)
target_include_directories(_jemalloc SYSTEM PRIVATE
    "${CMAKE_CURRENT_BINARY_DIR}/${JEMALLOC_INCLUDE_PREFIX}/jemalloc/internal")

# This is only about private functions, and we cannot use private namespace since we do not provide private_namespace.h
target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_NO_PRIVATE_NAMESPACE)
# Do not rename je_malloc to malloc (and friends)
target_compile_definitions(_jemalloc INTERFACE -DJEMALLOC_NO_RENAME)

# Because our coverage callbacks call malloc, and recursive call of malloc could not work.
target_compile_options(_jemalloc PRIVATE ${WITHOUT_COVERAGE_FLAGS_LIST})

target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_PROF=1)

# jemalloc provides support two unwind flavors:
# - JEMALLOC_PROF_LIBUNWIND - unw_backtrace() - gnu libunwind (compatible with llvm libunwind)
# - JEMALLOC_PROF_LIBGCC - _Unwind_Backtrace() - the original HP libunwind and the one coming with gcc / g++ / libstdc++.
#
# But for JEMALLOC_PROF_LIBGCC it also calls _Unwind_Backtrace() during
# bootstraping of jemalloc, which may lead to deadlock, if the dlsym will do
# allocations somewhere (like glibc does prio 2.34, see [1]).
#
#  [1]: https://sourceware.org/git/?p=glibc.git;a=commit;h=fada9018199c21c469ff0e731ef75c6020074ac9
#
# And since ClickHouse unwind already supports unw_backtrace() we can safely
# switch to it to avoid this deadlock.
target_compile_definitions (_jemalloc PRIVATE -DJEMALLOC_PROF_LIBUNWIND=1)
target_link_libraries (_jemalloc PRIVATE unwind)

# for RTLD_NEXT
target_compile_options(_jemalloc PRIVATE -D_GNU_SOURCE)

add_library(ch_contrib::jemalloc ALIAS _jemalloc)
