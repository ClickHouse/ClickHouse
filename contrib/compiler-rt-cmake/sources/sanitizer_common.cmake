# sanitizer_common — foundation library shared by all sanitizers.
#
# Source: contrib/llvm-project/compiler-rt/lib/sanitizer_common/CMakeLists.txt
#   - SANITIZER_COMMON_SOURCES        ← upstream SANITIZER_SOURCES
#   - SANITIZER_COMMON_LIBC_SOURCES   ← upstream SANITIZER_LIBCDEP_SOURCES
#   - SANITIZER_COMMON_COVERAGE_SOURCES ← upstream SANITIZER_COVERAGE_SOURCES
#   - SANITIZER_COMMON_SYMBOLIZER_SOURCES ← upstream SANITIZER_SYMBOLIZER_SOURCES
#
# We compile every per-platform .cpp file even on Linux: each of them is wrapped
# in a SANITIZER_<PLATFORM> #ifdef in the source, so e.g. sanitizer_mac.cpp
# compiles to a no-op object on Linux. This avoids drifting from upstream when
# files are added or moved between platform groups.

set(SANITIZER_COMMON_SOURCES
    sanitizer_allocator.cpp
    sanitizer_common.cpp
    sanitizer_deadlock_detector1.cpp
    sanitizer_deadlock_detector2.cpp
    sanitizer_errno.cpp
    sanitizer_file.cpp
    sanitizer_flags.cpp
    sanitizer_flag_parser.cpp
    sanitizer_fuchsia.cpp
    sanitizer_haiku.cpp
    sanitizer_libc.cpp
    sanitizer_libignore.cpp
    sanitizer_linux.cpp
    sanitizer_linux_s390.cpp
    sanitizer_mac.cpp
    sanitizer_mutex.cpp
    sanitizer_netbsd.cpp
    sanitizer_platform_limits_freebsd.cpp
    sanitizer_platform_limits_linux.cpp
    sanitizer_platform_limits_netbsd.cpp
    sanitizer_platform_limits_posix.cpp
    sanitizer_platform_limits_solaris.cpp
    sanitizer_posix.cpp
    sanitizer_printf.cpp
    sanitizer_procmaps_common.cpp
    sanitizer_procmaps_bsd.cpp
    sanitizer_procmaps_fuchsia.cpp
    sanitizer_procmaps_haiku.cpp
    sanitizer_procmaps_linux.cpp
    sanitizer_procmaps_mac.cpp
    sanitizer_procmaps_solaris.cpp
    sanitizer_range.cpp
    sanitizer_solaris.cpp
    sanitizer_stoptheworld_fuchsia.cpp
    sanitizer_stoptheworld_mac.cpp
    sanitizer_stoptheworld_win.cpp
    sanitizer_suppressions.cpp
    sanitizer_tls_get_addr.cpp
    sanitizer_thread_arg_retval.cpp
    sanitizer_thread_registry.cpp
    sanitizer_type_traits.cpp
    sanitizer_win.cpp
    sanitizer_win_interception.cpp
    sanitizer_termination.cpp
)

set(SANITIZER_COMMON_LIBC_SOURCES
    sanitizer_common_libcdep.cpp
    sanitizer_allocator_checks.cpp
    sanitizer_dl.cpp
    sanitizer_linux_libcdep.cpp
    sanitizer_mac_libcdep.cpp
    sanitizer_posix_libcdep.cpp
    sanitizer_stoptheworld_linux_libcdep.cpp
    sanitizer_stoptheworld_netbsd_libcdep.cpp
)

set(SANITIZER_COMMON_COVERAGE_SOURCES
    sancov_flags.cpp
    sanitizer_coverage_fuchsia.cpp
    sanitizer_coverage_libcdep_new.cpp
    sanitizer_coverage_win_sections.cpp
)

set(SANITIZER_COMMON_SYMBOLIZER_SOURCES
    sanitizer_allocator_report.cpp
    sanitizer_chained_origin_depot.cpp
    sanitizer_stack_store.cpp
    sanitizer_stackdepot.cpp
    sanitizer_stacktrace.cpp
    sanitizer_stacktrace_libcdep.cpp
    sanitizer_stacktrace_printer.cpp
    sanitizer_stacktrace_sparc.cpp
    sanitizer_symbolizer.cpp
    sanitizer_symbolizer_libbacktrace.cpp
    sanitizer_symbolizer_libcdep.cpp
    sanitizer_symbolizer_mac.cpp
    sanitizer_symbolizer_markup.cpp
    sanitizer_symbolizer_markup_fuchsia.cpp
    sanitizer_symbolizer_posix_libcdep.cpp
    sanitizer_symbolizer_report.cpp
    sanitizer_symbolizer_report_fuchsia.cpp
    sanitizer_symbolizer_win.cpp
    sanitizer_thread_history.cpp
    sanitizer_unwind_linux_libcdep.cpp
    sanitizer_unwind_fuchsia.cpp
    sanitizer_unwind_win.cpp
)
