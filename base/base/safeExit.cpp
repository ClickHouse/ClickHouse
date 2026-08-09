#if defined(OS_LINUX)
#    include <sys/syscall.h>
#endif
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <base/safeExit.h>
#include <base/defines.h> /// for THREAD_SANITIZER

#if defined(ADDRESS_SANITIZER)
#    include <sanitizer/lsan_interface.h>
#endif

[[noreturn]] void safeExit(int code, [[maybe_unused]] bool run_leak_check)
{
#if defined(THREAD_SANITIZER) && defined(OS_LINUX)
    /// Thread sanitizer tries to do something on exit that we don't need if we want to exit immediately,
    /// while connection handling threads are still run.
    (void)syscall(SYS_exit_group, code);
    UNREACHABLE();
#else
#    if defined(ADDRESS_SANITIZER)
    /// Run the leak check now, while all memory is still reachable through global pointers.
    /// _exit() bypasses static destructors and atexit handlers, so cleanup routines
    /// (e.g., OPENSSL_cleanup) never run, causing their global state to appear leaked
    /// at the at-exit LSan check. Calling __lsan_do_leak_check() early also disables
    /// the subsequent at-exit check, preventing these false positives.
    if (run_leak_check)
        __lsan_do_leak_check();
    else
        /// Keep this free of "<Name>Sanitizer:", which CI log parsing reads as a failure.
        fputs("Not running the leak check: other threads are still running.\n", stderr);
#    endif
    _exit(code);
#endif
}
