#if defined(OS_LINUX)
#    include <sys/syscall.h>
#endif
#include <cstdlib>
#include <unistd.h>
#include <base/safeExit.h>
#include <base/defines.h> /// for THREAD_SANITIZER

#if defined(ADDRESS_SANITIZER)
#    include <sanitizer/lsan_interface.h>
#endif

[[noreturn]] void safeExit(int code, [[maybe_unused]] LeakCheck leak_check)
{
#if defined(THREAD_SANITIZER) && defined(OS_LINUX)
    /// Thread sanitizer tries to do something on exit that we don't need if we want to exit immediately,
    /// while connection handling threads are still run.
    (void)syscall(SYS_exit_group, code);
    UNREACHABLE();
#else
#    if defined(ADDRESS_SANITIZER)
    /// This is the only leak check that can happen: `_exit(2)` below bypasses `atexit`, so
    /// skipping here skips the check entirely rather than deferring it to LSan's at-exit handler.
    if (leak_check == LeakCheck::Run)
        /// Check now, while all memory is still reachable through global pointers: `_exit()` skips
        /// static destructors, so cleanup routines (e.g. OPENSSL_cleanup) never run and their
        /// global state would look leaked.
        __lsan_do_leak_check();
    else if (leak_check == LeakCheck::SkipAndReport)
    {
        /// write(2), not stderr: nothing here may allocate or take a lock.
        static constexpr char message[] = "Not running the leak check: other threads are still running.\n";
        auto res = write(STDERR_FILENO, message, sizeof(message) - 1);
        (void)res;
    }
#    endif
    _exit(code);
#endif
}
