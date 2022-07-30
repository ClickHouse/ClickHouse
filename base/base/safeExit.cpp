#if defined(OS_LINUX)
#    include <sys/syscall.h>
#endif
#include <unistd.h>
#include <base/safeExit.h>
#include <base/defines.h>

[[noreturn]] void safeExit(int code)
{
#if defined(THREAD_SANITIZER) && defined(OS_LINUX)
    /// Thread sanitizer tries to do something on exit that we don't need if we want to exit immediately,
    /// while connection handling threads are still run.
    (void)syscall(SYS_exit_group, code);
    __builtin_unreachable();
#else
    _exit(code);
#endif
}
