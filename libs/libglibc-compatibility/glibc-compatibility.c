/** Allows to build programs with libc 2.18 and run on systems with at least libc 2.11,
  *  such as Ubuntu Lucid or CentOS 6.
  *
  * Highly experimental, not recommended, disabled by default.
  *
  * Also look at http://www.lightofdawn.org/wiki/wiki.cgi/NewAppsOnOldGlibc
  *
  * If you want even older systems, such as Ubuntu Hardy,
  *  add fallocate, pipe2, __longjmp_chk, __vasprintf_chk.
  */

#if defined (__cplusplus)
extern "C" {
#endif

#include <sys/select.h>
#include <stdlib.h>

long int __fdelt_chk(long int d)
{
    if (d < 0 || d >= FD_SETSIZE)
        abort();
    return d / __NFDBITS;
}

#include <sys/poll.h>
#include <stddef.h>

int __poll_chk(struct pollfd * fds, nfds_t nfds, int timeout, size_t fdslen)
{
    if (fdslen / sizeof(*fds) < nfds)
        abort();
    return poll(fds, nfds, timeout);
}

#include <pthread.h>

size_t __pthread_get_minstack(const pthread_attr_t * attr)
{
    return 1048576;        /// This is a guess. Don't sure it is correct.
}

#include <signal.h>
#include <unistd.h>
#include <string.h>
#include <sys/syscall.h>

extern long int syscall (long int __sysno, ...) __THROW;

int __gai_sigqueue(int sig, const union sigval val, pid_t caller_pid)
{
    siginfo_t info;

    memset(&info, 0, sizeof(siginfo_t));
    info.si_signo = sig;
    info.si_code = SI_ASYNCNL;
    info.si_pid = caller_pid;
    info.si_uid = getuid();
    info.si_value = val;

    return syscall(__NR_rt_sigqueueinfo, info.si_pid, sig, &info);
}

#if defined (__cplusplus)
}
#endif
