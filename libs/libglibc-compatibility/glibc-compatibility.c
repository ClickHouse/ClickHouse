/** Allows to build programs with libc 2.18 and run on systems with at least libc 2.4,
  *  such as Ubuntu Lucid or CentOS 6.
  *
  * Also look at http://www.lightofdawn.org/wiki/wiki.cgi/NewAppsOnOldGlibc
  */

#if defined (__cplusplus)
extern "C" {
#endif

#include <pthread.h>

size_t __pthread_get_minstack(const pthread_attr_t * attr)
{
    return 1048576;        /// This is a guess. Don't sure it is correct.
}

#include <signal.h>
#include <unistd.h>
#include <string.h>
#include <sys/syscall.h>

long int syscall(long int __sysno, ...) __THROW;

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


#include <sys/select.h>
#include <stdlib.h>
#include <features.h>

#if __GLIBC__ > 2 || (__GLIBC__ == 2  && __GLIBC_MINOR__ >= 16)
long int __fdelt_chk(long int d)
{
    if (d < 0)
        abort();
#else
unsigned long int __fdelt_chk(unsigned long int d)
{
#endif
    if (d >= FD_SETSIZE)
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

#include <setjmp.h>

void musl_glibc_longjmp(jmp_buf env, int val);

/// NOTE This disables some of FORTIFY_SOURCE functionality.

void __longjmp_chk(jmp_buf env, int val)
{
    musl_glibc_longjmp(env, val);
}

#include <stdarg.h>

int vasprintf(char **s, const char *fmt, va_list ap);

int __vasprintf_chk(char **s, const char *fmt, va_list ap)
{
    return vasprintf(s, fmt, ap);
}

size_t fread(void *ptr, size_t size, size_t nmemb, void *stream);

size_t __fread_chk(void *ptr, size_t size, size_t nmemb, void *stream)
{
    return fread(ptr, size, nmemb, stream);
}

int vsscanf(const char *str, const char *format, va_list ap);

int __isoc99_vsscanf(const char *str, const char *format, va_list ap)
{
    return vsscanf(str, format, ap);
}

int sscanf(const char *restrict s, const char *restrict fmt, ...)
{
    int ret;
    va_list ap;
    va_start(ap, fmt);
    ret = vsscanf(s, fmt, ap);
    va_end(ap);
    return ret;
}

int __isoc99_sscanf(const char *str, const char *format, ...) __attribute__((weak, alias("sscanf")));

int open(const char *path, int oflag);

int __open_2(const char *path, int oflag)
{
    return open(path, oflag);
}


#if defined (__cplusplus)
}
#endif
