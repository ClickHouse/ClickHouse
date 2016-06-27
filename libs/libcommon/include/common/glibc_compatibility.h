/// Include this file with -include compiler parameter.
/// And add -Wl,--wrap=memcpy for linking.


#if defined (__cplusplus)
extern "C" {
#endif


#include <stdlib.h>
#include <sys/select.h>

__attribute__((__weak__)) long int __fdelt_chk(long int d)
{
	if (d < 0 || d >= FD_SETSIZE)
		abort();
	return d / __NFDBITS;
}

#include <sys/poll.h>

__attribute__((__weak__)) int __poll_chk(struct pollfd * fds, nfds_t nfds, int timeout, __SIZE_TYPE__ fdslen)
{
	if (fdslen / sizeof(*fds) < nfds)
		abort();
	return poll(fds, nfds, timeout);
}


__attribute__((__weak__)) void * __memcpy_glibc_2_2_5(void *, const void *, size_t);

__asm__(".symver __memcpy_glibc_2_2_5, memcpy@GLIBC_2.2.5");

__attribute__((__weak__)) void * __wrap_memcpy(void * dest, const void * src, size_t n)
{
    return __memcpy_glibc_2_2_5(dest, src, n);
}


__attribute__((__weak__)) size_t __pthread_get_minstack(const pthread_attr_t * attr)
{
	return 1048576;
}

#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <sys/syscall.h>

__attribute__((__weak__)) int __gai_sigqueue(int sig, const union sigval val, pid_t caller_pid)
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
