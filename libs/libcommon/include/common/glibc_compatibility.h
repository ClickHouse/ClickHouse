/// Include this file with -include compiler parameter.
/// And add -Wl,--wrap=memcpy for linking.


#if defined (__cplusplus)
extern "C" {
#endif


#include <stdlib.h>
#include <sys/select.h>

inline long int __fdelt_chk(long int d)
{
	if (d < 0 || d >= FD_SETSIZE)
		abort();
	return d / __NFDBITS;
}

#include <sys/poll.h>

inline int __poll_chk(struct pollfd * fds, nfds_t nfds, int timeout, __SIZE_TYPE__ fdslen)
{
	if (fdslen / sizeof(*fds) < nfds)
		abort();
	return poll(fds, nfds, timeout);
}


void * __memcpy_glibc_2_2_5(void *, const void *, size_t);

__asm__(".symver __memcpy_glibc_2_2_5, memcpy@GLIBC_2.2.5");

inline void * __wrap_memcpy(void * dest, const void * src, size_t n)
{
    return __memcpy_glibc_2_2_5(dest, src, n);
}


#if defined (__cplusplus)
}
#endif
