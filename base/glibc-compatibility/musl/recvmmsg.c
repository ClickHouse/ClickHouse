#define _GNU_SOURCE
#include <sys/socket.h>
#include <limits.h>
#include <errno.h>
#include <time.h>
#include "syscall.h"

#define IS32BIT(x) !((x)+0x80000000ULL>>32)
#define CLAMP(x) (int)(IS32BIT(x) ? (x) : 0x7fffffffU+((0ULL+(x))>>63))

hidden void __convert_scm_timestamps(struct msghdr *, socklen_t);

int recvmmsg(int fd, struct mmsghdr *msgvec, unsigned int vlen, unsigned int flags, struct timespec *timeout)
{
#if LONG_MAX > INT_MAX
	struct mmsghdr *mh = msgvec;
	unsigned int i;
	for (i = vlen; i; i--, mh++)
		mh->msg_hdr.__pad1 = mh->msg_hdr.__pad2 = 0;
#endif
#ifdef SYS_recvmmsg_time64
	time_t s = timeout ? timeout->tv_sec : 0;
	long ns = timeout ? timeout->tv_nsec : 0;
	int r = __syscall_cp(SYS_recvmmsg_time64, fd, msgvec, vlen, flags,
			timeout ? ((long long[]){s, ns}) : 0);
	if (SYS_recvmmsg == SYS_recvmmsg_time64 || r!=-ENOSYS)
		return __syscall_ret(r);
	if (vlen > IOV_MAX) vlen = IOV_MAX;
	socklen_t csize[vlen];
	for (int i=0; i<vlen; i++) csize[i] = msgvec[i].msg_hdr.msg_controllen;
	r = __syscall_cp(SYS_recvmmsg, fd, msgvec, vlen, flags,
		timeout ? ((long[]){CLAMP(s), ns}) : 0);
	for (int i=0; i<r; i++)
		__convert_scm_timestamps(&msgvec[i].msg_hdr, csize[i]);
	return __syscall_ret(r);
#else
	return syscall_cp(SYS_recvmmsg, fd, msgvec, vlen, flags, timeout);
#endif
}
