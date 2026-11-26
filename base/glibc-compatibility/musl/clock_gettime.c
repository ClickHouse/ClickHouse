#include <errno.h>
#include <stdint.h>
#include <time.h>
#include "syscall.h"

#ifdef VDSO_CGT_SYM
/// Not checked by CFI: the target is in the vDSO, and its address is read-only (see vdso.c).
__attribute__((no_sanitize("cfi-icall")))
static int vdso_clock_gettime(void *f, clockid_t clk, struct timespec *ts)
{
	return ((int (*)(clockid_t, struct timespec *))f)(clk, ts);
}
#endif

int clock_gettime(clockid_t clk, struct timespec *ts)
{
	int r;

#ifdef VDSO_CGT_SYM
	void *f = __vdso_funcs->clock_gettime;
	if (f) {
		r = vdso_clock_gettime(f, clk, ts);
		if (!r) return r;
		if (r == -EINVAL) return __syscall_ret(r);
		/* Fall through on errors other than EINVAL. Some buggy
		 * vdso implementations return ENOSYS for clocks they
		 * can't handle, rather than making the syscall. */
	}
#endif

#ifdef SYS_clock_gettime64
	r = -ENOSYS;
	if (sizeof(time_t) > 4)
		r = __syscall(SYS_clock_gettime64, clk, ts);
	if (SYS_clock_gettime == SYS_clock_gettime64 || r!=-ENOSYS)
		return __syscall_ret(r);
	long ts32[2];
	r = __syscall(SYS_clock_gettime, clk, ts32);
	if (r==-ENOSYS && clk==CLOCK_REALTIME) {
		r = __syscall(SYS_gettimeofday, ts32, 0);
		ts32[1] *= 1000;
	}
	if (!r) {
		ts->tv_sec = ts32[0];
		ts->tv_nsec = ts32[1];
		return r;
	}
	return __syscall_ret(r);
#else
	r = __syscall(SYS_clock_gettime, clk, ts);
	if (r == -ENOSYS) {
		if (clk == CLOCK_REALTIME) {
			__syscall(SYS_gettimeofday, ts, 0);
			ts->tv_nsec = (int)ts->tv_nsec * 1000;
			return 0;
		}
		r = -EINVAL;
	}
	return __syscall_ret(r);
#endif
}
