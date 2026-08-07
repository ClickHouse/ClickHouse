#include <errno.h>
#include <stdint.h>
#include <time.h>
#include "atomic.h"
#include "syscall.h"

#ifdef VDSO_CGT_SYM

static void *volatile vdso_func;

#ifdef VDSO_CGT32_SYM
static void *volatile vdso_func_32;
static int cgt_time32_wrap(clockid_t clk, struct timespec *ts)
{
	long ts32[2];
	int (*f)(clockid_t, long[2]) =
		(int (*)(clockid_t, long[2]))vdso_func_32;
	int r = f(clk, ts32);
	if (!r) {
		/* Fallback to syscalls if time32 overflowed. Maybe
		 * we lucked out and somehow migrated to a kernel with
		 * time64 syscalls available. */
		if (ts32[0] < 0) {
			a_cas_p(&vdso_func, (void *)cgt_time32_wrap, 0);
			return -ENOSYS;
		}
		ts->tv_sec = ts32[0];
		ts->tv_nsec = ts32[1];
	}
	return r;
}
#endif

static int cgt_init(clockid_t clk, struct timespec *ts)
{
	void *p = __vdsosym(VDSO_CGT_VER, VDSO_CGT_SYM);
#ifdef VDSO_CGT32_SYM
	if (!p) {
		void *q = __vdsosym(VDSO_CGT32_VER, VDSO_CGT32_SYM);
		if (q) {
			a_cas_p(&vdso_func_32, 0, q);
			p = cgt_time32_wrap;
		}
	}
#endif
	int (*f)(clockid_t, struct timespec *) =
		(int (*)(clockid_t, struct timespec *))p;
	a_cas_p(&vdso_func, (void *)cgt_init, p);
	return f ? f(clk, ts) : -ENOSYS;
}

static void *volatile vdso_func = (void *)cgt_init;

#endif

int clock_gettime(clockid_t clk, struct timespec *ts)
{
	int r;

#ifdef VDSO_CGT_SYM
	int (*f)(clockid_t, struct timespec *) =
		(int (*)(clockid_t, struct timespec *))vdso_func;
	if (f) {
		r = f(clk, ts);
		if (!r) return r;
		if (r == -EINVAL) return __syscall_ret(r);
		/* Fall through on errors other than EINVAL. Some buggy
		 * vdso implementations return ENOSYS for clocks they
		 * can't handle, rather than making the syscall. This
		 * also handles the case where cgt_init fails to find
		 * a vdso function to use. */
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
