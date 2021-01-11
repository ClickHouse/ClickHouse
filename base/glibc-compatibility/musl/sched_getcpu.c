#define _GNU_SOURCE
#include <errno.h>
#include <sched.h>
#include "syscall.h"
#include "atomic.h"

#ifdef VDSO_GETCPU_SYM

static void *volatile vdso_func;

typedef long (*getcpu_f)(unsigned *, unsigned *, void *);

static long getcpu_init(unsigned *cpu, unsigned *node, void *unused)
{
	void *p = __vdsosym(VDSO_GETCPU_VER, VDSO_GETCPU_SYM);
	getcpu_f f = (getcpu_f)p;
	a_cas_p(&vdso_func, (void *)getcpu_init, p);
	return f ? f(cpu, node, unused) : -ENOSYS;
}

static void *volatile vdso_func = (void *)getcpu_init;

#endif

int sched_getcpu(void)
{
	int r;
	unsigned cpu;

#ifdef VDSO_GETCPU_SYM
	getcpu_f f = (getcpu_f)vdso_func;
	if (f) {
		r = f(&cpu, 0, 0);
		if (!r) return cpu;
		if (r != -ENOSYS) return __syscall_ret(r);
	}
#endif

	r = __syscall(SYS_getcpu, &cpu, 0, 0);
	if (!r) return cpu;
	return __syscall_ret(r);
}
