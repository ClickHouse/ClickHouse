#define _GNU_SOURCE
#include <errno.h>
#include <sched.h>
#include <stdint.h>
#include <stddef.h>
#include "syscall.h"

#if defined(__has_feature)
#if __has_feature(memory_sanitizer)
#include <sanitizer/msan_interface.h>
#endif
#endif

#ifdef VDSO_GETCPU_SYM
/// Not checked by CFI: the target is in the vDSO, and its address is read-only (see vdso.c).
__attribute__((no_sanitize("cfi-icall")))
static long vdso_getcpu(void *f, unsigned *cpu, unsigned *node, void *unused)
{
	return ((long (*)(unsigned *, unsigned *, void *))f)(cpu, node, unused);
}
#endif

extern int32_t rseq_cpu_id(void);

int sched_getcpu(void)
{
	{
		int32_t rseq_cpu = rseq_cpu_id();
		if (rseq_cpu >= 0)
			return rseq_cpu;
	}

	int r;
	unsigned cpu = 0;
#ifdef VDSO_GETCPU_SYM
	void *f = __vdso_funcs->getcpu;
	if (f) {
		r = vdso_getcpu(f, &cpu, 0, 0);
		if (!r) return cpu;
		if (r != -ENOSYS) return __syscall_ret(r);
	}
#endif

	r = __syscall(SYS_getcpu, &cpu, 0, 0);
	if (!r) {
#if defined(__has_feature)
#if __has_feature(memory_sanitizer)
		__msan_unpoison(&cpu, sizeof(cpu));
#endif
#endif
		return cpu;
	}
	return __syscall_ret(r);
}
