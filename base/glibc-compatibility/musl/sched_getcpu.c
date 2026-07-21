#define _GNU_SOURCE
#include <errno.h>
#include <sched.h>
#include <stdint.h>
#include <stddef.h>
#include "syscall.h"
#include "atomic.h"

#if defined(__has_feature)
#if __has_feature(memory_sanitizer)
#include <sanitizer/msan_interface.h>
#endif
#endif

/* Reuse glibc's per-thread rseq registration when available.
 *
 * Since glibc 2.35 every thread is registered with the kernel rseq syscall
 * during thread setup, and the offset/size of the area inside the TCB is
 * exposed via the weak symbols below. Reading `cpu_id` from there is what
 * glibc's own `sched_getcpu` does - a single TLS load, no syscall.
 *
 * On older glibc these symbols resolve to NULL/0 and we fall back to the
 * vDSO/`getcpu` syscall path. We deliberately do *not* call rseq() ourselves:
 * the runtime libc is glibc.
 *
 * musl does not expose <linux/rseq.h>, so the kernel ABI struct is bundled
 * here. Layout matches include/uapi/linux/rseq.h. */
struct kernel_rseq
{
    uint32_t cpu_id_start;
    uint32_t cpu_id;
    uint64_t rseq_cs;
    uint32_t flags;
    uint32_t node_id;
    uint32_t mm_cid;
} __attribute__((aligned(32)));

extern const ptrdiff_t __rseq_offset __attribute__((weak));
extern const unsigned int __rseq_size __attribute__((weak));

static inline int read_cpu_id_from_glibc_rseq(uint32_t *out)
{
    /* Weak symbols: address is NULL when glibc doesn't provide rseq.
     * `__rseq_size` is set to 0 by glibc if kernel registration failed. */
    if (&__rseq_size == NULL || __rseq_size < offsetof(struct kernel_rseq, cpu_id) + sizeof(uint32_t))
        return 0;

    const char * tp = (const char *) __builtin_thread_pointer();
    const volatile struct kernel_rseq * rseq = (const volatile struct kernel_rseq *)(tp + __rseq_offset);
    /* The kernel uses negative sentinels in this field: -1 (UNINITIALIZED)
     * and -2 (REGISTRATION_FAILED). Reject all negative values, like glibc. */
    int32_t cpu = (int32_t) rseq->cpu_id;
    if (cpu < 0)
        return 0;
    *out = (uint32_t) cpu;
    return 1;
}

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
	unsigned cpu = 0;

	{
		uint32_t c;
		if (read_cpu_id_from_glibc_rseq(&c))
			return (int)c;
	}

#ifdef VDSO_GETCPU_SYM
	getcpu_f f = (getcpu_f)vdso_func;
	if (f) {
		r = f(&cpu, 0, 0);
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
