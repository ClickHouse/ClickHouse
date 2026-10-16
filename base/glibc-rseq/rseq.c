#include <stddef.h>
#include "rseq.h"

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
 * Layout matches include/uapi/linux/rseq.h. */
struct kernel_rseq // NOLINT
{
    uint32_t cpu_id_start;
    uint32_t cpu_id;
    uint64_t rseq_cs;
    uint32_t flags;
    uint32_t node_id;
    uint32_t mm_cid;
} __attribute__((aligned(32)));

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-identifier"
extern const ptrdiff_t __rseq_offset __attribute__((weak)); // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
extern const unsigned int __rseq_size __attribute__((weak)); // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
#pragma clang diagnostic pop

int32_t rseq_cpu_id(void)
{
    if (&__rseq_size == NULL || __rseq_size < offsetof(struct kernel_rseq, cpu_id) + sizeof(uint32_t))
        return -1;

    const char * tp = (const char *) __builtin_thread_pointer();
    const volatile struct kernel_rseq * rseq = (const volatile struct kernel_rseq *)((uintptr_t)tp + __rseq_offset);
    return (int32_t)rseq->cpu_id;
}
