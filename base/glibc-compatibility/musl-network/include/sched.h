#ifndef _SCHED_H
#define _SCHED_H
#ifdef __cplusplus
extern "C" {
#endif

#include <features.h>

#define __NEED_struct_timespec
#define __NEED_pid_t
#define __NEED_time_t

#ifdef _GNU_SOURCE
#define __NEED_size_t
#endif

#include <bits/alltypes.h>

struct sched_param {
	int sched_priority;
	int __reserved1;
#if _REDIR_TIME64
	long __reserved2[4];
#else
	struct {
		time_t __reserved1;
		long __reserved2;
	} __reserved2[2];
#endif
	int __reserved3;
};

int    sched_get_priority_max(int);
int    sched_get_priority_min(int);
int    sched_getparam(pid_t, struct sched_param *);
int    sched_getscheduler(pid_t);
int    sched_rr_get_interval(pid_t, struct timespec *);
int    sched_setparam(pid_t, const struct sched_param *);
int    sched_setscheduler(pid_t, int, const struct sched_param *);
int     sched_yield(void);

#define SCHED_OTHER 0
#define SCHED_FIFO 1
#define SCHED_RR 2
#define SCHED_BATCH 3
#define SCHED_IDLE 5
#define SCHED_DEADLINE 6
#define SCHED_RESET_ON_FORK 0x40000000

#ifdef _GNU_SOURCE
#define CSIGNAL		0x000000ff
#define CLONE_NEWTIME	0x00000080
#define CLONE_VM	0x00000100
#define CLONE_FS	0x00000200
#define CLONE_FILES	0x00000400
#define CLONE_SIGHAND	0x00000800
#define CLONE_PIDFD	0x00001000
#define CLONE_PTRACE	0x00002000
#define CLONE_VFORK	0x00004000
#define CLONE_PARENT	0x00008000
#define CLONE_THREAD	0x00010000
#define CLONE_NEWNS	0x00020000
#define CLONE_SYSVSEM	0x00040000
#define CLONE_SETTLS	0x00080000
#define CLONE_PARENT_SETTID	0x00100000
#define CLONE_CHILD_CLEARTID	0x00200000
#define CLONE_DETACHED	0x00400000
#define CLONE_UNTRACED	0x00800000
#define CLONE_CHILD_SETTID	0x01000000
#define CLONE_NEWCGROUP	0x02000000
#define CLONE_NEWUTS	0x04000000
#define CLONE_NEWIPC	0x08000000
#define CLONE_NEWUSER	0x10000000
#define CLONE_NEWPID	0x20000000
#define CLONE_NEWNET	0x40000000
#define CLONE_IO	0x80000000
int clone (int (*)(void *), void *, int, void *, ...);
int unshare(int);
int setns(int, int);

void *memcpy(void *__restrict, const void *__restrict, size_t);
int memcmp(const void *, const void *, size_t);
void *memset (void *, int, size_t);
void *calloc(size_t, size_t);
void free(void *);

typedef struct cpu_set_t { unsigned long __bits[128/sizeof(long)]; } cpu_set_t;
int __sched_cpucount(size_t, const cpu_set_t *);
int sched_getcpu(void);
int sched_getaffinity(pid_t, size_t, cpu_set_t *);
int sched_setaffinity(pid_t, size_t, const cpu_set_t *);

#define __CPU_op_S(i, size, set, op) ( (i)/8U >= (size) ? 0 : \
	(((unsigned long *)(set))[(i)/8/sizeof(long)] op (1UL<<((i)%(8*sizeof(long))))) )

#define CPU_SET_S(i, size, set) __CPU_op_S(i, size, set, |=)
#define CPU_CLR_S(i, size, set) __CPU_op_S(i, size, set, &=~)
#define CPU_ISSET_S(i, size, set) __CPU_op_S(i, size, set, &)

#define __CPU_op_func_S(func, op) \
static __inline void __CPU_##func##_S(size_t __size, cpu_set_t *__dest, \
	const cpu_set_t *__src1, const cpu_set_t *__src2) \
{ \
	size_t __i; \
	for (__i=0; __i<__size/sizeof(long); __i++) \
		((unsigned long *)__dest)[__i] = ((unsigned long *)__src1)[__i] \
			op ((unsigned long *)__src2)[__i] ; \
}

__CPU_op_func_S(AND, &)
__CPU_op_func_S(OR, |)
__CPU_op_func_S(XOR, ^)

#define CPU_AND_S(a,b,c,d) __CPU_AND_S(a,b,c,d)
#define CPU_OR_S(a,b,c,d) __CPU_OR_S(a,b,c,d)
#define CPU_XOR_S(a,b,c,d) __CPU_XOR_S(a,b,c,d)

#define CPU_COUNT_S(size,set) __sched_cpucount(size,set)
#define CPU_ZERO_S(size,set) memset(set,0,size)
#define CPU_EQUAL_S(size,set1,set2) (!memcmp(set1,set2,size))

#define CPU_ALLOC_SIZE(n) (sizeof(long) * ( (n)/(8*sizeof(long)) \
	+ ((n)%(8*sizeof(long)) + 8*sizeof(long)-1)/(8*sizeof(long)) ) )
#define CPU_ALLOC(n) ((cpu_set_t *)calloc(1,CPU_ALLOC_SIZE(n)))
#define CPU_FREE(set) free(set)

#define CPU_SETSIZE 128

#define CPU_SET(i, set) CPU_SET_S(i,sizeof(cpu_set_t),set)
#define CPU_CLR(i, set) CPU_CLR_S(i,sizeof(cpu_set_t),set)
#define CPU_ISSET(i, set) CPU_ISSET_S(i,sizeof(cpu_set_t),set)
#define CPU_AND(d,s1,s2) CPU_AND_S(sizeof(cpu_set_t),d,s1,s2)
#define CPU_OR(d,s1,s2) CPU_OR_S(sizeof(cpu_set_t),d,s1,s2)
#define CPU_XOR(d,s1,s2) CPU_XOR_S(sizeof(cpu_set_t),d,s1,s2)
#define CPU_COUNT(set) CPU_COUNT_S(sizeof(cpu_set_t),set)
#define CPU_ZERO(set) CPU_ZERO_S(sizeof(cpu_set_t),set)
#define CPU_EQUAL(s1,s2) CPU_EQUAL_S(sizeof(cpu_set_t),s1,s2)

#endif

#if _REDIR_TIME64
__REDIR(sched_rr_get_interval, __sched_rr_get_interval_time64);
#endif

#ifdef __cplusplus
}
#endif
#endif
