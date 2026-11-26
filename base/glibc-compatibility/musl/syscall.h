#pragma once

#include <sys/syscall.h>
#include <syscall_arch.h>

typedef long syscall_arg_t;

__attribute__((visibility("hidden")))
long __syscall_ret(unsigned long);

__attribute__((visibility("hidden")))
long __syscall(syscall_arg_t, ...);

__attribute__((visibility("hidden")))
void *__vdsosym(const char *, const char *);

#ifdef VDSO_USEFUL
/// Addresses of the vDSO functions, null until a constructor in vdso.c resolves them and makes them read-only.
struct vdso_funcs
{
	void *clock_gettime;
	void *getcpu;
};

__attribute__((visibility("hidden")))
extern const struct vdso_funcs *const __vdso_funcs;
#endif

#define syscall(...) __syscall_ret(__syscall(__VA_ARGS__))

#define socketcall(...) __syscall_ret(__socketcall(__VA_ARGS__))

#define __socketcall(nm,a,b,c,d,e,f) __syscall(SYS_##nm, a, b, c, d, e, f)

#define socketcall_cp socketcall
