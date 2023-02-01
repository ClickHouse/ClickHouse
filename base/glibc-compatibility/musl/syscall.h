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

#define syscall(...) __syscall_ret(__syscall(__VA_ARGS__))

#define socketcall(...) __syscall_ret(__socketcall(__VA_ARGS__))

#define __socketcall(nm,a,b,c,d,e,f) __syscall(SYS_##nm, a, b, c, d, e, f)

#define socketcall_cp socketcall

#ifndef __scc
#define __scc(X) ((long) (X))
typedef long syscall_arg_t;
#endif

__attribute__((visibility("hidden")))
long __syscall_ret(unsigned long),
	__syscall_cp(syscall_arg_t, syscall_arg_t, syscall_arg_t, syscall_arg_t,
	             syscall_arg_t, syscall_arg_t, syscall_arg_t);

#define __syscall1(n,a) __syscall1(n,__scc(a))
#define __syscall2(n,a,b) __syscall2(n,__scc(a),__scc(b))
#define __syscall3(n,a,b,c) __syscall3(n,__scc(a),__scc(b),__scc(c))
#define __syscall4(n,a,b,c,d) __syscall4(n,__scc(a),__scc(b),__scc(c),__scc(d))
#define __syscall5(n,a,b,c,d,e) __syscall5(n,__scc(a),__scc(b),__scc(c),__scc(d),__scc(e))
#define __syscall6(n,a,b,c,d,e,f) __syscall6(n,__scc(a),__scc(b),__scc(c),__scc(d),__scc(e),__scc(f))
#define __syscall7(n,a,b,c,d,e,f,g) __syscall7(n,__scc(a),__scc(b),__scc(c),__scc(d),__scc(e),__scc(f),__scc(g))

#define __SYSCALL_NARGS_X(a,b,c,d,e,f,g,h,n,...) n
#define __SYSCALL_NARGS(...) __SYSCALL_NARGS_X(__VA_ARGS__,7,6,5,4,3,2,1,0,)
#define __SYSCALL_CONCAT_X(a,b) a##b
#define __SYSCALL_CONCAT(a,b) __SYSCALL_CONCAT_X(a,b)
#define __SYSCALL_DISP(b,...) __SYSCALL_CONCAT(b,__SYSCALL_NARGS(__VA_ARGS__))(__VA_ARGS__)

#define __syscall_cp0(n) (__syscall_cp)(n,0,0,0,0,0,0)
#define __syscall_cp1(n,a) (__syscall_cp)(n,__scc(a),0,0,0,0,0)
#define __syscall_cp2(n,a,b) (__syscall_cp)(n,__scc(a),__scc(b),0,0,0,0)
#define __syscall_cp3(n,a,b,c) (__syscall_cp)(n,__scc(a),__scc(b),__scc(c),0,0,0)
#define __syscall_cp4(n,a,b,c,d) (__syscall_cp)(n,__scc(a),__scc(b),__scc(c),__scc(d),0,0)
#define __syscall_cp5(n,a,b,c,d,e) (__syscall_cp)(n,__scc(a),__scc(b),__scc(c),__scc(d),__scc(e),0)
#define __syscall_cp6(n,a,b,c,d,e,f) (__syscall_cp)(n,__scc(a),__scc(b),__scc(c),__scc(d),__scc(e),__scc(f))

#define __syscall_cp(...) __SYSCALL_DISP(__syscall_cp,__VA_ARGS__)
#define syscall_cp(...) __syscall_ret(__syscall_cp(__VA_ARGS__))
