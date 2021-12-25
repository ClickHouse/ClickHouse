#ifndef _UCONTEXT_H
#define _UCONTEXT_H
#ifdef __cplusplus
extern "C" {
#endif

#include <features.h>

#include <signal.h>

#if defined(_GNU_SOURCE) || defined(_BSD_SOURCE)
#define NGREG (sizeof(gregset_t)/sizeof(greg_t))
#endif

struct __ucontext;

int  getcontext(struct __ucontext *);
void makecontext(struct __ucontext *, void (*)(), int, ...);
int  setcontext(const struct __ucontext *);
int  swapcontext(struct __ucontext *, const struct __ucontext *);

#ifdef __cplusplus
}
#endif
#endif
