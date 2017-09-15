#pragma once

/** Allows to build programs with libc 2.18 and run on systems with at least libc 2.11,
  *  such as Ubuntu Lucid or CentOS 6.
  *
  * Highly experimental, not recommended, disabled by default.
  *
  * Also look at http://www.lightofdawn.org/wiki/wiki.cgi/NewAppsOnOldGlibc
  *
  * If you want even older systems, such as Ubuntu Hardy,
  *  add fallocate, pipe2, __longjmp_chk, __vasprintf_chk.
  */

#if defined (__cplusplus)
extern "C" {
#endif


__attribute__((__weak__)) long int __fdelt_chk(long int d);

#include <sys/poll.h>
#include <stddef.h>

__attribute__((__weak__)) int __poll_chk(struct pollfd * fds, nfds_t nfds, int timeout, size_t fdslen);

#include <pthread.h>

__attribute__((__weak__)) size_t __pthread_get_minstack(const pthread_attr_t * attr);

#include <signal.h>
#include <unistd.h>

__attribute__((__weak__)) int __gai_sigqueue(int sig, const union sigval val, pid_t caller_pid);

#if defined (__cplusplus)
}
#endif
