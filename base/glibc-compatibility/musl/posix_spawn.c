/// Partial implementation derived from musl. The file_actions walking branch
/// from musl is intentionally NOT restored: callers are compiled against
/// glibc's <spawn.h>, whose posix_spawn_file_actions_t::__actions is an array
/// of `struct __spawn_action` (tagged union), not musl's doubly-linked list of
/// `struct fdop`. The two layouts share a field offset but nothing else, so
/// walking it as `struct fdop` would dereference garbage. Posix_spawn callers
/// that need file actions must avoid this stub or fall back to fork+exec.
///
/// posix_spawnattr_t fields that are read here (__flags low bits, __pgrp,
/// __sd, __ss) live at offsets and use flag values that are compatible
/// between musl and glibc layouts.

#define _GNU_SOURCE

/// For complete posix_spawnattr_t struct
#include "../spawn.h"

#include <sched.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <syscall.h>
#include <sys/signal.h>
#include <pthread.h>
#include <errno.h>
#include "syscall.h"

struct args {
	int p[2];
	sigset_t oldmask;
	const char *path;
	int (*exec)(const char *, char *const *, char *const *);
	const posix_spawn_file_actions_t *fa;
	const posix_spawnattr_t *restrict attr;
	char *const *argv, *const *envp;
};

void __get_handler_set(sigset_t *);

static int child(void *args_vp)
{
	int i, ret;
	struct sigaction sa = {0};
	struct args *args = args_vp;
	int p = args->p[1];
	const posix_spawnattr_t *restrict attr = args->attr;

	close(args->p[0]);

	/* Close-on-exec flag may have been lost if we moved the pipe
	 * to a different fd. We don't use F_DUPFD_CLOEXEC above because
	 * it would fail on older kernels and atomicity is not needed --
	 * in this process there are no threads or signal handlers. */
	__syscall(SYS_fcntl, p, F_SETFD, FD_CLOEXEC);

	if ((attr->__flags & POSIX_SPAWN_SETPGROUP)
		&& (ret=__syscall(SYS_setpgid, 0, attr->__pgrp)))
		goto fail;

	if (attr->__flags & POSIX_SPAWN_RESETIDS)
		if ((ret=__syscall(SYS_setgid, __syscall(SYS_getgid))) ||
			(ret=__syscall(SYS_setuid, __syscall(SYS_getuid))))
			goto fail;

	/* Reset signal handlers requested via posix_spawnattr_setsigdefault.
	 * Iterate via sigismember rather than poking sigset_t internals --
	 * the local spawn.h pulls in glibc's sigset_t (which exposes __val,
	 * not musl's __bits), so direct field access would not compile. */
	if (attr->__flags & POSIX_SPAWN_SETSIGDEF) {
		for (i=1; i<_NSIG; i++) {
			if (sigismember(&attr->__sd, i) == 1) {
				sa.sa_handler = SIG_DFL;
				ret = __syscall(SYS_rt_sigaction, i, &sa, 0, _NSIG/8);
				if (ret) goto fail;
			}
		}
	}

	pthread_sigmask(SIG_SETMASK, (attr->__flags & POSIX_SPAWN_SETSIGMASK)
		? &attr->__ss : &args->oldmask, 0);

	args->exec(args->path, args->argv, args->envp);
	ret = -errno;

fail:
	/* Since sizeof errno < PIPE_BUF, the write is atomic. */
	ret = -ret;
	if (ret) while (__syscall(SYS_write, p, &ret, sizeof ret) < 0);
	_exit(127);
}

typedef int (*posix_spawn_exec_fn)(const char *, char *const *, char *const *);
static int __posix_spawnx(pid_t *restrict res, const char *restrict path,
	posix_spawn_exec_fn exec,
	const posix_spawn_file_actions_t *fa,
	const posix_spawnattr_t *restrict attr,
	char *const argv[restrict], char *const envp[restrict])
{
	pid_t pid;
	char stack[16384];
	int ec=0, cs;
	struct args args;

	if (pipe2(args.p, O_CLOEXEC))
		return errno;

	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cs);

	args.path = path;
	args.exec = exec;
	args.fa = fa;
	args.attr = attr ? attr : &(const posix_spawnattr_t){0};
	args.argv = argv;
	args.envp = envp;

	pid = clone(child, stack+sizeof stack,
		CLONE_VM|CLONE_VFORK|SIGCHLD, &args);
	close(args.p[1]);

	if (pid > 0) {
		if (read(args.p[0], &ec, sizeof ec) != sizeof ec) ec = 0;
		else waitpid(pid, &(int){0}, 0);
	} else {
		ec = -pid;
	}

	close(args.p[0]);

	if (!ec && res) *res = pid;

	pthread_setcancelstate(cs, 0);

	return ec;
}

int posix_spawn(pid_t *restrict res, const char *restrict path,
	const posix_spawn_file_actions_t *fa,
	const posix_spawnattr_t *restrict attr,
	char *const argv[restrict], char *const envp[restrict])
{
	posix_spawn_exec_fn fn = execve;
	if (attr && attr->__fn)
		fn = (posix_spawn_exec_fn)attr->__fn;
	return __posix_spawnx(res, path, fn, fa, attr, argv, envp);
}
