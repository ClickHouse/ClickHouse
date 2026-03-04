/// Very limited implementation. Half of code from Musl was cut.
/// This is Ok, because for now, this function is used only from clang driver.

#define _GNU_SOURCE
#include <spawn.h>
#include <sched.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <syscall.h>
#include <sys/signal.h>
#include <pthread.h>
#include <spawn.h>
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
	int ret;
	struct args *args = args_vp;
	int p = args->p[1];
	const posix_spawnattr_t *restrict attr = args->attr;

	close(args->p[0]);

	/* Close-on-exec flag may have been lost if we moved the pipe
	 * to a different fd. We don't use F_DUPFD_CLOEXEC above because
	 * it would fail on older kernels and atomicity is not needed --
	 * in this process there are no threads or signal handlers. */
	__syscall(SYS_fcntl, p, F_SETFD, FD_CLOEXEC);

	pthread_sigmask(SIG_SETMASK, (attr->__flags & POSIX_SPAWN_SETSIGMASK)
		? &attr->__ss : &args->oldmask, 0);

	args->exec(args->path, args->argv, args->envp);
	ret = -errno;

	/* Since sizeof errno < PIPE_BUF, the write is atomic. */
	ret = -ret;
	if (ret) while (__syscall(SYS_write, p, &ret, sizeof ret) < 0);
	_exit(127);
}


int __posix_spawnx(pid_t *restrict res, const char *restrict path,
	int (*exec)(const char *, char *const *, char *const *),
	const posix_spawn_file_actions_t *fa,
	const posix_spawnattr_t *restrict attr,
	char *const argv[restrict], char *const envp[restrict])
{
	pid_t pid;
	char stack[1024];
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
	return __posix_spawnx(res, path, execve, fa, attr, argv, envp);
}
