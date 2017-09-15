#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/syscall.h>

extern long int syscall (long int __sysno, ...) __THROW;

int pipe2(int fd[2], int flag)
{
	if (!flag) return pipe(fd);
	int ret = syscall(SYS_pipe2, fd, flag);
	if (ret != -ENOSYS) return -ret;
	ret = pipe(fd);
	if (ret) return ret;
	if (flag & O_CLOEXEC) {
		syscall(SYS_fcntl, fd[0], F_SETFD, FD_CLOEXEC);
		syscall(SYS_fcntl, fd[1], F_SETFD, FD_CLOEXEC);
	}
	if (flag & O_NONBLOCK) {
		syscall(SYS_fcntl, fd[0], F_SETFL, O_NONBLOCK);
		syscall(SYS_fcntl, fd[1], F_SETFL, O_NONBLOCK);
	}
	return 0;
}
