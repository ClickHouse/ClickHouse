/** Allows to build programs with libc 2.27 and run on systems with at least libc 2.4,
  *  such as Ubuntu Hardy or CentOS 5.
  *
  * Also look at http://www.lightofdawn.org/wiki/wiki.cgi/NewAppsOnOldGlibc
  */

#if defined (__cplusplus)
extern "C" {
#endif

#include <signal.h>
#include <unistd.h>
#include <string.h>
#include <sys/syscall.h>

long int syscall(long int __sysno, ...) __THROW;

int __gai_sigqueue(int sig, const union sigval val, pid_t caller_pid)
{
    siginfo_t info;

    memset(&info, 0, sizeof(siginfo_t));
    info.si_signo = sig;
    info.si_code = SI_ASYNCNL;
    info.si_pid = caller_pid;
    info.si_uid = getuid();
    info.si_value = val;

    return syscall(__NR_rt_sigqueueinfo, info.si_pid, sig, &info);
}


#include <stdlib.h>
#include <features.h>

#if __GLIBC__ > 2 || (__GLIBC__ == 2  && __GLIBC_MINOR__ >= 16)
long int __fdelt_chk(long int d)
{
    if (d < 0)
        abort();
#else
unsigned long int __fdelt_chk(unsigned long int d)
{
#endif
    if (d >= FD_SETSIZE)
        abort();
    return d / __NFDBITS;
}

#include <sys/poll.h>
#include <stddef.h>

int __poll_chk(struct pollfd * fds, nfds_t nfds, int timeout, size_t fdslen)
{
    if (fdslen / sizeof(*fds) < nfds)
        abort();
    return poll(fds, nfds, timeout);
}

#include <setjmp.h>

void musl_glibc_longjmp(jmp_buf env, int val);

/// NOTE This disables some of FORTIFY_SOURCE functionality.

void __longjmp_chk(jmp_buf env, int val)
{
    musl_glibc_longjmp(env, val);
}

#include <stdarg.h>

int vasprintf(char **s, const char *fmt, va_list ap);

int __vasprintf_chk(char **s, int unused, const char *fmt, va_list ap)
{
    return vasprintf(s, fmt, ap);
}

int __asprintf_chk(char **result_ptr, int unused, const char *format, ...)
{
    int ret;
    va_list ap;
    va_start (ap, format);
    ret = vasprintf(result_ptr, format, ap);
    va_end (ap);
    return ret;
}

int vdprintf(int fd, const char *format, va_list ap);

int __dprintf_chk (int d, int unused, const char *format, ...)
{
  int ret;
  va_list ap;
  va_start (ap, format);
  ret = vdprintf(d, format, ap);
  va_end (ap);
  return ret;
}

size_t fread(void *ptr, size_t size, size_t nmemb, void *stream);

size_t __fread_chk(void *ptr, size_t unused, size_t size, size_t nmemb, void *stream)
{
    return fread(ptr, size, nmemb, stream);
}

int vsscanf(const char *str, const char *format, va_list ap);

int __isoc99_vsscanf(const char *str, const char *format, va_list ap)
{
    return vsscanf(str, format, ap);
}

int sscanf(const char *restrict s, const char *restrict fmt, ...)
{
    int ret;
    va_list ap;
    va_start(ap, fmt);
    ret = vsscanf(s, fmt, ap);
    va_end(ap);
    return ret;
}

int __isoc99_sscanf(const char *str, const char *format, ...) __attribute__((weak, nonnull, nothrow, alias("sscanf")));

int open(const char *path, int oflag);

int __open_2(const char *path, int oflag)
{
    return open(path, oflag);
}


#include <pthread.h>

/// No-ops.
int pthread_setname_np(pthread_t thread, const char *name) { return 0; }
int pthread_getname_np(pthread_t thread, char *name, size_t len) { name[0] = '\0'; return 0; };


#define SHMDIR "/dev/shm/"
const char * __shm_directory(size_t * len)
{
    *len = sizeof(SHMDIR) - 1;
    return SHMDIR;
}


/// https://boringssl.googlesource.com/boringssl/+/ad1907fe73334d6c696c8539646c21b11178f20f%5E!/#F0
/* Copyright (c) 2015, Google Inc.
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
 * OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
void __attribute__((__weak__)) explicit_bzero(void * buf, size_t len)
{
    memset(buf, 0, len);
    __asm__ __volatile__("" :: "r"(buf) : "memory");
}

void __explicit_bzero_chk(void * buf, size_t len, size_t unused)
{
    return explicit_bzero(buf, len);
}


#include <unistd.h>
#include "syscall.h"

ssize_t copy_file_range(int fd_in, off_t *off_in, int fd_out, off_t *off_out, size_t len, unsigned flags)
{
	return syscall(SYS_copy_file_range, fd_in, off_in, fd_out, off_out, len, flags);
}


long splice(int fd_in, off_t *off_in, int fd_out, off_t *off_out, size_t len, unsigned flags)
{
	return syscall(SYS_splice, fd_in, off_in, fd_out, off_out, len, flags);
}


#define _BSD_SOURCE
#include <sys/stat.h>
#include <stdint.h>

struct statx {
	uint32_t stx_mask;
	uint32_t stx_blksize;
	uint64_t stx_attributes;
	uint32_t stx_nlink;
	uint32_t stx_uid;
	uint32_t stx_gid;
	uint16_t stx_mode;
	uint16_t pad1;
	uint64_t stx_ino;
	uint64_t stx_size;
	uint64_t stx_blocks;
	uint64_t stx_attributes_mask;
	struct {
		int64_t tv_sec;
		uint32_t tv_nsec;
		int32_t pad;
	} stx_atime, stx_btime, stx_ctime, stx_mtime;
	uint32_t stx_rdev_major;
	uint32_t stx_rdev_minor;
	uint32_t stx_dev_major;
	uint32_t stx_dev_minor;
	uint64_t spare[14];
};

int statx(int fd, const char *restrict path, int flag,
                 unsigned int mask, struct statx *restrict statxbuf)
{
	return syscall(SYS_statx, fd, path, flag, mask, statxbuf);
}


#include <syscall.h>

ssize_t getrandom(void *buf, size_t buflen, unsigned flags)
{
    /// There was cancellable syscall (syscall_cp), but I don't care too.
    return syscall(SYS_getrandom, buf, buflen, flags);
}

/* Structure for scatter/gather I/O.  */
struct iovec
{
    void *iov_base;    /* Pointer to data.  */
    size_t iov_len;    /* Length of data.  */
};

ssize_t preadv(int __fd, const struct iovec *__iovec, int __count, __off_t __offset)
{
    return syscall(SYS_preadv, __fd, __iovec, __count, (long)(__offset), (long)(__offset>>32));
}

#include <errno.h>
#include <limits.h>

#define ALIGN (sizeof(size_t))
#define ONES ((size_t)-1/UCHAR_MAX)
#define HIGHS (ONES * (UCHAR_MAX/2+1))
#define HASZERO(x) ((x)-ONES & ~(x) & HIGHS)

char *__strchrnul(const char *s, int c)
{
	c = (unsigned char)c;
	if (!c) return (char *)s + strlen(s);

#ifdef __GNUC__
	typedef size_t __attribute__((__may_alias__)) word;
	const word *w;
	for (; (uintptr_t)s % ALIGN; s++)
		if (!*s || *(unsigned char *)s == c) return (char *)s;
	size_t k = ONES * c;
	for (w = (void *)s; !HASZERO(*w) && !HASZERO(*w^k); w++);
	s = (void *)w;
#endif
	for (; *s && *(unsigned char *)s != c; s++);
	return (char *)s;
}

int __execvpe(const char *file, char *const argv[], char *const envp[])
{
	const char *p, *z, *path = getenv("PATH");
	size_t l, k;
	int seen_eacces = 0;

	errno = ENOENT;
	if (!*file) return -1;

	if (strchr(file, '/'))
		return execve(file, argv, envp);

	if (!path) path = "/usr/local/bin:/bin:/usr/bin";
	k = strnlen(file, NAME_MAX+1);
	if (k > NAME_MAX) {
		errno = ENAMETOOLONG;
		return -1;
	}
	l = strnlen(path, PATH_MAX-1)+1;

	for(p=path; ; p=z) {
		char b[l+k+1];
		z = __strchrnul(p, ':');
		if (z-p >= l) {
			if (!*z++) break;
			continue;
		}
		memcpy(b, p, z-p);
		b[z-p] = '/';
		memcpy(b+(z-p)+(z>p), file, k+1);
		execve(b, argv, envp);
		switch (errno) {
		case EACCES:
			seen_eacces = 1;
		case ENOENT:
		case ENOTDIR:
			break;
		default:
			return -1;
		}
		if (!*z++) break;
	}
	if (seen_eacces) errno = EACCES;
	return -1;
}


#include "spawn.h"

int posix_spawnp(pid_t *restrict res, const char *restrict file,
	const posix_spawn_file_actions_t *fa,
	const posix_spawnattr_t *restrict attr,
	char *const argv[restrict], char *const envp[restrict])
{
	posix_spawnattr_t spawnp_attr = { 0 };
	if (attr) spawnp_attr = *attr;
	spawnp_attr.__fn = (void *)__execvpe;
	return posix_spawn(res, file, fa, &spawnp_attr, argv, envp);
}

#define FDOP_CLOSE 1
#define FDOP_DUP2 2
#define FDOP_OPEN 3
#define FDOP_CHDIR 4
#define FDOP_FCHDIR 5

#define ENOMEM 12
#define EBADF 9

struct fdop {
	struct fdop *next, *prev;
	int cmd, fd, srcfd, oflag;
	mode_t mode;
	char path[];
};

int posix_spawn_file_actions_init(posix_spawn_file_actions_t *fa) {
	fa->__actions = 0;
	return 0;
}

int posix_spawn_file_actions_addchdir_np(posix_spawn_file_actions_t *restrict fa, const char *restrict path) {
	struct fdop *op = malloc(sizeof *op + strlen(path) + 1);
	if (!op) return ENOMEM;
	op->cmd = FDOP_CHDIR;
	op->fd = -1;
	strcpy(op->path, path);
	if ((op->next = fa->__actions)) op->next->prev = op;
	op->prev = 0;
	fa->__actions = op;
	return 0;
}

int posix_spawn_file_actions_addclose(posix_spawn_file_actions_t *fa, int fd) {
	if (fd < 0) return EBADF;
	struct fdop *op = malloc(sizeof *op);
	if (!op) return ENOMEM;
	op->cmd = FDOP_CLOSE;
	op->fd = fd;
	if ((op->next = fa->__actions)) op->next->prev = op;
	op->prev = 0;
	fa->__actions = op;
	return 0;
}

int posix_spawn_file_actions_adddup2(posix_spawn_file_actions_t *fa, int srcfd, int fd) {
	if (srcfd < 0 || fd < 0) return EBADF;
	struct fdop *op = malloc(sizeof *op);
	if (!op) return ENOMEM;
	op->cmd = FDOP_DUP2;
	op->srcfd = srcfd;
	op->fd = fd;
	if ((op->next = fa->__actions)) op->next->prev = op;
	op->prev = 0;
	fa->__actions = op;
	return 0;
}

int posix_spawn_file_actions_addfchdir_np(posix_spawn_file_actions_t *fa, int fd) {
	if (fd < 0) return EBADF;
	struct fdop *op = malloc(sizeof *op);
	if (!op) return ENOMEM;
	op->cmd = FDOP_FCHDIR;
	op->fd = fd;
	if ((op->next = fa->__actions)) op->next->prev = op;
	op->prev = 0;
	fa->__actions = op;
	return 0;
}

int posix_spawn_file_actions_addopen(posix_spawn_file_actions_t *restrict fa, int fd, const char *restrict path, int flags, mode_t mode) {
	if (fd < 0) return EBADF;
	struct fdop *op = malloc(sizeof *op + strlen(path) + 1);
	if (!op) return ENOMEM;
	op->cmd = FDOP_OPEN;
	op->fd = fd;
	op->oflag = flags;
	op->mode = mode;
	strcpy(op->path, path);
	if ((op->next = fa->__actions)) op->next->prev = op;
	op->prev = 0;
	fa->__actions = op;
	return 0;
}

int posix_spawn_file_actions_destroy(posix_spawn_file_actions_t *fa) {
	struct fdop *op = fa->__actions, *next;
	while (op) {
		next = op->next;
		free(op);
		op = next;
	}
	return 0;
}

#if defined (__cplusplus)
}
#endif
