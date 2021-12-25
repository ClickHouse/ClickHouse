#ifndef	_SYS_MMAN_H
#define	_SYS_MMAN_H
#ifdef __cplusplus
extern "C" {
#endif

#include <features.h>

#define __NEED_mode_t
#define __NEED_size_t
#define __NEED_off_t

#if defined(_GNU_SOURCE)
#define __NEED_ssize_t
#endif

#include <bits/alltypes.h>

#define MAP_FAILED ((void *) -1)

#define MAP_SHARED     0x01
#define MAP_PRIVATE    0x02
#define MAP_SHARED_VALIDATE 0x03
#define MAP_TYPE       0x0f
#define MAP_FIXED      0x10
#define MAP_ANON       0x20
#define MAP_ANONYMOUS  MAP_ANON
#define MAP_NORESERVE  0x4000
#define MAP_GROWSDOWN  0x0100
#define MAP_DENYWRITE  0x0800
#define MAP_EXECUTABLE 0x1000
#define MAP_LOCKED     0x2000
#define MAP_POPULATE   0x8000
#define MAP_NONBLOCK   0x10000
#define MAP_STACK      0x20000
#define MAP_HUGETLB    0x40000
#define MAP_SYNC       0x80000
#define MAP_FIXED_NOREPLACE 0x100000
#define MAP_FILE       0

#define MAP_HUGE_SHIFT 26
#define MAP_HUGE_MASK  0x3f
#define MAP_HUGE_16KB  (14 << 26)
#define MAP_HUGE_64KB  (16 << 26)
#define MAP_HUGE_512KB (19 << 26)
#define MAP_HUGE_1MB   (20 << 26)
#define MAP_HUGE_2MB   (21 << 26)
#define MAP_HUGE_8MB   (23 << 26)
#define MAP_HUGE_16MB  (24 << 26)
#define MAP_HUGE_32MB  (25 << 26)
#define MAP_HUGE_256MB (28 << 26)
#define MAP_HUGE_512MB (29 << 26)
#define MAP_HUGE_1GB   (30 << 26)
#define MAP_HUGE_2GB   (31 << 26)
#define MAP_HUGE_16GB  (34U << 26)

#define PROT_NONE      0
#define PROT_READ      1
#define PROT_WRITE     2
#define PROT_EXEC      4
#define PROT_GROWSDOWN 0x01000000
#define PROT_GROWSUP   0x02000000

#define MS_ASYNC       1
#define MS_INVALIDATE  2
#define MS_SYNC        4

#define MCL_CURRENT    1
#define MCL_FUTURE     2
#define MCL_ONFAULT    4

#define POSIX_MADV_NORMAL     0
#define POSIX_MADV_RANDOM     1
#define POSIX_MADV_SEQUENTIAL 2
#define POSIX_MADV_WILLNEED   3
#define POSIX_MADV_DONTNEED   4

#if defined(_GNU_SOURCE) || defined(_BSD_SOURCE)
#define MADV_NORMAL      0
#define MADV_RANDOM      1
#define MADV_SEQUENTIAL  2
#define MADV_WILLNEED    3
#define MADV_DONTNEED    4
#define MADV_FREE        8
#define MADV_REMOVE      9
#define MADV_DONTFORK    10
#define MADV_DOFORK      11
#define MADV_MERGEABLE   12
#define MADV_UNMERGEABLE 13
#define MADV_HUGEPAGE    14
#define MADV_NOHUGEPAGE  15
#define MADV_DONTDUMP    16
#define MADV_DODUMP      17
#define MADV_WIPEONFORK  18
#define MADV_KEEPONFORK  19
#define MADV_COLD        20
#define MADV_PAGEOUT     21
#define MADV_HWPOISON    100
#define MADV_SOFT_OFFLINE 101
#endif

#ifdef _GNU_SOURCE
#define MREMAP_MAYMOVE 1
#define MREMAP_FIXED 2
#define MREMAP_DONTUNMAP 4

#define MLOCK_ONFAULT 0x01

#define MFD_CLOEXEC 0x0001U
#define MFD_ALLOW_SEALING 0x0002U
#define MFD_HUGETLB 0x0004U
#endif

#include <bits/mman.h>

void *mmap (void *, size_t, int, int, int, off_t);
int munmap (void *, size_t);

int mprotect (void *, size_t, int);
int msync (void *, size_t, int);

int posix_madvise (void *, size_t, int);

int mlock (const void *, size_t);
int munlock (const void *, size_t);
int mlockall (int);
int munlockall (void);

#ifdef _GNU_SOURCE
void *mremap (void *, size_t, size_t, int, ...);
int remap_file_pages (void *, size_t, int, size_t, int);
int memfd_create (const char *, unsigned);
int mlock2 (const void *, size_t, unsigned);
#endif

#if defined(_GNU_SOURCE) || defined(_BSD_SOURCE)
int madvise (void *, size_t, int);
int mincore (void *, size_t, unsigned char *);
#endif

int shm_open (const char *, int, mode_t);
int shm_unlink (const char *);

#if defined(_LARGEFILE64_SOURCE) || defined(_GNU_SOURCE)
#define mmap64 mmap
#define off64_t off_t
#endif

#ifdef __cplusplus
}
#endif
#endif
