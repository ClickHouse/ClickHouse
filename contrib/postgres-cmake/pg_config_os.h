#if defined(OS_DARWIN)

/* src/include/port/darwin.h */
#define __darwin__	1

#if HAVE_DECL_F_FULLFSYNC		/* not present before macOS 10.3 */
#define HAVE_FSYNC_WRITETHROUGH
#endif

#else
/* src/include/port/linux.h */
/*
 * As of July 2007, all known versions of the Linux kernel will sometimes
 * return EIDRM for a shmctl() operation when EINVAL is correct (it happens
 * when the low-order 15 bits of the supplied shm ID match the slot number
 * assigned to a newer shmem segment).  We deal with this by assuming that
 * EIDRM means EINVAL in PGSharedMemoryIsInUse().  This is reasonably safe
 * since in fact Linux has no excuse for ever returning EIDRM; it doesn't
 * track removed segments in a way that would allow distinguishing them from
 * private ones.  But someday that code might get upgraded, and we'd have
 * to have a kernel version test here.
 */
#define HAVE_LINUX_EIDRM_BUG

/*
 * Set the default wal_sync_method to fdatasync.  With recent Linux versions,
 * xlogdefs.h's normal rules will prefer open_datasync, which (a) doesn't
 * perform better and (b) causes outright failures on ext4 data=journal
 * filesystems, because those don't support O_DIRECT.
 */
#define PLATFORM_DEFAULT_SYNC_METHOD	SYNC_METHOD_FDATASYNC

#endif

