#ifndef _SYS_MOUNT_H
#define _SYS_MOUNT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/ioctl.h>

#define BLKROSET   _IO(0x12, 93)
#define BLKROGET   _IO(0x12, 94)
#define BLKRRPART  _IO(0x12, 95)
#define BLKGETSIZE _IO(0x12, 96)
#define BLKFLSBUF  _IO(0x12, 97)
#define BLKRASET   _IO(0x12, 98)
#define BLKRAGET   _IO(0x12, 99)
#define BLKFRASET  _IO(0x12,100)
#define BLKFRAGET  _IO(0x12,101)
#define BLKSECTSET _IO(0x12,102)
#define BLKSECTGET _IO(0x12,103)
#define BLKSSZGET  _IO(0x12,104)
#define BLKBSZGET  _IOR(0x12,112,size_t)
#define BLKBSZSET  _IOW(0x12,113,size_t)
#define BLKGETSIZE64 _IOR(0x12,114,size_t)

#define MS_RDONLY      1
#define MS_NOSUID      2
#define MS_NODEV       4
#define MS_NOEXEC      8
#define MS_SYNCHRONOUS 16
#define MS_REMOUNT     32
#define MS_MANDLOCK    64
#define MS_DIRSYNC     128
#define MS_NOSYMFOLLOW 256
#define MS_NOATIME     1024
#define MS_NODIRATIME  2048
#define MS_BIND        4096
#define MS_MOVE        8192
#define MS_REC         16384
#define MS_SILENT      32768
#define MS_POSIXACL    (1<<16)
#define MS_UNBINDABLE  (1<<17)
#define MS_PRIVATE     (1<<18)
#define MS_SLAVE       (1<<19)
#define MS_SHARED      (1<<20)
#define MS_RELATIME    (1<<21)
#define MS_KERNMOUNT   (1<<22)
#define MS_I_VERSION   (1<<23)
#define MS_STRICTATIME (1<<24)
#define MS_LAZYTIME    (1<<25)
#define MS_NOREMOTELOCK (1<<27)
#define MS_NOSEC       (1<<28)
#define MS_BORN        (1<<29)
#define MS_ACTIVE      (1<<30)
#define MS_NOUSER      (1U<<31)

#define MS_RMT_MASK (MS_RDONLY|MS_SYNCHRONOUS|MS_MANDLOCK|MS_I_VERSION|MS_LAZYTIME)

#define MS_MGC_VAL 0xc0ed0000
#define MS_MGC_MSK 0xffff0000

#define MNT_FORCE       1
#define MNT_DETACH      2
#define MNT_EXPIRE      4
#define UMOUNT_NOFOLLOW 8

int mount(const char *, const char *, const char *, unsigned long, const void *);
int umount(const char *);
int umount2(const char *, int);

#ifdef __cplusplus
}
#endif

#endif
