/*
 * Public domain
 * sys/stat.h compatibility shim
 */

#ifndef LIBCRYPTOCOMPAT_SYS_STAT_H
#define LIBCRYPTOCOMPAT_SYS_STAT_H

#ifndef _MSC_VER
#include_next <sys/stat.h>

/* for old MinGW */
#ifndef S_IRGRP
#define S_IRGRP         0
#endif
#ifndef S_IROTH
#define S_IROTH         0
#endif

#else

#include <windows.h>
#if _MSC_VER >= 1900
#include <../ucrt/sys/stat.h>
#else
#include <../include/sys/stat.h>
#endif

/* File type and permission flags for stat() */
#if !defined(S_IFMT)
#   define S_IFMT   _S_IFMT                     /* File type mask */
#endif
#if !defined(S_IFDIR)
#   define S_IFDIR  _S_IFDIR                    /* Directory */
#endif
#if !defined(S_IFCHR)
#   define S_IFCHR  _S_IFCHR                    /* Character device */
#endif
#if !defined(S_IFFIFO)
#   define S_IFFIFO _S_IFFIFO                   /* Pipe */
#endif
#if !defined(S_IFREG)
#   define S_IFREG  _S_IFREG                    /* Regular file */
#endif
#if !defined(S_IREAD)
#   define S_IREAD  _S_IREAD                    /* Read permission */
#endif
#if !defined(S_IWRITE)
#   define S_IWRITE _S_IWRITE                   /* Write permission */
#endif
#if !defined(S_IEXEC)
#   define S_IEXEC  _S_IEXEC                    /* Execute permission */
#endif
#if !defined(S_IFIFO)
#   define S_IFIFO _S_IFIFO                     /* Pipe */
#endif
#if !defined(S_IFBLK)
#   define S_IFBLK   0                          /* Block device */
#endif
#if !defined(S_IFLNK)
#   define S_IFLNK   0                          /* Link */
#endif
#if !defined(S_IFSOCK)
#   define S_IFSOCK  0                          /* Socket */
#endif

#if defined(_MSC_VER)
#   define S_IRUSR  S_IREAD                     /* Read user */
#   define S_IWUSR  S_IWRITE                    /* Write user */
#   define S_IXUSR  0                           /* Execute user */
#   define S_IRGRP  0                           /* Read group */
#   define S_IWGRP  0                           /* Write group */
#   define S_IXGRP  0                           /* Execute group */
#   define S_IROTH  0                           /* Read others */
#   define S_IWOTH  0                           /* Write others */
#   define S_IXOTH  0                           /* Execute others */
#endif

/* File type flags for d_type */
#define DT_UNKNOWN  0
#define DT_REG      S_IFREG
#define DT_DIR      S_IFDIR
#define DT_FIFO     S_IFIFO
#define DT_SOCK     S_IFSOCK
#define DT_CHR      S_IFCHR
#define DT_BLK      S_IFBLK
#define DT_LNK      S_IFLNK

/* Macros for converting between st_mode and d_type */
#define IFTODT(mode) ((mode) & S_IFMT)
#define DTTOIF(type) (type)

/*
 * File type macros.  Note that block devices, sockets and links cannot be
 * distinguished on Windows and the macros S_ISBLK, S_ISSOCK and S_ISLNK are
 * only defined for compatibility.  These macros should always return false
 * on Windows.
 */
#define	S_ISFIFO(mode) (((mode) & S_IFMT) == S_IFIFO)
#define	S_ISDIR(mode)  (((mode) & S_IFMT) == S_IFDIR)
#define	S_ISREG(mode)  (((mode) & S_IFMT) == S_IFREG)
#define	S_ISLNK(mode)  (((mode) & S_IFMT) == S_IFLNK)
#define	S_ISSOCK(mode) (((mode) & S_IFMT) == S_IFSOCK)
#define	S_ISCHR(mode)  (((mode) & S_IFMT) == S_IFCHR)
#define	S_ISBLK(mode)  (((mode) & S_IFMT) == S_IFBLK)

#endif

#endif
