#ifndef	_DIRENT_H
#define	_DIRENT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <features.h>

#define __NEED_ino_t
#define __NEED_off_t
#if defined(_BSD_SOURCE) || defined(_GNU_SOURCE)
#define __NEED_size_t
#endif

#include <bits/alltypes.h>

#include <bits/dirent.h>

typedef struct __dirstream DIR;

#define d_fileno d_ino

int            closedir(DIR *);
DIR           *fdopendir(int);
DIR           *opendir(const char *);
struct dirent *readdir(DIR *);
int            readdir_r(DIR *__restrict, struct dirent *__restrict, struct dirent **__restrict);
void           rewinddir(DIR *);
int            dirfd(DIR *);

int alphasort(const struct dirent **, const struct dirent **);
int scandir(const char *, struct dirent ***, int (*)(const struct dirent *), int (*)(const struct dirent **, const struct dirent **));

#if defined(_XOPEN_SOURCE) || defined(_GNU_SOURCE) || defined(_BSD_SOURCE)
void           seekdir(DIR *, long);
long           telldir(DIR *);
#endif

#if defined(_GNU_SOURCE) || defined(_BSD_SOURCE)
#define DT_UNKNOWN 0
#define DT_FIFO 1
#define DT_CHR 2
#define DT_DIR 4
#define DT_BLK 6
#define DT_REG 8
#define DT_LNK 10
#define DT_SOCK 12
#define DT_WHT 14
#define IFTODT(x) ((x)>>12 & 017)
#define DTTOIF(x) ((x)<<12)
int getdents(int, struct dirent *, size_t);
#endif

#ifdef _GNU_SOURCE
int versionsort(const struct dirent **, const struct dirent **);
#endif

#if defined(_LARGEFILE64_SOURCE) || defined(_GNU_SOURCE)
#define dirent64 dirent
#define readdir64 readdir
#define readdir64_r readdir_r
#define scandir64 scandir
#define alphasort64 alphasort
#define versionsort64 versionsort
#define off64_t off_t
#define ino64_t ino_t
#define getdents64 getdents
#endif

#ifdef __cplusplus
}
#endif

#endif
