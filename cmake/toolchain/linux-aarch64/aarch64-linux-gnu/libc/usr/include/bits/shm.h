/* Copyright (C) 2011-2018 Free Software Foundation, Inc.
   This file is part of the GNU C Library.
   Contributed by Chris Metcalf <cmetcalf@tilera.com>, 2011.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library.  If not, see
   <http://www.gnu.org/licenses/>.  */

#ifndef _SYS_SHM_H
# error "Never include <bits/shm.h> directly; use <sys/shm.h> instead."
#endif

#include <bits/types.h>
#include <bits/wordsize.h>

/* Permission flag for shmget.  */
#define SHM_R		0400		/* or S_IRUGO from <linux/stat.h> */
#define SHM_W		0200		/* or S_IWUGO from <linux/stat.h> */

/* Flags for `shmat'.  */
#define SHM_RDONLY	010000		/* attach read-only else read-write */
#define SHM_RND		020000		/* round attach address to SHMLBA */
#define SHM_REMAP	040000		/* take-over region on attach */
#define SHM_EXEC	0100000		/* execution access */

/* Commands for `shmctl'.  */
#define SHM_LOCK	11		/* lock segment (root only) */
#define SHM_UNLOCK	12		/* unlock segment (root only) */

__BEGIN_DECLS

/* Segment low boundary address multiple.  */
#define SHMLBA		(__getpagesize ())
extern int __getpagesize (void) __THROW __attribute__ ((__const__));


/* Type to count number of attaches.  */
typedef unsigned long int shmatt_t;

/* Data structure describing a shared memory segment.  */
struct shmid_ds
  {
    struct ipc_perm shm_perm;		/* operation permission struct */
    size_t shm_segsz;			/* size of segment in bytes */
    __time_t shm_atime;			/* time of last shmat() */
#if __WORDSIZE == 32
    unsigned long int __glibc_reserved1;
#endif
    __time_t shm_dtime;			/* time of last shmdt() */
#if __WORDSIZE == 32
    unsigned long int __glibc_reserved2;
#endif
    __time_t shm_ctime;			/* time of last change by shmctl() */
#if __WORDSIZE == 32
    unsigned long int __glibc_reserved3;
#endif
    __pid_t shm_cpid;			/* pid of creator */
    __pid_t shm_lpid;			/* pid of last shmop */
    shmatt_t shm_nattch;		/* number of current attaches */
    unsigned long int __glibc_reserved4;
    unsigned long int __glibc_reserved5;
  };

#ifdef __USE_MISC

/* ipcs ctl commands */
# define SHM_STAT 	13
# define SHM_INFO 	14
# define SHM_STAT_ANY	15

/* shm_mode upper byte flags */
# define SHM_DEST	01000	/* segment will be destroyed on last detach */
# define SHM_LOCKED	02000   /* segment will not be swapped */
# define SHM_HUGETLB	04000	/* segment is mapped via hugetlb */
# define SHM_NORESERVE	010000	/* don't check for reservations */

struct	shminfo
  {
    unsigned long int shmmax;
    unsigned long int shmmin;
    unsigned long int shmmni;
    unsigned long int shmseg;
    unsigned long int shmall;
    unsigned long int __glibc_reserved1;
    unsigned long int __glibc_reserved2;
    unsigned long int __glibc_reserved3;
    unsigned long int __glibc_reserved4;
  };

struct shm_info
  {
    int used_ids;
    unsigned long int shm_tot;	/* total allocated shm */
    unsigned long int shm_rss;	/* total resident shm */
    unsigned long int shm_swp;	/* total swapped shm */
    unsigned long int swap_attempts;
    unsigned long int swap_successes;
  };

#endif /* __USE_MISC */

__END_DECLS
