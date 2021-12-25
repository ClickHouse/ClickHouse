#ifndef _MNTENT_H
#define _MNTENT_H

#ifdef __cplusplus
extern "C" {
#endif

#define __NEED_FILE
#include <bits/alltypes.h>

#define MOUNTED "/etc/mtab"

#define MNTTYPE_IGNORE	"ignore"
#define MNTTYPE_NFS	"nfs"
#define MNTTYPE_SWAP	"swap"
#define MNTOPT_DEFAULTS	"defaults"
#define MNTOPT_RO	"ro"
#define MNTOPT_RW	"rw"
#define MNTOPT_SUID	"suid"
#define MNTOPT_NOSUID	"nosuid"
#define MNTOPT_NOAUTO	"noauto"

struct mntent {
	char *mnt_fsname;
	char *mnt_dir;
	char *mnt_type;
	char *mnt_opts;
	int mnt_freq;
	int mnt_passno;
};

FILE *setmntent(const char *, const char *);
int endmntent(FILE *);
struct mntent *getmntent(FILE *);
struct mntent *getmntent_r(FILE *, struct mntent *, char *, int);
int addmntent(FILE *, const struct mntent *);
char *hasmntopt(const struct mntent *, const char *);

#ifdef __cplusplus
}
#endif

#endif
