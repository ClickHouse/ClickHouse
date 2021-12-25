#ifndef _SYS_QUOTA_H
#define _SYS_QUOTA_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

#define _LINUX_QUOTA_VERSION 2

#define dbtob(num) ((num) << 10)
#define btodb(num) ((num) >> 10)
#define fs_to_dq_blocks(num, blksize) (((num) * (blksize)) / 1024)

#define MAX_IQ_TIME 604800
#define MAX_DQ_TIME 604800

#define MAXQUOTAS 2
#define USRQUOTA  0
#define GRPQUOTA  1

#define INITQFNAMES { "user", "group", "undefined" };

#define QUOTAFILENAME "quota"
#define QUOTAGROUP "staff"

#define NR_DQHASH 43
#define NR_DQUOTS 256

#define SUBCMDMASK       0x00ff
#define SUBCMDSHIFT      8
#define QCMD(cmd, type)  (((cmd) << SUBCMDSHIFT) | ((type) & SUBCMDMASK))

#define Q_SYNC     0x800001
#define Q_QUOTAON  0x800002
#define Q_QUOTAOFF 0x800003
#define Q_GETFMT   0x800004
#define Q_GETINFO  0x800005
#define Q_SETINFO  0x800006
#define Q_GETQUOTA 0x800007
#define Q_SETQUOTA 0x800008

#define	QFMT_VFS_OLD 1
#define	QFMT_VFS_V0 2
#define QFMT_OCFS2 3
#define	QFMT_VFS_V1 4

#define QIF_BLIMITS	1
#define QIF_SPACE	2
#define QIF_ILIMITS	4
#define QIF_INODES	8
#define QIF_BTIME	16
#define QIF_ITIME	32
#define QIF_LIMITS	(QIF_BLIMITS | QIF_ILIMITS)
#define QIF_USAGE	(QIF_SPACE | QIF_INODES)
#define QIF_TIMES	(QIF_BTIME | QIF_ITIME)
#define QIF_ALL		(QIF_LIMITS | QIF_USAGE | QIF_TIMES)

struct dqblk {
	uint64_t dqb_bhardlimit;
	uint64_t dqb_bsoftlimit;
	uint64_t dqb_curspace;
	uint64_t dqb_ihardlimit;
	uint64_t dqb_isoftlimit;
	uint64_t dqb_curinodes;
	uint64_t dqb_btime;
	uint64_t dqb_itime;
	uint32_t dqb_valid;
};

#define	dq_bhardlimit	dq_dqb.dqb_bhardlimit
#define	dq_bsoftlimit	dq_dqb.dqb_bsoftlimit
#define dq_curspace	dq_dqb.dqb_curspace
#define dq_valid	dq_dqb.dqb_valid
#define	dq_ihardlimit	dq_dqb.dqb_ihardlimit
#define	dq_isoftlimit	dq_dqb.dqb_isoftlimit
#define	dq_curinodes	dq_dqb.dqb_curinodes
#define	dq_btime	dq_dqb.dqb_btime
#define	dq_itime	dq_dqb.dqb_itime

#define dqoff(UID)      ((long long)(UID) * sizeof (struct dqblk))

#define IIF_BGRACE	1
#define IIF_IGRACE	2
#define IIF_FLAGS	4
#define IIF_ALL		(IIF_BGRACE | IIF_IGRACE | IIF_FLAGS)

struct dqinfo {
	uint64_t dqi_bgrace;
	uint64_t dqi_igrace;
	uint32_t dqi_flags;
	uint32_t dqi_valid;
};

int quotactl(int, const char *, int, char *);

#ifdef __cplusplus
}
#endif

#endif
