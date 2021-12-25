#ifndef _STROPTS_H
#define _STROPTS_H

#ifdef __cplusplus
extern "C" {
#endif

#define __SID		('S' << 8)

#define I_NREAD		(__SID | 1)
#define I_PUSH		(__SID | 2)
#define I_POP		(__SID | 3)
#define I_LOOK		(__SID | 4)
#define I_FLUSH		(__SID | 5)
#define I_SRDOPT	(__SID | 6)
#define I_GRDOPT	(__SID | 7)
#define I_STR		(__SID | 8)
#define I_SETSIG	(__SID | 9)
#define I_GETSIG	(__SID |10)
#define I_FIND		(__SID |11)
#define I_LINK		(__SID |12)
#define I_UNLINK	(__SID |13)
#define I_PEEK		(__SID |15)
#define I_FDINSERT	(__SID |16)
#define I_SENDFD	(__SID |17)
#define I_RECVFD	(__SID |14)
#define I_SWROPT	(__SID |19)
#define I_GWROPT	(__SID |20)
#define I_LIST		(__SID |21)
#define I_PLINK		(__SID |22)
#define I_PUNLINK	(__SID |23)
#define I_FLUSHBAND	(__SID |28)
#define I_CKBAND	(__SID |29)
#define I_GETBAND	(__SID |30)
#define I_ATMARK	(__SID |31)
#define I_SETCLTIME	(__SID |32)
#define I_GETCLTIME	(__SID |33)
#define I_CANPUT	(__SID |34)

#define FMNAMESZ	8

#define FLUSHR		0x01
#define FLUSHW		0x02
#define FLUSHRW		0x03
#define FLUSHBAND	0x04

#define S_INPUT		0x0001
#define S_HIPRI		0x0002
#define S_OUTPUT	0x0004
#define S_MSG		0x0008
#define S_ERROR		0x0010
#define S_HANGUP	0x0020
#define S_RDNORM	0x0040
#define S_WRNORM	S_OUTPUT
#define S_RDBAND	0x0080
#define S_WRBAND	0x0100
#define S_BANDURG	0x0200

#define RS_HIPRI	0x01

#define RNORM		0x0000
#define RMSGD		0x0001
#define RMSGN		0x0002
#define RPROTDAT	0x0004
#define RPROTDIS	0x0008
#define RPROTNORM	0x0010
#define RPROTMASK	0x001C

#define SNDZERO		0x001
#define SNDPIPE		0x002

#define ANYMARK		0x01
#define LASTMARK	0x02

#define MUXID_ALL	(-1)

#define MSG_HIPRI	0x01
#define MSG_ANY		0x02
#define MSG_BAND	0x04

#define MORECTL		1
#define MOREDATA	2

struct bandinfo {
	unsigned char bi_pri;
	int bi_flag;
};

struct strbuf {
	int maxlen;
	int len;
	char *buf;
};

struct strpeek {
	struct strbuf ctlbuf;
	struct strbuf databuf;
	unsigned flags;
};

struct strfdinsert {
	struct strbuf ctlbuf;
	struct strbuf databuf;
	unsigned flags;
	int fildes;
	int offset;
};

struct strioctl {
	int ic_cmd;
	int ic_timout;
	int ic_len;
	char *ic_dp;
};

struct strrecvfd {
	int fd;
	int uid;
	int gid;
	char __fill[8];
};

struct str_mlist {
	char l_name[FMNAMESZ + 1];
};

struct str_list {
	int sl_nmods;
	struct str_mlist *sl_modlist;
};

int isastream(int);
int ioctl(int, int, ...);

#ifdef __cplusplus
}
#endif

#endif
