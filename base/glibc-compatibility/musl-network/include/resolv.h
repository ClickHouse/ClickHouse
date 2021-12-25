#ifndef _RESOLV_H
#define _RESOLV_H

#include <stdint.h>
#include <arpa/nameser.h>
#include <netinet/in.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MAXNS			3
#define MAXDFLSRCH		3
#define MAXDNSRCH		6
#define LOCALDOMAINPARTS	2

#define RES_TIMEOUT		5
#define MAXRESOLVSORT		10
#define RES_MAXNDOTS		15
#define RES_MAXRETRANS		30
#define RES_MAXRETRY		5
#define RES_DFLRETRY		2
#define RES_MAXTIME		65535

/* unused; purely for broken apps */
typedef struct __res_state {
	int retrans;
	int retry;
	unsigned long options;
	int nscount;
	struct sockaddr_in nsaddr_list[MAXNS];
# define nsaddr	nsaddr_list[0]
	unsigned short id;
	char *dnsrch[MAXDNSRCH+1];
	char defdname[256];
	unsigned long pfcode;
	unsigned ndots:4;
	unsigned nsort:4;
	unsigned ipv6_unavail:1;
	unsigned unused:23;
	struct {
		struct in_addr addr;
		uint32_t mask;
	} sort_list[MAXRESOLVSORT];
	void *qhook;
	void *rhook;
	int res_h_errno;
	int _vcsock;
	unsigned _flags;
	union {
		char pad[52];
		struct {
			uint16_t		nscount;
			uint16_t		nsmap[MAXNS];
			int			nssocks[MAXNS];
			uint16_t		nscount6;
			uint16_t		nsinit;
			struct sockaddr_in6	*nsaddrs[MAXNS];
			unsigned int		_initstamp[2];
		} _ext;
	} _u;
} *res_state;

#define	__RES	19960801

#ifndef _PATH_RESCONF
#define _PATH_RESCONF        "/etc/resolv.conf"
#endif

struct res_sym {
	int number;
	char *name;
	char *humanname;
};

#define	RES_F_VC	0x00000001
#define	RES_F_CONN	0x00000002
#define RES_F_EDNS0ERR	0x00000004

#define	RES_EXHAUSTIVE	0x00000001

#define RES_INIT	0x00000001
#define RES_DEBUG	0x00000002
#define RES_AAONLY	0x00000004
#define RES_USEVC	0x00000008
#define RES_PRIMARY	0x00000010
#define RES_IGNTC	0x00000020
#define RES_RECURSE	0x00000040
#define RES_DEFNAMES	0x00000080
#define RES_STAYOPEN	0x00000100
#define RES_DNSRCH	0x00000200
#define	RES_INSECURE1	0x00000400
#define	RES_INSECURE2	0x00000800
#define	RES_NOALIASES	0x00001000
#define	RES_USE_INET6	0x00002000
#define RES_ROTATE	0x00004000
#define	RES_NOCHECKNAME	0x00008000
#define	RES_KEEPTSIG	0x00010000
#define	RES_BLAST	0x00020000
#define RES_USEBSTRING	0x00040000
#define RES_NOIP6DOTINT	0x00080000
#define RES_USE_EDNS0	0x00100000
#define RES_SNGLKUP	0x00200000
#define RES_SNGLKUPREOP	0x00400000
#define RES_USE_DNSSEC	0x00800000

#define RES_DEFAULT	(RES_RECURSE|RES_DEFNAMES|RES_DNSRCH|RES_NOIP6DOTINT)

#define RES_PRF_STATS	0x00000001
#define RES_PRF_UPDATE	0x00000002
#define RES_PRF_CLASS   0x00000004
#define RES_PRF_CMD	0x00000008
#define RES_PRF_QUES	0x00000010
#define RES_PRF_ANS	0x00000020
#define RES_PRF_AUTH	0x00000040
#define RES_PRF_ADD	0x00000080
#define RES_PRF_HEAD1	0x00000100
#define RES_PRF_HEAD2	0x00000200
#define RES_PRF_TTLID	0x00000400
#define RES_PRF_HEADX	0x00000800
#define RES_PRF_QUERY	0x00001000
#define RES_PRF_REPLY	0x00002000
#define RES_PRF_INIT	0x00004000

struct __res_state *__res_state(void);
#define _res (*__res_state())

int res_init(void);
int res_query(const char *, int, int, unsigned char *, int);
int res_querydomain(const char *, const char *, int, int, unsigned char *, int);
int res_search(const char *, int, int, unsigned char *, int);
int res_mkquery(int, const char *, int, int, const unsigned char *, int, const unsigned char*, unsigned char *, int);
int res_send(const unsigned char *, int, unsigned char *, int);
int dn_comp(const char *, unsigned char *, int, unsigned char **, unsigned char **);
int dn_expand(const unsigned char *, const unsigned char *, const unsigned char *, char *, int);
int dn_skipname(const unsigned char *, const unsigned char *);

#ifdef __cplusplus
}
#endif

#endif
