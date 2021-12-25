#include <stdint.h>

/* linux/netlink.h */

#define NETLINK_ROUTE 0

struct nlmsghdr {
	uint32_t	nlmsg_len;
	uint16_t	nlmsg_type;
	uint16_t	nlmsg_flags;
	uint32_t	nlmsg_seq;
	uint32_t	nlmsg_pid;
};

#define NLM_F_REQUEST	1
#define NLM_F_MULTI	2
#define NLM_F_ACK	4

#define NLM_F_ROOT	0x100
#define NLM_F_MATCH	0x200
#define NLM_F_ATOMIC	0x400
#define NLM_F_DUMP	(NLM_F_ROOT|NLM_F_MATCH)

#define NLMSG_NOOP	0x1
#define NLMSG_ERROR	0x2
#define NLMSG_DONE	0x3
#define NLMSG_OVERRUN	0x4

/* linux/rtnetlink.h */

#define RTM_NEWLINK	16
#define RTM_GETLINK	18
#define RTM_NEWADDR	20
#define RTM_GETADDR	22

struct rtattr {
	unsigned short	rta_len;
	unsigned short	rta_type;
};

struct rtgenmsg {
	unsigned char	rtgen_family;
};

struct ifinfomsg {
	unsigned char	ifi_family;
	unsigned char	__ifi_pad;
	unsigned short	ifi_type;
	int		ifi_index;
	unsigned	ifi_flags;
	unsigned	ifi_change;
};

/* linux/if_link.h */

#define IFLA_ADDRESS	1
#define IFLA_BROADCAST	2
#define IFLA_IFNAME	3
#define IFLA_STATS	7

/* linux/if_addr.h */

struct ifaddrmsg {
	uint8_t		ifa_family;
	uint8_t		ifa_prefixlen;
	uint8_t		ifa_flags;
	uint8_t		ifa_scope;
	uint32_t	ifa_index;
};

#define IFA_ADDRESS	1
#define IFA_LOCAL	2
#define IFA_LABEL	3
#define IFA_BROADCAST	4

/* musl */

#define NETLINK_ALIGN(len)	(((len)+3) & ~3)
#define NLMSG_DATA(nlh)		((void*)((char*)(nlh)+sizeof(struct nlmsghdr)))
#define NLMSG_DATALEN(nlh)	((nlh)->nlmsg_len-sizeof(struct nlmsghdr))
#define NLMSG_DATAEND(nlh)	((char*)(nlh)+(nlh)->nlmsg_len)
#define NLMSG_NEXT(nlh)		(struct nlmsghdr*)((char*)(nlh)+NETLINK_ALIGN((nlh)->nlmsg_len))
#define NLMSG_OK(nlh,end)	((char*)(end)-(char*)(nlh) >= sizeof(struct nlmsghdr))

#define RTA_DATA(rta)		((void*)((char*)(rta)+sizeof(struct rtattr)))
#define RTA_DATALEN(rta)	((rta)->rta_len-sizeof(struct rtattr))
#define RTA_DATAEND(rta)	((char*)(rta)+(rta)->rta_len)
#define RTA_NEXT(rta)		(struct rtattr*)((char*)(rta)+NETLINK_ALIGN((rta)->rta_len))
#define RTA_OK(nlh,end)		((char*)(end)-(char*)(rta) >= sizeof(struct rtattr))

#define NLMSG_RTA(nlh,len)	((void*)((char*)(nlh)+sizeof(struct nlmsghdr)+NETLINK_ALIGN(len)))
#define NLMSG_RTAOK(rta,nlh)	RTA_OK(rta,NLMSG_DATAEND(nlh))

hidden int __rtnetlink_enumerate(int link_af, int addr_af, int (*cb)(void *ctx, struct nlmsghdr *h), void *ctx);
