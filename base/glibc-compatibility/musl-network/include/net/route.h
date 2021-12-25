#ifndef _NET_ROUTE_H
#define _NET_ROUTE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>


struct rtentry {
	unsigned long int rt_pad1;
	struct sockaddr rt_dst;
	struct sockaddr rt_gateway;
	struct sockaddr rt_genmask;
	unsigned short int rt_flags;
	short int rt_pad2;
	unsigned long int rt_pad3;
	unsigned char rt_tos;
	unsigned char rt_class;
	short int rt_pad4[sizeof(long)/2-1];
	short int rt_metric;
	char *rt_dev;
	unsigned long int rt_mtu;
	unsigned long int rt_window;
	unsigned short int rt_irtt;
};

#define rt_mss	rt_mtu


struct in6_rtmsg {
	struct in6_addr rtmsg_dst;
	struct in6_addr rtmsg_src;
	struct in6_addr rtmsg_gateway;
	uint32_t rtmsg_type;
	uint16_t rtmsg_dst_len;
	uint16_t rtmsg_src_len;
	uint32_t rtmsg_metric;
	unsigned long int rtmsg_info;
	uint32_t rtmsg_flags;
	int rtmsg_ifindex;
};


#define	RTF_UP		0x0001
#define	RTF_GATEWAY	0x0002

#define	RTF_HOST	0x0004
#define RTF_REINSTATE	0x0008
#define	RTF_DYNAMIC	0x0010
#define	RTF_MODIFIED	0x0020
#define RTF_MTU		0x0040
#define RTF_MSS		RTF_MTU
#define RTF_WINDOW	0x0080
#define RTF_IRTT	0x0100
#define RTF_REJECT	0x0200
#define	RTF_STATIC	0x0400
#define	RTF_XRESOLVE	0x0800
#define RTF_NOFORWARD   0x1000
#define RTF_THROW	0x2000
#define RTF_NOPMTUDISC  0x4000

#define RTF_DEFAULT	0x00010000
#define RTF_ALLONLINK	0x00020000
#define RTF_ADDRCONF	0x00040000

#define RTF_LINKRT	0x00100000
#define RTF_NONEXTHOP	0x00200000

#define RTF_CACHE	0x01000000
#define RTF_FLOW	0x02000000
#define RTF_POLICY	0x04000000

#define RTCF_VALVE	0x00200000
#define RTCF_MASQ	0x00400000
#define RTCF_NAT	0x00800000
#define RTCF_DOREDIRECT 0x01000000
#define RTCF_LOG	0x02000000
#define RTCF_DIRECTSRC	0x04000000

#define RTF_LOCAL	0x80000000
#define RTF_INTERFACE	0x40000000
#define RTF_MULTICAST	0x20000000
#define RTF_BROADCAST	0x10000000
#define RTF_NAT		0x08000000

#define RTF_ADDRCLASSMASK	0xF8000000
#define RT_ADDRCLASS(flags)	((uint32_t) flags >> 23)

#define RT_TOS(tos)		((tos) & IPTOS_TOS_MASK)

#define RT_LOCALADDR(flags)	((flags & RTF_ADDRCLASSMASK) \
				 == (RTF_LOCAL|RTF_INTERFACE))

#define RT_CLASS_UNSPEC		0
#define RT_CLASS_DEFAULT	253

#define RT_CLASS_MAIN		254
#define RT_CLASS_LOCAL		255
#define RT_CLASS_MAX		255


#define RTMSG_ACK		NLMSG_ACK
#define RTMSG_OVERRUN		NLMSG_OVERRUN

#define RTMSG_NEWDEVICE		0x11
#define RTMSG_DELDEVICE		0x12
#define RTMSG_NEWROUTE		0x21
#define RTMSG_DELROUTE		0x22
#define RTMSG_NEWRULE		0x31
#define RTMSG_DELRULE		0x32
#define RTMSG_CONTROL		0x40

#define RTMSG_AR_FAILED		0x51

#ifdef __cplusplus
}
#endif

#endif
