#ifndef _IFADDRS_H
#define _IFADDRS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <features.h>
#include <netinet/in.h>
#include <sys/socket.h>

struct ifaddrs {
	struct ifaddrs *ifa_next;
	char *ifa_name;
	unsigned ifa_flags;
	struct sockaddr *ifa_addr;
	struct sockaddr *ifa_netmask;
	union {
		struct sockaddr *ifu_broadaddr;
		struct sockaddr *ifu_dstaddr;
	} ifa_ifu;
	void *ifa_data;
};
#define ifa_broadaddr ifa_ifu.ifu_broadaddr
#define ifa_dstaddr ifa_ifu.ifu_dstaddr

void freeifaddrs(struct ifaddrs *);
int getifaddrs(struct ifaddrs **);

#ifdef __cplusplus
}
#endif

#endif

