#ifndef _ARPA_INET_H
#define	_ARPA_INET_H

#ifdef __cplusplus
extern "C" {
#endif

#include <features.h>
#include <netinet/in.h>

uint32_t htonl(uint32_t);
uint16_t htons(uint16_t);
uint32_t ntohl(uint32_t);
uint16_t ntohs(uint16_t);

in_addr_t inet_addr (const char *);
in_addr_t inet_network (const char *);
char *inet_ntoa (struct in_addr);
int inet_pton (int, const char *__restrict, void *__restrict);
const char *inet_ntop (int, const void *__restrict, char *__restrict, socklen_t);

int inet_aton (const char *, struct in_addr *);
struct in_addr inet_makeaddr(in_addr_t, in_addr_t);
in_addr_t inet_lnaof(struct in_addr);
in_addr_t inet_netof(struct in_addr);

#ifdef __cplusplus
}
#endif

#endif
