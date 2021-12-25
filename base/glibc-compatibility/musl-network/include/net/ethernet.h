#ifndef _NET_ETHERNET_H
#define _NET_ETHERNET_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <sys/types.h>
#include <netinet/if_ether.h>

struct ether_addr {
	uint8_t ether_addr_octet[ETH_ALEN];
};

struct ether_header {
	uint8_t  ether_dhost[ETH_ALEN];
	uint8_t  ether_shost[ETH_ALEN];
	uint16_t ether_type;
};

#define	ETHERTYPE_PUP		0x0200
#define ETHERTYPE_SPRITE	0x0500
#define	ETHERTYPE_IP		0x0800
#define	ETHERTYPE_ARP		0x0806
#define	ETHERTYPE_REVARP	0x8035
#define ETHERTYPE_AT		0x809B
#define ETHERTYPE_AARP		0x80F3
#define	ETHERTYPE_VLAN		0x8100
#define ETHERTYPE_IPX		0x8137
#define	ETHERTYPE_IPV6		0x86dd
#define ETHERTYPE_LOOPBACK	0x9000


#define	ETHER_ADDR_LEN	ETH_ALEN
#define	ETHER_TYPE_LEN	2
#define	ETHER_CRC_LEN	4
#define	ETHER_HDR_LEN	ETH_HLEN
#define	ETHER_MIN_LEN	(ETH_ZLEN + ETHER_CRC_LEN)
#define	ETHER_MAX_LEN	(ETH_FRAME_LEN + ETHER_CRC_LEN)

#define	ETHER_IS_VALID_LEN(foo)	\
	((foo) >= ETHER_MIN_LEN && (foo) <= ETHER_MAX_LEN)

#define	ETHERTYPE_TRAIL		0x1000
#define	ETHERTYPE_NTRAILER	16

#define	ETHERMTU	ETH_DATA_LEN
#define	ETHERMIN	(ETHER_MIN_LEN - ETHER_HDR_LEN - ETHER_CRC_LEN)

#ifdef __cplusplus
}
#endif

#endif
