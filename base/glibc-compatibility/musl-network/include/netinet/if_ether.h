#ifndef _NETINET_IF_ETHER_H
#define _NETINET_IF_ETHER_H

#include <stdint.h>
#include <sys/types.h>

#define ETH_ALEN	6
#define ETH_TLEN	2
#define ETH_HLEN	14
#define ETH_ZLEN	60
#define ETH_DATA_LEN	1500
#define ETH_FRAME_LEN	1514
#define ETH_FCS_LEN	4
#define ETH_MIN_MTU	68
#define ETH_MAX_MTU	0xFFFFU

#define ETH_P_LOOP	0x0060
#define ETH_P_PUP	0x0200
#define ETH_P_PUPAT	0x0201
#define ETH_P_TSN	0x22F0
#define ETH_P_ERSPAN2	0x22EB
#define ETH_P_IP	0x0800
#define ETH_P_X25	0x0805
#define ETH_P_ARP	0x0806
#define	ETH_P_BPQ	0x08FF
#define ETH_P_IEEEPUP	0x0a00
#define ETH_P_IEEEPUPAT	0x0a01
#define ETH_P_BATMAN	0x4305
#define ETH_P_DEC       0x6000
#define ETH_P_DNA_DL    0x6001
#define ETH_P_DNA_RC    0x6002
#define ETH_P_DNA_RT    0x6003
#define ETH_P_LAT       0x6004
#define ETH_P_DIAG      0x6005
#define ETH_P_CUST      0x6006
#define ETH_P_SCA       0x6007
#define ETH_P_TEB	0x6558
#define ETH_P_RARP      0x8035
#define ETH_P_ATALK	0x809B
#define ETH_P_AARP	0x80F3
#define ETH_P_8021Q	0x8100
#define ETH_P_IPX	0x8137
#define ETH_P_IPV6	0x86DD
#define ETH_P_PAUSE	0x8808
#define ETH_P_SLOW	0x8809
#define ETH_P_WCCP	0x883E
#define ETH_P_MPLS_UC	0x8847
#define ETH_P_MPLS_MC	0x8848
#define ETH_P_ATMMPOA	0x884c
#define ETH_P_PPP_DISC	0x8863
#define ETH_P_PPP_SES	0x8864
#define ETH_P_LINK_CTL	0x886c
#define ETH_P_ATMFATE	0x8884
#define ETH_P_PAE	0x888E
#define ETH_P_AOE	0x88A2
#define ETH_P_8021AD	0x88A8
#define ETH_P_802_EX1	0x88B5
#define ETH_P_ERSPAN	0x88BE
#define ETH_P_PREAUTH	0x88C7
#define ETH_P_TIPC	0x88CA
#define ETH_P_LLDP	0x88CC
#define ETH_P_MRP	0x88E3
#define ETH_P_MACSEC	0x88E5
#define ETH_P_8021AH	0x88E7
#define ETH_P_MVRP	0x88F5
#define ETH_P_1588	0x88F7
#define ETH_P_NCSI	0x88F8
#define ETH_P_PRP	0x88FB
#define ETH_P_FCOE	0x8906
#define ETH_P_TDLS	0x890D
#define ETH_P_FIP	0x8914
#define ETH_P_IBOE	0x8915
#define ETH_P_80221	0x8917
#define ETH_P_HSR	0x892F
#define ETH_P_NSH	0x894F
#define ETH_P_LOOPBACK	0x9000
#define ETH_P_QINQ1	0x9100
#define ETH_P_QINQ2	0x9200
#define ETH_P_QINQ3	0x9300
#define ETH_P_EDSA	0xDADA
#define ETH_P_DSA_8021Q	0xDADB
#define ETH_P_IFE	0xED3E
#define ETH_P_AF_IUCV	0xFBFB

#define ETH_P_802_3_MIN	0x0600

#define ETH_P_802_3	0x0001
#define ETH_P_AX25	0x0002
#define ETH_P_ALL	0x0003
#define ETH_P_802_2	0x0004
#define ETH_P_SNAP	0x0005
#define ETH_P_DDCMP     0x0006
#define ETH_P_WAN_PPP   0x0007
#define ETH_P_PPP_MP    0x0008
#define ETH_P_LOCALTALK 0x0009
#define ETH_P_CAN	0x000C
#define ETH_P_CANFD	0x000D
#define ETH_P_PPPTALK	0x0010
#define ETH_P_TR_802_2	0x0011
#define ETH_P_MOBITEX	0x0015
#define ETH_P_CONTROL	0x0016
#define ETH_P_IRDA	0x0017
#define ETH_P_ECONET	0x0018
#define ETH_P_HDLC	0x0019
#define ETH_P_ARCNET	0x001A
#define ETH_P_DSA	0x001B
#define ETH_P_TRAILER	0x001C
#define ETH_P_PHONET	0x00F5
#define ETH_P_IEEE802154 0x00F6
#define ETH_P_CAIF	0x00F7
#define ETH_P_XDSA	0x00F8
#define ETH_P_MAP	0x00F9

struct ethhdr {
	uint8_t h_dest[ETH_ALEN];
	uint8_t h_source[ETH_ALEN];
	uint16_t h_proto;
};

#include <net/ethernet.h>
#include <net/if_arp.h>

struct	ether_arp {
	struct	arphdr ea_hdr;
	uint8_t arp_sha[ETH_ALEN];
	uint8_t arp_spa[4];
	uint8_t arp_tha[ETH_ALEN];
	uint8_t arp_tpa[4];
};
#define	arp_hrd	ea_hdr.ar_hrd
#define	arp_pro	ea_hdr.ar_pro
#define	arp_hln	ea_hdr.ar_hln
#define	arp_pln	ea_hdr.ar_pln
#define	arp_op	ea_hdr.ar_op

#define ETHER_MAP_IP_MULTICAST(ipaddr, enaddr) \
do { \
	(enaddr)[0] = 0x01; \
	(enaddr)[1] = 0x00; \
	(enaddr)[2] = 0x5e; \
	(enaddr)[3] = ((uint8_t *)ipaddr)[1] & 0x7f; \
	(enaddr)[4] = ((uint8_t *)ipaddr)[2]; \
	(enaddr)[5] = ((uint8_t *)ipaddr)[3]; \
} while(0)

#define __UAPI_DEF_ETHHDR       0

#endif
