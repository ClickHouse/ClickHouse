/*
 * Public domain
 * netinet/ip.h compatibility shim
 */

#if defined(__hpux)
#include <netinet/in_systm.h>
#endif

#ifndef _WIN32
#include_next <netinet/ip.h>
#else
#include <win32netcompat.h>
#endif

/*
 * Definitions for DiffServ Codepoints as per RFC2474
 */
#ifndef IPTOS_DSCP_CS0
#define	IPTOS_DSCP_CS0		0x00
#define	IPTOS_DSCP_CS1		0x20
#define	IPTOS_DSCP_CS2		0x40
#define	IPTOS_DSCP_CS3		0x60
#define	IPTOS_DSCP_CS4		0x80
#define	IPTOS_DSCP_CS5		0xa0
#define	IPTOS_DSCP_CS6		0xc0
#define	IPTOS_DSCP_CS7		0xe0
#endif

#ifndef IPTOS_DSCP_AF11
#define	IPTOS_DSCP_AF11		0x28
#define	IPTOS_DSCP_AF12		0x30
#define	IPTOS_DSCP_AF13		0x38
#define	IPTOS_DSCP_AF21		0x48
#define	IPTOS_DSCP_AF22		0x50
#define	IPTOS_DSCP_AF23		0x58
#define	IPTOS_DSCP_AF31		0x68
#define	IPTOS_DSCP_AF32		0x70
#define	IPTOS_DSCP_AF33		0x78
#define	IPTOS_DSCP_AF41		0x88
#define	IPTOS_DSCP_AF42		0x90
#define	IPTOS_DSCP_AF43		0x98
#endif

#ifndef IPTOS_DSCP_EF
#define	IPTOS_DSCP_EF		0xb8
#endif
