/* $OpenBSD: whrlpool.h,v 1.5 2014/07/10 22:45:58 jsing Exp $ */

#include <stddef.h>

#ifndef HEADER_WHRLPOOL_H
#define HEADER_WHRLPOOL_H

#include <openssl/opensslconf.h>

#ifdef __cplusplus
extern "C" {
#endif

#define WHIRLPOOL_DIGEST_LENGTH	(512/8)
#define WHIRLPOOL_BBLOCK	512
#define WHIRLPOOL_COUNTER	(256/8)

typedef struct	{
	union	{
		unsigned char	c[WHIRLPOOL_DIGEST_LENGTH];
		/* double q is here to ensure 64-bit alignment */
		double		q[WHIRLPOOL_DIGEST_LENGTH/sizeof(double)];
		}	H;
	unsigned char	data[WHIRLPOOL_BBLOCK/8];
	unsigned int	bitoff;
	size_t		bitlen[WHIRLPOOL_COUNTER/sizeof(size_t)];
	} WHIRLPOOL_CTX;

#ifndef OPENSSL_NO_WHIRLPOOL
int WHIRLPOOL_Init	(WHIRLPOOL_CTX *c);
int WHIRLPOOL_Update	(WHIRLPOOL_CTX *c,const void *inp,size_t bytes);
void WHIRLPOOL_BitUpdate(WHIRLPOOL_CTX *c,const void *inp,size_t bits);
int WHIRLPOOL_Final	(unsigned char *md,WHIRLPOOL_CTX *c);
unsigned char *WHIRLPOOL(const void *inp,size_t bytes,unsigned char *md);
#endif

#ifdef __cplusplus
}
#endif

#endif
