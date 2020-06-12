/* $OpenBSD: gost_locl.h,v 1.4 2016/12/21 15:49:29 jsing Exp $ */
/*
 * Copyright (c) 2014 Dmitry Eremin-Solenikov <dbaryshkov@gmail.com>
 * Copyright (c) 2005-2006 Cryptocom LTD
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. All advertising materials mentioning features or use of this
 *    software must display the following acknowledgment:
 *    "This product includes software developed by the OpenSSL Project
 *    for use in the OpenSSL Toolkit. (http://www.openssl.org/)"
 *
 * 4. The names "OpenSSL Toolkit" and "OpenSSL Project" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For written permission, please contact
 *    openssl-core@openssl.org.
 *
 * 5. Products derived from this software may not be called "OpenSSL"
 *    nor may "OpenSSL" appear in their names without prior written
 *    permission of the OpenSSL Project.
 *
 * 6. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by the OpenSSL Project
 *    for use in the OpenSSL Toolkit (http://www.openssl.org/)"
 *
 * THIS SOFTWARE IS PROVIDED BY THE OpenSSL PROJECT ``AS IS'' AND ANY
 * EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE OpenSSL PROJECT OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 * ====================================================================
 */

#ifndef HEADER_GOST_LOCL_H
#define HEADER_GOST_LOCL_H

#include <openssl/ec.h>
#include <openssl/ecdsa.h>

__BEGIN_HIDDEN_DECLS

/* Internal representation of GOST substitution blocks */
typedef struct {
	unsigned char k8[16];
	unsigned char k7[16];
	unsigned char k6[16];
	unsigned char k5[16];
	unsigned char k4[16];
	unsigned char k3[16];
	unsigned char k2[16];
	unsigned char k1[16];
} gost_subst_block;		

#if defined(__i386) || defined(__i386__) || defined(__x86_64) || defined(__x86_64__)
#  define c2l(c,l)	((l)=*((const unsigned int *)(c)), (c)+=4)
#  define l2c(l,c)	(*((unsigned int *)(c))=(l), (c)+=4)
#else
#define c2l(c,l)	(l =(((unsigned long)(*((c)++)))    ),		\
			 l|=(((unsigned long)(*((c)++)))<< 8),		\
			 l|=(((unsigned long)(*((c)++)))<<16),		\
			 l|=(((unsigned long)(*((c)++)))<<24))
#define l2c(l,c)	(*((c)++)=(unsigned char)(((l)    )&0xff),	\
			 *((c)++)=(unsigned char)(((l)>> 8)&0xff),	\
			 *((c)++)=(unsigned char)(((l)>>16)&0xff),	\
			 *((c)++)=(unsigned char)(((l)>>24)&0xff))
#endif

extern void Gost2814789_encrypt(const unsigned char *in, unsigned char *out,
	const GOST2814789_KEY *key);
extern void Gost2814789_decrypt(const unsigned char *in, unsigned char *out,
	const GOST2814789_KEY *key);
extern void Gost2814789_cryptopro_key_mesh(GOST2814789_KEY *key);

/* GOST 28147-89 key wrapping */
extern int gost_key_unwrap_crypto_pro(int nid,
    const unsigned char *keyExchangeKey, const unsigned char *wrappedKey,
    unsigned char *sessionKey);
extern int gost_key_wrap_crypto_pro(int nid,
    const unsigned char *keyExchangeKey, const unsigned char *ukm,
    const unsigned char *sessionKey, unsigned char *wrappedKey);
/* Pkey part */
extern int gost2001_compute_public(GOST_KEY *ec);
extern ECDSA_SIG *gost2001_do_sign(BIGNUM *md, GOST_KEY *eckey);
extern int gost2001_do_verify(BIGNUM *md, ECDSA_SIG *sig, GOST_KEY *ec);
extern int gost2001_keygen(GOST_KEY *ec);
extern int VKO_compute_key(BIGNUM *X, BIGNUM *Y, const GOST_KEY *pkey,
    GOST_KEY *priv_key, const BIGNUM *ukm);
extern BIGNUM *GOST_le2bn(const unsigned char *buf, size_t len, BIGNUM *bn);
extern int GOST_bn2le(BIGNUM *bn, unsigned char *buf, int len);

/* GOST R 34.10 parameters */
extern int GostR3410_get_md_digest(int nid);
extern int GostR3410_get_pk_digest(int nid);
extern int GostR3410_256_param_id(const char *value);
extern int GostR3410_512_param_id(const char *value);

__END_HIDDEN_DECLS

#endif
