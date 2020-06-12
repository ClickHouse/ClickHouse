/* $OpenBSD: gost89_keywrap.c,v 1.3 2014/11/09 19:28:44 miod Exp $ */
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

#include <string.h>

#include <openssl/opensslconf.h>

#ifndef OPENSSL_NO_GOST

#include <openssl/gost.h>

#include "gost_locl.h"

static void
key_diversify_crypto_pro(GOST2814789_KEY *ctx, const unsigned char *inputKey,
    const unsigned char *ukm, unsigned char *outputKey)
{
	unsigned long k, s1, s2;
	int i, mask;
	unsigned char S[8];
	unsigned char *p;

	memcpy(outputKey, inputKey, 32);
	for (i = 0; i < 8; i++) {
		/* Make array of integers from key */
		/* Compute IV S */
		s1 = 0, s2 = 0;
		p = outputKey;
		for (mask = 1; mask < 256; mask <<= 1) {
			c2l(p, k);
			if (mask & ukm[i]) {
				s1 += k;
			} else {
				s2 += k;
			}
		}
		p = S;
		l2c (s1, p);
		l2c (s2, p);
		Gost2814789_set_key(ctx, outputKey, 256);
		mask = 0;
		Gost2814789_cfb64_encrypt(outputKey, outputKey, 32, ctx, S,
		    &mask, 1);
	}
}

int
gost_key_wrap_crypto_pro(int nid, const unsigned char *keyExchangeKey,
    const unsigned char *ukm, const unsigned char *sessionKey,
    unsigned char *wrappedKey)
{
	GOST2814789_KEY ctx;
	unsigned char kek_ukm[32];

	Gost2814789_set_sbox(&ctx, nid);
	key_diversify_crypto_pro(&ctx, keyExchangeKey, ukm, kek_ukm);
	Gost2814789_set_key(&ctx, kek_ukm, 256);
	memcpy(wrappedKey, ukm, 8);
	Gost2814789_encrypt(sessionKey +  0, wrappedKey + 8 +  0, &ctx);
	Gost2814789_encrypt(sessionKey +  8, wrappedKey + 8 +  8, &ctx);
	Gost2814789_encrypt(sessionKey + 16, wrappedKey + 8 + 16, &ctx);
	Gost2814789_encrypt(sessionKey + 24, wrappedKey + 8 + 24, &ctx);
	GOST2814789IMIT(sessionKey, 32, wrappedKey + 40, nid, kek_ukm, ukm);
	return 1;
}

int
gost_key_unwrap_crypto_pro(int nid, const unsigned char *keyExchangeKey,
    const unsigned char *wrappedKey, unsigned char *sessionKey)
{
	unsigned char kek_ukm[32], cek_mac[4];
	GOST2814789_KEY ctx;

	Gost2814789_set_sbox(&ctx, nid);
	/* First 8 bytes of wrapped Key is ukm */
	key_diversify_crypto_pro(&ctx, keyExchangeKey, wrappedKey, kek_ukm);
	Gost2814789_set_key(&ctx, kek_ukm, 256);
	Gost2814789_decrypt(wrappedKey + 8 +  0, sessionKey +  0, &ctx);
	Gost2814789_decrypt(wrappedKey + 8 +  8, sessionKey +  8, &ctx);
	Gost2814789_decrypt(wrappedKey + 8 + 16, sessionKey + 16, &ctx);
	Gost2814789_decrypt(wrappedKey + 8 + 24, sessionKey + 24, &ctx);

	GOST2814789IMIT(sessionKey, 32, cek_mac, nid, kek_ukm, wrappedKey);
	if (memcmp(cek_mac, wrappedKey + 40, 4))
		return 0;

	return 1;
}

#endif
