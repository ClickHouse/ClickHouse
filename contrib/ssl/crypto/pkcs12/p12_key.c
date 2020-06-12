/* $OpenBSD: p12_key.c,v 1.26 2017/05/02 03:59:45 deraadt Exp $ */
/* Written by Dr Stephen N Henson (steve@openssl.org) for the OpenSSL
 * project 1999.
 */
/* ====================================================================
 * Copyright (c) 1999 The OpenSSL Project.  All rights reserved.
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
 *    for use in the OpenSSL Toolkit. (http://www.OpenSSL.org/)"
 *
 * 4. The names "OpenSSL Toolkit" and "OpenSSL Project" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For written permission, please contact
 *    licensing@OpenSSL.org.
 *
 * 5. Products derived from this software may not be called "OpenSSL"
 *    nor may "OpenSSL" appear in their names without prior written
 *    permission of the OpenSSL Project.
 *
 * 6. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by the OpenSSL Project
 *    for use in the OpenSSL Toolkit (http://www.OpenSSL.org/)"
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
 *
 * This product includes cryptographic software written by Eric Young
 * (eay@cryptsoft.com).  This product includes software written by Tim
 * Hudson (tjh@cryptsoft.com).
 *
 */

#include <stdio.h>
#include <string.h>

#include <openssl/bn.h>
#include <openssl/err.h>
#include <openssl/pkcs12.h>

/* PKCS12 compatible key/IV generation */
#ifndef min
#define min(a,b) ((a) < (b) ? (a) : (b))
#endif

int
PKCS12_key_gen_asc(const char *pass, int passlen, unsigned char *salt,
    int saltlen, int id, int iter, int n, unsigned char *out,
    const EVP_MD *md_type)
{
	int ret;
	unsigned char *unipass;
	int uniplen;

	if (!pass) {
		unipass = NULL;
		uniplen = 0;
	} else if (!OPENSSL_asc2uni(pass, passlen, &unipass, &uniplen)) {
		PKCS12error(ERR_R_MALLOC_FAILURE);
		return 0;
	}
	ret = PKCS12_key_gen_uni(unipass, uniplen, salt, saltlen,
	    id, iter, n, out, md_type);
	if (ret <= 0)
		return 0;
	freezero(unipass, uniplen);
	return ret;
}

int
PKCS12_key_gen_uni(unsigned char *pass, int passlen, unsigned char *salt,
    int saltlen, int id, int iter, int n, unsigned char *out,
    const EVP_MD *md_type)
{
	unsigned char *B, *D, *I, *p, *Ai;
	int Slen, Plen, Ilen, Ijlen;
	int i, j, u, v;
	int ret = 0;
	BIGNUM *Ij, *Bpl1;	/* These hold Ij and B + 1 */
	EVP_MD_CTX ctx;

	v = EVP_MD_block_size(md_type);
	u = EVP_MD_size(md_type);
	if (u < 0)
		return 0;

	EVP_MD_CTX_init(&ctx);
	D = malloc(v);
	Ai = malloc(u);
	B = malloc(v + 1);
	Slen = v * ((saltlen + v - 1) / v);
	if (passlen)
		Plen = v * ((passlen + v - 1)/v);
	else
		Plen = 0;
	Ilen = Slen + Plen;
	I = malloc(Ilen);
	Ij = BN_new();
	Bpl1 = BN_new();
	if (!D || !Ai || !B || !I || !Ij || !Bpl1)
		goto err;
	for (i = 0; i < v; i++)
		D[i] = id;
	p = I;
	for (i = 0; i < Slen; i++)
		*p++ = salt[i % saltlen];
	for (i = 0; i < Plen; i++)
		*p++ = pass[i % passlen];
	for (;;) {
		if (!EVP_DigestInit_ex(&ctx, md_type, NULL) ||
		    !EVP_DigestUpdate(&ctx, D, v) ||
		    !EVP_DigestUpdate(&ctx, I, Ilen) ||
		    !EVP_DigestFinal_ex(&ctx, Ai, NULL))
			goto err;
		for (j = 1; j < iter; j++) {
			if (!EVP_DigestInit_ex(&ctx, md_type, NULL) ||
			    !EVP_DigestUpdate(&ctx, Ai, u) ||
			    !EVP_DigestFinal_ex(&ctx, Ai, NULL))
				goto err;
		}
		memcpy (out, Ai, min (n, u));
		if (u >= n) {
			ret = 1;
			goto end;
		}
		n -= u;
		out += u;
		for (j = 0; j < v; j++)
			B[j] = Ai[j % u];
		/* Work out B + 1 first then can use B as tmp space */
		if (!BN_bin2bn (B, v, Bpl1))
			goto err;
		if (!BN_add_word (Bpl1, 1))
			goto err;
		for (j = 0; j < Ilen; j += v) {
			if (!BN_bin2bn(I + j, v, Ij))
				goto err;
			if (!BN_add(Ij, Ij, Bpl1))
				goto err;
			if (!BN_bn2bin(Ij, B))
				goto err;
			Ijlen = BN_num_bytes (Ij);
			/* If more than 2^(v*8) - 1 cut off MSB */
			if (Ijlen > v) {
				if (!BN_bn2bin (Ij, B))
					goto err;
				memcpy (I + j, B + 1, v);
#ifndef PKCS12_BROKEN_KEYGEN
				/* If less than v bytes pad with zeroes */
			} else if (Ijlen < v) {
				memset(I + j, 0, v - Ijlen);
				if (!BN_bn2bin(Ij, I + j + v - Ijlen))
					goto err;
#endif
			} else if (!BN_bn2bin (Ij, I + j))
				goto err;
		}
	}

err:
	PKCS12error(ERR_R_MALLOC_FAILURE);

end:
	free(Ai);
	free(B);
	free(D);
	free(I);
	BN_free(Ij);
	BN_free(Bpl1);
	EVP_MD_CTX_cleanup(&ctx);
	return ret;
}
