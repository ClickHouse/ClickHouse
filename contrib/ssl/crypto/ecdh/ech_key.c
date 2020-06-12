/* $OpenBSD: ech_key.c,v 1.7 2017/01/29 17:49:23 beck Exp $ */
/* ====================================================================
 * Copyright 2002 Sun Microsystems, Inc. ALL RIGHTS RESERVED.
 *
 * The Elliptic Curve Public-Key Crypto Library (ECC Code) included
 * herein is developed by SUN MICROSYSTEMS, INC., and is contributed
 * to the OpenSSL project.
 *
 * The ECC Code is licensed pursuant to the OpenSSL open source
 * license provided below.
 *
 * The ECDH software is originally written by Douglas Stebila of
 * Sun Microsystems Laboratories.
 *
 */
/* ====================================================================
 * Copyright (c) 1998-2003 The OpenSSL Project.  All rights reserved.
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
 *    openssl-core@OpenSSL.org.
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

#include <limits.h>
#include <string.h>

#include <openssl/opensslconf.h>

#include <openssl/bn.h>
#include <openssl/err.h>
#include <openssl/obj_mac.h>
#include <openssl/sha.h>

#include "ech_locl.h"

static int ecdh_compute_key(void *out, size_t len, const EC_POINT *pub_key,
    EC_KEY *ecdh,
    void *(*KDF)(const void *in, size_t inlen, void *out, size_t *outlen));

/*
 * This implementation is based on the following primitives in the IEEE 1363
 * standard:
 *  - ECKAS-DH1
 *  - ECSVDP-DH
 * Finally an optional KDF is applied.
 */
static int
ecdh_compute_key(void *out, size_t outlen, const EC_POINT *pub_key,
    EC_KEY *ecdh,
    void *(*KDF)(const void *in, size_t inlen, void *out, size_t *outlen))
{
	BN_CTX *ctx;
	EC_POINT *tmp = NULL;
	BIGNUM *x = NULL, *y = NULL;
	const BIGNUM *priv_key;
	const EC_GROUP* group;
	int ret = -1;
	size_t buflen, len;
	unsigned char *buf = NULL;

	if (outlen > INT_MAX) {
		/* Sort of, anyway. */
		ECDHerror(ERR_R_MALLOC_FAILURE);
		return -1;
	}

	if ((ctx = BN_CTX_new()) == NULL)
		goto err;
	BN_CTX_start(ctx);
	if ((x = BN_CTX_get(ctx)) == NULL)
		goto err;
	if ((y = BN_CTX_get(ctx)) == NULL)
		goto err;

	priv_key = EC_KEY_get0_private_key(ecdh);
	if (priv_key == NULL) {
		ECDHerror(ECDH_R_NO_PRIVATE_VALUE);
		goto err;
	}

	group = EC_KEY_get0_group(ecdh);
	if ((tmp = EC_POINT_new(group)) == NULL) {
		ECDHerror(ERR_R_MALLOC_FAILURE);
		goto err;
	}

	if (!EC_POINT_mul(group, tmp, NULL, pub_key, priv_key, ctx)) {
		ECDHerror(ECDH_R_POINT_ARITHMETIC_FAILURE);
		goto err;
	}

	if (EC_METHOD_get_field_type(EC_GROUP_method_of(group)) ==
	    NID_X9_62_prime_field) {
		if (!EC_POINT_get_affine_coordinates_GFp(group, tmp, x, y,
		    ctx)) {
			ECDHerror(ECDH_R_POINT_ARITHMETIC_FAILURE);
			goto err;
		}
	}
#ifndef OPENSSL_NO_EC2M
	else {
		if (!EC_POINT_get_affine_coordinates_GF2m(group, tmp, x, y,
		    ctx)) {
			ECDHerror(ECDH_R_POINT_ARITHMETIC_FAILURE);
			goto err;
		}
	}
#endif

	buflen = ECDH_size(ecdh);
	len = BN_num_bytes(x);
	if (len > buflen) {
		ECDHerror(ERR_R_INTERNAL_ERROR);
		goto err;
	}
	if (KDF == NULL && outlen < buflen) {
		/* The resulting key would be truncated. */
		ECDHerror(ECDH_R_KEY_TRUNCATION);
		goto err;
	}
	if ((buf = malloc(buflen)) == NULL) {
		ECDHerror(ERR_R_MALLOC_FAILURE);
		goto err;
	}

	memset(buf, 0, buflen - len);
	if (len != (size_t)BN_bn2bin(x, buf + buflen - len)) {
		ECDHerror(ERR_R_BN_LIB);
		goto err;
	}

	if (KDF != NULL) {
		if (KDF(buf, buflen, out, &outlen) == NULL) {
			ECDHerror(ECDH_R_KDF_FAILED);
			goto err;
		}
		ret = outlen;
	} else {
		/* No KDF, just copy out the key and zero the rest. */
		if (outlen > buflen) {
			memset((void *)((uintptr_t)out + buflen), 0, outlen - buflen);
			outlen = buflen;
		}
		memcpy(out, buf, outlen);
		ret = outlen;
	}

err:
	EC_POINT_free(tmp);
	if (ctx)
		BN_CTX_end(ctx);
	BN_CTX_free(ctx);
	free(buf);
	return (ret);
}

static ECDH_METHOD openssl_ecdh_meth = {
	.name = "OpenSSL ECDH method",
	.compute_key = ecdh_compute_key
};

const ECDH_METHOD *
ECDH_OpenSSL(void)
{
	return &openssl_ecdh_meth;
}

int
ECDH_compute_key(void *out, size_t outlen, const EC_POINT *pub_key,
    EC_KEY *eckey,
    void *(*KDF)(const void *in, size_t inlen, void *out, size_t *outlen))
{
	ECDH_DATA *ecdh = ecdh_check(eckey);
	if (ecdh == NULL)
		return 0;
	return ecdh->meth->compute_key(out, outlen, pub_key, eckey, KDF);
}
