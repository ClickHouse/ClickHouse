/* $OpenBSD: gostr341001_key.c,v 1.8 2017/05/02 03:59:44 deraadt Exp $ */
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
#include <openssl/bn.h>
#include <openssl/err.h>
#include <openssl/gost.h>
#include <openssl/objects.h>
#include "gost_locl.h"

struct gost_key_st {
	EC_GROUP *group;

	EC_POINT *pub_key;
	BIGNUM	 *priv_key;

	int	references;

	int digest_nid;
};

GOST_KEY *
GOST_KEY_new(void)
{
	GOST_KEY *ret;

	ret = malloc(sizeof(GOST_KEY));
	if (ret == NULL) {
		GOSTerror(ERR_R_MALLOC_FAILURE);
		return (NULL);
	}
	ret->group = NULL;
	ret->pub_key = NULL;
	ret->priv_key = NULL;
	ret->references = 1;
	ret->digest_nid = NID_undef;
	return (ret);
}

void
GOST_KEY_free(GOST_KEY *r)
{
	int i;

	if (r == NULL)
		return;

	i = CRYPTO_add(&r->references, -1, CRYPTO_LOCK_EC);
	if (i > 0)
		return;

	EC_GROUP_free(r->group);
	EC_POINT_free(r->pub_key);
	BN_clear_free(r->priv_key);

	freezero(r, sizeof(GOST_KEY));
}

int
GOST_KEY_check_key(const GOST_KEY *key)
{
	int ok = 0;
	BN_CTX *ctx = NULL;
	BIGNUM *order = NULL;
	EC_POINT *point = NULL;

	if (key == NULL || key->group == NULL || key->pub_key == NULL) {
		GOSTerror(ERR_R_PASSED_NULL_PARAMETER);
		return 0;
	}
	if (EC_POINT_is_at_infinity(key->group, key->pub_key) != 0) {
		GOSTerror(EC_R_POINT_AT_INFINITY);
		goto err;
	}
	if ((ctx = BN_CTX_new()) == NULL)
		goto err;
	if ((point = EC_POINT_new(key->group)) == NULL)
		goto err;

	/* testing whether the pub_key is on the elliptic curve */
	if (EC_POINT_is_on_curve(key->group, key->pub_key, ctx) == 0) {
		GOSTerror(EC_R_POINT_IS_NOT_ON_CURVE);
		goto err;
	}
	/* testing whether pub_key * order is the point at infinity */
	if ((order = BN_new()) == NULL)
		goto err;
	if (EC_GROUP_get_order(key->group, order, ctx) == 0) {
		GOSTerror(EC_R_INVALID_GROUP_ORDER);
		goto err;
	}
	if (EC_POINT_mul(key->group, point, NULL, key->pub_key, order,
	    ctx) == 0) {
		GOSTerror(ERR_R_EC_LIB);
		goto err;
	}
	if (EC_POINT_is_at_infinity(key->group, point) == 0) {
		GOSTerror(EC_R_WRONG_ORDER);
		goto err;
	}
	/*
	 * in case the priv_key is present : check if generator * priv_key ==
	 * pub_key
	 */
	if (key->priv_key != NULL) {
		if (BN_cmp(key->priv_key, order) >= 0) {
			GOSTerror(EC_R_WRONG_ORDER);
			goto err;
		}
		if (EC_POINT_mul(key->group, point, key->priv_key, NULL, NULL,
		    ctx) == 0) {
			GOSTerror(ERR_R_EC_LIB);
			goto err;
		}
		if (EC_POINT_cmp(key->group, point, key->pub_key, ctx) != 0) {
			GOSTerror(EC_R_INVALID_PRIVATE_KEY);
			goto err;
		}
	}
	ok = 1;
err:
	BN_free(order);
	BN_CTX_free(ctx);
	EC_POINT_free(point);
	return (ok);
}

int
GOST_KEY_set_public_key_affine_coordinates(GOST_KEY *key, BIGNUM *x, BIGNUM *y)
{
	BN_CTX *ctx = NULL;
	BIGNUM *tx, *ty;
	EC_POINT *point = NULL;
	int ok = 0;

	if (key == NULL || key->group == NULL || x == NULL || y == NULL) {
		GOSTerror(ERR_R_PASSED_NULL_PARAMETER);
		return 0;
	}
	ctx = BN_CTX_new();
	if (ctx == NULL)
		goto err;

	point = EC_POINT_new(key->group);
	if (point == NULL)
		goto err;

	if ((tx = BN_CTX_get(ctx)) == NULL)
		goto err;
	if ((ty = BN_CTX_get(ctx)) == NULL)
		goto err;
	if (EC_POINT_set_affine_coordinates_GFp(key->group, point, x, y,
	    ctx) == 0)
		goto err;
	if (EC_POINT_get_affine_coordinates_GFp(key->group, point, tx, ty,
	    ctx) == 0)
		goto err;
	/*
	 * Check if retrieved coordinates match originals: if not, values are
	 * out of range.
	 */
	if (BN_cmp(x, tx) != 0 || BN_cmp(y, ty) != 0) {
		GOSTerror(EC_R_COORDINATES_OUT_OF_RANGE);
		goto err;
	}
	if (GOST_KEY_set_public_key(key, point) == 0)
		goto err;

	if (GOST_KEY_check_key(key) == 0)
		goto err;

	ok = 1;

err:
	EC_POINT_free(point);
	BN_CTX_free(ctx);
	return ok;

}

const EC_GROUP *
GOST_KEY_get0_group(const GOST_KEY *key)
{
	return key->group;
}

int
GOST_KEY_set_group(GOST_KEY *key, const EC_GROUP *group)
{
	EC_GROUP_free(key->group);
	key->group = EC_GROUP_dup(group);
	return (key->group == NULL) ? 0 : 1;
}

const BIGNUM *
GOST_KEY_get0_private_key(const GOST_KEY *key)
{
	return key->priv_key;
}

int
GOST_KEY_set_private_key(GOST_KEY *key, const BIGNUM *priv_key)
{
	BN_clear_free(key->priv_key);
	key->priv_key = BN_dup(priv_key);
	return (key->priv_key == NULL) ? 0 : 1;
}

const EC_POINT *
GOST_KEY_get0_public_key(const GOST_KEY *key)
{
	return key->pub_key;
}

int
GOST_KEY_set_public_key(GOST_KEY *key, const EC_POINT *pub_key)
{
	EC_POINT_free(key->pub_key);
	key->pub_key = EC_POINT_dup(pub_key, key->group);
	return (key->pub_key == NULL) ? 0 : 1;
}

int
GOST_KEY_get_digest(const GOST_KEY *key)
{
	return key->digest_nid;
}
int
GOST_KEY_set_digest(GOST_KEY *key, int digest_nid)
{
	if (digest_nid == NID_id_GostR3411_94_CryptoProParamSet ||
	    digest_nid == NID_id_tc26_gost3411_2012_256 ||
	    digest_nid == NID_id_tc26_gost3411_2012_512) {
		key->digest_nid = digest_nid;
		return 1;
	}

	return 0;
}

size_t
GOST_KEY_get_size(const GOST_KEY *r)
{
	int i;
	BIGNUM *order = NULL;
	const EC_GROUP *group;

	if (r == NULL)
		return 0;
	group = GOST_KEY_get0_group(r);
	if (group == NULL)
		return 0;

	if ((order = BN_new()) == NULL)
		return 0;

	if (EC_GROUP_get_order(group, order, NULL) == 0) {
		BN_clear_free(order);
		return 0;
	}

	i = BN_num_bytes(order);
	BN_clear_free(order);
	return (i);
}
#endif
