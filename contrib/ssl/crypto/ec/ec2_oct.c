/* $OpenBSD: ec2_oct.c,v 1.8 2017/01/29 17:49:23 beck Exp $ */
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
 * The software is originally written by Sheueling Chang Shantz and
 * Douglas Stebila of Sun Microsystems Laboratories.
 *
 */
/* ====================================================================
 * Copyright (c) 1998-2005 The OpenSSL Project.  All rights reserved.
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
 *
 * This product includes cryptographic software written by Eric Young
 * (eay@cryptsoft.com).  This product includes software written by Tim
 * Hudson (tjh@cryptsoft.com).
 *
 */

#include <openssl/opensslconf.h>

#include <openssl/err.h>

#include "ec_lcl.h"

#ifndef OPENSSL_NO_EC2M

/* Calculates and sets the affine coordinates of an EC_POINT from the given
 * compressed coordinates.  Uses algorithm 2.3.4 of SEC 1.
 * Note that the simple implementation only uses affine coordinates.
 *
 * The method is from the following publication:
 *
 *     Harper, Menezes, Vanstone:
 *     "Public-Key Cryptosystems with Very Small Key Lengths",
 *     EUROCRYPT '92, Springer-Verlag LNCS 658,
 *     published February 1993
 *
 * US Patents 6,141,420 and 6,618,483 (Vanstone, Mullin, Agnew) describe
 * the same method, but claim no priority date earlier than July 29, 1994
 * (and additionally fail to cite the EUROCRYPT '92 publication as prior art).
 */
int 
ec_GF2m_simple_set_compressed_coordinates(const EC_GROUP *group, EC_POINT *point,
    const BIGNUM *x_, int y_bit, BN_CTX *ctx)
{
	BN_CTX *new_ctx = NULL;
	BIGNUM *tmp, *x, *y, *z;
	int ret = 0, z0;

	/* clear error queue */
	ERR_clear_error();

	if (ctx == NULL) {
		ctx = new_ctx = BN_CTX_new();
		if (ctx == NULL)
			return 0;
	}
	y_bit = (y_bit != 0) ? 1 : 0;

	BN_CTX_start(ctx);
	if ((tmp = BN_CTX_get(ctx)) == NULL)
		goto err;
	if ((x = BN_CTX_get(ctx)) == NULL)
		goto err;
	if ((y = BN_CTX_get(ctx)) == NULL)
		goto err;
	if ((z = BN_CTX_get(ctx)) == NULL)
		goto err;

	if (!BN_GF2m_mod_arr(x, x_, group->poly))
		goto err;
	if (BN_is_zero(x)) {
		if (!BN_GF2m_mod_sqrt_arr(y, &group->b, group->poly, ctx))
			goto err;
	} else {
		if (!group->meth->field_sqr(group, tmp, x, ctx))
			goto err;
		if (!group->meth->field_div(group, tmp, &group->b, tmp, ctx))
			goto err;
		if (!BN_GF2m_add(tmp, &group->a, tmp))
			goto err;
		if (!BN_GF2m_add(tmp, x, tmp))
			goto err;
		if (!BN_GF2m_mod_solve_quad_arr(z, tmp, group->poly, ctx)) {
			unsigned long err = ERR_peek_last_error();

			if (ERR_GET_LIB(err) == ERR_LIB_BN &&
			    ERR_GET_REASON(err) == BN_R_NO_SOLUTION) {
				ERR_clear_error();
				ECerror(EC_R_INVALID_COMPRESSED_POINT);
			} else
				ECerror(ERR_R_BN_LIB);
			goto err;
		}
		z0 = (BN_is_odd(z)) ? 1 : 0;
		if (!group->meth->field_mul(group, y, x, z, ctx))
			goto err;
		if (z0 != y_bit) {
			if (!BN_GF2m_add(y, y, x))
				goto err;
		}
	}

	if (!EC_POINT_set_affine_coordinates_GF2m(group, point, x, y, ctx))
		goto err;

	ret = 1;

err:
	BN_CTX_end(ctx);
	BN_CTX_free(new_ctx);
	return ret;
}


/* Converts an EC_POINT to an octet string.
 * If buf is NULL, the encoded length will be returned.
 * If the length len of buf is smaller than required an error will be returned.
 */
size_t 
ec_GF2m_simple_point2oct(const EC_GROUP *group, const EC_POINT *point,
    point_conversion_form_t form,
    unsigned char *buf, size_t len, BN_CTX * ctx)
{
	size_t ret;
	BN_CTX *new_ctx = NULL;
	int used_ctx = 0;
	BIGNUM *x, *y, *yxi;
	size_t field_len, i, skip;

	if ((form != POINT_CONVERSION_COMPRESSED)
	    && (form != POINT_CONVERSION_UNCOMPRESSED)
	    && (form != POINT_CONVERSION_HYBRID)) {
		ECerror(EC_R_INVALID_FORM);
		goto err;
	}
	if (EC_POINT_is_at_infinity(group, point) > 0) {
		/* encodes to a single 0 octet */
		if (buf != NULL) {
			if (len < 1) {
				ECerror(EC_R_BUFFER_TOO_SMALL);
				return 0;
			}
			buf[0] = 0;
		}
		return 1;
	}
	/* ret := required output buffer length */
	field_len = (EC_GROUP_get_degree(group) + 7) / 8;
	ret = (form == POINT_CONVERSION_COMPRESSED) ? 1 + field_len :
	    1 + 2 * field_len;

	/* if 'buf' is NULL, just return required length */
	if (buf != NULL) {
		if (len < ret) {
			ECerror(EC_R_BUFFER_TOO_SMALL);
			goto err;
		}
		if (ctx == NULL) {
			ctx = new_ctx = BN_CTX_new();
			if (ctx == NULL)
				return 0;
		}
		BN_CTX_start(ctx);
		used_ctx = 1;
		if ((x = BN_CTX_get(ctx)) == NULL)
			goto err;
		if ((y = BN_CTX_get(ctx)) == NULL)
			goto err;
		if ((yxi = BN_CTX_get(ctx)) == NULL)
			goto err;

		if (!EC_POINT_get_affine_coordinates_GF2m(group, point, x, y, ctx))
			goto err;

		buf[0] = form;
		if ((form != POINT_CONVERSION_UNCOMPRESSED) && !BN_is_zero(x)) {
			if (!group->meth->field_div(group, yxi, y, x, ctx))
				goto err;
			if (BN_is_odd(yxi))
				buf[0]++;
		}
		i = 1;

		skip = field_len - BN_num_bytes(x);
		if (skip > field_len) {
			ECerror(ERR_R_INTERNAL_ERROR);
			goto err;
		}
		while (skip > 0) {
			buf[i++] = 0;
			skip--;
		}
		skip = BN_bn2bin(x, buf + i);
		i += skip;
		if (i != 1 + field_len) {
			ECerror(ERR_R_INTERNAL_ERROR);
			goto err;
		}
		if (form == POINT_CONVERSION_UNCOMPRESSED ||
		    form == POINT_CONVERSION_HYBRID) {
			skip = field_len - BN_num_bytes(y);
			if (skip > field_len) {
				ECerror(ERR_R_INTERNAL_ERROR);
				goto err;
			}
			while (skip > 0) {
				buf[i++] = 0;
				skip--;
			}
			skip = BN_bn2bin(y, buf + i);
			i += skip;
		}
		if (i != ret) {
			ECerror(ERR_R_INTERNAL_ERROR);
			goto err;
		}
	}
	if (used_ctx)
		BN_CTX_end(ctx);
	BN_CTX_free(new_ctx);
	return ret;

err:
	if (used_ctx)
		BN_CTX_end(ctx);
	BN_CTX_free(new_ctx);
	return 0;
}


/* Converts an octet string representation to an EC_POINT.
 * Note that the simple implementation only uses affine coordinates.
 */
int 
ec_GF2m_simple_oct2point(const EC_GROUP *group, EC_POINT *point,
    const unsigned char *buf, size_t len, BN_CTX *ctx)
{
	point_conversion_form_t form;
	int y_bit;
	BN_CTX *new_ctx = NULL;
	BIGNUM *x, *y, *yxi;
	size_t field_len, enc_len;
	int ret = 0;

	if (len == 0) {
		ECerror(EC_R_BUFFER_TOO_SMALL);
		return 0;
	}
	form = buf[0];
	y_bit = form & 1;
	form = form & ~1U;
	if ((form != 0) && (form != POINT_CONVERSION_COMPRESSED) &&
	    (form != POINT_CONVERSION_UNCOMPRESSED) &&
	    (form != POINT_CONVERSION_HYBRID)) {
		ECerror(EC_R_INVALID_ENCODING);
		return 0;
	}
	if ((form == 0 || form == POINT_CONVERSION_UNCOMPRESSED) && y_bit) {
		ECerror(EC_R_INVALID_ENCODING);
		return 0;
	}
	if (form == 0) {
		if (len != 1) {
			ECerror(EC_R_INVALID_ENCODING);
			return 0;
		}
		return EC_POINT_set_to_infinity(group, point);
	}
	field_len = (EC_GROUP_get_degree(group) + 7) / 8;
	enc_len = (form == POINT_CONVERSION_COMPRESSED) ? 1 + field_len :
	    1 + 2 * field_len;

	if (len != enc_len) {
		ECerror(EC_R_INVALID_ENCODING);
		return 0;
	}
	if (ctx == NULL) {
		ctx = new_ctx = BN_CTX_new();
		if (ctx == NULL)
			return 0;
	}
	BN_CTX_start(ctx);
	if ((x = BN_CTX_get(ctx)) == NULL)
		goto err;
	if ((y = BN_CTX_get(ctx)) == NULL)
		goto err;
	if ((yxi = BN_CTX_get(ctx)) == NULL)
		goto err;

	if (!BN_bin2bn(buf + 1, field_len, x))
		goto err;
	if (BN_ucmp(x, &group->field) >= 0) {
		ECerror(EC_R_INVALID_ENCODING);
		goto err;
	}
	if (form == POINT_CONVERSION_COMPRESSED) {
		if (!EC_POINT_set_compressed_coordinates_GF2m(group, point, x, y_bit, ctx))
			goto err;
	} else {
		if (!BN_bin2bn(buf + 1 + field_len, field_len, y))
			goto err;
		if (BN_ucmp(y, &group->field) >= 0) {
			ECerror(EC_R_INVALID_ENCODING);
			goto err;
		}
		if (form == POINT_CONVERSION_HYBRID) {
			if (!group->meth->field_div(group, yxi, y, x, ctx))
				goto err;
			if (y_bit != BN_is_odd(yxi)) {
				ECerror(EC_R_INVALID_ENCODING);
				goto err;
			}
		}
		if (!EC_POINT_set_affine_coordinates_GF2m(group, point, x, y, ctx))
			goto err;
	}

	/* test required by X9.62 */
	if (EC_POINT_is_on_curve(group, point, ctx) <= 0) {
		ECerror(EC_R_POINT_IS_NOT_ON_CURVE);
		goto err;
	}
	ret = 1;

err:
	BN_CTX_end(ctx);
	BN_CTX_free(new_ctx);
	return ret;
}
#endif
