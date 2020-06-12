/* $OpenBSD: gostr341001_ameth.c,v 1.11 2017/01/29 17:49:23 beck Exp $ */
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
#include <openssl/evp.h>
#include <openssl/ec.h>
#include <openssl/err.h>
#include <openssl/x509.h>
#include <openssl/gost.h>


#include "asn1_locl.h"
#include "gost_locl.h"
#include "gost_asn1.h"

static void
pkey_free_gost01(EVP_PKEY *key)
{
	GOST_KEY_free(key->pkey.gost);
}

/*
 * Parses GOST algorithm parameters from X509_ALGOR and
 * modifies pkey setting NID and parameters
 */
static int
decode_gost01_algor_params(EVP_PKEY *pkey, const unsigned char **p, int len)
{
	int param_nid = NID_undef, digest_nid = NID_undef;
	GOST_KEY_PARAMS *gkp = NULL;
	EC_GROUP *group;
	GOST_KEY *ec;

	gkp = d2i_GOST_KEY_PARAMS(NULL, p, len);
	if (gkp == NULL) {
		GOSTerror(GOST_R_BAD_PKEY_PARAMETERS_FORMAT);
		return 0;
	}
	param_nid = OBJ_obj2nid(gkp->key_params);
	digest_nid = OBJ_obj2nid(gkp->hash_params);
	GOST_KEY_PARAMS_free(gkp);

	ec = pkey->pkey.gost;
	if (ec == NULL) {
		ec = GOST_KEY_new();
		if (ec == NULL)
			return 0;
		if (EVP_PKEY_assign_GOST(pkey, ec) == 0)
			return 0;
	}

	group = EC_GROUP_new_by_curve_name(param_nid);
	if (group == NULL)
		return 0;
	EC_GROUP_set_asn1_flag(group, OPENSSL_EC_NAMED_CURVE);
	if (GOST_KEY_set_group(ec, group) == 0) {
		EC_GROUP_free(group);
		return 0;
	}
	EC_GROUP_free(group);
	if (GOST_KEY_set_digest(ec, digest_nid) == 0)
		return 0;
	return 1;
}

static ASN1_STRING *
encode_gost01_algor_params(const EVP_PKEY *key)
{
	ASN1_STRING *params = ASN1_STRING_new();
	GOST_KEY_PARAMS *gkp = GOST_KEY_PARAMS_new();
	int pkey_param_nid = NID_undef;

	if (params == NULL || gkp == NULL) {
		GOSTerror(ERR_R_MALLOC_FAILURE);
		ASN1_STRING_free(params);
		params = NULL;
		goto err;
	}

	pkey_param_nid =
	    EC_GROUP_get_curve_name(GOST_KEY_get0_group(key->pkey.gost));
	gkp->key_params = OBJ_nid2obj(pkey_param_nid);
	gkp->hash_params = OBJ_nid2obj(GOST_KEY_get_digest(key->pkey.gost));
	/*gkp->cipher_params = OBJ_nid2obj(cipher_param_nid); */
	params->length = i2d_GOST_KEY_PARAMS(gkp, &params->data);
	if (params->length <= 0) {
		GOSTerror(ERR_R_MALLOC_FAILURE);
		ASN1_STRING_free(params);
		params = NULL;
		goto err;
	}
	params->type = V_ASN1_SEQUENCE;
err:
	GOST_KEY_PARAMS_free(gkp);
	return params;
}

static int
pub_cmp_gost01(const EVP_PKEY *a, const EVP_PKEY *b)
{
	const GOST_KEY *ea = a->pkey.gost;
	const GOST_KEY *eb = b->pkey.gost;
	const EC_POINT *ka, *kb;
	int ret = 0;

	if (ea == NULL || eb == NULL)
		return 0;
	ka = GOST_KEY_get0_public_key(ea);
	kb = GOST_KEY_get0_public_key(eb);
	if (ka == NULL || kb == NULL)
		return 0;
	ret = (0 == EC_POINT_cmp(GOST_KEY_get0_group(ea), ka, kb, NULL));
	return ret;
}

static int
pkey_size_gost01(const EVP_PKEY *pk)
{
	if (GOST_KEY_get_digest(pk->pkey.gost) == NID_id_tc26_gost3411_2012_512)
		return 128;
	return 64;
}

static int
pkey_bits_gost01(const EVP_PKEY *pk)
{
	if (GOST_KEY_get_digest(pk->pkey.gost) == NID_id_tc26_gost3411_2012_512)
		return 512;
	return 256;
}

static int
pub_decode_gost01(EVP_PKEY *pk, X509_PUBKEY *pub)
{
	X509_ALGOR *palg = NULL;
	const unsigned char *pubkey_buf = NULL;
	const unsigned char *p;
	ASN1_OBJECT *palgobj = NULL;
	int pub_len;
	BIGNUM *X, *Y;
	ASN1_OCTET_STRING *octet = NULL;
	int len;
	int ret;
	int ptype = V_ASN1_UNDEF;
	ASN1_STRING *pval = NULL;

	if (X509_PUBKEY_get0_param(&palgobj, &pubkey_buf, &pub_len, &palg, pub)
	    == 0)
		return 0;
	(void)EVP_PKEY_assign_GOST(pk, NULL);
	X509_ALGOR_get0(NULL, &ptype, (void **)&pval, palg);
	if (ptype != V_ASN1_SEQUENCE) {
		GOSTerror(GOST_R_BAD_KEY_PARAMETERS_FORMAT);
		return 0;
	}
	p = pval->data;
	if (decode_gost01_algor_params(pk, &p, pval->length) == 0)
		return 0;

	octet = d2i_ASN1_OCTET_STRING(NULL, &pubkey_buf, pub_len);
	if (octet == NULL) {
		GOSTerror(ERR_R_MALLOC_FAILURE);
		return 0;
	}
	len = octet->length / 2;

	X = GOST_le2bn(octet->data, len, NULL);
	Y = GOST_le2bn(octet->data + len, len, NULL);

	ASN1_OCTET_STRING_free(octet);

	ret = GOST_KEY_set_public_key_affine_coordinates(pk->pkey.gost, X, Y);
	if (ret == 0)
		GOSTerror(ERR_R_EC_LIB);

	BN_free(X);
	BN_free(Y);

	return ret;
}

static int
pub_encode_gost01(X509_PUBKEY *pub, const EVP_PKEY *pk)
{
	ASN1_OBJECT *algobj = NULL;
	ASN1_OCTET_STRING *octet = NULL;
	ASN1_STRING *params = NULL;
	void *pval = NULL;
	unsigned char *buf = NULL, *sptr;
	int key_size, ret = 0;
	const EC_POINT *pub_key;
	BIGNUM *X = NULL, *Y = NULL;
	const GOST_KEY *ec = pk->pkey.gost;
	int ptype = V_ASN1_UNDEF;

	algobj = OBJ_nid2obj(GostR3410_get_pk_digest(GOST_KEY_get_digest(ec)));
	if (pk->save_parameters) {
		params = encode_gost01_algor_params(pk);
		if (params == NULL)
			return 0;
		pval = params;
		ptype = V_ASN1_SEQUENCE;
	}

	key_size = GOST_KEY_get_size(ec);

	pub_key = GOST_KEY_get0_public_key(ec);
	if (pub_key == NULL) {
		GOSTerror(GOST_R_PUBLIC_KEY_UNDEFINED);
		goto err;
	}

	octet = ASN1_OCTET_STRING_new();
	if (octet == NULL) {
		GOSTerror(ERR_R_MALLOC_FAILURE);
		goto err;
	}

	ret = ASN1_STRING_set(octet, NULL, 2 * key_size);
	if (ret == 0) {
		GOSTerror(ERR_R_INTERNAL_ERROR);
		goto err;
	}

	sptr = ASN1_STRING_data(octet);

	X = BN_new();
	Y = BN_new();
	if (X == NULL || Y == NULL) {
		GOSTerror(ERR_R_MALLOC_FAILURE);
		goto err;
	}

	if (EC_POINT_get_affine_coordinates_GFp(GOST_KEY_get0_group(ec),
	    pub_key, X, Y, NULL) == 0) {
		GOSTerror(ERR_R_EC_LIB);
		goto err;
	}

	GOST_bn2le(X, sptr, key_size);
	GOST_bn2le(Y, sptr + key_size, key_size);

	BN_free(Y);
	BN_free(X);

	ret = i2d_ASN1_OCTET_STRING(octet, &buf);
	ASN1_BIT_STRING_free(octet);
	if (ret < 0)
		return 0;

	return X509_PUBKEY_set0_param(pub, algobj, ptype, pval, buf, ret);

err:
	BN_free(Y);
	BN_free(X);
	ASN1_BIT_STRING_free(octet);
	ASN1_STRING_free(params);
	return 0;
}

static int
param_print_gost01(BIO *out, const EVP_PKEY *pkey, int indent, ASN1_PCTX *pctx)
{
	int param_nid =
	    EC_GROUP_get_curve_name(GOST_KEY_get0_group(pkey->pkey.gost));

	if (BIO_indent(out, indent, 128) == 0)
		return 0;
	BIO_printf(out, "Parameter set: %s\n", OBJ_nid2ln(param_nid));
	if (BIO_indent(out, indent, 128) == 0)
		return 0;
	BIO_printf(out, "Digest Algorithm: %s\n",
	    OBJ_nid2ln(GOST_KEY_get_digest(pkey->pkey.gost)));
	return 1;
}

static int
pub_print_gost01(BIO *out, const EVP_PKEY *pkey, int indent, ASN1_PCTX *pctx)
{
	BN_CTX *ctx = BN_CTX_new();
	BIGNUM *X, *Y;
	const EC_POINT *pubkey;
	const EC_GROUP *group;

	if (ctx == NULL) {
		GOSTerror(ERR_R_MALLOC_FAILURE);
		return 0;
	}
	BN_CTX_start(ctx);
	if ((X = BN_CTX_get(ctx)) == NULL)
		goto err;
	if ((Y = BN_CTX_get(ctx)) == NULL)
		goto err;
	pubkey = GOST_KEY_get0_public_key(pkey->pkey.gost);
	group = GOST_KEY_get0_group(pkey->pkey.gost);
	if (EC_POINT_get_affine_coordinates_GFp(group, pubkey, X, Y,
	    ctx) == 0) {
		GOSTerror(ERR_R_EC_LIB);
		goto err;
	}
	if (BIO_indent(out, indent, 128) == 0)
		goto err;
	BIO_printf(out, "Public key:\n");
	if (BIO_indent(out, indent + 3, 128) == 0)
		goto err;
	BIO_printf(out, "X:");
	BN_print(out, X);
	BIO_printf(out, "\n");
	BIO_indent(out, indent + 3, 128);
	BIO_printf(out, "Y:");
	BN_print(out, Y);
	BIO_printf(out, "\n");

	BN_CTX_end(ctx);
	BN_CTX_free(ctx);

	return param_print_gost01(out, pkey, indent, pctx);

err:
	BN_CTX_end(ctx);
	BN_CTX_free(ctx);
	return 0;
}

static int
priv_print_gost01(BIO *out, const EVP_PKEY *pkey, int indent, ASN1_PCTX *pctx)
{
	const BIGNUM *key;

	if (BIO_indent(out, indent, 128) == 0)
		return 0;
	BIO_printf(out, "Private key: ");
	key = GOST_KEY_get0_private_key(pkey->pkey.gost);
	if (key == NULL)
		BIO_printf(out, "<undefined)");
	else
		BN_print(out, key);
	BIO_printf(out, "\n");

	return pub_print_gost01(out, pkey, indent, pctx);
}

static int
priv_decode_gost01(EVP_PKEY *pk, PKCS8_PRIV_KEY_INFO *p8inf)
{
	const unsigned char *pkey_buf = NULL, *p = NULL;
	int priv_len = 0;
	BIGNUM *pk_num = NULL;
	int ret = 0;
	X509_ALGOR *palg = NULL;
	ASN1_OBJECT *palg_obj = NULL;
	ASN1_INTEGER *priv_key = NULL;
	GOST_KEY *ec;
	int ptype = V_ASN1_UNDEF;
	ASN1_STRING *pval = NULL;

	if (PKCS8_pkey_get0(&palg_obj, &pkey_buf, &priv_len, &palg, p8inf) == 0)
		return 0;
	(void)EVP_PKEY_assign_GOST(pk, NULL);
	X509_ALGOR_get0(NULL, &ptype, (void **)&pval, palg);
	if (ptype != V_ASN1_SEQUENCE) {
		GOSTerror(GOST_R_BAD_KEY_PARAMETERS_FORMAT);
		return 0;
	}
	p = pval->data;
	if (decode_gost01_algor_params(pk, &p, pval->length) == 0)
		return 0;
	p = pkey_buf;
	if (V_ASN1_OCTET_STRING == *p) {
		/* New format - Little endian octet string */
		unsigned char rev_buf[32];
		int i;
		ASN1_OCTET_STRING *s =
		    d2i_ASN1_OCTET_STRING(NULL, &p, priv_len);

		if (s == NULL || s->length != 32) {
			GOSTerror(EVP_R_DECODE_ERROR);
			ASN1_STRING_free(s);
			return 0;
		}
		for (i = 0; i < 32; i++) {
			rev_buf[31 - i] = s->data[i];
		}
		ASN1_STRING_free(s);
		pk_num = BN_bin2bn(rev_buf, 32, NULL);
	} else {
		priv_key = d2i_ASN1_INTEGER(NULL, &p, priv_len);
		if (priv_key == NULL)
			return 0;
		ret = ((pk_num = ASN1_INTEGER_to_BN(priv_key, NULL)) != NULL);
		ASN1_INTEGER_free(priv_key);
		if (ret == 0) {
			GOSTerror(EVP_R_DECODE_ERROR);
			return 0;
		}
	}

	ec = pk->pkey.gost;
	if (ec == NULL) {
		ec = GOST_KEY_new();
		if (ec == NULL) {
			BN_free(pk_num);
			return 0;
		}
		if (EVP_PKEY_assign_GOST(pk, ec) == 0) {
			BN_free(pk_num);
			GOST_KEY_free(ec);
			return 0;
		}
	}
	if (GOST_KEY_set_private_key(ec, pk_num) == 0) {
		BN_free(pk_num);
		return 0;
	}
	ret = 0;
	if (EVP_PKEY_missing_parameters(pk) == 0)
		ret = gost2001_compute_public(ec) != 0;
	BN_free(pk_num);

	return ret;
}

static int
priv_encode_gost01(PKCS8_PRIV_KEY_INFO *p8, const EVP_PKEY *pk)
{
	ASN1_OBJECT *algobj =
	    OBJ_nid2obj(GostR3410_get_pk_digest(GOST_KEY_get_digest(pk->pkey.gost)));
	ASN1_STRING *params = encode_gost01_algor_params(pk);
	unsigned char *priv_buf = NULL;
	int priv_len;
	ASN1_INTEGER *asn1key = NULL;

	if (params == NULL)
		return 0;

	asn1key = BN_to_ASN1_INTEGER(GOST_KEY_get0_private_key(pk->pkey.gost),
	    NULL);
	if (asn1key == NULL) {
		ASN1_STRING_free(params);
		return 0;
	}
	priv_len = i2d_ASN1_INTEGER(asn1key, &priv_buf);
	ASN1_INTEGER_free(asn1key);
	return PKCS8_pkey_set0(p8, algobj, 0, V_ASN1_SEQUENCE, params, priv_buf,
	    priv_len);
}

static int
param_encode_gost01(const EVP_PKEY *pkey, unsigned char **pder)
{
	ASN1_STRING *params = encode_gost01_algor_params(pkey);
	int len;

	if (params == NULL)
		return 0;
	len = params->length;
	if (pder != NULL)
		memcpy(*pder, params->data, params->length);
	ASN1_STRING_free(params);
	return len;
}

static int
param_decode_gost01(EVP_PKEY *pkey, const unsigned char **pder, int derlen)
{
	ASN1_OBJECT *obj = NULL;
	int nid;
	GOST_KEY *ec;
	EC_GROUP *group;
	int ret;

	/* New format */
	if ((V_ASN1_SEQUENCE | V_ASN1_CONSTRUCTED) == **pder)
		return decode_gost01_algor_params(pkey, pder, derlen);

	/* Compatibility */
	if (d2i_ASN1_OBJECT(&obj, pder, derlen) == NULL) {
		GOSTerror(ERR_R_MALLOC_FAILURE);
		return 0;
	}
	nid = OBJ_obj2nid(obj);
	ASN1_OBJECT_free(obj);

	ec = GOST_KEY_new();
	if (ec == NULL) {
		GOSTerror(ERR_R_MALLOC_FAILURE);
		return 0;
	}
	group = EC_GROUP_new_by_curve_name(nid);
	if (group == NULL) {
		GOSTerror(EC_R_EC_GROUP_NEW_BY_NAME_FAILURE);
		GOST_KEY_free(ec);
		return 0;
	}

	EC_GROUP_set_asn1_flag(group, OPENSSL_EC_NAMED_CURVE);
	if (GOST_KEY_set_group(ec, group) == 0) {
		GOSTerror(ERR_R_EC_LIB);
		EC_GROUP_free(group);
		GOST_KEY_free(ec);
		return 0;
	}
	EC_GROUP_free(group);
	if (GOST_KEY_set_digest(ec,
	    NID_id_GostR3411_94_CryptoProParamSet) == 0) {
		GOSTerror(GOST_R_INVALID_DIGEST_TYPE);
		GOST_KEY_free(ec);
		return 0;
	}
	ret = EVP_PKEY_assign_GOST(pkey, ec);
	if (ret == 0)
		GOST_KEY_free(ec);
	return ret;
}

static int
param_missing_gost01(const EVP_PKEY *pk)
{
	const GOST_KEY *ec = pk->pkey.gost;

	if (ec == NULL)
		return 1;
	if (GOST_KEY_get0_group(ec) == NULL)
		return 1;
	if (GOST_KEY_get_digest(ec) == NID_undef)
		return 1;
	return 0;
}

static int
param_copy_gost01(EVP_PKEY *to, const EVP_PKEY *from)
{
	GOST_KEY *eto = to->pkey.gost;
	const GOST_KEY *efrom = from->pkey.gost;
	int ret = 1;

	if (EVP_PKEY_base_id(from) != EVP_PKEY_base_id(to)) {
		GOSTerror(GOST_R_INCOMPATIBLE_ALGORITHMS);
		return 0;
	}
	if (efrom == NULL) {
		GOSTerror(GOST_R_KEY_PARAMETERS_MISSING);
		return 0;
	}
	if (eto == NULL) {
		eto = GOST_KEY_new();
		if (eto == NULL) {
			GOSTerror(ERR_R_MALLOC_FAILURE);
			return 0;
		}
		if (EVP_PKEY_assign(to, EVP_PKEY_base_id(from), eto) == 0) {
			GOST_KEY_free(eto);
			return 0;
		}
	}
	GOST_KEY_set_group(eto, GOST_KEY_get0_group(efrom));
	GOST_KEY_set_digest(eto, GOST_KEY_get_digest(efrom));
	if (GOST_KEY_get0_private_key(eto) != NULL)
		ret = gost2001_compute_public(eto);

	return ret;
}

static int
param_cmp_gost01(const EVP_PKEY *a, const EVP_PKEY *b)
{
	if (EC_GROUP_get_curve_name(GOST_KEY_get0_group(a->pkey.gost)) !=
	    EC_GROUP_get_curve_name(GOST_KEY_get0_group(b->pkey.gost)))
		return 0;

	if (GOST_KEY_get_digest(a->pkey.gost) !=
	    GOST_KEY_get_digest(b->pkey.gost))
		return 0;

	return 1;
}

static int
pkey_ctrl_gost01(EVP_PKEY *pkey, int op, long arg1, void *arg2)
{
	X509_ALGOR *alg1 = NULL, *alg2 = NULL, *alg3 = NULL;
	int digest = GOST_KEY_get_digest(pkey->pkey.gost);

	switch (op) {
	case ASN1_PKEY_CTRL_PKCS7_SIGN:
		if (arg1 == 0)
			PKCS7_SIGNER_INFO_get0_algs(arg2, NULL, &alg1, &alg2);
		break;

	case ASN1_PKEY_CTRL_PKCS7_ENCRYPT:
		if (arg1 == 0)
			PKCS7_RECIP_INFO_get0_alg(arg2, &alg3);
		break;
	case ASN1_PKEY_CTRL_DEFAULT_MD_NID:
		*(int *)arg2 = GostR3410_get_md_digest(digest);
		return 2;

	default:
		return -2;
	}

	if (alg1)
		X509_ALGOR_set0(alg1, OBJ_nid2obj(GostR3410_get_md_digest(digest)), V_ASN1_NULL, 0);
	if (alg2)
		X509_ALGOR_set0(alg2, OBJ_nid2obj(GostR3410_get_pk_digest(digest)), V_ASN1_NULL, 0);
	if (alg3) {
		ASN1_STRING *params = encode_gost01_algor_params(pkey);
		if (params == NULL) {
			return -1;
		}
		X509_ALGOR_set0(alg3,
		    OBJ_nid2obj(GostR3410_get_pk_digest(digest)),
		    V_ASN1_SEQUENCE, params);
	}

	return 1;
}

const EVP_PKEY_ASN1_METHOD gostr01_asn1_meths[] = {
	{
		.pkey_id = EVP_PKEY_GOSTR01,
		.pkey_base_id = EVP_PKEY_GOSTR01,
		.pkey_flags = ASN1_PKEY_SIGPARAM_NULL,

		.pem_str = "GOST2001",
		.info = "GOST R 34.10-2001",

		.pkey_free = pkey_free_gost01,
		.pkey_ctrl = pkey_ctrl_gost01,

		.priv_decode = priv_decode_gost01,
		.priv_encode = priv_encode_gost01,
		.priv_print = priv_print_gost01,

		.param_decode = param_decode_gost01,
		.param_encode = param_encode_gost01,
		.param_missing = param_missing_gost01,
		.param_copy = param_copy_gost01,
		.param_cmp = param_cmp_gost01,
		.param_print = param_print_gost01,

		.pub_decode = pub_decode_gost01,
		.pub_encode = pub_encode_gost01,
		.pub_cmp = pub_cmp_gost01,
		.pub_print = pub_print_gost01,
		.pkey_size = pkey_size_gost01,
		.pkey_bits = pkey_bits_gost01,
	},
	{
		.pkey_id = EVP_PKEY_GOSTR12_256,
		.pkey_base_id = EVP_PKEY_GOSTR01,
		.pkey_flags = ASN1_PKEY_ALIAS
	},
	{
		.pkey_id = EVP_PKEY_GOSTR12_512,
		.pkey_base_id = EVP_PKEY_GOSTR01,
		.pkey_flags = ASN1_PKEY_ALIAS
	},
};

#endif
