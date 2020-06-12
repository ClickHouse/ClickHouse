/* $OpenBSD: gostr341001_pmeth.c,v 1.14 2017/01/29 17:49:23 beck Exp $ */
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
#include <openssl/err.h>
#include <openssl/gost.h>
#include <openssl/ec.h>
#include <openssl/ecdsa.h>
#include <openssl/x509.h>

#include "evp_locl.h"
#include "gost_locl.h"
#include "gost_asn1.h"

static ECDSA_SIG *
unpack_signature_cp(const unsigned char *sig, size_t siglen)
{
	ECDSA_SIG *s;

	s = ECDSA_SIG_new();
	if (s == NULL) {
		GOSTerror(ERR_R_MALLOC_FAILURE);
		return NULL;
	}
	BN_bin2bn(sig, siglen / 2, s->s);
	BN_bin2bn(sig + siglen / 2, siglen / 2, s->r);
	return s;
}

static int
pack_signature_cp(ECDSA_SIG *s, int order, unsigned char *sig, size_t *siglen)
{
	int r_len = BN_num_bytes(s->r);
	int s_len = BN_num_bytes(s->s);

	if (r_len > order || s_len > order)
		return 0;

	*siglen = 2 * order;

	memset(sig, 0, *siglen);
	BN_bn2bin(s->s, sig + order - s_len);
	BN_bn2bin(s->r, sig + 2 * order - r_len);
	ECDSA_SIG_free(s);
	return 1;
}

static ECDSA_SIG *
unpack_signature_le(const unsigned char *sig, size_t siglen)
{
	ECDSA_SIG *s;

	s = ECDSA_SIG_new();
	if (s == NULL) {
		GOSTerror(ERR_R_MALLOC_FAILURE);
		return NULL;
	}
	GOST_le2bn(sig, siglen / 2, s->r);
	GOST_le2bn(sig + siglen / 2, siglen / 2, s->s);
	return s;
}

static int
pack_signature_le(ECDSA_SIG *s, int order, unsigned char *sig, size_t *siglen)
{
	*siglen = 2 * order;
	memset(sig, 0, *siglen);
	GOST_bn2le(s->r, sig, order);
	GOST_bn2le(s->s, sig + order, order);
	ECDSA_SIG_free(s);
	return 1;
}

struct gost_pmeth_data {
	int sign_param_nid; /* Should be set whenever parameters are filled */
	int digest_nid;
	EVP_MD *md;
	unsigned char *shared_ukm;
	int peer_key_used;
	int sig_format;
};

static int
pkey_gost01_init(EVP_PKEY_CTX *ctx)
{
	struct gost_pmeth_data *data;
	EVP_PKEY *pkey = EVP_PKEY_CTX_get0_pkey(ctx);

	data = calloc(1, sizeof(struct gost_pmeth_data));
	if (data == NULL)
		return 0;

	if (pkey != NULL && pkey->pkey.gost != NULL) {
		data->sign_param_nid =
		    EC_GROUP_get_curve_name(GOST_KEY_get0_group(pkey->pkey.gost));
		data->digest_nid = GOST_KEY_get_digest(pkey->pkey.gost);
	}
	EVP_PKEY_CTX_set_data(ctx, data);
	return 1;
}

/* Copies contents of gost_pmeth_data structure */
static int
pkey_gost01_copy(EVP_PKEY_CTX *dst, EVP_PKEY_CTX *src)
{
	struct gost_pmeth_data *dst_data, *src_data;

	if (pkey_gost01_init(dst) == 0)
		return 0;

	src_data = EVP_PKEY_CTX_get_data(src);
	dst_data = EVP_PKEY_CTX_get_data(dst);
	*dst_data = *src_data;
	if (src_data->shared_ukm != NULL)
		dst_data->shared_ukm = NULL;
	return 1;
}

/* Frees up gost_pmeth_data structure */
static void
pkey_gost01_cleanup(EVP_PKEY_CTX *ctx)
{
	struct gost_pmeth_data *data = EVP_PKEY_CTX_get_data(ctx);

	free(data->shared_ukm);
	free(data);
}

static int
pkey_gost01_paramgen(EVP_PKEY_CTX *ctx, EVP_PKEY *pkey)
{
	struct gost_pmeth_data *data = EVP_PKEY_CTX_get_data(ctx);
	EC_GROUP *group = NULL;
	GOST_KEY *gost = NULL;
	int ret = 0;

	if (data->sign_param_nid == NID_undef ||
	    data->digest_nid == NID_undef) {
		GOSTerror(GOST_R_NO_PARAMETERS_SET);
		return 0;
	}

	group = EC_GROUP_new_by_curve_name(data->sign_param_nid);
	if (group == NULL)
		goto done;

	EC_GROUP_set_asn1_flag(group, OPENSSL_EC_NAMED_CURVE);

	gost = GOST_KEY_new();
	if (gost == NULL)
		goto done;

	if (GOST_KEY_set_digest(gost, data->digest_nid) == 0)
		goto done;

	if (GOST_KEY_set_group(gost, group) != 0)
		ret = EVP_PKEY_assign_GOST(pkey, gost);

done:
	if (ret == 0)
		GOST_KEY_free(gost);
	EC_GROUP_free(group);
	return ret;
}

static int
pkey_gost01_keygen(EVP_PKEY_CTX *ctx, EVP_PKEY *pkey)
{
	if (pkey_gost01_paramgen(ctx, pkey) == 0)
		return 0;
	return gost2001_keygen(pkey->pkey.gost) != 0;
}

static int
pkey_gost01_sign(EVP_PKEY_CTX *ctx, unsigned char *sig, size_t *siglen,
    const unsigned char *tbs, size_t tbs_len)
{
	ECDSA_SIG *unpacked_sig = NULL;
	EVP_PKEY *pkey = EVP_PKEY_CTX_get0_pkey(ctx);
	struct gost_pmeth_data *pctx = EVP_PKEY_CTX_get_data(ctx);
	BIGNUM *md;
	size_t size;
	int ret;

	if (pkey == NULL || pkey->pkey.gost == NULL)
		return 0;
	size = GOST_KEY_get_size(pkey->pkey.gost);

	if (siglen == NULL)
		return 0;
	if (sig == NULL) {
		*siglen = 2 * size;
		return 1;
	} else if (*siglen < 2 * size) {
		GOSTerror(EC_R_BUFFER_TOO_SMALL);
		return 0;
	}
	if (tbs_len != 32 && tbs_len != 64) {
		GOSTerror(EVP_R_BAD_BLOCK_LENGTH);
		return 0;
	}
	md = GOST_le2bn(tbs, tbs_len, NULL);
	if (md == NULL)
		return 0;
	unpacked_sig = gost2001_do_sign(md, pkey->pkey.gost);
	BN_free(md);
	if (unpacked_sig == NULL) {
		return 0;
	}
	switch (pctx->sig_format) {
	case GOST_SIG_FORMAT_SR_BE:
		ret = pack_signature_cp(unpacked_sig, size, sig, siglen);
		break;
	case GOST_SIG_FORMAT_RS_LE:
		ret = pack_signature_le(unpacked_sig, size, sig, siglen);
		break;
	default:
		ret = -1;
		break;
	}
	if (ret <= 0)
		ECDSA_SIG_free(unpacked_sig);
	return ret;
}

static int
pkey_gost01_verify(EVP_PKEY_CTX *ctx, const unsigned char *sig, size_t siglen,
    const unsigned char *tbs, size_t tbs_len)
{
	int ok = 0;
	EVP_PKEY *pub_key = EVP_PKEY_CTX_get0_pkey(ctx);
	struct gost_pmeth_data *pctx = EVP_PKEY_CTX_get_data(ctx);
	ECDSA_SIG *s = NULL;
	BIGNUM *md;

	if (pub_key == NULL)
		return 0;
	switch (pctx->sig_format) {
	case GOST_SIG_FORMAT_SR_BE:
		s = unpack_signature_cp(sig, siglen);
		break;
	case GOST_SIG_FORMAT_RS_LE:
		s = unpack_signature_le(sig, siglen);
		break;
	}
	if (s == NULL)
		return 0;
	md = GOST_le2bn(tbs, tbs_len, NULL);
	if (md == NULL)
		goto err;
	ok = gost2001_do_verify(md, s, pub_key->pkey.gost);

err:
	BN_free(md);
	ECDSA_SIG_free(s);
	return ok;
}

static int
gost01_VKO_key(EVP_PKEY *pub_key, EVP_PKEY *priv_key, const unsigned char *ukm,
    unsigned char *key)
{
	unsigned char hashbuf[128];
	int digest_nid;
	int ret = 0;
	BN_CTX *ctx = BN_CTX_new();
	BIGNUM *UKM, *X, *Y;

	if (ctx == NULL)
		return 0;

	BN_CTX_start(ctx);
	if ((UKM = BN_CTX_get(ctx)) == NULL)
		goto err;
	if ((X = BN_CTX_get(ctx)) == NULL)
		goto err;
	if ((Y = BN_CTX_get(ctx)) == NULL)
		goto err;

	GOST_le2bn(ukm, 8, UKM);

	digest_nid = GOST_KEY_get_digest(priv_key->pkey.gost);
	if (VKO_compute_key(X, Y, pub_key->pkey.gost, priv_key->pkey.gost,
	    UKM) == 0)
		goto err;

	switch (digest_nid) {
	case NID_id_GostR3411_94_CryptoProParamSet:
		GOST_bn2le(X, hashbuf, 32);
		GOST_bn2le(Y, hashbuf + 32, 32);
		GOSTR341194(hashbuf, 64, key, digest_nid);
		ret = 1;
		break;
	case NID_id_tc26_gost3411_2012_256:
		GOST_bn2le(X, hashbuf, 32);
		GOST_bn2le(Y, hashbuf + 32, 32);
		STREEBOG256(hashbuf, 64, key);
		ret = 1;
		break;
	case NID_id_tc26_gost3411_2012_512:
		GOST_bn2le(X, hashbuf, 64);
		GOST_bn2le(Y, hashbuf + 64, 64);
		STREEBOG256(hashbuf, 128, key);
		ret = 1;
		break;
	default:
		ret = -2;
		break;
	}
err:
	BN_CTX_end(ctx);
	BN_CTX_free(ctx);
	return ret;
}

int
pkey_gost01_decrypt(EVP_PKEY_CTX *pctx, unsigned char *key, size_t *key_len,
    const unsigned char *in, size_t in_len)
{
	const unsigned char *p = in;
	EVP_PKEY *priv = EVP_PKEY_CTX_get0_pkey(pctx);
	GOST_KEY_TRANSPORT *gkt = NULL;
	int ret = 0;
	unsigned char wrappedKey[44];
	unsigned char sharedKey[32];
	EVP_PKEY *eph_key = NULL, *peerkey = NULL;
	int nid;

	if (key == NULL) {
		*key_len = 32;
		return 1;
	}
	gkt = d2i_GOST_KEY_TRANSPORT(NULL, (const unsigned char **)&p, in_len);
	if (gkt == NULL) {
		GOSTerror(GOST_R_ERROR_PARSING_KEY_TRANSPORT_INFO);
		return -1;
	}

	/* If key transport structure contains public key, use it */
	eph_key = X509_PUBKEY_get(gkt->key_agreement_info->ephem_key);
	if (eph_key != NULL) {
		if (EVP_PKEY_derive_set_peer(pctx, eph_key) <= 0) {
			GOSTerror(GOST_R_INCOMPATIBLE_PEER_KEY);
			goto err;
		}
	} else {
		/* Set control "public key from client certificate used" */
		if (EVP_PKEY_CTX_ctrl(pctx, -1, -1, EVP_PKEY_CTRL_PEER_KEY, 3,
		    NULL) <= 0) {
			GOSTerror(GOST_R_CTRL_CALL_FAILED);
			goto err;
		}
	}
	peerkey = EVP_PKEY_CTX_get0_peerkey(pctx);
	if (peerkey == NULL) {
		GOSTerror(GOST_R_NO_PEER_KEY);
		goto err;
	}

	nid = OBJ_obj2nid(gkt->key_agreement_info->cipher);

	if (gkt->key_agreement_info->eph_iv->length != 8) {
		GOSTerror(GOST_R_INVALID_IV_LENGTH);
		goto err;
	}
	memcpy(wrappedKey, gkt->key_agreement_info->eph_iv->data, 8);
	if (gkt->key_info->encrypted_key->length != 32) {
		GOSTerror(EVP_R_BAD_KEY_LENGTH);
		goto err;
	}
	memcpy(wrappedKey + 8, gkt->key_info->encrypted_key->data, 32);
	if (gkt->key_info->imit->length != 4) {
		GOSTerror(ERR_R_INTERNAL_ERROR);
		goto err;
	}
	memcpy(wrappedKey + 40, gkt->key_info->imit->data, 4);
	if (gost01_VKO_key(peerkey, priv, wrappedKey, sharedKey) <= 0)
		goto err;
	if (gost_key_unwrap_crypto_pro(nid, sharedKey, wrappedKey, key) == 0) {
		GOSTerror(GOST_R_ERROR_COMPUTING_SHARED_KEY);
		goto err;
	}

	ret = 1;
err:
	EVP_PKEY_free(eph_key);
	GOST_KEY_TRANSPORT_free(gkt);
	return ret;
}

int
pkey_gost01_derive(EVP_PKEY_CTX *ctx, unsigned char *key, size_t *keylen)
{
	/*
	 * Public key of peer in the ctx field peerkey
	 * Our private key in the ctx pkey
	 * ukm is in the algorithm specific context data
	 */
	EVP_PKEY *my_key = EVP_PKEY_CTX_get0_pkey(ctx);
	EVP_PKEY *peer_key = EVP_PKEY_CTX_get0_peerkey(ctx);
	struct gost_pmeth_data *data = EVP_PKEY_CTX_get_data(ctx);

	if (data->shared_ukm == NULL) {
		GOSTerror(GOST_R_UKM_NOT_SET);
		return 0;
	}

	if (key == NULL) {
		*keylen = 32;
		return 32;
	}

	if (gost01_VKO_key(peer_key, my_key, data->shared_ukm, key) <= 0)
		return 0;

	*keylen = 32;
	return 1;
}

int
pkey_gost01_encrypt(EVP_PKEY_CTX *pctx, unsigned char *out, size_t *out_len,
    const unsigned char *key, size_t key_len)
{
	GOST_KEY_TRANSPORT *gkt = NULL;
	EVP_PKEY *pubk = EVP_PKEY_CTX_get0_pkey(pctx);
	struct gost_pmeth_data *data = EVP_PKEY_CTX_get_data(pctx);
	unsigned char ukm[8], shared_key[32], crypted_key[44];
	int ret = 0;
	int key_is_ephemeral;
	EVP_PKEY *sec_key = EVP_PKEY_CTX_get0_peerkey(pctx);
	int nid = NID_id_Gost28147_89_CryptoPro_A_ParamSet;

	if (data->shared_ukm != NULL) {
		memcpy(ukm, data->shared_ukm, 8);
	} else /* if (out != NULL) */ {
		arc4random_buf(ukm, 8);
	}
	/* Check for private key in the peer_key of context */
	if (sec_key) {
		key_is_ephemeral = 0;
		if (GOST_KEY_get0_private_key(sec_key->pkey.gost) == 0) {
			GOSTerror(GOST_R_NO_PRIVATE_PART_OF_NON_EPHEMERAL_KEYPAIR);
			goto err;
		}
	} else {
		key_is_ephemeral = 1;
		if (out != NULL) {
			GOST_KEY *tmp_key;

			sec_key = EVP_PKEY_new();
			if (sec_key == NULL)
				goto err;
			tmp_key = GOST_KEY_new();
			if (tmp_key == NULL)
				goto err;
			if (EVP_PKEY_assign(sec_key, EVP_PKEY_base_id(pubk),
			    tmp_key) == 0) {
				GOST_KEY_free(tmp_key);
				goto err;
			}
			if (EVP_PKEY_copy_parameters(sec_key, pubk) == 0)
				goto err;
			if (gost2001_keygen(sec_key->pkey.gost) == 0) {
				goto err;
			}
		}
	}

	if (out != NULL) {
		if (gost01_VKO_key(pubk, sec_key, ukm, shared_key) <= 0)
			goto err;
		gost_key_wrap_crypto_pro(nid, shared_key, ukm, key,
		    crypted_key);
	}
	gkt = GOST_KEY_TRANSPORT_new();
	if (gkt == NULL)
		goto err;
	if (ASN1_OCTET_STRING_set(gkt->key_agreement_info->eph_iv, ukm, 8) == 0)
		goto err;
	if (ASN1_OCTET_STRING_set(gkt->key_info->imit, crypted_key + 40,
	    4) == 0)
		goto err;
	if (ASN1_OCTET_STRING_set(gkt->key_info->encrypted_key, crypted_key + 8,
	    32) == 0)
		goto err;
	if (key_is_ephemeral) {
		if (X509_PUBKEY_set(&gkt->key_agreement_info->ephem_key,
		    out != NULL ? sec_key : pubk) == 0) {
			GOSTerror(GOST_R_CANNOT_PACK_EPHEMERAL_KEY);
			goto err;
		}
	}
	ASN1_OBJECT_free(gkt->key_agreement_info->cipher);
	gkt->key_agreement_info->cipher = OBJ_nid2obj(nid);
	if (key_is_ephemeral)
		EVP_PKEY_free(sec_key);
	else {
		/* Set control "public key from client certificate used" */
		if (EVP_PKEY_CTX_ctrl(pctx, -1, -1, EVP_PKEY_CTRL_PEER_KEY, 3,
		    NULL) <= 0) {
			GOSTerror(GOST_R_CTRL_CALL_FAILED);
			goto err;
		}
	}
	if ((*out_len = i2d_GOST_KEY_TRANSPORT(gkt, out ? &out : NULL)) > 0)
		ret = 1;
	GOST_KEY_TRANSPORT_free(gkt);
	return ret;

err:
	if (key_is_ephemeral)
		EVP_PKEY_free(sec_key);
	GOST_KEY_TRANSPORT_free(gkt);
	return -1;
}


static int
pkey_gost01_ctrl(EVP_PKEY_CTX *ctx, int type, int p1, void *p2)
{
	struct gost_pmeth_data *pctx = EVP_PKEY_CTX_get_data(ctx);

	switch (type) {
	case EVP_PKEY_CTRL_MD:
		if (EVP_MD_type(p2) !=
		    GostR3410_get_md_digest(pctx->digest_nid)) {
			GOSTerror(GOST_R_INVALID_DIGEST_TYPE);
			return 0;
		}
		pctx->md = p2;
		return 1;
	case EVP_PKEY_CTRL_PKCS7_ENCRYPT:
	case EVP_PKEY_CTRL_PKCS7_DECRYPT:
	case EVP_PKEY_CTRL_PKCS7_SIGN:
	case EVP_PKEY_CTRL_DIGESTINIT:
		return 1;

	case EVP_PKEY_CTRL_GOST_PARAMSET:
		pctx->sign_param_nid = (int)p1;
		return 1;

	case EVP_PKEY_CTRL_SET_IV:
	    {
		char *ukm = malloc(p1);

		if (ukm == NULL) {
			GOSTerror(ERR_R_MALLOC_FAILURE);
			return 0;
		}
		memcpy(ukm, p2, p1);
		free(pctx->shared_ukm);
		pctx->shared_ukm = ukm;
		return 1;
	    }

	case EVP_PKEY_CTRL_PEER_KEY:
		if (p1 == 0 || p1 == 1)	/* call from EVP_PKEY_derive_set_peer */
			return 1;
		if (p1 == 2)	/* TLS: peer key used? */
			return pctx->peer_key_used;
		if (p1 == 3)	/* TLS: peer key used! */
			return (pctx->peer_key_used = 1);
		return -2;
	case EVP_PKEY_CTRL_GOST_SIG_FORMAT:
		switch (p1) {
		case GOST_SIG_FORMAT_SR_BE:
		case GOST_SIG_FORMAT_RS_LE:
			pctx->sig_format = p1;
			return 1;
		default:
			return 0;
		}
		break;
	case EVP_PKEY_CTRL_GOST_SET_DIGEST:
		pctx->digest_nid = (int)p1;
		return 1;
	case EVP_PKEY_CTRL_GOST_GET_DIGEST:
		*(int *)p2 = pctx->digest_nid;
		return 1;
	default:
		return -2;
	}
}

static int
pkey_gost01_ctrl_str(EVP_PKEY_CTX *ctx, const char *type, const char *value)
{
	int param_nid = NID_undef;
	int digest_nid = NID_undef;

	if (strcmp(type, "paramset") == 0) {
		if (value == NULL)
			return 0;
		if (pkey_gost01_ctrl(ctx, EVP_PKEY_CTRL_GOST_GET_DIGEST, 0,
		    &digest_nid) == 0)
			return 0;
		if (digest_nid == NID_id_tc26_gost3411_2012_512)
			param_nid = GostR3410_512_param_id(value);
		else
			param_nid = GostR3410_256_param_id(value);
		if (param_nid == NID_undef)
			param_nid = OBJ_txt2nid(value);
		if (param_nid == NID_undef)
			return 0;

		return pkey_gost01_ctrl(ctx, EVP_PKEY_CTRL_GOST_PARAMSET,
		    param_nid, NULL);
	}
	if (strcmp(type, "dgst") == 0) {
		if (value == NULL)
			return 0;
		else if (strcmp(value, "gost94") == 0 ||
		    strcmp(value, "md_gost94") == 0)
			digest_nid = NID_id_GostR3411_94_CryptoProParamSet;
		else if (strcmp(value, "streebog256") == 0)
			digest_nid = NID_id_tc26_gost3411_2012_256;
		else if (strcmp(value, "streebog512") == 0)
			digest_nid = NID_id_tc26_gost3411_2012_512;

		if (digest_nid == NID_undef)
			return 0;

		return pkey_gost01_ctrl(ctx, EVP_PKEY_CTRL_GOST_SET_DIGEST,
		    digest_nid, NULL);
	}
	return -2;
}

const EVP_PKEY_METHOD gostr01_pkey_meth = {
	.pkey_id = EVP_PKEY_GOSTR01,

	.init = pkey_gost01_init,
	.copy = pkey_gost01_copy,
	.cleanup = pkey_gost01_cleanup,

	.paramgen = pkey_gost01_paramgen,
	.keygen = pkey_gost01_keygen,
	.sign = pkey_gost01_sign,
	.verify = pkey_gost01_verify,

	.encrypt = pkey_gost01_encrypt,
	.decrypt = pkey_gost01_decrypt,
	.derive = pkey_gost01_derive,

	.ctrl = pkey_gost01_ctrl,
	.ctrl_str = pkey_gost01_ctrl_str,
};
#endif
