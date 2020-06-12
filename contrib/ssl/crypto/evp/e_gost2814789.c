/* $OpenBSD: e_gost2814789.c,v 1.4 2017/01/29 17:49:23 beck Exp $ */
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
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/gost.h>
#include "evp_locl.h"

typedef struct {
	GOST2814789_KEY ks;
	int param_nid;
} EVP_GOST2814789_CTX;

static int
gost2814789_ctl(EVP_CIPHER_CTX *ctx, int type, int arg, void *ptr)
{
	EVP_GOST2814789_CTX *c = ctx->cipher_data;

	switch (type) {
	case EVP_CTRL_PBE_PRF_NID:
		if (ptr != NULL) {
			*((int *)ptr) = NID_id_HMACGostR3411_94;
			return 1;
		} else {
			return 0;
		}
	case EVP_CTRL_INIT:
		/* Default value to have any s-box set at all */
		c->param_nid = NID_id_Gost28147_89_CryptoPro_A_ParamSet;
		return Gost2814789_set_sbox(&c->ks, c->param_nid);
	case EVP_CTRL_GOST_SET_SBOX:
		return Gost2814789_set_sbox(&c->ks, arg);
	default:
		return -1;
	}
}

static int
gost2814789_init_key(EVP_CIPHER_CTX *ctx, const unsigned char *key,
    const unsigned char *iv, int enc)
{
	EVP_GOST2814789_CTX *c = ctx->cipher_data;

	return Gost2814789_set_key(&c->ks, key, ctx->key_len * 8);
}

int
gost2814789_set_asn1_params(EVP_CIPHER_CTX *ctx, ASN1_TYPE *params)
{
	int len = 0;
	unsigned char *buf = NULL;
	unsigned char *p = NULL;
	EVP_GOST2814789_CTX *c = ctx->cipher_data;
	ASN1_OCTET_STRING *os = NULL;
	GOST_CIPHER_PARAMS *gcp = GOST_CIPHER_PARAMS_new();

	if (gcp == NULL) {
		GOSTerror(ERR_R_MALLOC_FAILURE);
		return 0;
	}
	if (ASN1_OCTET_STRING_set(gcp->iv, ctx->iv, ctx->cipher->iv_len) == 0) {
		GOST_CIPHER_PARAMS_free(gcp);
		GOSTerror(ERR_R_ASN1_LIB);
		return 0;
	}
	ASN1_OBJECT_free(gcp->enc_param_set);
	gcp->enc_param_set = OBJ_nid2obj(c->param_nid);

	len = i2d_GOST_CIPHER_PARAMS(gcp, NULL);
	p = buf = malloc(len);
	if (buf == NULL) {
		GOST_CIPHER_PARAMS_free(gcp);
		GOSTerror(ERR_R_MALLOC_FAILURE);
		return 0;
	}
	i2d_GOST_CIPHER_PARAMS(gcp, &p);
	GOST_CIPHER_PARAMS_free(gcp);

	os = ASN1_OCTET_STRING_new();
	if (os == NULL) {
		free(buf);
		GOSTerror(ERR_R_MALLOC_FAILURE);
		return 0;
	}
	if (ASN1_OCTET_STRING_set(os, buf, len) == 0) {
		ASN1_OCTET_STRING_free(os);
		free(buf);
		GOSTerror(ERR_R_ASN1_LIB);
		return 0;
	}
	free(buf);

	ASN1_TYPE_set(params, V_ASN1_SEQUENCE, os);
	return 1;
}

int
gost2814789_get_asn1_params(EVP_CIPHER_CTX *ctx, ASN1_TYPE *params)
{
	int ret = -1;
	int len;
	GOST_CIPHER_PARAMS *gcp = NULL;
	EVP_GOST2814789_CTX *c = ctx->cipher_data;
	unsigned char *p;

	if (ASN1_TYPE_get(params) != V_ASN1_SEQUENCE)
		return ret;

	p = params->value.sequence->data;

	gcp = d2i_GOST_CIPHER_PARAMS(NULL, (const unsigned char **)&p,
	    params->value.sequence->length);

	len = gcp->iv->length;
	if (len != ctx->cipher->iv_len) {
		GOST_CIPHER_PARAMS_free(gcp);
		GOSTerror(GOST_R_INVALID_IV_LENGTH);
		return -1;
	}

	if (!Gost2814789_set_sbox(&c->ks, OBJ_obj2nid(gcp->enc_param_set))) {
		GOST_CIPHER_PARAMS_free(gcp);
		return -1;
	}
	c->param_nid = OBJ_obj2nid(gcp->enc_param_set);

	memcpy(ctx->oiv, gcp->iv->data, len);
	memcpy(ctx->iv, gcp->iv->data, len);

	GOST_CIPHER_PARAMS_free(gcp);

	return 1;
}

BLOCK_CIPHER_func_ecb(gost2814789, Gost2814789, EVP_GOST2814789_CTX, ks)
BLOCK_CIPHER_func_cfb(gost2814789, Gost2814789, 64, EVP_GOST2814789_CTX, ks)

static int
gost2814789_cnt_cipher(EVP_CIPHER_CTX *ctx, unsigned char *out,
    const unsigned char *in, size_t inl)
{
	EVP_GOST2814789_CTX *c = ctx->cipher_data;

	while (inl >= EVP_MAXCHUNK) {
		Gost2814789_cnt_encrypt(in, out, (long)EVP_MAXCHUNK, &c->ks,
		    ctx->iv, ctx->buf, &ctx->num);
		inl -= EVP_MAXCHUNK;
		in += EVP_MAXCHUNK;
		out += EVP_MAXCHUNK;
	}

	if (inl)
		Gost2814789_cnt_encrypt(in, out, inl, &c->ks, ctx->iv, ctx->buf,
		    &ctx->num);
	return 1;
}

/* gost89 is CFB-64 */
#define NID_gost89_cfb64 NID_id_Gost28147_89

BLOCK_CIPHER_def_ecb(gost2814789, EVP_GOST2814789_CTX, NID_gost89, 8, 32,
		     EVP_CIPH_NO_PADDING | EVP_CIPH_CTRL_INIT,
		     gost2814789_init_key, NULL, gost2814789_set_asn1_params,
		     gost2814789_get_asn1_params, gost2814789_ctl)
BLOCK_CIPHER_def_cfb(gost2814789, EVP_GOST2814789_CTX, NID_gost89, 32, 8, 64,
		     EVP_CIPH_NO_PADDING | EVP_CIPH_CTRL_INIT,
		     gost2814789_init_key, NULL, gost2814789_set_asn1_params,
		     gost2814789_get_asn1_params, gost2814789_ctl)
BLOCK_CIPHER_def1(gost2814789, cnt, cnt, OFB, EVP_GOST2814789_CTX, NID_gost89,
		  1, 32, 8, EVP_CIPH_NO_PADDING | EVP_CIPH_CTRL_INIT,
		  gost2814789_init_key, NULL, gost2814789_set_asn1_params,
		  gost2814789_get_asn1_params, gost2814789_ctl)
#endif
