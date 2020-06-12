/* $OpenBSD: rsa_pmeth.c,v 1.20 2017/08/28 17:41:59 jsing Exp $ */
/* Written by Dr Stephen N Henson (steve@openssl.org) for the OpenSSL
 * project 2006.
 */
/* ====================================================================
 * Copyright (c) 2006 The OpenSSL Project.  All rights reserved.
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

#include <limits.h>
#include <stdio.h>
#include <string.h>

#include <openssl/opensslconf.h>

#include <openssl/asn1t.h>
#include <openssl/bn.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>


#include "evp_locl.h"
#include "rsa_locl.h"

/* RSA pkey context structure */

typedef struct {
	/* Key gen parameters */
	int nbits;
	BIGNUM *pub_exp;
	/* Keygen callback info */
	int gentmp[2];
	/* RSA padding mode */
	int pad_mode;
	/* message digest */
	const EVP_MD *md;
	/* message digest for MGF1 */
	const EVP_MD *mgf1md;
	/* PSS/OAEP salt length */
	int saltlen;
	/* Temp buffer */
	unsigned char *tbuf;
} RSA_PKEY_CTX;

static int
pkey_rsa_init(EVP_PKEY_CTX *ctx)
{
	RSA_PKEY_CTX *rctx;

	rctx = malloc(sizeof(RSA_PKEY_CTX));
	if (!rctx)
		return 0;
	rctx->nbits = 2048;
	rctx->pub_exp = NULL;
	rctx->pad_mode = RSA_PKCS1_PADDING;
	rctx->md = NULL;
	rctx->mgf1md = NULL;
	rctx->tbuf = NULL;

	rctx->saltlen = -2;

	ctx->data = rctx;
	ctx->keygen_info = rctx->gentmp;
	ctx->keygen_info_count = 2;

	return 1;
}

static int
pkey_rsa_copy(EVP_PKEY_CTX *dst, EVP_PKEY_CTX *src)
{
	RSA_PKEY_CTX *dctx, *sctx;

	if (!pkey_rsa_init(dst))
		return 0;
	sctx = src->data;
	dctx = dst->data;
	dctx->nbits = sctx->nbits;
	if (sctx->pub_exp) {
		dctx->pub_exp = BN_dup(sctx->pub_exp);
		if (!dctx->pub_exp)
			return 0;
	}
	dctx->pad_mode = sctx->pad_mode;
	dctx->md = sctx->md;
	return 1;
}

static int
setup_tbuf(RSA_PKEY_CTX *ctx, EVP_PKEY_CTX *pk)
{
	if (ctx->tbuf)
		return 1;
	ctx->tbuf = malloc(EVP_PKEY_size(pk->pkey));
	if (!ctx->tbuf)
		return 0;
	return 1;
}

static void
pkey_rsa_cleanup(EVP_PKEY_CTX *ctx)
{
	RSA_PKEY_CTX *rctx = ctx->data;

	if (rctx) {
		BN_free(rctx->pub_exp);
		free(rctx->tbuf);
		free(rctx);
	}
}

static int
pkey_rsa_sign(EVP_PKEY_CTX *ctx, unsigned char *sig, size_t *siglen,
    const unsigned char *tbs, size_t tbslen)
{
	int ret;
	RSA_PKEY_CTX *rctx = ctx->data;
	RSA *rsa = ctx->pkey->pkey.rsa;

	if (rctx->md) {
		if (tbslen != (size_t)EVP_MD_size(rctx->md)) {
			RSAerror(RSA_R_INVALID_DIGEST_LENGTH);
			return -1;
		}

		if (rctx->pad_mode == RSA_X931_PADDING) {
			if (!setup_tbuf(rctx, ctx))
				return -1;
			memcpy(rctx->tbuf, tbs, tbslen);
			rctx->tbuf[tbslen] =
			    RSA_X931_hash_id(EVP_MD_type(rctx->md));
			ret = RSA_private_encrypt(tbslen + 1, rctx->tbuf, sig,
			    rsa, RSA_X931_PADDING);
		} else if (rctx->pad_mode == RSA_PKCS1_PADDING) {
			unsigned int sltmp;

			ret = RSA_sign(EVP_MD_type(rctx->md), tbs, tbslen, sig,
			    &sltmp, rsa);
			if (ret <= 0)
				return ret;
			ret = sltmp;
		} else if (rctx->pad_mode == RSA_PKCS1_PSS_PADDING) {
			if (!setup_tbuf(rctx, ctx))
				return -1;
			if (!RSA_padding_add_PKCS1_PSS_mgf1(rsa, rctx->tbuf,
			    tbs, rctx->md, rctx->mgf1md, rctx->saltlen))
				return -1;
			ret = RSA_private_encrypt(RSA_size(rsa), rctx->tbuf,
			    sig, rsa, RSA_NO_PADDING);
		} else
			return -1;
	} else
		ret = RSA_private_encrypt(tbslen, tbs, sig, ctx->pkey->pkey.rsa,
		    rctx->pad_mode);
	if (ret < 0)
		return ret;
	*siglen = ret;
	return 1;
}

static int
pkey_rsa_verifyrecover(EVP_PKEY_CTX *ctx, unsigned char *rout, size_t *routlen,
    const unsigned char *sig, size_t siglen)
{
	int ret;
	RSA_PKEY_CTX *rctx = ctx->data;

	if (rctx->md) {
		if (rctx->pad_mode == RSA_X931_PADDING) {
			if (!setup_tbuf(rctx, ctx))
				return -1;
			ret = RSA_public_decrypt(siglen, sig, rctx->tbuf,
			    ctx->pkey->pkey.rsa, RSA_X931_PADDING);
			if (ret < 1)
				return 0;
			ret--;
			if (rctx->tbuf[ret] !=
				RSA_X931_hash_id(EVP_MD_type(rctx->md))) {
				RSAerror(RSA_R_ALGORITHM_MISMATCH);
				return 0;
			}
			if (ret != EVP_MD_size(rctx->md)) {
				RSAerror(RSA_R_INVALID_DIGEST_LENGTH);
				return 0;
			}
			if (rout)
				memcpy(rout, rctx->tbuf, ret);
		} else if (rctx->pad_mode == RSA_PKCS1_PADDING) {
			size_t sltmp;

			ret = int_rsa_verify(EVP_MD_type(rctx->md), NULL, 0,
			    rout, &sltmp, sig, siglen, ctx->pkey->pkey.rsa);
			if (ret <= 0)
				return 0;
			ret = sltmp;
		} else
			return -1;
	} else
		ret = RSA_public_decrypt(siglen, sig, rout, ctx->pkey->pkey.rsa,
		    rctx->pad_mode);
	if (ret < 0)
		return ret;
	*routlen = ret;
	return 1;
}

static int
pkey_rsa_verify(EVP_PKEY_CTX *ctx, const unsigned char *sig, size_t siglen,
    const unsigned char *tbs, size_t tbslen)
{
	RSA_PKEY_CTX *rctx = ctx->data;
	RSA *rsa = ctx->pkey->pkey.rsa;
	size_t rslen;

	if (rctx->md) {
		if (rctx->pad_mode == RSA_PKCS1_PADDING)
			return RSA_verify(EVP_MD_type(rctx->md), tbs, tbslen,
			    sig, siglen, rsa);
		if (rctx->pad_mode == RSA_X931_PADDING) {
			if (pkey_rsa_verifyrecover(ctx, NULL, &rslen, sig,
			    siglen) <= 0)
				return 0;
		} else if (rctx->pad_mode == RSA_PKCS1_PSS_PADDING) {
			int ret;

			if (!setup_tbuf(rctx, ctx))
				return -1;
			ret = RSA_public_decrypt(siglen, sig, rctx->tbuf,
			    rsa, RSA_NO_PADDING);
			if (ret <= 0)
				return 0;
			ret = RSA_verify_PKCS1_PSS_mgf1(rsa, tbs, rctx->md,
			    rctx->mgf1md, rctx->tbuf, rctx->saltlen);
			if (ret <= 0)
				return 0;
			return 1;
		} else
			return -1;
	} else {
		if (!setup_tbuf(rctx, ctx))
			return -1;
		rslen = RSA_public_decrypt(siglen, sig, rctx->tbuf, rsa,
		    rctx->pad_mode);
		if (rslen == 0)
			return 0;
	}

	if (rslen != tbslen || memcmp(tbs, rctx->tbuf, rslen))
		return 0;

	return 1;
}

static int
pkey_rsa_encrypt(EVP_PKEY_CTX *ctx, unsigned char *out, size_t *outlen,
    const unsigned char *in, size_t inlen)
{
	int ret;
	RSA_PKEY_CTX *rctx = ctx->data;

	ret = RSA_public_encrypt(inlen, in, out, ctx->pkey->pkey.rsa,
	    rctx->pad_mode);
	if (ret < 0)
		return ret;
	*outlen = ret;
	return 1;
}

static int
pkey_rsa_decrypt(EVP_PKEY_CTX *ctx, unsigned char *out, size_t *outlen,
    const unsigned char *in, size_t inlen)
{
	int ret;
	RSA_PKEY_CTX *rctx = ctx->data;

	ret = RSA_private_decrypt(inlen, in, out, ctx->pkey->pkey.rsa,
	    rctx->pad_mode);
	if (ret < 0)
		return ret;
	*outlen = ret;
	return 1;
}

static int
check_padding_md(const EVP_MD *md, int padding)
{
	if (!md)
		return 1;

	if (padding == RSA_NO_PADDING) {
		RSAerror(RSA_R_INVALID_PADDING_MODE);
		return 0;
	}

	if (padding == RSA_X931_PADDING) {
		if (RSA_X931_hash_id(EVP_MD_type(md)) == -1) {
			RSAerror(RSA_R_INVALID_X931_DIGEST);
			return 0;
		}
		return 1;
	}

	return 1;
}

static int
pkey_rsa_ctrl(EVP_PKEY_CTX *ctx, int type, int p1, void *p2)
{
	RSA_PKEY_CTX *rctx = ctx->data;

	switch (type) {
	case EVP_PKEY_CTRL_RSA_PADDING:
		if (p1 >= RSA_PKCS1_PADDING && p1 <= RSA_PKCS1_PSS_PADDING) {
			if (!check_padding_md(rctx->md, p1))
				return 0;
			if (p1 == RSA_PKCS1_PSS_PADDING) {
				if (!(ctx->operation &
				    (EVP_PKEY_OP_SIGN | EVP_PKEY_OP_VERIFY)))
					goto bad_pad;
				if (!rctx->md)
					rctx->md = EVP_sha1();
			}
			if (p1 == RSA_PKCS1_OAEP_PADDING) {
				if (!(ctx->operation & EVP_PKEY_OP_TYPE_CRYPT))
					goto bad_pad;
				if (!rctx->md)
					rctx->md = EVP_sha1();
			}
			rctx->pad_mode = p1;
			return 1;
		}
bad_pad:
		RSAerror(RSA_R_ILLEGAL_OR_UNSUPPORTED_PADDING_MODE);
		return -2;

	case EVP_PKEY_CTRL_GET_RSA_PADDING:
		*(int *)p2 = rctx->pad_mode;
		return 1;

	case EVP_PKEY_CTRL_RSA_PSS_SALTLEN:
	case EVP_PKEY_CTRL_GET_RSA_PSS_SALTLEN:
		if (rctx->pad_mode != RSA_PKCS1_PSS_PADDING) {
			RSAerror(RSA_R_INVALID_PSS_SALTLEN);
			return -2;
		}
		if (type == EVP_PKEY_CTRL_GET_RSA_PSS_SALTLEN)
			*(int *)p2 = rctx->saltlen;
		else {
			if (p1 < -2)
				return -2;
			rctx->saltlen = p1;
		}
		return 1;

	case EVP_PKEY_CTRL_RSA_KEYGEN_BITS:
		if (p1 < 256) {
			RSAerror(RSA_R_INVALID_KEYBITS);
			return -2;
		}
		rctx->nbits = p1;
		return 1;

	case EVP_PKEY_CTRL_RSA_KEYGEN_PUBEXP:
		if (!p2)
			return -2;
		rctx->pub_exp = p2;
		return 1;

	case EVP_PKEY_CTRL_MD:
		if (!check_padding_md(p2, rctx->pad_mode))
			return 0;
		rctx->md = p2;
		return 1;

	case EVP_PKEY_CTRL_RSA_MGF1_MD:
	case EVP_PKEY_CTRL_GET_RSA_MGF1_MD:
		if (rctx->pad_mode != RSA_PKCS1_PSS_PADDING) {
			RSAerror(RSA_R_INVALID_MGF1_MD);
			return -2;
		}
		if (type == EVP_PKEY_CTRL_GET_RSA_MGF1_MD) {
			if (rctx->mgf1md)
				*(const EVP_MD **)p2 = rctx->mgf1md;
			else
				*(const EVP_MD **)p2 = rctx->md;
		} else
			rctx->mgf1md = p2;
		return 1;

	case EVP_PKEY_CTRL_DIGESTINIT:
	case EVP_PKEY_CTRL_PKCS7_ENCRYPT:
	case EVP_PKEY_CTRL_PKCS7_DECRYPT:
	case EVP_PKEY_CTRL_PKCS7_SIGN:
		return 1;
	case EVP_PKEY_CTRL_PEER_KEY:
		RSAerror(RSA_R_OPERATION_NOT_SUPPORTED_FOR_THIS_KEYTYPE);
		return -2;

	default:
		return -2;
	}
}

static int
pkey_rsa_ctrl_str(EVP_PKEY_CTX *ctx, const char *type, const char *value)
{
	long lval;
	char *ep;

	if (!value) {
		RSAerror(RSA_R_VALUE_MISSING);
		return 0;
	}
	if (!strcmp(type, "rsa_padding_mode")) {
		int pm;
		if (!strcmp(value, "pkcs1"))
			pm = RSA_PKCS1_PADDING;
		else if (!strcmp(value, "none"))
			pm = RSA_NO_PADDING;
		else if (!strcmp(value, "oeap"))
			pm = RSA_PKCS1_OAEP_PADDING;
		else if (!strcmp(value, "oaep"))
			pm = RSA_PKCS1_OAEP_PADDING;
		else if (!strcmp(value, "x931"))
			pm = RSA_X931_PADDING;
		else if (!strcmp(value, "pss"))
			pm = RSA_PKCS1_PSS_PADDING;
		else {
			RSAerror(RSA_R_UNKNOWN_PADDING_TYPE);
			return -2;
		}
		return EVP_PKEY_CTX_set_rsa_padding(ctx, pm);
	}

	if (!strcmp(type, "rsa_pss_saltlen")) {
		int saltlen;

		errno = 0;
		lval = strtol(value, &ep, 10);
		if (value[0] == '\0' || *ep != '\0')
			goto not_a_number;
		if ((errno == ERANGE &&
		    (lval == LONG_MAX || lval == LONG_MIN)) ||
		    (lval > INT_MAX || lval < INT_MIN))
			goto out_of_range;
		saltlen = lval;
		return EVP_PKEY_CTX_set_rsa_pss_saltlen(ctx, saltlen);
	}

	if (!strcmp(type, "rsa_keygen_bits")) {
		int nbits;

		errno = 0;
		lval = strtol(value, &ep, 10);
		if (value[0] == '\0' || *ep != '\0')
			goto not_a_number;
		if ((errno == ERANGE &&
		    (lval == LONG_MAX || lval == LONG_MIN)) ||
		    (lval > INT_MAX || lval < INT_MIN))
			goto out_of_range;
		nbits = lval;
		return EVP_PKEY_CTX_set_rsa_keygen_bits(ctx, nbits);
	}

	if (!strcmp(type, "rsa_keygen_pubexp")) {
		int ret;
		BIGNUM *pubexp = NULL;

		if (!BN_asc2bn(&pubexp, value))
			return 0;
		ret = EVP_PKEY_CTX_set_rsa_keygen_pubexp(ctx, pubexp);
		if (ret <= 0)
			BN_free(pubexp);
		return ret;
	}

not_a_number:
out_of_range:
	return -2;
}

static int
pkey_rsa_keygen(EVP_PKEY_CTX *ctx, EVP_PKEY *pkey)
{
	RSA *rsa = NULL;
	RSA_PKEY_CTX *rctx = ctx->data;
	BN_GENCB *pcb, cb;
	int ret;

	if (!rctx->pub_exp) {
		rctx->pub_exp = BN_new();
		if (!rctx->pub_exp || !BN_set_word(rctx->pub_exp, RSA_F4))
			return 0;
	}
	rsa = RSA_new();
	if (!rsa)
		return 0;
	if (ctx->pkey_gencb) {
		pcb = &cb;
		evp_pkey_set_cb_translate(pcb, ctx);
	} else
		pcb = NULL;
	ret = RSA_generate_key_ex(rsa, rctx->nbits, rctx->pub_exp, pcb);
	if (ret > 0)
		EVP_PKEY_assign_RSA(pkey, rsa);
	else
		RSA_free(rsa);
	return ret;
}

const EVP_PKEY_METHOD rsa_pkey_meth = {
	.pkey_id = EVP_PKEY_RSA,
	.flags = EVP_PKEY_FLAG_AUTOARGLEN,

	.init = pkey_rsa_init,
	.copy = pkey_rsa_copy,
	.cleanup = pkey_rsa_cleanup,

	.keygen = pkey_rsa_keygen,

	.sign = pkey_rsa_sign,

	.verify = pkey_rsa_verify,

	.verify_recover = pkey_rsa_verifyrecover,

	.encrypt = pkey_rsa_encrypt,

	.decrypt = pkey_rsa_decrypt,

	.ctrl = pkey_rsa_ctrl,
	.ctrl_str = pkey_rsa_ctrl_str
};
