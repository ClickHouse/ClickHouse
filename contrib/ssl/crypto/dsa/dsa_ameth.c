/* $OpenBSD: dsa_ameth.c,v 1.23 2017/01/29 17:49:22 beck Exp $ */
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

#include <stdio.h>

#include <openssl/opensslconf.h>

#include <openssl/asn1.h>
#include <openssl/bn.h>
#include <openssl/dsa.h>
#include <openssl/err.h>
#include <openssl/x509.h>

#include "asn1_locl.h"
#include "bn_lcl.h"

static int
dsa_pub_decode(EVP_PKEY *pkey, X509_PUBKEY *pubkey)
{
	const unsigned char *p, *pm;
	int pklen, pmlen;
	int ptype;
	void *pval;
	ASN1_STRING *pstr;
	X509_ALGOR *palg;
	ASN1_INTEGER *public_key = NULL;

	DSA *dsa = NULL;

	if (!X509_PUBKEY_get0_param(NULL, &p, &pklen, &palg, pubkey))
		return 0;
	X509_ALGOR_get0(NULL, &ptype, &pval, palg);

	if (ptype == V_ASN1_SEQUENCE) {
		pstr = pval;	
		pm = pstr->data;
		pmlen = pstr->length;

		if (!(dsa = d2i_DSAparams(NULL, &pm, pmlen))) {
			DSAerror(DSA_R_DECODE_ERROR);
			goto err;
		}
	} else if (ptype == V_ASN1_NULL || ptype == V_ASN1_UNDEF) {
		if (!(dsa = DSA_new())) {
			DSAerror(ERR_R_MALLOC_FAILURE);
			goto err;
			}
	} else {
		DSAerror(DSA_R_PARAMETER_ENCODING_ERROR);
		goto err;
	}

	if (!(public_key=d2i_ASN1_INTEGER(NULL, &p, pklen))) {
		DSAerror(DSA_R_DECODE_ERROR);
		goto err;
	}

	if (!(dsa->pub_key = ASN1_INTEGER_to_BN(public_key, NULL))) {
		DSAerror(DSA_R_BN_DECODE_ERROR);
		goto err;
	}

	ASN1_INTEGER_free(public_key);
	EVP_PKEY_assign_DSA(pkey, dsa);
	return 1;

err:
	if (public_key)
		ASN1_INTEGER_free(public_key);
	DSA_free(dsa);
	return 0;
}

static int
dsa_pub_encode(X509_PUBKEY *pk, const EVP_PKEY *pkey)
{
	DSA *dsa;
	void *pval = NULL;
	int ptype;
	unsigned char *penc = NULL;
	int penclen;

	dsa = pkey->pkey.dsa;
	if (pkey->save_parameters && dsa->p && dsa->q && dsa->g) {
		ASN1_STRING *str;

		str = ASN1_STRING_new();
		if (str == NULL) {
			DSAerror(ERR_R_MALLOC_FAILURE);
			goto err;
		}
		str->length = i2d_DSAparams(dsa, &str->data);
		if (str->length <= 0) {
			DSAerror(ERR_R_MALLOC_FAILURE);
			ASN1_STRING_free(str);
			goto err;
		}
		pval = str;
		ptype = V_ASN1_SEQUENCE;
	} else
		ptype = V_ASN1_UNDEF;

	dsa->write_params = 0;

	penclen = i2d_DSAPublicKey(dsa, &penc);

	if (penclen <= 0) {
		DSAerror(ERR_R_MALLOC_FAILURE);
		goto err;
	}

	if (X509_PUBKEY_set0_param(pk, OBJ_nid2obj(EVP_PKEY_DSA), ptype, pval,
	    penc, penclen))
		return 1;

err:
	free(penc);
	ASN1_STRING_free(pval);

	return 0;
}

/* In PKCS#8 DSA: you just get a private key integer and parameters in the
 * AlgorithmIdentifier the pubkey must be recalculated.
 */
static int
dsa_priv_decode(EVP_PKEY *pkey, PKCS8_PRIV_KEY_INFO *p8)
{
	const unsigned char *p, *pm;
	int pklen, pmlen;
	int ptype;
	void *pval;
	ASN1_STRING *pstr;
	X509_ALGOR *palg;
	ASN1_INTEGER *privkey = NULL;
	BN_CTX *ctx = NULL;
	DSA *dsa = NULL;

	int ret = 0;

	if (!PKCS8_pkey_get0(NULL, &p, &pklen, &palg, p8))
		return 0;
	X509_ALGOR_get0(NULL, &ptype, &pval, palg);
	if (ptype != V_ASN1_SEQUENCE)
		goto decerr;

	if ((privkey = d2i_ASN1_INTEGER(NULL, &p, pklen)) == NULL)
		goto decerr;
	if (privkey->type == V_ASN1_NEG_INTEGER)
		goto decerr;

	pstr = pval;
	pm = pstr->data;
	pmlen = pstr->length;
	if (!(dsa = d2i_DSAparams(NULL, &pm, pmlen)))
		goto decerr;
	/* We have parameters now set private key */
	if (!(dsa->priv_key = ASN1_INTEGER_to_BN(privkey, NULL))) {
		DSAerror(DSA_R_BN_ERROR);
		goto dsaerr;
	}
	/* Calculate public key */
	if (!(dsa->pub_key = BN_new())) {
		DSAerror(ERR_R_MALLOC_FAILURE);
		goto dsaerr;
	}
	if (!(ctx = BN_CTX_new())) {
		DSAerror(ERR_R_MALLOC_FAILURE);
		goto dsaerr;
	}

	if (!BN_mod_exp_ct(dsa->pub_key, dsa->g, dsa->priv_key, dsa->p, ctx)) {
		DSAerror(DSA_R_BN_ERROR);
		goto dsaerr;
	}

	if (!EVP_PKEY_assign_DSA(pkey, dsa))
		goto decerr;

	ret = 1;
	goto done;

decerr:
	DSAerror(DSA_R_DECODE_ERROR);
dsaerr:
	DSA_free(dsa);
done:
	BN_CTX_free(ctx);
	ASN1_INTEGER_free(privkey);
	return ret;
}

static int
dsa_priv_encode(PKCS8_PRIV_KEY_INFO *p8, const EVP_PKEY *pkey)
{
	ASN1_STRING *params = NULL;
	ASN1_INTEGER *prkey = NULL;
	unsigned char *dp = NULL;
	int dplen;

	params = ASN1_STRING_new();
	if (!params) {
		DSAerror(ERR_R_MALLOC_FAILURE);
		goto err;
	}

	params->length = i2d_DSAparams(pkey->pkey.dsa, &params->data);
	if (params->length <= 0) {
		DSAerror(ERR_R_MALLOC_FAILURE);
		goto err;
	}
	params->type = V_ASN1_SEQUENCE;

	/* Get private key into integer */
	prkey = BN_to_ASN1_INTEGER(pkey->pkey.dsa->priv_key, NULL);
	if (!prkey) {
		DSAerror(DSA_R_BN_ERROR);
		goto err;
	}

	dplen = i2d_ASN1_INTEGER(prkey, &dp);

	ASN1_INTEGER_free(prkey);
	prkey = NULL;

	if (!PKCS8_pkey_set0(p8, OBJ_nid2obj(NID_dsa), 0, V_ASN1_SEQUENCE,
	    params, dp, dplen))
		goto err;

	return 1;

err:
	free(dp);
	ASN1_STRING_free(params);
	ASN1_INTEGER_free(prkey);
	return 0;
}

static int
int_dsa_size(const EVP_PKEY *pkey)
{
	return DSA_size(pkey->pkey.dsa);
}

static int
dsa_bits(const EVP_PKEY *pkey)
{
	return BN_num_bits(pkey->pkey.dsa->p);
}

static int
dsa_missing_parameters(const EVP_PKEY *pkey)
{
	DSA *dsa;

	dsa = pkey->pkey.dsa;
	if (dsa->p == NULL || dsa->q == NULL || dsa->g == NULL)
		return 1;
	return 0;
}

static int
dsa_copy_parameters(EVP_PKEY *to, const EVP_PKEY *from)
{
	BIGNUM *a;

	if ((a = BN_dup(from->pkey.dsa->p)) == NULL)
		return 0;
	BN_free(to->pkey.dsa->p);
	to->pkey.dsa->p = a;

	if ((a = BN_dup(from->pkey.dsa->q)) == NULL)
		return 0;
	BN_free(to->pkey.dsa->q);
	to->pkey.dsa->q = a;

	if ((a = BN_dup(from->pkey.dsa->g)) == NULL)
		return 0;
	BN_free(to->pkey.dsa->g);
	to->pkey.dsa->g = a;
	return 1;
}

static int
dsa_cmp_parameters(const EVP_PKEY *a, const EVP_PKEY *b)
{
	if (BN_cmp(a->pkey.dsa->p, b->pkey.dsa->p) ||
	    BN_cmp(a->pkey.dsa->q, b->pkey.dsa->q) ||
	    BN_cmp(a->pkey.dsa->g, b->pkey.dsa->g))
		return 0;
	else
		return 1;
}

static int
dsa_pub_cmp(const EVP_PKEY *a, const EVP_PKEY *b)
{
	if (BN_cmp(b->pkey.dsa->pub_key, a->pkey.dsa->pub_key) != 0)
		return 0;
	else
		return 1;
}

static void
int_dsa_free(EVP_PKEY *pkey)
{
	DSA_free(pkey->pkey.dsa);
}

static void
update_buflen(const BIGNUM *b, size_t *pbuflen)
{
	size_t i;

	if (!b)
		return;
	if (*pbuflen < (i = (size_t)BN_num_bytes(b)))
		*pbuflen = i;
}

static int
do_dsa_print(BIO *bp, const DSA *x, int off, int ptype)
{
	unsigned char *m = NULL;
	int ret = 0;
	size_t buf_len = 0;
	const char *ktype = NULL;
	const BIGNUM *priv_key, *pub_key;

	if (ptype == 2)
		priv_key = x->priv_key;
	else
		priv_key = NULL;

	if (ptype > 0)
		pub_key = x->pub_key;
	else
		pub_key = NULL;

	if (ptype == 2)
		ktype = "Private-Key";
	else if (ptype == 1)
		ktype = "Public-Key";
	else
		ktype = "DSA-Parameters";

	update_buflen(x->p, &buf_len);
	update_buflen(x->q, &buf_len);
	update_buflen(x->g, &buf_len);
	update_buflen(priv_key, &buf_len);
	update_buflen(pub_key, &buf_len);

	m = malloc(buf_len + 10);
	if (m == NULL) {
		DSAerror(ERR_R_MALLOC_FAILURE);
		goto err;
	}

	if (priv_key) {
		if (!BIO_indent(bp, off, 128))
			goto err;
		if (BIO_printf(bp, "%s: (%d bit)\n", ktype,
		    BN_num_bits(x->p)) <= 0)
			goto err;
	}

	if (!ASN1_bn_print(bp, "priv:", priv_key, m, off))
		goto err;
	if (!ASN1_bn_print(bp, "pub: ", pub_key, m, off))
		goto err;
	if (!ASN1_bn_print(bp, "P:   ", x->p, m, off))
		goto err;
	if (!ASN1_bn_print(bp, "Q:   ", x->q, m, off))
		goto err;
	if (!ASN1_bn_print(bp, "G:   ", x->g, m, off))
		goto err;
	ret = 1;
err:
	free(m);
	return(ret);
}

static int
dsa_param_decode(EVP_PKEY *pkey, const unsigned char **pder, int derlen)
{
	DSA *dsa;

	if (!(dsa = d2i_DSAparams(NULL, pder, derlen))) {
		DSAerror(ERR_R_DSA_LIB);
		return 0;
	}
	EVP_PKEY_assign_DSA(pkey, dsa);
	return 1;
}

static int
dsa_param_encode(const EVP_PKEY *pkey, unsigned char **pder)
{
	return i2d_DSAparams(pkey->pkey.dsa, pder);
}

static int
dsa_param_print(BIO *bp, const EVP_PKEY *pkey, int indent, ASN1_PCTX *ctx)
{
	return do_dsa_print(bp, pkey->pkey.dsa, indent, 0);
}

static int
dsa_pub_print(BIO *bp, const EVP_PKEY *pkey, int indent, ASN1_PCTX *ctx)
{
	return do_dsa_print(bp, pkey->pkey.dsa, indent, 1);
}

static int
dsa_priv_print(BIO *bp, const EVP_PKEY *pkey, int indent, ASN1_PCTX *ctx)
{
	return do_dsa_print(bp, pkey->pkey.dsa, indent, 2);
}

static int
old_dsa_priv_decode(EVP_PKEY *pkey, const unsigned char **pder, int derlen)
{
	DSA *dsa;
	BN_CTX *ctx = NULL;
	BIGNUM *j, *p1, *newp1;

	if (!(dsa = d2i_DSAPrivateKey(NULL, pder, derlen))) {
		DSAerror(ERR_R_DSA_LIB);
		return 0;
	}

	ctx = BN_CTX_new();
	if (ctx == NULL)
		goto err;

	/*
	 * Check that p and q are consistent with each other.
	 */

	j = BN_CTX_get(ctx);
	p1 = BN_CTX_get(ctx);
	newp1 = BN_CTX_get(ctx);
	if (j == NULL || p1 == NULL || newp1 == NULL)
		goto err;
	/* p1 = p - 1 */
	if (BN_sub(p1, dsa->p, BN_value_one()) == 0)
		goto err;
	/* j = (p - 1) / q */
	if (BN_div_ct(j, NULL, p1, dsa->q, ctx) == 0)
		goto err;
	/* q * j should == p - 1 */
	if (BN_mul(newp1, dsa->q, j, ctx) == 0)
		goto err;
	if (BN_cmp(newp1, p1) != 0) {
		DSAerror(DSA_R_BAD_Q_VALUE);
		goto err;
	}

	/*
	 * Check that q is not a composite number.
	 */

	if (BN_is_prime_ex(dsa->q, BN_prime_checks, ctx, NULL) == 0) {
		DSAerror(DSA_R_BAD_Q_VALUE);
		goto err;
	}

	BN_CTX_free(ctx);

	EVP_PKEY_assign_DSA(pkey, dsa);
	return 1;

err:
	BN_CTX_free(ctx);
	DSA_free(dsa);
	return 0;
}

static int
old_dsa_priv_encode(const EVP_PKEY *pkey, unsigned char **pder)
{
	return i2d_DSAPrivateKey(pkey->pkey.dsa, pder);
}

static int
dsa_sig_print(BIO *bp, const X509_ALGOR *sigalg, const ASN1_STRING *sig,
    int indent, ASN1_PCTX *pctx)
{
	DSA_SIG *dsa_sig;
	const unsigned char *p;

	if (!sig) {
		if (BIO_puts(bp, "\n") <= 0)
			return 0;
		else
			return 1;
	}
	p = sig->data;
	dsa_sig = d2i_DSA_SIG(NULL, &p, sig->length);
	if (dsa_sig) {
		int rv = 0;
		size_t buf_len = 0;
		unsigned char *m = NULL;

		update_buflen(dsa_sig->r, &buf_len);
		update_buflen(dsa_sig->s, &buf_len);
		m = malloc(buf_len + 10);
		if (m == NULL) {
			DSAerror(ERR_R_MALLOC_FAILURE);
			goto err;
		}

		if (BIO_write(bp, "\n", 1) != 1)
			goto err;

		if (!ASN1_bn_print(bp, "r:   ", dsa_sig->r, m, indent))
			goto err;
		if (!ASN1_bn_print(bp, "s:   ", dsa_sig->s, m, indent))
			goto err;
		rv = 1;
err:
		free(m);
		DSA_SIG_free(dsa_sig);
		return rv;
	}
	return X509_signature_dump(bp, sig, indent);
}

static int
dsa_pkey_ctrl(EVP_PKEY *pkey, int op, long arg1, void *arg2)
{
	switch (op) {
	case ASN1_PKEY_CTRL_PKCS7_SIGN:
		if (arg1 == 0) {
			int snid, hnid;
			X509_ALGOR *alg1, *alg2;

			PKCS7_SIGNER_INFO_get0_algs(arg2, NULL, &alg1, &alg2);
			if (alg1 == NULL || alg1->algorithm == NULL)
				return -1;
			hnid = OBJ_obj2nid(alg1->algorithm);
			if (hnid == NID_undef)
				return -1;
			if (!OBJ_find_sigid_by_algs(&snid, hnid,
			    EVP_PKEY_id(pkey)))
				return -1; 
			X509_ALGOR_set0(alg2, OBJ_nid2obj(snid), V_ASN1_UNDEF,
			    0);
		}
		return 1;

	case ASN1_PKEY_CTRL_DEFAULT_MD_NID:
		*(int *)arg2 = NID_sha1;
		return 2;

	default:
		return -2;
	}
}

/* NB these are sorted in pkey_id order, lowest first */

const EVP_PKEY_ASN1_METHOD dsa_asn1_meths[] = {
	{
		.pkey_id = EVP_PKEY_DSA2,
		.pkey_base_id = EVP_PKEY_DSA,
		.pkey_flags = ASN1_PKEY_ALIAS
	},

	{
		.pkey_id = EVP_PKEY_DSA1,
		.pkey_base_id = EVP_PKEY_DSA,
		.pkey_flags = ASN1_PKEY_ALIAS
	},

	{
		.pkey_id = EVP_PKEY_DSA4,
		.pkey_base_id = EVP_PKEY_DSA,
		.pkey_flags = ASN1_PKEY_ALIAS
	},

	{
		.pkey_id = EVP_PKEY_DSA3,
		.pkey_base_id = EVP_PKEY_DSA,
		.pkey_flags = ASN1_PKEY_ALIAS
	},

	{
		.pkey_id = EVP_PKEY_DSA,
		.pkey_base_id = EVP_PKEY_DSA,

		.pem_str = "DSA",
		.info = "OpenSSL DSA method",

		.pub_decode = dsa_pub_decode,
		.pub_encode = dsa_pub_encode,
		.pub_cmp = dsa_pub_cmp,
		.pub_print = dsa_pub_print,

		.priv_decode = dsa_priv_decode,
		.priv_encode = dsa_priv_encode,
		.priv_print = dsa_priv_print,

		.pkey_size = int_dsa_size,
		.pkey_bits = dsa_bits,

		.param_decode = dsa_param_decode,
		.param_encode = dsa_param_encode,
		.param_missing = dsa_missing_parameters,
		.param_copy = dsa_copy_parameters,
		.param_cmp = dsa_cmp_parameters,
		.param_print = dsa_param_print,
		.sig_print = dsa_sig_print,

		.pkey_free = int_dsa_free,
		.pkey_ctrl = dsa_pkey_ctrl,
		.old_priv_decode = old_dsa_priv_decode,
		.old_priv_encode = old_dsa_priv_encode
	}
};
