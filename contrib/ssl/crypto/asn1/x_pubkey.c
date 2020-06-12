/* $OpenBSD: x_pubkey.c,v 1.26 2017/01/29 17:49:22 beck Exp $ */
/* Copyright (C) 1995-1998 Eric Young (eay@cryptsoft.com)
 * All rights reserved.
 *
 * This package is an SSL implementation written
 * by Eric Young (eay@cryptsoft.com).
 * The implementation was written so as to conform with Netscapes SSL.
 *
 * This library is free for commercial and non-commercial use as long as
 * the following conditions are aheared to.  The following conditions
 * apply to all code found in this distribution, be it the RC4, RSA,
 * lhash, DES, etc., code; not just the SSL code.  The SSL documentation
 * included with this distribution is covered by the same copyright terms
 * except that the holder is Tim Hudson (tjh@cryptsoft.com).
 *
 * Copyright remains Eric Young's, and as such any Copyright notices in
 * the code are not to be removed.
 * If this package is used in a product, Eric Young should be given attribution
 * as the author of the parts of the library used.
 * This can be in the form of a textual message at program startup or
 * in documentation (online or textual) provided with the package.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *    "This product includes cryptographic software written by
 *     Eric Young (eay@cryptsoft.com)"
 *    The word 'cryptographic' can be left out if the rouines from the library
 *    being used are not cryptographic related :-).
 * 4. If you include any Windows specific code (or a derivative thereof) from
 *    the apps directory (application code) you must include an acknowledgement:
 *    "This product includes software written by Tim Hudson (tjh@cryptsoft.com)"
 *
 * THIS SOFTWARE IS PROVIDED BY ERIC YOUNG ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * The licence and distribution terms for any publically available version or
 * derivative of this code cannot be changed.  i.e. this code cannot simply be
 * copied and put under another distribution licence
 * [including the GNU Public Licence.]
 */

#include <stdio.h>

#include <openssl/opensslconf.h>

#include <openssl/asn1t.h>
#include <openssl/err.h>
#include <openssl/x509.h>

#ifndef OPENSSL_NO_DSA
#include <openssl/dsa.h>
#endif
#ifndef OPENSSL_NO_RSA
#include <openssl/rsa.h>
#endif

#include "asn1_locl.h"

/* Minor tweak to operation: free up EVP_PKEY */
static int
pubkey_cb(int operation, ASN1_VALUE **pval, const ASN1_ITEM *it, void *exarg)
{
	if (operation == ASN1_OP_FREE_POST) {
		X509_PUBKEY *pubkey = (X509_PUBKEY *)*pval;
		EVP_PKEY_free(pubkey->pkey);
	}
	return 1;
}

static const ASN1_AUX X509_PUBKEY_aux = {
	.asn1_cb = pubkey_cb,
};
static const ASN1_TEMPLATE X509_PUBKEY_seq_tt[] = {
	{
		.offset = offsetof(X509_PUBKEY, algor),
		.field_name = "algor",
		.item = &X509_ALGOR_it,
	},
	{
		.offset = offsetof(X509_PUBKEY, public_key),
		.field_name = "public_key",
		.item = &ASN1_BIT_STRING_it,
	},
};

const ASN1_ITEM X509_PUBKEY_it = {
	.itype = ASN1_ITYPE_SEQUENCE,
	.utype = V_ASN1_SEQUENCE,
	.templates = X509_PUBKEY_seq_tt,
	.tcount = sizeof(X509_PUBKEY_seq_tt) / sizeof(ASN1_TEMPLATE),
	.funcs = &X509_PUBKEY_aux,
	.size = sizeof(X509_PUBKEY),
	.sname = "X509_PUBKEY",
};


X509_PUBKEY *
d2i_X509_PUBKEY(X509_PUBKEY **a, const unsigned char **in, long len)
{
	return (X509_PUBKEY *)ASN1_item_d2i((ASN1_VALUE **)a, in, len,
	    &X509_PUBKEY_it);
}

int
i2d_X509_PUBKEY(X509_PUBKEY *a, unsigned char **out)
{
	return ASN1_item_i2d((ASN1_VALUE *)a, out, &X509_PUBKEY_it);
}

X509_PUBKEY *
X509_PUBKEY_new(void)
{
	return (X509_PUBKEY *)ASN1_item_new(&X509_PUBKEY_it);
}

void
X509_PUBKEY_free(X509_PUBKEY *a)
{
	ASN1_item_free((ASN1_VALUE *)a, &X509_PUBKEY_it);
}

int
X509_PUBKEY_set(X509_PUBKEY **x, EVP_PKEY *pkey)
{
	X509_PUBKEY *pk = NULL;

	if (x == NULL)
		return (0);
	if ((pk = X509_PUBKEY_new()) == NULL)
		goto error;

	if (pkey->ameth) {
		if (pkey->ameth->pub_encode) {
			if (!pkey->ameth->pub_encode(pk, pkey)) {
				X509error(X509_R_PUBLIC_KEY_ENCODE_ERROR);
				goto error;
			}
		} else {
			X509error(X509_R_METHOD_NOT_SUPPORTED);
			goto error;
		}
	} else {
		X509error(X509_R_UNSUPPORTED_ALGORITHM);
		goto error;
	}

	if (*x != NULL)
		X509_PUBKEY_free(*x);

	*x = pk;

	return 1;

error:
	if (pk != NULL)
		X509_PUBKEY_free(pk);
	return 0;
}

EVP_PKEY *
X509_PUBKEY_get(X509_PUBKEY *key)
{
	EVP_PKEY *ret = NULL;

	if (key == NULL)
		goto error;

	if (key->pkey != NULL) {
		CRYPTO_add(&key->pkey->references, 1, CRYPTO_LOCK_EVP_PKEY);
		return key->pkey;
	}

	if (key->public_key == NULL)
		goto error;

	if ((ret = EVP_PKEY_new()) == NULL) {
		X509error(ERR_R_MALLOC_FAILURE);
		goto error;
	}

	if (!EVP_PKEY_set_type(ret, OBJ_obj2nid(key->algor->algorithm))) {
		X509error(X509_R_UNSUPPORTED_ALGORITHM);
		goto error;
	}

	if (ret->ameth->pub_decode) {
		if (!ret->ameth->pub_decode(ret, key)) {
			X509error(X509_R_PUBLIC_KEY_DECODE_ERROR);
			goto error;
		}
	} else {
		X509error(X509_R_METHOD_NOT_SUPPORTED);
		goto error;
	}

	/* Check to see if another thread set key->pkey first */
	CRYPTO_w_lock(CRYPTO_LOCK_EVP_PKEY);
	if (key->pkey) {
		CRYPTO_w_unlock(CRYPTO_LOCK_EVP_PKEY);
		EVP_PKEY_free(ret);
		ret = key->pkey;
	} else {
		key->pkey = ret;
		CRYPTO_w_unlock(CRYPTO_LOCK_EVP_PKEY);
	}
	CRYPTO_add(&ret->references, 1, CRYPTO_LOCK_EVP_PKEY);

	return ret;

error:
	EVP_PKEY_free(ret);
	return (NULL);
}

/* Now two pseudo ASN1 routines that take an EVP_PKEY structure
 * and encode or decode as X509_PUBKEY
 */

EVP_PKEY *
d2i_PUBKEY(EVP_PKEY **a, const unsigned char **pp, long length)
{
	X509_PUBKEY *xpk;
	EVP_PKEY *pktmp;
	xpk = d2i_X509_PUBKEY(NULL, pp, length);
	if (!xpk)
		return NULL;
	pktmp = X509_PUBKEY_get(xpk);
	X509_PUBKEY_free(xpk);
	if (!pktmp)
		return NULL;
	if (a) {
		EVP_PKEY_free(*a);
		*a = pktmp;
	}
	return pktmp;
}

int
i2d_PUBKEY(EVP_PKEY *a, unsigned char **pp)
{
	X509_PUBKEY *xpk = NULL;
	int ret;
	if (!a)
		return 0;
	if (!X509_PUBKEY_set(&xpk, a))
		return 0;
	ret = i2d_X509_PUBKEY(xpk, pp);
	X509_PUBKEY_free(xpk);
	return ret;
}

/* The following are equivalents but which return RSA and DSA
 * keys
 */
#ifndef OPENSSL_NO_RSA
RSA *
d2i_RSA_PUBKEY(RSA **a, const unsigned char **pp, long length)
{
	EVP_PKEY *pkey;
	RSA *key;
	const unsigned char *q;
	q = *pp;
	pkey = d2i_PUBKEY(NULL, &q, length);
	if (!pkey)
		return NULL;
	key = EVP_PKEY_get1_RSA(pkey);
	EVP_PKEY_free(pkey);
	if (!key)
		return NULL;
	*pp = q;
	if (a) {
		RSA_free(*a);
		*a = key;
	}
	return key;
}

int
i2d_RSA_PUBKEY(RSA *a, unsigned char **pp)
{
	EVP_PKEY *pktmp;
	int ret;
	if (!a)
		return 0;
	pktmp = EVP_PKEY_new();
	if (!pktmp) {
		ASN1error(ERR_R_MALLOC_FAILURE);
		return 0;
	}
	EVP_PKEY_set1_RSA(pktmp, a);
	ret = i2d_PUBKEY(pktmp, pp);
	EVP_PKEY_free(pktmp);
	return ret;
}
#endif

#ifndef OPENSSL_NO_DSA
DSA *
d2i_DSA_PUBKEY(DSA **a, const unsigned char **pp, long length)
{
	EVP_PKEY *pkey;
	DSA *key;
	const unsigned char *q;
	q = *pp;
	pkey = d2i_PUBKEY(NULL, &q, length);
	if (!pkey)
		return NULL;
	key = EVP_PKEY_get1_DSA(pkey);
	EVP_PKEY_free(pkey);
	if (!key)
		return NULL;
	*pp = q;
	if (a) {
		DSA_free(*a);
		*a = key;
	}
	return key;
}

int
i2d_DSA_PUBKEY(DSA *a, unsigned char **pp)
{
	EVP_PKEY *pktmp;
	int ret;
	if (!a)
		return 0;
	pktmp = EVP_PKEY_new();
	if (!pktmp) {
		ASN1error(ERR_R_MALLOC_FAILURE);
		return 0;
	}
	EVP_PKEY_set1_DSA(pktmp, a);
	ret = i2d_PUBKEY(pktmp, pp);
	EVP_PKEY_free(pktmp);
	return ret;
}
#endif

#ifndef OPENSSL_NO_EC
EC_KEY *
d2i_EC_PUBKEY(EC_KEY **a, const unsigned char **pp, long length)
{
	EVP_PKEY *pkey;
	EC_KEY *key;
	const unsigned char *q;
	q = *pp;
	pkey = d2i_PUBKEY(NULL, &q, length);
	if (!pkey)
		return (NULL);
	key = EVP_PKEY_get1_EC_KEY(pkey);
	EVP_PKEY_free(pkey);
	if (!key)
		return (NULL);
	*pp = q;
	if (a) {
		EC_KEY_free(*a);
		*a = key;
	}
	return (key);
}

int
i2d_EC_PUBKEY(EC_KEY *a, unsigned char **pp)
{
	EVP_PKEY *pktmp;
	int ret;
	if (!a)
		return (0);
	if ((pktmp = EVP_PKEY_new()) == NULL) {
		ASN1error(ERR_R_MALLOC_FAILURE);
		return (0);
	}
	EVP_PKEY_set1_EC_KEY(pktmp, a);
	ret = i2d_PUBKEY(pktmp, pp);
	EVP_PKEY_free(pktmp);
	return (ret);
}
#endif

int
X509_PUBKEY_set0_param(X509_PUBKEY *pub, ASN1_OBJECT *aobj, int ptype,
    void *pval, unsigned char *penc, int penclen)
{
	if (!X509_ALGOR_set0(pub->algor, aobj, ptype, pval))
		return 0;
	if (penc) {
		free(pub->public_key->data);
		pub->public_key->data = penc;
		pub->public_key->length = penclen;
		/* Set number of unused bits to zero */
		pub->public_key->flags&= ~(ASN1_STRING_FLAG_BITS_LEFT|0x07);
		pub->public_key->flags |= ASN1_STRING_FLAG_BITS_LEFT;
	}
	return 1;
}

int
X509_PUBKEY_get0_param(ASN1_OBJECT **ppkalg, const unsigned char **pk,
    int *ppklen, X509_ALGOR **pa, X509_PUBKEY *pub)
{
	if (ppkalg)
		*ppkalg = pub->algor->algorithm;
	if (pk) {
		*pk = pub->public_key->data;
		*ppklen = pub->public_key->length;
	}
	if (pa)
		*pa = pub->algor;
	return 1;
}
