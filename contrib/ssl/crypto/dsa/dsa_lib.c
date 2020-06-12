/* $OpenBSD: dsa_lib.c,v 1.23 2017/01/29 17:49:22 beck Exp $ */
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

/* Original version from Steven Schoch <schoch@sheba.arc.nasa.gov> */

#include <stdio.h>

#include <openssl/opensslconf.h>

#include <openssl/asn1.h>
#include <openssl/bn.h>
#include <openssl/dsa.h>
#include <openssl/err.h>

#ifndef OPENSSL_NO_DH
#include <openssl/dh.h>
#endif
#ifndef OPENSSL_NO_ENGINE
#include <openssl/engine.h>
#endif

static const DSA_METHOD *default_DSA_method = NULL;

void
DSA_set_default_method(const DSA_METHOD *meth)
{
	default_DSA_method = meth;
}

const DSA_METHOD *
DSA_get_default_method(void)
{
	if (!default_DSA_method)
		default_DSA_method = DSA_OpenSSL();
	return default_DSA_method;
}

DSA *
DSA_new(void)
{
	return DSA_new_method(NULL);
}

int
DSA_set_method(DSA *dsa, const DSA_METHOD *meth)
{
	/*
	 * NB: The caller is specifically setting a method, so it's not up to us
	 * to deal with which ENGINE it comes from.
	 */
        const DSA_METHOD *mtmp;
        mtmp = dsa->meth;
        if (mtmp->finish)
		mtmp->finish(dsa);
#ifndef OPENSSL_NO_ENGINE
	if (dsa->engine) {
		ENGINE_finish(dsa->engine);
		dsa->engine = NULL;
	}
#endif
        dsa->meth = meth;
        if (meth->init)
		meth->init(dsa);
        return 1;
}

DSA *
DSA_new_method(ENGINE *engine)
{
	DSA *ret;

	ret = malloc(sizeof(DSA));
	if (ret == NULL) {
		DSAerror(ERR_R_MALLOC_FAILURE);
		return NULL;
	}
	ret->meth = DSA_get_default_method();
#ifndef OPENSSL_NO_ENGINE
	if (engine) {
		if (!ENGINE_init(engine)) {
			DSAerror(ERR_R_ENGINE_LIB);
			free(ret);
			return NULL;
		}
		ret->engine = engine;
	} else
		ret->engine = ENGINE_get_default_DSA();
	if (ret->engine) {
		ret->meth = ENGINE_get_DSA(ret->engine);
		if (!ret->meth) {
			DSAerror(ERR_R_ENGINE_LIB);
			ENGINE_finish(ret->engine);
			free(ret);
			return NULL;
		}
	}
#endif

	ret->pad = 0;
	ret->version = 0;
	ret->write_params = 1;
	ret->p = NULL;
	ret->q = NULL;
	ret->g = NULL;

	ret->pub_key = NULL;
	ret->priv_key = NULL;

	ret->kinv = NULL;
	ret->r = NULL;
	ret->method_mont_p = NULL;

	ret->references = 1;
	ret->flags = ret->meth->flags & ~DSA_FLAG_NON_FIPS_ALLOW;
	CRYPTO_new_ex_data(CRYPTO_EX_INDEX_DSA, ret, &ret->ex_data);
	if (ret->meth->init != NULL && !ret->meth->init(ret)) {
#ifndef OPENSSL_NO_ENGINE
		if (ret->engine)
			ENGINE_finish(ret->engine);
#endif
		CRYPTO_free_ex_data(CRYPTO_EX_INDEX_DSA, ret, &ret->ex_data);
		free(ret);
		ret = NULL;
	}
	
	return ret;
}

void
DSA_free(DSA *r)
{
	int i;

	if (r == NULL)
		return;

	i = CRYPTO_add(&r->references, -1, CRYPTO_LOCK_DSA);
	if (i > 0)
		return;

	if (r->meth->finish)
		r->meth->finish(r);
#ifndef OPENSSL_NO_ENGINE
	if (r->engine)
		ENGINE_finish(r->engine);
#endif

	CRYPTO_free_ex_data(CRYPTO_EX_INDEX_DSA, r, &r->ex_data);

	BN_clear_free(r->p);
	BN_clear_free(r->q);
	BN_clear_free(r->g);
	BN_clear_free(r->pub_key);
	BN_clear_free(r->priv_key);
	BN_clear_free(r->kinv);
	BN_clear_free(r->r);
	free(r);
}

int
DSA_up_ref(DSA *r)
{
	int i = CRYPTO_add(&r->references, 1, CRYPTO_LOCK_DSA);
	return i > 1 ? 1 : 0;
}

int
DSA_size(const DSA *r)
{
	int ret, i;
	ASN1_INTEGER bs;
	unsigned char buf[4];	/* 4 bytes looks really small.
				   However, i2d_ASN1_INTEGER() will not look
				   beyond the first byte, as long as the second
				   parameter is NULL. */

	i = BN_num_bits(r->q);
	bs.length = (i + 7) / 8;
	bs.data = buf;
	bs.type = V_ASN1_INTEGER;
	/* If the top bit is set the asn1 encoding is 1 larger. */
	buf[0] = 0xff;

	i = i2d_ASN1_INTEGER(&bs, NULL);
	i += i; /* r and s */
	ret = ASN1_object_size(1, i, V_ASN1_SEQUENCE);
	return ret;
}

int
DSA_get_ex_new_index(long argl, void *argp, CRYPTO_EX_new *new_func,
    CRYPTO_EX_dup *dup_func, CRYPTO_EX_free *free_func)
{
	return CRYPTO_get_ex_new_index(CRYPTO_EX_INDEX_DSA, argl, argp,
	    new_func, dup_func, free_func);
}

int
DSA_set_ex_data(DSA *d, int idx, void *arg)
{
	return CRYPTO_set_ex_data(&d->ex_data, idx, arg);
}

void *
DSA_get_ex_data(DSA *d, int idx)
{
	return CRYPTO_get_ex_data(&d->ex_data, idx);
}

#ifndef OPENSSL_NO_DH
DH *
DSA_dup_DH(const DSA *r)
{
	/*
	 * DSA has p, q, g, optional pub_key, optional priv_key.
	 * DH has p, optional length, g, optional pub_key, optional priv_key,
	 * optional q.
	 */ 
	DH *ret = NULL;

	if (r == NULL)
		goto err;
	ret = DH_new();
	if (ret == NULL)
		goto err;
	if (r->p != NULL) 
		if ((ret->p = BN_dup(r->p)) == NULL)
			goto err;
	if (r->q != NULL) {
		ret->length = BN_num_bits(r->q);
		if ((ret->q = BN_dup(r->q)) == NULL)
			goto err;
	}
	if (r->g != NULL)
		if ((ret->g = BN_dup(r->g)) == NULL)
			goto err;
	if (r->pub_key != NULL)
		if ((ret->pub_key = BN_dup(r->pub_key)) == NULL)
			goto err;
	if (r->priv_key != NULL)
		if ((ret->priv_key = BN_dup(r->priv_key)) == NULL)
			goto err;

	return ret;

err:
	DH_free(ret);
	return NULL;
}
#endif
