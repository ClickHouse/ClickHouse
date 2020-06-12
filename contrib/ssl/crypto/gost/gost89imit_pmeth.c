/* $OpenBSD: gost89imit_pmeth.c,v 1.4 2017/01/29 17:49:23 beck Exp $ */
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
#include <openssl/x509v3.h> /* For string_to_hex */

#include "evp_locl.h"
#include "gost_locl.h"

struct gost_mac_pmeth_data {
	EVP_MD *md;
	unsigned char key[32];
	unsigned key_set :1;
};

static int
pkey_gost_mac_init(EVP_PKEY_CTX *ctx)
{
	struct gost_mac_pmeth_data *data;

	data = calloc(1, sizeof(struct gost_mac_pmeth_data));
	if (data == NULL)
		return 0;
	EVP_PKEY_CTX_set_data(ctx, data);
	return 1;
}

static void
pkey_gost_mac_cleanup(EVP_PKEY_CTX *ctx)
{
	struct gost_mac_pmeth_data *data = EVP_PKEY_CTX_get_data(ctx);
	free(data);
}

static int
pkey_gost_mac_copy(EVP_PKEY_CTX *dst, EVP_PKEY_CTX *src)
{
	struct gost_mac_pmeth_data *dst_data, *src_data;

	if (pkey_gost_mac_init(dst) == 0)
		return 0;

	src_data = EVP_PKEY_CTX_get_data(src);
	dst_data = EVP_PKEY_CTX_get_data(dst);

	*dst_data = *src_data;

	return 1;
}

static int
pkey_gost_mac_keygen(EVP_PKEY_CTX *ctx, EVP_PKEY *pkey)
{
	struct gost_mac_pmeth_data *data = EVP_PKEY_CTX_get_data(ctx);
	unsigned char *keydata;

	if (!data->key_set) {
		GOSTerror(GOST_R_MAC_KEY_NOT_SET);
		return 0;
	}

	keydata = malloc(32);
	if (keydata == NULL) {
		GOSTerror(ERR_R_MALLOC_FAILURE);
		return 0;
	}
	memcpy(keydata, data->key, 32);
	EVP_PKEY_assign(pkey, NID_id_Gost28147_89_MAC, keydata);

	return 1;
}

static int
pkey_gost_mac_ctrl(EVP_PKEY_CTX *ctx, int type, int p1, void *p2)
{
	struct gost_mac_pmeth_data *data = EVP_PKEY_CTX_get_data(ctx);

	switch (type) {
	case EVP_PKEY_CTRL_MD:
		if (EVP_MD_type(p2) != NID_id_Gost28147_89_MAC) {
			GOSTerror(GOST_R_INVALID_DIGEST_TYPE);
			return 0;
		}
		data->md = p2;
		return 1;

	case EVP_PKEY_CTRL_SET_MAC_KEY:
		if (p1 != 32) {
			GOSTerror(GOST_R_INVALID_MAC_KEY_LENGTH);
			return 0;
		}

		memcpy(data->key, p2, 32);
		data->key_set = 1;
		return 1;

	case EVP_PKEY_CTRL_DIGESTINIT:
	    {
		EVP_MD_CTX *mctx = p2;
		void *key;

		if (!data->key_set) {
			EVP_PKEY *pkey = EVP_PKEY_CTX_get0_pkey(ctx);
			if (pkey == NULL) {
				GOSTerror(GOST_R_MAC_KEY_NOT_SET);
				return 0;
			}
			key = EVP_PKEY_get0(pkey);
			if (key == NULL) {
				GOSTerror(GOST_R_MAC_KEY_NOT_SET);
				return 0;
			}
		} else {
			key = &(data->key);
		}
		if (mctx->digest->md_ctrl == NULL)
			return 0;
		return mctx->digest->md_ctrl(mctx, EVP_MD_CTRL_SET_KEY, 32 * 8,
		    key);
	    }

	}

	return -2;
}

static int
pkey_gost_mac_ctrl_str(EVP_PKEY_CTX *ctx, const char *type, const char *value)
{
	if (value == NULL)
		return 0;
	if (strcmp(type, "key") == 0) {
		void *p = (void *)value;
		return pkey_gost_mac_ctrl(ctx, EVP_PKEY_CTRL_SET_MAC_KEY,
		    strlen(value), p);
	}
	if (strcmp(type, "hexkey") == 0) {
		unsigned char *key;
		int r;
		long keylen;

		key = string_to_hex(value, &keylen);
		if (key == NULL)
			return 0;
		r = pkey_gost_mac_ctrl(ctx, EVP_PKEY_CTRL_SET_MAC_KEY, keylen,
		    key);
		free(key);
		return r;
	}
	return -2;
}

static int
pkey_gost_mac_signctx_init(EVP_PKEY_CTX *ctx, EVP_MD_CTX *mctx)
{
	return 1;
}

static int
pkey_gost_mac_signctx(EVP_PKEY_CTX *ctx, unsigned char *sig, size_t *siglen,
    EVP_MD_CTX *mctx)
{
	/* for platforms where sizeof(int) != sizeof(size_t)*/
	unsigned int tmpsiglen = *siglen;
	int ret;

	if (sig == NULL) {
		*siglen = 4;
		return 1;
	}

	ret = EVP_DigestFinal_ex(mctx, sig, &tmpsiglen);
	*siglen = tmpsiglen;
	return ret;
}

const EVP_PKEY_METHOD gostimit_pkey_meth = {
	.pkey_id = EVP_PKEY_GOSTIMIT,

	.init = pkey_gost_mac_init,
	.cleanup = pkey_gost_mac_cleanup,
	.copy = pkey_gost_mac_copy,

	.keygen = pkey_gost_mac_keygen,

	.signctx_init = pkey_gost_mac_signctx_init,
	.signctx = pkey_gost_mac_signctx,

	.ctrl = pkey_gost_mac_ctrl,
	.ctrl_str = pkey_gost_mac_ctrl_str,
};

#endif
