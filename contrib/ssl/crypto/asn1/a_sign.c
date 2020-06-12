/* $OpenBSD: a_sign.c,v 1.23 2017/05/02 03:59:44 deraadt Exp $ */
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
/* ====================================================================
 * Copyright (c) 1998-2003 The OpenSSL Project.  All rights reserved.
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

#include <sys/types.h>

#include <stdio.h>
#include <string.h>
#include <time.h>

#include <openssl/bn.h>
#include <openssl/buffer.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/objects.h>
#include <openssl/x509.h>

#include "asn1_locl.h"

int
ASN1_item_sign(const ASN1_ITEM *it, X509_ALGOR *algor1, X509_ALGOR *algor2,
    ASN1_BIT_STRING *signature, void *asn, EVP_PKEY *pkey, const EVP_MD *type)
{
	EVP_MD_CTX ctx;
	EVP_MD_CTX_init(&ctx);
	if (!EVP_DigestSignInit(&ctx, NULL, type, NULL, pkey)) {
		EVP_MD_CTX_cleanup(&ctx);
		return 0;
	}
	return ASN1_item_sign_ctx(it, algor1, algor2, signature, asn, &ctx);
}


int
ASN1_item_sign_ctx(const ASN1_ITEM *it, X509_ALGOR *algor1, X509_ALGOR *algor2,
    ASN1_BIT_STRING *signature, void *asn, EVP_MD_CTX *ctx)
{
	const EVP_MD *type;
	EVP_PKEY *pkey;
	unsigned char *buf_in = NULL, *buf_out = NULL;
	size_t inl = 0, outl = 0, outll = 0;
	int signid, paramtype;
	int rv;

	type = EVP_MD_CTX_md(ctx);
	pkey = EVP_PKEY_CTX_get0_pkey(ctx->pctx);

	if (!type || !pkey) {
		ASN1error(ASN1_R_CONTEXT_NOT_INITIALISED);
		return 0;
	}

	if (pkey->ameth->item_sign) {
		rv = pkey->ameth->item_sign(ctx, it, asn, algor1, algor2,
		    signature);
		if (rv == 1)
			outl = signature->length;
		/* Return value meanings:
		 * <=0: error.
		 *   1: method does everything.
		 *   2: carry on as normal.
		 *   3: ASN1 method sets algorithm identifiers: just sign.
		 */
		if (rv <= 0)
			ASN1error(ERR_R_EVP_LIB);
		if (rv <= 1)
			goto err;
	} else
		rv = 2;

	if (rv == 2) {
		if (type->flags & EVP_MD_FLAG_PKEY_METHOD_SIGNATURE) {
			if (!pkey->ameth ||
			    !OBJ_find_sigid_by_algs(&signid,
				EVP_MD_nid(type), pkey->ameth->pkey_id)) {
				ASN1error(ASN1_R_DIGEST_AND_KEY_TYPE_NOT_SUPPORTED);
				return 0;
			}
		} else
			signid = type->pkey_type;

		if (pkey->ameth->pkey_flags & ASN1_PKEY_SIGPARAM_NULL)
			paramtype = V_ASN1_NULL;
		else
			paramtype = V_ASN1_UNDEF;

		if (algor1)
			X509_ALGOR_set0(algor1,
			    OBJ_nid2obj(signid), paramtype, NULL);
		if (algor2)
			X509_ALGOR_set0(algor2,
			    OBJ_nid2obj(signid), paramtype, NULL);

	}

	inl = ASN1_item_i2d(asn, &buf_in, it);
	outll = outl = EVP_PKEY_size(pkey);
	buf_out = malloc(outl);
	if ((buf_in == NULL) || (buf_out == NULL)) {
		outl = 0;
		ASN1error(ERR_R_MALLOC_FAILURE);
		goto err;
	}

	if (!EVP_DigestSignUpdate(ctx, buf_in, inl) ||
	    !EVP_DigestSignFinal(ctx, buf_out, &outl)) {
		outl = 0;
		ASN1error(ERR_R_EVP_LIB);
		goto err;
	}
	free(signature->data);
	signature->data = buf_out;
	buf_out = NULL;
	signature->length = outl;
	/* In the interests of compatibility, I'll make sure that
	 * the bit string has a 'not-used bits' value of 0
	 */
	signature->flags &= ~(ASN1_STRING_FLAG_BITS_LEFT|0x07);
	signature->flags |= ASN1_STRING_FLAG_BITS_LEFT;

err:
	EVP_MD_CTX_cleanup(ctx);
	freezero((char *)buf_in, inl);
	freezero((char *)buf_out, outll);
	return (outl);
}
