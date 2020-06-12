/* $OpenBSD: p5_crpt.c,v 1.18 2017/01/29 17:49:23 beck Exp $ */
/* Written by Dr Stephen N Henson (steve@openssl.org) for the OpenSSL
 * project 1999.
 */
/* ====================================================================
 * Copyright (c) 1999 The OpenSSL Project.  All rights reserved.
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
#include <stdlib.h>
#include <string.h>

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/x509.h>

/* Doesn't do anything now: Builtin PBE algorithms in static table.
 */

void
PKCS5_PBE_add(void)
{
}

int
PKCS5_PBE_keyivgen(EVP_CIPHER_CTX *cctx, const char *pass, int passlen,
    ASN1_TYPE *param, const EVP_CIPHER *cipher, const EVP_MD *md, int en_de)
{
	EVP_MD_CTX ctx;
	unsigned char md_tmp[EVP_MAX_MD_SIZE];
	unsigned char key[EVP_MAX_KEY_LENGTH], iv[EVP_MAX_IV_LENGTH];
	int i;
	PBEPARAM *pbe;
	int saltlen, iter;
	unsigned char *salt;
	const unsigned char *pbuf;
	int mdsize;
	int rv = 0;

	/* Extract useful info from parameter */
	if (param == NULL || param->type != V_ASN1_SEQUENCE ||
	    param->value.sequence == NULL) {
		EVPerror(EVP_R_DECODE_ERROR);
		return 0;
	}

	mdsize = EVP_MD_size(md);
	if (mdsize < 0)
		return 0;

	pbuf = param->value.sequence->data;
	if (!(pbe = d2i_PBEPARAM(NULL, &pbuf, param->value.sequence->length))) {
		EVPerror(EVP_R_DECODE_ERROR);
		return 0;
	}

	if (!pbe->iter)
		iter = 1;
	else if ((iter = ASN1_INTEGER_get(pbe->iter)) <= 0) {
		EVPerror(EVP_R_UNSUPORTED_NUMBER_OF_ROUNDS);
		return 0;
	}
	salt = pbe->salt->data;
	saltlen = pbe->salt->length;

	if (!pass)
		passlen = 0;
	else if (passlen == -1)
		passlen = strlen(pass);

	EVP_MD_CTX_init(&ctx);

	if (!EVP_DigestInit_ex(&ctx, md, NULL))
		goto err;
	if (!EVP_DigestUpdate(&ctx, pass, passlen))
		goto err;
	if (!EVP_DigestUpdate(&ctx, salt, saltlen))
		goto err;
	if (!EVP_DigestFinal_ex(&ctx, md_tmp, NULL))
		goto err;
	for (i = 1; i < iter; i++) {
		if (!EVP_DigestInit_ex(&ctx, md, NULL))
			goto err;
		if (!EVP_DigestUpdate(&ctx, md_tmp, mdsize))
			goto err;
		if (!EVP_DigestFinal_ex (&ctx, md_tmp, NULL))
			goto err;
	}
	if ((size_t)EVP_CIPHER_key_length(cipher) > sizeof(md_tmp)) {
		EVPerror(EVP_R_BAD_KEY_LENGTH);
		goto err;
	}
	memcpy(key, md_tmp, EVP_CIPHER_key_length(cipher));
	if ((size_t)EVP_CIPHER_iv_length(cipher) > 16) {
		EVPerror(EVP_R_IV_TOO_LARGE);
		goto err;
	}
	memcpy(iv, md_tmp + (16 - EVP_CIPHER_iv_length(cipher)),
	    EVP_CIPHER_iv_length(cipher));
	if (!EVP_CipherInit_ex(cctx, cipher, NULL, key, iv, en_de))
		goto err;
	explicit_bzero(md_tmp, EVP_MAX_MD_SIZE);
	explicit_bzero(key, EVP_MAX_KEY_LENGTH);
	explicit_bzero(iv, EVP_MAX_IV_LENGTH);
	rv = 1;
err:
	EVP_MD_CTX_cleanup(&ctx);
	PBEPARAM_free(pbe);
	return rv;
}
