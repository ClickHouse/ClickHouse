/* $OpenBSD: a_verify.c,v 1.24 2017/05/02 03:59:44 deraadt Exp $ */
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
ASN1_item_verify(const ASN1_ITEM *it, X509_ALGOR *a,
    ASN1_BIT_STRING *signature, void *asn, EVP_PKEY *pkey)
{
	EVP_MD_CTX ctx;
	unsigned char *buf_in = NULL;
	int ret = -1, inl;

	int mdnid, pknid;

	if (!pkey) {
		ASN1error(ERR_R_PASSED_NULL_PARAMETER);
		return -1;
	}

	if (signature->type == V_ASN1_BIT_STRING && signature->flags & 0x7)
	{
		ASN1error(ASN1_R_INVALID_BIT_STRING_BITS_LEFT);
		return -1;
	}

	EVP_MD_CTX_init(&ctx);

	/* Convert signature OID into digest and public key OIDs */
	if (!OBJ_find_sigid_algs(OBJ_obj2nid(a->algorithm), &mdnid, &pknid)) {
		ASN1error(ASN1_R_UNKNOWN_SIGNATURE_ALGORITHM);
		goto err;
	}
	if (mdnid == NID_undef) {
		if (!pkey->ameth || !pkey->ameth->item_verify) {
			ASN1error(ASN1_R_UNKNOWN_SIGNATURE_ALGORITHM);
			goto err;
		}
		ret = pkey->ameth->item_verify(&ctx, it, asn, a,
		    signature, pkey);
		/* Return value of 2 means carry on, anything else means we
		 * exit straight away: either a fatal error of the underlying
		 * verification routine handles all verification.
		 */
		if (ret != 2)
			goto err;
		ret = -1;
	} else {
		const EVP_MD *type;
		type = EVP_get_digestbynid(mdnid);
		if (type == NULL) {
			ASN1error(ASN1_R_UNKNOWN_MESSAGE_DIGEST_ALGORITHM);
			goto err;
		}

		/* Check public key OID matches public key type */
		if (EVP_PKEY_type(pknid) != pkey->ameth->pkey_id) {
			ASN1error(ASN1_R_WRONG_PUBLIC_KEY_TYPE);
			goto err;
		}

		if (!EVP_DigestVerifyInit(&ctx, NULL, type, NULL, pkey)) {
			ASN1error(ERR_R_EVP_LIB);
			ret = 0;
			goto err;
		}

	}

	inl = ASN1_item_i2d(asn, &buf_in, it);

	if (buf_in == NULL) {
		ASN1error(ERR_R_MALLOC_FAILURE);
		goto err;
	}

	if (!EVP_DigestVerifyUpdate(&ctx, buf_in, inl)) {
		ASN1error(ERR_R_EVP_LIB);
		ret = 0;
		goto err;
	}

	freezero(buf_in, (unsigned int)inl);

	if (EVP_DigestVerifyFinal(&ctx, signature->data,
	    (size_t)signature->length) <= 0) {
		ASN1error(ERR_R_EVP_LIB);
		ret = 0;
		goto err;
	}
	/* we don't need to zero the 'ctx' because we just checked
	 * public information */
	/* memset(&ctx,0,sizeof(ctx)); */
	ret = 1;

err:
	EVP_MD_CTX_cleanup(&ctx);
	return (ret);
}
