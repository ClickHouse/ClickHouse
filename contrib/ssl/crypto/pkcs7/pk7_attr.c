/* $OpenBSD: pk7_attr.c,v 1.12 2017/01/29 17:49:23 beck Exp $ */
/* Written by Dr Stephen N Henson (steve@openssl.org) for the OpenSSL
 * project 2001.
 */
/* ====================================================================
 * Copyright (c) 2001-2004 The OpenSSL Project.  All rights reserved.
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
#include <openssl/bio.h>
#include <openssl/asn1.h>
#include <openssl/asn1t.h>
#include <openssl/pem.h>
#include <openssl/pkcs7.h>
#include <openssl/x509.h>
#include <openssl/err.h>

int
PKCS7_add_attrib_smimecap(PKCS7_SIGNER_INFO *si, STACK_OF(X509_ALGOR) *cap)
{
	ASN1_STRING *seq;
	if (!(seq = ASN1_STRING_new())) {
		PKCS7error(ERR_R_MALLOC_FAILURE);
		return 0;
	}
	seq->length = ASN1_item_i2d((ASN1_VALUE *)cap, &seq->data,
	    &X509_ALGORS_it);
	return PKCS7_add_signed_attribute(si, NID_SMIMECapabilities,
	    V_ASN1_SEQUENCE, seq);
}

STACK_OF(X509_ALGOR) *
PKCS7_get_smimecap(PKCS7_SIGNER_INFO *si)
{
	ASN1_TYPE *cap;
	const unsigned char *p;

	cap = PKCS7_get_signed_attribute(si, NID_SMIMECapabilities);
	if (!cap || (cap->type != V_ASN1_SEQUENCE))
		return NULL;
	p = cap->value.sequence->data;
	return (STACK_OF(X509_ALGOR) *)
	ASN1_item_d2i(NULL, &p, cap->value.sequence->length,
	    &X509_ALGORS_it);
}

/* Basic smime-capabilities OID and optional integer arg */
int
PKCS7_simple_smimecap(STACK_OF(X509_ALGOR) *sk, int nid, int arg)
{
	X509_ALGOR *alg;

	if (!(alg = X509_ALGOR_new())) {
		PKCS7error(ERR_R_MALLOC_FAILURE);
		return 0;
	}
	ASN1_OBJECT_free(alg->algorithm);
	alg->algorithm = OBJ_nid2obj(nid);
	if (arg > 0) {
		ASN1_INTEGER *nbit;

		if (!(alg->parameter = ASN1_TYPE_new()))
			goto err;
		if (!(nbit = ASN1_INTEGER_new()))
			goto err;
		if (!ASN1_INTEGER_set(nbit, arg)) {
			ASN1_INTEGER_free(nbit);
			goto err;
		}
		alg->parameter->value.integer = nbit;
		alg->parameter->type = V_ASN1_INTEGER;
	}
	if (sk_X509_ALGOR_push(sk, alg) == 0)
		goto err;
	return 1;

err:
	PKCS7error(ERR_R_MALLOC_FAILURE);
	X509_ALGOR_free(alg);
	return 0;
}

int
PKCS7_add_attrib_content_type(PKCS7_SIGNER_INFO *si, ASN1_OBJECT *coid)
{
	if (PKCS7_get_signed_attribute(si, NID_pkcs9_contentType))
		return 0;
	if (!coid)
		coid = OBJ_nid2obj(NID_pkcs7_data);
	return PKCS7_add_signed_attribute(si, NID_pkcs9_contentType,
	    V_ASN1_OBJECT, coid);
}

int
PKCS7_add0_attrib_signing_time(PKCS7_SIGNER_INFO *si, ASN1_TIME *t)
{
	if (!t && !(t = X509_gmtime_adj(NULL, 0))) {
		PKCS7error(ERR_R_MALLOC_FAILURE);
		return 0;
	}
	return PKCS7_add_signed_attribute(si, NID_pkcs9_signingTime,
	    V_ASN1_UTCTIME, t);
}

int
PKCS7_add1_attrib_digest(PKCS7_SIGNER_INFO *si, const unsigned char *md,
    int mdlen)
{
	ASN1_OCTET_STRING *os;

	os = ASN1_OCTET_STRING_new();
	if (!os)
		return 0;
	if (!ASN1_STRING_set(os, md, mdlen) ||
	    !PKCS7_add_signed_attribute(si, NID_pkcs9_messageDigest,
	    V_ASN1_OCTET_STRING, os)) {
		ASN1_OCTET_STRING_free(os);
		return 0;
	}
	return 1;
}
