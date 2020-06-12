/* $OpenBSD: x_algor.c,v 1.21 2015/07/24 15:09:52 jsing Exp $ */
/* Written by Dr Stephen N Henson (steve@openssl.org) for the OpenSSL
 * project 2000.
 */
/* ====================================================================
 * Copyright (c) 2000 The OpenSSL Project.  All rights reserved.
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

#include <stddef.h>
#include <openssl/x509.h>
#include <openssl/asn1.h>
#include <openssl/asn1t.h>

static const ASN1_TEMPLATE X509_ALGOR_seq_tt[] = {
	{
		.offset = offsetof(X509_ALGOR, algorithm),
		.field_name = "algorithm",
		.item = &ASN1_OBJECT_it,
	},
	{
		.flags = ASN1_TFLG_OPTIONAL,
		.offset = offsetof(X509_ALGOR, parameter),
		.field_name = "parameter",
		.item = &ASN1_ANY_it,
	},
};

const ASN1_ITEM X509_ALGOR_it = {
	.itype = ASN1_ITYPE_SEQUENCE,
	.utype = V_ASN1_SEQUENCE,
	.templates = X509_ALGOR_seq_tt,
	.tcount = sizeof(X509_ALGOR_seq_tt) / sizeof(ASN1_TEMPLATE),
	.size = sizeof(X509_ALGOR),
	.sname = "X509_ALGOR",
};

static const ASN1_TEMPLATE X509_ALGORS_item_tt = {
	.flags = ASN1_TFLG_SEQUENCE_OF,
	.tag = 0,
	.offset = 0,
	.field_name = "algorithms",
	.item = &X509_ALGOR_it,
};

const ASN1_ITEM X509_ALGORS_it = {
	.itype = ASN1_ITYPE_PRIMITIVE,
	.utype = -1,
	.templates = &X509_ALGORS_item_tt,
	.tcount = 0,
	.funcs = NULL,
	.size = 0,
	.sname = "X509_ALGORS",
};


X509_ALGOR *
d2i_X509_ALGOR(X509_ALGOR **a, const unsigned char **in, long len)
{
	return (X509_ALGOR *)ASN1_item_d2i((ASN1_VALUE **)a, in, len,
	    &X509_ALGOR_it);
}

int
i2d_X509_ALGOR(X509_ALGOR *a, unsigned char **out)
{
	return ASN1_item_i2d((ASN1_VALUE *)a, out, &X509_ALGOR_it);
}

X509_ALGOR *
X509_ALGOR_new(void)
{
	return (X509_ALGOR *)ASN1_item_new(&X509_ALGOR_it);
}

void
X509_ALGOR_free(X509_ALGOR *a)
{
	ASN1_item_free((ASN1_VALUE *)a, &X509_ALGOR_it);
}

X509_ALGORS *
d2i_X509_ALGORS(X509_ALGORS **a, const unsigned char **in, long len)
{
	return (X509_ALGORS *)ASN1_item_d2i((ASN1_VALUE **)a, in, len,
	    &X509_ALGORS_it);
}

int
i2d_X509_ALGORS(X509_ALGORS *a, unsigned char **out)
{
	return ASN1_item_i2d((ASN1_VALUE *)a, out, &X509_ALGORS_it);
}

X509_ALGOR *
X509_ALGOR_dup(X509_ALGOR *x)
{
	return ASN1_item_dup(&X509_ALGOR_it, x);
}

int
X509_ALGOR_set0(X509_ALGOR *alg, ASN1_OBJECT *aobj, int ptype, void *pval)
{
	if (!alg)
		return 0;
	if (ptype != V_ASN1_UNDEF) {
		if (alg->parameter == NULL)
			alg->parameter = ASN1_TYPE_new();
		if (alg->parameter == NULL)
			return 0;
	}
	if (alg) {
		if (alg->algorithm)
			ASN1_OBJECT_free(alg->algorithm);
		alg->algorithm = aobj;
	}
	if (ptype == 0)
		return 1;
	if (ptype == V_ASN1_UNDEF) {
		if (alg->parameter) {
			ASN1_TYPE_free(alg->parameter);
			alg->parameter = NULL;
		}
	} else
		ASN1_TYPE_set(alg->parameter, ptype, pval);
	return 1;
}

void
X509_ALGOR_get0(ASN1_OBJECT **paobj, int *pptype, void **ppval,
    X509_ALGOR *algor)
{
	if (paobj)
		*paobj = algor->algorithm;
	if (pptype) {
		if (algor->parameter == NULL) {
			*pptype = V_ASN1_UNDEF;
			return;
		} else
			*pptype = algor->parameter->type;
		if (ppval)
			*ppval = algor->parameter->value.ptr;
	}
}

/* Set up an X509_ALGOR DigestAlgorithmIdentifier from an EVP_MD */

void
X509_ALGOR_set_md(X509_ALGOR *alg, const EVP_MD *md)
{
	int param_type;

	if (md->flags & EVP_MD_FLAG_DIGALGID_ABSENT)
		param_type = V_ASN1_UNDEF;
	else
		param_type = V_ASN1_NULL;

	X509_ALGOR_set0(alg, OBJ_nid2obj(EVP_MD_type(md)), param_type, NULL);
}

/* Returns 0 if they are equal, != 0 otherwise. */
int
X509_ALGOR_cmp(const X509_ALGOR *a, const X509_ALGOR *b)
{
	int rv = OBJ_cmp(a->algorithm, b->algorithm);
	if (!rv) {
		if (!a->parameter && !b->parameter)
			rv = 0;
		else
			rv = ASN1_TYPE_cmp(a->parameter, b->parameter);
	}
	return(rv);
}
