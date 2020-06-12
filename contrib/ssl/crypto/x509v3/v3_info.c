/* $OpenBSD: v3_info.c,v 1.25 2017/01/29 17:49:23 beck Exp $ */
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
#include <string.h>

#include <openssl/asn1.h>
#include <openssl/asn1t.h>
#include <openssl/conf.h>
#include <openssl/err.h>
#include <openssl/x509v3.h>

static STACK_OF(CONF_VALUE) *i2v_AUTHORITY_INFO_ACCESS(
    X509V3_EXT_METHOD *method, AUTHORITY_INFO_ACCESS *ainfo,
    STACK_OF(CONF_VALUE) *ret);
static AUTHORITY_INFO_ACCESS *v2i_AUTHORITY_INFO_ACCESS(
    X509V3_EXT_METHOD *method, X509V3_CTX *ctx, STACK_OF(CONF_VALUE) *nval);

const X509V3_EXT_METHOD v3_info = {
	.ext_nid = NID_info_access,
	.ext_flags = X509V3_EXT_MULTILINE,
	.it = &AUTHORITY_INFO_ACCESS_it,
	.ext_new = NULL,
	.ext_free = NULL,
	.d2i = NULL,
	.i2d = NULL,
	.i2s = NULL,
	.s2i = NULL,
	.i2v = (X509V3_EXT_I2V)i2v_AUTHORITY_INFO_ACCESS,
	.v2i = (X509V3_EXT_V2I)v2i_AUTHORITY_INFO_ACCESS,
	.i2r = NULL,
	.r2i = NULL,
	.usr_data = NULL,
};

const X509V3_EXT_METHOD v3_sinfo = {
	.ext_nid = NID_sinfo_access,
	.ext_flags = X509V3_EXT_MULTILINE,
	.it = &AUTHORITY_INFO_ACCESS_it,
	.ext_new = NULL,
	.ext_free = NULL,
	.d2i = NULL,
	.i2d = NULL,
	.i2s = NULL,
	.s2i = NULL,
	.i2v = (X509V3_EXT_I2V)i2v_AUTHORITY_INFO_ACCESS,
	.v2i = (X509V3_EXT_V2I)v2i_AUTHORITY_INFO_ACCESS,
	.i2r = NULL,
	.r2i = NULL,
	.usr_data = NULL,
};

static const ASN1_TEMPLATE ACCESS_DESCRIPTION_seq_tt[] = {
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(ACCESS_DESCRIPTION, method),
		.field_name = "method",
		.item = &ASN1_OBJECT_it,
	},
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(ACCESS_DESCRIPTION, location),
		.field_name = "location",
		.item = &GENERAL_NAME_it,
	},
};

const ASN1_ITEM ACCESS_DESCRIPTION_it = {
	.itype = ASN1_ITYPE_SEQUENCE,
	.utype = V_ASN1_SEQUENCE,
	.templates = ACCESS_DESCRIPTION_seq_tt,
	.tcount = sizeof(ACCESS_DESCRIPTION_seq_tt) / sizeof(ASN1_TEMPLATE),
	.funcs = NULL,
	.size = sizeof(ACCESS_DESCRIPTION),
	.sname = "ACCESS_DESCRIPTION",
};


ACCESS_DESCRIPTION *
d2i_ACCESS_DESCRIPTION(ACCESS_DESCRIPTION **a, const unsigned char **in, long len)
{
	return (ACCESS_DESCRIPTION *)ASN1_item_d2i((ASN1_VALUE **)a, in, len,
	    &ACCESS_DESCRIPTION_it);
}

int
i2d_ACCESS_DESCRIPTION(ACCESS_DESCRIPTION *a, unsigned char **out)
{
	return ASN1_item_i2d((ASN1_VALUE *)a, out, &ACCESS_DESCRIPTION_it);
}

ACCESS_DESCRIPTION *
ACCESS_DESCRIPTION_new(void)
{
	return (ACCESS_DESCRIPTION *)ASN1_item_new(&ACCESS_DESCRIPTION_it);
}

void
ACCESS_DESCRIPTION_free(ACCESS_DESCRIPTION *a)
{
	ASN1_item_free((ASN1_VALUE *)a, &ACCESS_DESCRIPTION_it);
}

static const ASN1_TEMPLATE AUTHORITY_INFO_ACCESS_item_tt = {
	.flags = ASN1_TFLG_SEQUENCE_OF,
	.tag = 0,
	.offset = 0,
	.field_name = "GeneralNames",
	.item = &ACCESS_DESCRIPTION_it,
};

const ASN1_ITEM AUTHORITY_INFO_ACCESS_it = {
	.itype = ASN1_ITYPE_PRIMITIVE,
	.utype = -1,
	.templates = &AUTHORITY_INFO_ACCESS_item_tt,
	.tcount = 0,
	.funcs = NULL,
	.size = 0,
	.sname = "AUTHORITY_INFO_ACCESS",
};


AUTHORITY_INFO_ACCESS *
d2i_AUTHORITY_INFO_ACCESS(AUTHORITY_INFO_ACCESS **a, const unsigned char **in, long len)
{
	return (AUTHORITY_INFO_ACCESS *)ASN1_item_d2i((ASN1_VALUE **)a, in, len,
	    &AUTHORITY_INFO_ACCESS_it);
}

int
i2d_AUTHORITY_INFO_ACCESS(AUTHORITY_INFO_ACCESS *a, unsigned char **out)
{
	return ASN1_item_i2d((ASN1_VALUE *)a, out, &AUTHORITY_INFO_ACCESS_it);
}

AUTHORITY_INFO_ACCESS *
AUTHORITY_INFO_ACCESS_new(void)
{
	return (AUTHORITY_INFO_ACCESS *)ASN1_item_new(&AUTHORITY_INFO_ACCESS_it);
}

void
AUTHORITY_INFO_ACCESS_free(AUTHORITY_INFO_ACCESS *a)
{
	ASN1_item_free((ASN1_VALUE *)a, &AUTHORITY_INFO_ACCESS_it);
}

static STACK_OF(CONF_VALUE) *
i2v_AUTHORITY_INFO_ACCESS(X509V3_EXT_METHOD *method,
    AUTHORITY_INFO_ACCESS *ainfo, STACK_OF(CONF_VALUE) *ret)
{
	ACCESS_DESCRIPTION *desc;
	int i, nlen;
	char objtmp[80], *ntmp;
	CONF_VALUE *vtmp;

	for (i = 0; i < sk_ACCESS_DESCRIPTION_num(ainfo); i++) {
		desc = sk_ACCESS_DESCRIPTION_value(ainfo, i);
		ret = i2v_GENERAL_NAME(method, desc->location, ret);
		if (!ret)
			break;
		vtmp = sk_CONF_VALUE_value(ret, i);
		i2t_ASN1_OBJECT(objtmp, sizeof objtmp, desc->method);
		nlen = strlen(objtmp) + strlen(vtmp->name) + 5;
		ntmp = malloc(nlen);
		if (!ntmp) {
			X509V3error(ERR_R_MALLOC_FAILURE);
			return NULL;
		}
		strlcpy(ntmp, objtmp, nlen);
		strlcat(ntmp, " - ", nlen);
		strlcat(ntmp, vtmp->name, nlen);
		free(vtmp->name);
		vtmp->name = ntmp;

	}
	if (!ret)
		return sk_CONF_VALUE_new_null();
	return ret;
}

static AUTHORITY_INFO_ACCESS *
v2i_AUTHORITY_INFO_ACCESS(X509V3_EXT_METHOD *method, X509V3_CTX *ctx,
    STACK_OF(CONF_VALUE) *nval)
{
	AUTHORITY_INFO_ACCESS *ainfo = NULL;
	CONF_VALUE *cnf, ctmp;
	ACCESS_DESCRIPTION *acc;
	int i, objlen;
	char *objtmp, *ptmp;

	if (!(ainfo = sk_ACCESS_DESCRIPTION_new_null())) {
		X509V3error(ERR_R_MALLOC_FAILURE);
		return NULL;
	}
	for (i = 0; i < sk_CONF_VALUE_num(nval); i++) {
		cnf = sk_CONF_VALUE_value(nval, i);
		if ((acc = ACCESS_DESCRIPTION_new()) == NULL) {
			X509V3error(ERR_R_MALLOC_FAILURE);
			goto err;
		}
		if (sk_ACCESS_DESCRIPTION_push(ainfo, acc) == 0) {
			ACCESS_DESCRIPTION_free(acc);
			X509V3error(ERR_R_MALLOC_FAILURE);
			goto err;
		}
		ptmp = strchr(cnf->name, ';');
		if (!ptmp) {
			X509V3error(X509V3_R_INVALID_SYNTAX);
			goto err;
		}
		objlen = ptmp - cnf->name;
		ctmp.name = ptmp + 1;
		ctmp.value = cnf->value;
		if (!v2i_GENERAL_NAME_ex(acc->location, method, ctx, &ctmp, 0))
			goto err;
		if (!(objtmp = malloc(objlen + 1))) {
			X509V3error(ERR_R_MALLOC_FAILURE);
			goto err;
		}
		strlcpy(objtmp, cnf->name, objlen + 1);
		acc->method = OBJ_txt2obj(objtmp, 0);
		if (!acc->method) {
			X509V3error(X509V3_R_BAD_OBJECT);
			ERR_asprintf_error_data("value=%s", objtmp);
			free(objtmp);
			goto err;
		}
		free(objtmp);
	}
	return ainfo;

err:
	sk_ACCESS_DESCRIPTION_pop_free(ainfo, ACCESS_DESCRIPTION_free);
	return NULL;
}

int
i2a_ACCESS_DESCRIPTION(BIO *bp, ACCESS_DESCRIPTION* a)
{
	i2a_ASN1_OBJECT(bp, a->method);
	return 2;
}
