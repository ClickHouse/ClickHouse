/* $OpenBSD: v3_sxnet.c,v 1.19 2017/01/29 17:49:23 beck Exp $ */
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

/* Support for Thawte strong extranet extension */

#define SXNET_TEST

static int sxnet_i2r(X509V3_EXT_METHOD *method, SXNET *sx, BIO *out,
    int indent);
#ifdef SXNET_TEST
static SXNET * sxnet_v2i(X509V3_EXT_METHOD *method, X509V3_CTX *ctx,
    STACK_OF(CONF_VALUE) *nval);
#endif

const X509V3_EXT_METHOD v3_sxnet = {
	.ext_nid = NID_sxnet,
	.ext_flags = X509V3_EXT_MULTILINE,
	.it = &SXNET_it,
	.ext_new = NULL,
	.ext_free = NULL,
	.d2i = NULL,
	.i2d = NULL,
	.i2s = NULL,
	.s2i = NULL,
	.i2v = NULL,
#ifdef SXNET_TEST
	.v2i = (X509V3_EXT_V2I)sxnet_v2i,
#else
	.v2i = NULL,
#endif
	.i2r = (X509V3_EXT_I2R)sxnet_i2r,
	.r2i = NULL,
	.usr_data = NULL,
};

static const ASN1_TEMPLATE SXNETID_seq_tt[] = {
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(SXNETID, zone),
		.field_name = "zone",
		.item = &ASN1_INTEGER_it,
	},
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(SXNETID, user),
		.field_name = "user",
		.item = &ASN1_OCTET_STRING_it,
	},
};

const ASN1_ITEM SXNETID_it = {
	.itype = ASN1_ITYPE_SEQUENCE,
	.utype = V_ASN1_SEQUENCE,
	.templates = SXNETID_seq_tt,
	.tcount = sizeof(SXNETID_seq_tt) / sizeof(ASN1_TEMPLATE),
	.funcs = NULL,
	.size = sizeof(SXNETID),
	.sname = "SXNETID",
};


SXNETID *
d2i_SXNETID(SXNETID **a, const unsigned char **in, long len)
{
	return (SXNETID *)ASN1_item_d2i((ASN1_VALUE **)a, in, len,
	    &SXNETID_it);
}

int
i2d_SXNETID(SXNETID *a, unsigned char **out)
{
	return ASN1_item_i2d((ASN1_VALUE *)a, out, &SXNETID_it);
}

SXNETID *
SXNETID_new(void)
{
	return (SXNETID *)ASN1_item_new(&SXNETID_it);
}

void
SXNETID_free(SXNETID *a)
{
	ASN1_item_free((ASN1_VALUE *)a, &SXNETID_it);
}

static const ASN1_TEMPLATE SXNET_seq_tt[] = {
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(SXNET, version),
		.field_name = "version",
		.item = &ASN1_INTEGER_it,
	},
	{
		.flags = ASN1_TFLG_SEQUENCE_OF,
		.tag = 0,
		.offset = offsetof(SXNET, ids),
		.field_name = "ids",
		.item = &SXNETID_it,
	},
};

const ASN1_ITEM SXNET_it = {
	.itype = ASN1_ITYPE_SEQUENCE,
	.utype = V_ASN1_SEQUENCE,
	.templates = SXNET_seq_tt,
	.tcount = sizeof(SXNET_seq_tt) / sizeof(ASN1_TEMPLATE),
	.funcs = NULL,
	.size = sizeof(SXNET),
	.sname = "SXNET",
};


SXNET *
d2i_SXNET(SXNET **a, const unsigned char **in, long len)
{
	return (SXNET *)ASN1_item_d2i((ASN1_VALUE **)a, in, len,
	    &SXNET_it);
}

int
i2d_SXNET(SXNET *a, unsigned char **out)
{
	return ASN1_item_i2d((ASN1_VALUE *)a, out, &SXNET_it);
}

SXNET *
SXNET_new(void)
{
	return (SXNET *)ASN1_item_new(&SXNET_it);
}

void
SXNET_free(SXNET *a)
{
	ASN1_item_free((ASN1_VALUE *)a, &SXNET_it);
}

static int
sxnet_i2r(X509V3_EXT_METHOD *method, SXNET *sx, BIO *out, int indent)
{
	long v;
	char *tmp;
	SXNETID *id;
	int i;

	v = ASN1_INTEGER_get(sx->version);
	BIO_printf(out, "%*sVersion: %ld (0x%lX)", indent, "", v + 1, v);
	for (i = 0; i < sk_SXNETID_num(sx->ids); i++) {
		id = sk_SXNETID_value(sx->ids, i);
		tmp = i2s_ASN1_INTEGER(NULL, id->zone);
		BIO_printf(out, "\n%*sZone: %s, User: ", indent, "", tmp);
		free(tmp);
		ASN1_STRING_print(out, id->user);
	}
	return 1;
}

#ifdef SXNET_TEST

/* NBB: this is used for testing only. It should *not* be used for anything
 * else because it will just take static IDs from the configuration file and
 * they should really be separate values for each user.
 */

static SXNET *
sxnet_v2i(X509V3_EXT_METHOD *method, X509V3_CTX *ctx,
    STACK_OF(CONF_VALUE) *nval)
{
	CONF_VALUE *cnf;
	SXNET *sx = NULL;
	int i;

	for (i = 0; i < sk_CONF_VALUE_num(nval); i++) {
		cnf = sk_CONF_VALUE_value(nval, i);
		if (!SXNET_add_id_asc(&sx, cnf->name, cnf->value, -1))
			return NULL;
	}
	return sx;
}

#endif

/* Strong Extranet utility functions */

/* Add an id given the zone as an ASCII number */

int
SXNET_add_id_asc(SXNET **psx, char *zone, char *user, int userlen)
{
	ASN1_INTEGER *izone = NULL;

	if (!(izone = s2i_ASN1_INTEGER(NULL, zone))) {
		X509V3error(X509V3_R_ERROR_CONVERTING_ZONE);
		return 0;
	}
	return SXNET_add_id_INTEGER(psx, izone, user, userlen);
}

/* Add an id given the zone as an unsigned long */

int
SXNET_add_id_ulong(SXNET **psx, unsigned long lzone, char *user, int userlen)
{
	ASN1_INTEGER *izone = NULL;

	if (!(izone = ASN1_INTEGER_new()) ||
	    !ASN1_INTEGER_set(izone, lzone)) {
		X509V3error(ERR_R_MALLOC_FAILURE);
		ASN1_INTEGER_free(izone);
		return 0;
	}
	return SXNET_add_id_INTEGER(psx, izone, user, userlen);
}

/* Add an id given the zone as an ASN1_INTEGER.
 * Note this version uses the passed integer and doesn't make a copy so don't
 * free it up afterwards.
 */

int
SXNET_add_id_INTEGER(SXNET **psx, ASN1_INTEGER *zone, char *user, int userlen)
{
	SXNET *sx = NULL;
	SXNETID *id = NULL;

	if (!psx || !zone || !user) {
		X509V3error(X509V3_R_INVALID_NULL_ARGUMENT);
		return 0;
	}
	if (userlen == -1)
		userlen = strlen(user);
	if (userlen > 64) {
		X509V3error(X509V3_R_USER_TOO_LONG);
		return 0;
	}
	if (!*psx) {
		if (!(sx = SXNET_new()))
			goto err;
		if (!ASN1_INTEGER_set(sx->version, 0))
			goto err;
		*psx = sx;
	} else
		sx = *psx;
	if (SXNET_get_id_INTEGER(sx, zone)) {
		X509V3error(X509V3_R_DUPLICATE_ZONE_ID);
		return 0;
	}

	if (!(id = SXNETID_new()))
		goto err;
	if (userlen == -1)
		userlen = strlen(user);

	if (!ASN1_STRING_set(id->user, user, userlen))
		goto err;
	if (!sk_SXNETID_push(sx->ids, id))
		goto err;
	id->zone = zone;
	return 1;

err:
	X509V3error(ERR_R_MALLOC_FAILURE);
	SXNETID_free(id);
	SXNET_free(sx);
	*psx = NULL;
	return 0;
}

ASN1_OCTET_STRING *
SXNET_get_id_asc(SXNET *sx, char *zone)
{
	ASN1_INTEGER *izone = NULL;
	ASN1_OCTET_STRING *oct;

	if (!(izone = s2i_ASN1_INTEGER(NULL, zone))) {
		X509V3error(X509V3_R_ERROR_CONVERTING_ZONE);
		return NULL;
	}
	oct = SXNET_get_id_INTEGER(sx, izone);
	ASN1_INTEGER_free(izone);
	return oct;
}

ASN1_OCTET_STRING *
SXNET_get_id_ulong(SXNET *sx, unsigned long lzone)
{
	ASN1_INTEGER *izone = NULL;
	ASN1_OCTET_STRING *oct;

	if (!(izone = ASN1_INTEGER_new()) ||
	    !ASN1_INTEGER_set(izone, lzone)) {
		X509V3error(ERR_R_MALLOC_FAILURE);
		ASN1_INTEGER_free(izone);
		return NULL;
	}
	oct = SXNET_get_id_INTEGER(sx, izone);
	ASN1_INTEGER_free(izone);
	return oct;
}

ASN1_OCTET_STRING *
SXNET_get_id_INTEGER(SXNET *sx, ASN1_INTEGER *zone)
{
	SXNETID *id;
	int i;

	for (i = 0; i < sk_SXNETID_num(sx->ids); i++) {
		id = sk_SXNETID_value(sx->ids, i);
		if (!ASN1_STRING_cmp(id->zone, zone))
			return id->user;
	}
	return NULL;
}
