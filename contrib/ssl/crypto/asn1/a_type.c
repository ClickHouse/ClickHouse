/* $OpenBSD: a_type.c,v 1.19 2016/05/04 15:00:24 tedu Exp $ */
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

#include <openssl/asn1t.h>
#include <openssl/objects.h>

int
ASN1_TYPE_get(ASN1_TYPE *a)
{
	if ((a->value.ptr != NULL) || (a->type == V_ASN1_NULL))
		return (a->type);
	else
		return (0);
}

void
ASN1_TYPE_set(ASN1_TYPE *a, int type, void *value)
{
	if (a->value.ptr != NULL) {
		ASN1_TYPE **tmp_a = &a;
		ASN1_primitive_free((ASN1_VALUE **)tmp_a, NULL);
	}
	a->type = type;
	if (type == V_ASN1_BOOLEAN)
		a->value.boolean = value ? 0xff : 0;
	else
		a->value.ptr = value;
}

int
ASN1_TYPE_set1(ASN1_TYPE *a, int type, const void *value)
{
	if (!value || (type == V_ASN1_BOOLEAN)) {
		void *p = (void *)value;
		ASN1_TYPE_set(a, type, p);
	} else if (type == V_ASN1_OBJECT) {
		ASN1_OBJECT *odup;
		odup = OBJ_dup(value);
		if (!odup)
			return 0;
		ASN1_TYPE_set(a, type, odup);
	} else {
		ASN1_STRING *sdup;
		sdup = ASN1_STRING_dup(value);
		if (!sdup)
			return 0;
		ASN1_TYPE_set(a, type, sdup);
	}
	return 1;
}

/* Returns 0 if they are equal, != 0 otherwise. */
int
ASN1_TYPE_cmp(ASN1_TYPE *a, ASN1_TYPE *b)
{
	int result = -1;

	if (!a || !b || a->type != b->type)
		return -1;

	switch (a->type) {
	case V_ASN1_OBJECT:
		result = OBJ_cmp(a->value.object, b->value.object);
		break;
	case V_ASN1_BOOLEAN:
		result = a->value.boolean - b->value.boolean;
		break;
	case V_ASN1_NULL:
		result = 0;	/* They do not have content. */
		break;

	case V_ASN1_INTEGER:
	case V_ASN1_ENUMERATED:
	case V_ASN1_BIT_STRING:
	case V_ASN1_OCTET_STRING:
	case V_ASN1_SEQUENCE:
	case V_ASN1_SET:
	case V_ASN1_NUMERICSTRING:
	case V_ASN1_PRINTABLESTRING:
	case V_ASN1_T61STRING:
	case V_ASN1_VIDEOTEXSTRING:
	case V_ASN1_IA5STRING:
	case V_ASN1_UTCTIME:
	case V_ASN1_GENERALIZEDTIME:
	case V_ASN1_GRAPHICSTRING:
	case V_ASN1_VISIBLESTRING:
	case V_ASN1_GENERALSTRING:
	case V_ASN1_UNIVERSALSTRING:
	case V_ASN1_BMPSTRING:
	case V_ASN1_UTF8STRING:
	case V_ASN1_OTHER:
	default:
		result = ASN1_STRING_cmp((ASN1_STRING *)a->value.ptr,
		    (ASN1_STRING *)b->value.ptr);
		break;
	}

	return result;
}
