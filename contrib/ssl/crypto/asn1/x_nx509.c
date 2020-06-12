/* $OpenBSD: x_nx509.c,v 1.6 2015/02/11 04:00:39 jsing Exp $ */
/* Written by Dr Stephen N Henson (steve@openssl.org) for the OpenSSL
 * project 2005.
 */
/* ====================================================================
 * Copyright (c) 2005 The OpenSSL Project.  All rights reserved.
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

/* Old netscape certificate wrapper format */

static const ASN1_TEMPLATE NETSCAPE_X509_seq_tt[] = {
	{
		.offset = offsetof(NETSCAPE_X509, header),
		.field_name = "header",
		.item = &ASN1_OCTET_STRING_it,
	},
	{
		.flags = ASN1_TFLG_OPTIONAL,
		.offset = offsetof(NETSCAPE_X509, cert),
		.field_name = "cert",
		.item = &X509_it,
	},
};

const ASN1_ITEM NETSCAPE_X509_it = {
	.itype = ASN1_ITYPE_SEQUENCE,
	.utype = V_ASN1_SEQUENCE,
	.templates = NETSCAPE_X509_seq_tt,
	.tcount = sizeof(NETSCAPE_X509_seq_tt) / sizeof(ASN1_TEMPLATE),
	.size = sizeof(NETSCAPE_X509),
	.sname = "NETSCAPE_X509",
};


NETSCAPE_X509 *
d2i_NETSCAPE_X509(NETSCAPE_X509 **a, const unsigned char **in, long len)
{
	return (NETSCAPE_X509 *)ASN1_item_d2i((ASN1_VALUE **)a, in, len,
	    &NETSCAPE_X509_it);
}

int
i2d_NETSCAPE_X509(NETSCAPE_X509 *a, unsigned char **out)
{
	return ASN1_item_i2d((ASN1_VALUE *)a, out, &NETSCAPE_X509_it);
}

NETSCAPE_X509 *
NETSCAPE_X509_new(void)
{
	return (NETSCAPE_X509 *)ASN1_item_new(&NETSCAPE_X509_it);
}

void
NETSCAPE_X509_free(NETSCAPE_X509 *a)
{
	ASN1_item_free((ASN1_VALUE *)a, &NETSCAPE_X509_it);
}
