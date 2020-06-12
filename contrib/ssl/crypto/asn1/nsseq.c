/* $OpenBSD: nsseq.c,v 1.10 2015/02/11 04:00:39 jsing Exp $ */
/* Written by Dr Stephen N Henson (steve@openssl.org) for the OpenSSL
 * project 1999.
 */
/* ====================================================================
 * Copyright (c) 1999-2005 The OpenSSL Project.  All rights reserved.
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
#include <openssl/asn1t.h>
#include <openssl/x509.h>
#include <openssl/objects.h>

static int
nsseq_cb(int operation, ASN1_VALUE **pval, const ASN1_ITEM *it, void *exarg)
{
	if (operation == ASN1_OP_NEW_POST) {
		NETSCAPE_CERT_SEQUENCE *nsseq;
		nsseq = (NETSCAPE_CERT_SEQUENCE *)*pval;
		nsseq->type = OBJ_nid2obj(NID_netscape_cert_sequence);
	}
	return 1;
}

/* Netscape certificate sequence structure */

static const ASN1_AUX NETSCAPE_CERT_SEQUENCE_aux = {
	.asn1_cb = nsseq_cb,
};
static const ASN1_TEMPLATE NETSCAPE_CERT_SEQUENCE_seq_tt[] = {
	{
		.offset = offsetof(NETSCAPE_CERT_SEQUENCE, type),
		.field_name = "type",
		.item = &ASN1_OBJECT_it,
	},
	{
		.flags = ASN1_TFLG_EXPLICIT | ASN1_TFLG_SEQUENCE_OF | ASN1_TFLG_OPTIONAL,
		.offset = offsetof(NETSCAPE_CERT_SEQUENCE, certs),
		.field_name = "certs",
		.item = &X509_it,
	},
};

const ASN1_ITEM NETSCAPE_CERT_SEQUENCE_it = {
	.itype = ASN1_ITYPE_SEQUENCE,
	.utype = V_ASN1_SEQUENCE,
	.templates = NETSCAPE_CERT_SEQUENCE_seq_tt,
	.tcount = sizeof(NETSCAPE_CERT_SEQUENCE_seq_tt) / sizeof(ASN1_TEMPLATE),
	.funcs = &NETSCAPE_CERT_SEQUENCE_aux,
	.size = sizeof(NETSCAPE_CERT_SEQUENCE),
	.sname = "NETSCAPE_CERT_SEQUENCE",
};


NETSCAPE_CERT_SEQUENCE *
d2i_NETSCAPE_CERT_SEQUENCE(NETSCAPE_CERT_SEQUENCE **a, const unsigned char **in, long len)
{
	return (NETSCAPE_CERT_SEQUENCE *)ASN1_item_d2i((ASN1_VALUE **)a, in, len,
	    &NETSCAPE_CERT_SEQUENCE_it);
}

int
i2d_NETSCAPE_CERT_SEQUENCE(NETSCAPE_CERT_SEQUENCE *a, unsigned char **out)
{
	return ASN1_item_i2d((ASN1_VALUE *)a, out, &NETSCAPE_CERT_SEQUENCE_it);
}

NETSCAPE_CERT_SEQUENCE *
NETSCAPE_CERT_SEQUENCE_new(void)
{
	return (NETSCAPE_CERT_SEQUENCE *)ASN1_item_new(&NETSCAPE_CERT_SEQUENCE_it);
}

void
NETSCAPE_CERT_SEQUENCE_free(NETSCAPE_CERT_SEQUENCE *a)
{
	ASN1_item_free((ASN1_VALUE *)a, &NETSCAPE_CERT_SEQUENCE_it);
}
