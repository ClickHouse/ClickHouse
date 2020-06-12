/* $OpenBSD: ecs_asn1.c,v 1.8 2015/10/16 15:15:39 jsing Exp $ */
/* ====================================================================
 * Copyright (c) 2000-2002 The OpenSSL Project.  All rights reserved.
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

#include "ecs_locl.h"
#include <openssl/err.h>
#include <openssl/asn1t.h>

static const ASN1_TEMPLATE ECDSA_SIG_seq_tt[] = {
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(ECDSA_SIG, r),
		.field_name = "r",
		.item = &CBIGNUM_it,
	},
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(ECDSA_SIG, s),
		.field_name = "s",
		.item = &CBIGNUM_it,
	},
};

const ASN1_ITEM ECDSA_SIG_it = {
	.itype = ASN1_ITYPE_SEQUENCE,
	.utype = V_ASN1_SEQUENCE,
	.templates = ECDSA_SIG_seq_tt,
	.tcount = sizeof(ECDSA_SIG_seq_tt) / sizeof(ASN1_TEMPLATE),
	.funcs = NULL,
	.size = sizeof(ECDSA_SIG),
	.sname = "ECDSA_SIG",
};

ECDSA_SIG *ECDSA_SIG_new(void);
void ECDSA_SIG_free(ECDSA_SIG *a);
ECDSA_SIG *d2i_ECDSA_SIG(ECDSA_SIG **a, const unsigned char **in, long len);
int i2d_ECDSA_SIG(const ECDSA_SIG *a, unsigned char **out);

ECDSA_SIG *
d2i_ECDSA_SIG(ECDSA_SIG **a, const unsigned char **in, long len)
{
	return (ECDSA_SIG *)ASN1_item_d2i((ASN1_VALUE **)a, in, len,
	    &ECDSA_SIG_it);
}

int
i2d_ECDSA_SIG(const ECDSA_SIG *a, unsigned char **out)
{
	return ASN1_item_i2d((ASN1_VALUE *)a, out, &ECDSA_SIG_it);
}

ECDSA_SIG *
ECDSA_SIG_new(void)
{
	return (ECDSA_SIG *)ASN1_item_new(&ECDSA_SIG_it);
}

void
ECDSA_SIG_free(ECDSA_SIG *a)
{
	ASN1_item_free((ASN1_VALUE *)a, &ECDSA_SIG_it);
}
