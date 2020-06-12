/* $OpenBSD: x_bignum.c,v 1.8 2015/07/25 17:07:17 jsing Exp $ */
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

#include <stdio.h>

#include <openssl/asn1t.h>
#include <openssl/bn.h>

/*
 * Custom primitive type for BIGNUM handling. This reads in an ASN1_INTEGER as a
 * BIGNUM directly. Currently it ignores the sign which isn't a problem since
 * all BIGNUMs used are non negative and anything that looks negative is
 * normally due to an encoding error.
 */

static int bn_new(ASN1_VALUE **pval, const ASN1_ITEM *it);
static void bn_free(ASN1_VALUE **pval, const ASN1_ITEM *it);

static int bn_i2c(ASN1_VALUE **pval, unsigned char *cont, int *putype,
    const ASN1_ITEM *it);
static int bn_c2i(ASN1_VALUE **pval, const unsigned char *cont, int len,
    int utype, char *free_cont, const ASN1_ITEM *it);

static ASN1_PRIMITIVE_FUNCS bignum_pf = {
	NULL,
	0,
	bn_new,
	bn_free,
	0,
	bn_c2i,
	bn_i2c
};

const ASN1_ITEM BIGNUM_it = {
        .itype = ASN1_ITYPE_PRIMITIVE,
        .utype = V_ASN1_INTEGER,
        .templates = NULL,
        .tcount = 0,
        .funcs = &bignum_pf,
        .size = 0,
        .sname = "BIGNUM",
};

const ASN1_ITEM CBIGNUM_it = {
        .itype = ASN1_ITYPE_PRIMITIVE,
        .utype = V_ASN1_INTEGER,
        .templates = NULL,
        .tcount = 0,
        .funcs = &bignum_pf,
        .size = 0,
        .sname = "BIGNUM",
};

static int
bn_new(ASN1_VALUE **pval, const ASN1_ITEM *it)
{
	*pval = (ASN1_VALUE *)BN_new();
	if (*pval)
		return 1;
	else
		return 0;
}

static void
bn_free(ASN1_VALUE **pval, const ASN1_ITEM *it)
{
	if (*pval == NULL)
		return;
	BN_clear_free((BIGNUM *)*pval);
	*pval = NULL;
}

static int
bn_i2c(ASN1_VALUE **pval, unsigned char *cont, int *putype, const ASN1_ITEM *it)
{
	BIGNUM *bn;
	int pad, len;

	if (*pval == NULL)
		return -1;
	bn = (BIGNUM *)*pval;
	/* If MSB set in an octet we need a padding byte */
	if (BN_num_bits(bn) & 0x7)
		pad = 0;
	else
		pad = 1;
	if (cont) {
		if (pad)
			*cont++ = 0;
		len = BN_bn2bin(bn, cont);
	} else
		len = BN_num_bytes(bn);
	return pad + len;
}

static int
bn_c2i(ASN1_VALUE **pval, const unsigned char *cont, int len, int utype,
    char *free_cont, const ASN1_ITEM *it)
{
	BIGNUM *bn;

	if (*pval == NULL) {
		if (bn_new(pval, it) == 0)
			return 0;
	}
	bn = (BIGNUM *)*pval;
	if (!BN_bin2bn(cont, len, bn)) {
		bn_free(pval, it);
		return 0;
	}
	return 1;
}
