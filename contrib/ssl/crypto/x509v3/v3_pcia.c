/* $OpenBSD: v3_pcia.c,v 1.6 2015/07/25 16:00:14 jsing Exp $ */
/* Contributed to the OpenSSL Project 2004
 * by Richard Levitte (richard@levitte.org)
 */
/* Copyright (c) 2004 Kungliga Tekniska Högskolan
 * (Royal Institute of Technology, Stockholm, Sweden).
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the Institute nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE INSTITUTE AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE INSTITUTE OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <openssl/asn1.h>
#include <openssl/asn1t.h>
#include <openssl/x509v3.h>

static const ASN1_TEMPLATE PROXY_POLICY_seq_tt[] = {
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(PROXY_POLICY, policyLanguage),
		.field_name = "policyLanguage",
		.item = &ASN1_OBJECT_it,
	},
	{
		.flags = ASN1_TFLG_OPTIONAL,
		.tag = 0,
		.offset = offsetof(PROXY_POLICY, policy),
		.field_name = "policy",
		.item = &ASN1_OCTET_STRING_it,
	},
};

const ASN1_ITEM PROXY_POLICY_it = {
	.itype = ASN1_ITYPE_SEQUENCE,
	.utype = V_ASN1_SEQUENCE,
	.templates = PROXY_POLICY_seq_tt,
	.tcount = sizeof(PROXY_POLICY_seq_tt) / sizeof(ASN1_TEMPLATE),
	.funcs = NULL,
	.size = sizeof(PROXY_POLICY),
	.sname = "PROXY_POLICY",
};


PROXY_POLICY *
d2i_PROXY_POLICY(PROXY_POLICY **a, const unsigned char **in, long len)
{
	return (PROXY_POLICY *)ASN1_item_d2i((ASN1_VALUE **)a, in, len,
	    &PROXY_POLICY_it);
}

int
i2d_PROXY_POLICY(PROXY_POLICY *a, unsigned char **out)
{
	return ASN1_item_i2d((ASN1_VALUE *)a, out, &PROXY_POLICY_it);
}

PROXY_POLICY *
PROXY_POLICY_new(void)
{
	return (PROXY_POLICY *)ASN1_item_new(&PROXY_POLICY_it);
}

void
PROXY_POLICY_free(PROXY_POLICY *a)
{
	ASN1_item_free((ASN1_VALUE *)a, &PROXY_POLICY_it);
}

static const ASN1_TEMPLATE PROXY_CERT_INFO_EXTENSION_seq_tt[] = {
	{
		.flags = ASN1_TFLG_OPTIONAL,
		.tag = 0,
		.offset = offsetof(PROXY_CERT_INFO_EXTENSION, pcPathLengthConstraint),
		.field_name = "pcPathLengthConstraint",
		.item = &ASN1_INTEGER_it,
	},
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(PROXY_CERT_INFO_EXTENSION, proxyPolicy),
		.field_name = "proxyPolicy",
		.item = &PROXY_POLICY_it,
	},
};

const ASN1_ITEM PROXY_CERT_INFO_EXTENSION_it = {
	.itype = ASN1_ITYPE_SEQUENCE,
	.utype = V_ASN1_SEQUENCE,
	.templates = PROXY_CERT_INFO_EXTENSION_seq_tt,
	.tcount = sizeof(PROXY_CERT_INFO_EXTENSION_seq_tt) / sizeof(ASN1_TEMPLATE),
	.funcs = NULL,
	.size = sizeof(PROXY_CERT_INFO_EXTENSION),
	.sname = "PROXY_CERT_INFO_EXTENSION",
};


PROXY_CERT_INFO_EXTENSION *
d2i_PROXY_CERT_INFO_EXTENSION(PROXY_CERT_INFO_EXTENSION **a, const unsigned char **in, long len)
{
	return (PROXY_CERT_INFO_EXTENSION *)ASN1_item_d2i((ASN1_VALUE **)a, in, len,
	    &PROXY_CERT_INFO_EXTENSION_it);
}

int
i2d_PROXY_CERT_INFO_EXTENSION(PROXY_CERT_INFO_EXTENSION *a, unsigned char **out)
{
	return ASN1_item_i2d((ASN1_VALUE *)a, out, &PROXY_CERT_INFO_EXTENSION_it);
}

PROXY_CERT_INFO_EXTENSION *
PROXY_CERT_INFO_EXTENSION_new(void)
{
	return (PROXY_CERT_INFO_EXTENSION *)ASN1_item_new(&PROXY_CERT_INFO_EXTENSION_it);
}

void
PROXY_CERT_INFO_EXTENSION_free(PROXY_CERT_INFO_EXTENSION *a)
{
	ASN1_item_free((ASN1_VALUE *)a, &PROXY_CERT_INFO_EXTENSION_it);
}
