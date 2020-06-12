/* $OpenBSD: gost_asn1.h,v 1.3 2016/12/21 15:49:29 jsing Exp $ */
/*
 * Copyright (c) 2014 Dmitry Eremin-Solenikov <dbaryshkov@gmail.com>
 * Copyright (c) 2005-2006 Cryptocom LTD
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
 *    for use in the OpenSSL Toolkit. (http://www.openssl.org/)"
 *
 * 4. The names "OpenSSL Toolkit" and "OpenSSL Project" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For written permission, please contact
 *    openssl-core@openssl.org.
 *
 * 5. Products derived from this software may not be called "OpenSSL"
 *    nor may "OpenSSL" appear in their names without prior written
 *    permission of the OpenSSL Project.
 *
 * 6. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by the OpenSSL Project
 *    for use in the OpenSSL Toolkit (http://www.openssl.org/)"
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
 */

#ifndef HEADER_GOST_ASN1_H
#define HEADER_GOST_ASN1_H

#include <openssl/asn1.h>

__BEGIN_HIDDEN_DECLS

typedef struct {
	ASN1_OCTET_STRING *encrypted_key;
	ASN1_OCTET_STRING *imit;
} GOST_KEY_INFO;

GOST_KEY_INFO *GOST_KEY_INFO_new(void);
void GOST_KEY_INFO_free(GOST_KEY_INFO *a);
GOST_KEY_INFO *d2i_GOST_KEY_INFO(GOST_KEY_INFO **a, const unsigned char **in, long len);
int i2d_GOST_KEY_INFO(GOST_KEY_INFO *a, unsigned char **out);
extern const ASN1_ITEM GOST_KEY_INFO_it;

typedef struct {
	ASN1_OBJECT *cipher;
	X509_PUBKEY *ephem_key;
	ASN1_OCTET_STRING *eph_iv;
} GOST_KEY_AGREEMENT_INFO;

GOST_KEY_AGREEMENT_INFO *GOST_KEY_AGREEMENT_INFO_new(void);
void GOST_KEY_AGREEMENT_INFO_free(GOST_KEY_AGREEMENT_INFO *a);
GOST_KEY_AGREEMENT_INFO *d2i_GOST_KEY_AGREEMENT_INFO(GOST_KEY_AGREEMENT_INFO **a, const unsigned char **in, long len);
int i2d_GOST_KEY_AGREEMENT_INFO(GOST_KEY_AGREEMENT_INFO *a, unsigned char **out);
extern const ASN1_ITEM GOST_KEY_AGREEMENT_INFO_it;

typedef struct {
	GOST_KEY_INFO *key_info;
	GOST_KEY_AGREEMENT_INFO *key_agreement_info;
} GOST_KEY_TRANSPORT;

GOST_KEY_TRANSPORT *GOST_KEY_TRANSPORT_new(void);
void GOST_KEY_TRANSPORT_free(GOST_KEY_TRANSPORT *a);
GOST_KEY_TRANSPORT *d2i_GOST_KEY_TRANSPORT(GOST_KEY_TRANSPORT **a, const unsigned char **in, long len);
int i2d_GOST_KEY_TRANSPORT(GOST_KEY_TRANSPORT *a, unsigned char **out);
extern const ASN1_ITEM GOST_KEY_TRANSPORT_it;

typedef struct {
	ASN1_OBJECT *key_params;
	ASN1_OBJECT *hash_params;
	ASN1_OBJECT *cipher_params;
} GOST_KEY_PARAMS;

GOST_KEY_PARAMS *GOST_KEY_PARAMS_new(void);
void GOST_KEY_PARAMS_free(GOST_KEY_PARAMS *a);
GOST_KEY_PARAMS *d2i_GOST_KEY_PARAMS(GOST_KEY_PARAMS **a, const unsigned char **in, long len);
int i2d_GOST_KEY_PARAMS(GOST_KEY_PARAMS *a, unsigned char **out);
extern const ASN1_ITEM GOST_KEY_PARAMS_it;

__END_HIDDEN_DECLS

#endif
