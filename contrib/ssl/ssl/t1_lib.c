/* $OpenBSD: t1_lib.c,v 1.137 2017/08/30 16:44:37 jsing Exp $ */
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
/* ====================================================================
 * Copyright (c) 1998-2007 The OpenSSL Project.  All rights reserved.
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
 *
 * This product includes cryptographic software written by Eric Young
 * (eay@cryptsoft.com).  This product includes software written by Tim
 * Hudson (tjh@cryptsoft.com).
 *
 */

#include <stdio.h>

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/objects.h>
#include <openssl/ocsp.h>

#include "ssl_locl.h"

#include "bytestring.h"
#include "ssl_tlsext.h"

static int tls_decrypt_ticket(SSL *s, const unsigned char *tick, int ticklen,
    const unsigned char *sess_id, int sesslen,
    SSL_SESSION **psess);

SSL3_ENC_METHOD TLSv1_enc_data = {
	.enc = tls1_enc,
	.enc_flags = 0,
};

SSL3_ENC_METHOD TLSv1_1_enc_data = {
	.enc = tls1_enc,
	.enc_flags = SSL_ENC_FLAG_EXPLICIT_IV,
};

SSL3_ENC_METHOD TLSv1_2_enc_data = {
	.enc = tls1_enc,
	.enc_flags = SSL_ENC_FLAG_EXPLICIT_IV|SSL_ENC_FLAG_SIGALGS|
	    SSL_ENC_FLAG_SHA256_PRF|SSL_ENC_FLAG_TLS1_2_CIPHERS,
};

long
tls1_default_timeout(void)
{
	/* 2 hours, the 24 hours mentioned in the TLSv1 spec
	 * is way too long for http, the cache would over fill */
	return (60 * 60 * 2);
}

int
tls1_new(SSL *s)
{
	if (!ssl3_new(s))
		return (0);
	s->method->internal->ssl_clear(s);
	return (1);
}

void
tls1_free(SSL *s)
{
	if (s == NULL)
		return;

	free(s->internal->tlsext_session_ticket);
	ssl3_free(s);
}

void
tls1_clear(SSL *s)
{
	ssl3_clear(s);
	s->version = s->method->internal->version;
}

static int nid_list[] = {
	NID_sect163k1,		/* sect163k1 (1) */
	NID_sect163r1,		/* sect163r1 (2) */
	NID_sect163r2,		/* sect163r2 (3) */
	NID_sect193r1,		/* sect193r1 (4) */
	NID_sect193r2,		/* sect193r2 (5) */
	NID_sect233k1,		/* sect233k1 (6) */
	NID_sect233r1,		/* sect233r1 (7) */
	NID_sect239k1,		/* sect239k1 (8) */
	NID_sect283k1,		/* sect283k1 (9) */
	NID_sect283r1,		/* sect283r1 (10) */
	NID_sect409k1,		/* sect409k1 (11) */
	NID_sect409r1,		/* sect409r1 (12) */
	NID_sect571k1,		/* sect571k1 (13) */
	NID_sect571r1,		/* sect571r1 (14) */
	NID_secp160k1,		/* secp160k1 (15) */
	NID_secp160r1,		/* secp160r1 (16) */
	NID_secp160r2,		/* secp160r2 (17) */
	NID_secp192k1,		/* secp192k1 (18) */
	NID_X9_62_prime192v1,	/* secp192r1 (19) */
	NID_secp224k1,		/* secp224k1 (20) */
	NID_secp224r1,		/* secp224r1 (21) */
	NID_secp256k1,		/* secp256k1 (22) */
	NID_X9_62_prime256v1,	/* secp256r1 (23) */
	NID_secp384r1,		/* secp384r1 (24) */
	NID_secp521r1,		/* secp521r1 (25) */
	NID_brainpoolP256r1,	/* brainpoolP256r1 (26) */
	NID_brainpoolP384r1,	/* brainpoolP384r1 (27) */
	NID_brainpoolP512r1,	/* brainpoolP512r1 (28) */
	NID_X25519,		/* X25519 (29) */
};

#if 0
static const uint8_t ecformats_list[] = {
	TLSEXT_ECPOINTFORMAT_uncompressed,
	TLSEXT_ECPOINTFORMAT_ansiX962_compressed_prime,
	TLSEXT_ECPOINTFORMAT_ansiX962_compressed_char2
};
#endif

static const uint8_t ecformats_default[] = {
	TLSEXT_ECPOINTFORMAT_uncompressed,
};

#if 0
static const uint16_t eccurves_list[] = {
	29,			/* X25519 (29) */
	14,			/* sect571r1 (14) */
	13,			/* sect571k1 (13) */
	25,			/* secp521r1 (25) */
	28,			/* brainpoolP512r1 (28) */
	11,			/* sect409k1 (11) */
	12,			/* sect409r1 (12) */
	27,			/* brainpoolP384r1 (27) */
	24,			/* secp384r1 (24) */
	9,			/* sect283k1 (9) */
	10,			/* sect283r1 (10) */
	26,			/* brainpoolP256r1 (26) */
	22,			/* secp256k1 (22) */
	23,			/* secp256r1 (23) */
	8,			/* sect239k1 (8) */
	6,			/* sect233k1 (6) */
	7,			/* sect233r1 (7) */
	20,			/* secp224k1 (20) */
	21,			/* secp224r1 (21) */
	4,			/* sect193r1 (4) */
	5,			/* sect193r2 (5) */
	18,			/* secp192k1 (18) */
	19,			/* secp192r1 (19) */
	1,			/* sect163k1 (1) */
	2,			/* sect163r1 (2) */
	3,			/* sect163r2 (3) */
	15,			/* secp160k1 (15) */
	16,			/* secp160r1 (16) */
	17,			/* secp160r2 (17) */
};
#endif

static const uint16_t eccurves_default[] = {
	29,			/* X25519 (29) */
	23,			/* secp256r1 (23) */
	24,			/* secp384r1 (24) */
};

int
tls1_ec_curve_id2nid(const uint16_t curve_id)
{
	/* ECC curves from draft-ietf-tls-ecc-12.txt (Oct. 17, 2005) */
	if ((curve_id < 1) ||
	    ((unsigned int)curve_id > sizeof(nid_list) / sizeof(nid_list[0])))
		return 0;
	return nid_list[curve_id - 1];
}

uint16_t
tls1_ec_nid2curve_id(const int nid)
{
	/* ECC curves from draft-ietf-tls-ecc-12.txt (Oct. 17, 2005) */
	switch (nid) {
	case NID_sect163k1: /* sect163k1 (1) */
		return 1;
	case NID_sect163r1: /* sect163r1 (2) */
		return 2;
	case NID_sect163r2: /* sect163r2 (3) */
		return 3;
	case NID_sect193r1: /* sect193r1 (4) */
		return 4;
	case NID_sect193r2: /* sect193r2 (5) */
		return 5;
	case NID_sect233k1: /* sect233k1 (6) */
		return 6;
	case NID_sect233r1: /* sect233r1 (7) */
		return 7;
	case NID_sect239k1: /* sect239k1 (8) */
		return 8;
	case NID_sect283k1: /* sect283k1 (9) */
		return 9;
	case NID_sect283r1: /* sect283r1 (10) */
		return 10;
	case NID_sect409k1: /* sect409k1 (11) */
		return 11;
	case NID_sect409r1: /* sect409r1 (12) */
		return 12;
	case NID_sect571k1: /* sect571k1 (13) */
		return 13;
	case NID_sect571r1: /* sect571r1 (14) */
		return 14;
	case NID_secp160k1: /* secp160k1 (15) */
		return 15;
	case NID_secp160r1: /* secp160r1 (16) */
		return 16;
	case NID_secp160r2: /* secp160r2 (17) */
		return 17;
	case NID_secp192k1: /* secp192k1 (18) */
		return 18;
	case NID_X9_62_prime192v1: /* secp192r1 (19) */
		return 19;
	case NID_secp224k1: /* secp224k1 (20) */
		return 20;
	case NID_secp224r1: /* secp224r1 (21) */
		return 21;
	case NID_secp256k1: /* secp256k1 (22) */
		return 22;
	case NID_X9_62_prime256v1: /* secp256r1 (23) */
		return 23;
	case NID_secp384r1: /* secp384r1 (24) */
		return 24;
	case NID_secp521r1: /* secp521r1 (25) */
		return 25;
	case NID_brainpoolP256r1: /* brainpoolP256r1 (26) */
		return 26;
	case NID_brainpoolP384r1: /* brainpoolP384r1 (27) */
		return 27;
	case NID_brainpoolP512r1: /* brainpoolP512r1 (28) */
		return 28;
	case NID_X25519:		/* X25519 (29) */
		return 29;
	default:
		return 0;
	}
}

/*
 * Return the appropriate format list. If client_formats is non-zero, return
 * the client/session formats. Otherwise return the custom format list if one
 * exists, or the default formats if a custom list has not been specified.
 */
void
tls1_get_formatlist(SSL *s, int client_formats, const uint8_t **pformats,
    size_t *pformatslen)
{
	if (client_formats != 0) {
		*pformats = SSI(s)->tlsext_ecpointformatlist;
		*pformatslen = SSI(s)->tlsext_ecpointformatlist_length;
		return;
	}

	*pformats = s->internal->tlsext_ecpointformatlist;
	*pformatslen = s->internal->tlsext_ecpointformatlist_length;
	if (*pformats == NULL) {
		*pformats = ecformats_default;
		*pformatslen = sizeof(ecformats_default);
	}
}

/*
 * Return the appropriate curve list. If client_curves is non-zero, return
 * the client/session curves. Otherwise return the custom curve list if one
 * exists, or the default curves if a custom list has not been specified.
 */
void
tls1_get_curvelist(SSL *s, int client_curves, const uint16_t **pcurves,
    size_t *pcurveslen)
{
	if (client_curves != 0) {
		*pcurves = SSI(s)->tlsext_supportedgroups;
		*pcurveslen = SSI(s)->tlsext_supportedgroups_length;
		return;
	}

	*pcurves = s->internal->tlsext_supportedgroups;
	*pcurveslen = s->internal->tlsext_supportedgroups_length;
	if (*pcurves == NULL) {
		*pcurves = eccurves_default;
		*pcurveslen = sizeof(eccurves_default) / 2;
	}
}

int
tls1_set_groups(uint16_t **out_group_ids, size_t *out_group_ids_len,
    const int *groups, size_t ngroups)
{
	uint16_t *group_ids;
	size_t i;

	group_ids = calloc(ngroups, sizeof(uint16_t));
	if (group_ids == NULL)
		return 0;

	for (i = 0; i < ngroups; i++) {
		group_ids[i] = tls1_ec_nid2curve_id(groups[i]);
		if (group_ids[i] == 0) {
			free(group_ids);
			return 0;
		}
	}

	free(*out_group_ids);
	*out_group_ids = group_ids;
	*out_group_ids_len = ngroups;

	return 1;
}

int
tls1_set_groups_list(uint16_t **out_group_ids, size_t *out_group_ids_len,
    const char *groups)
{
	uint16_t *new_group_ids, *group_ids = NULL;
	size_t ngroups = 0;
	char *gs, *p, *q;
	int nid;

	if ((gs = strdup(groups)) == NULL)
		return 0;

	q = gs;
	while ((p = strsep(&q, ":")) != NULL) {
		nid = OBJ_sn2nid(p);
		if (nid == NID_undef)
			nid = OBJ_ln2nid(p);
		if (nid == NID_undef)
			nid = EC_curve_nist2nid(p);
		if (nid == NID_undef)
			goto err;

		if ((new_group_ids = reallocarray(group_ids, ngroups + 1,
		    sizeof(uint16_t))) == NULL)
			goto err;
		group_ids = new_group_ids;

		group_ids[ngroups] = tls1_ec_nid2curve_id(nid);
		if (group_ids[ngroups] == 0)
			goto err;

		ngroups++;
	}

	free(gs);
	free(*out_group_ids);
	*out_group_ids = group_ids;
	*out_group_ids_len = ngroups;

	return 1;

 err:
	free(gs);
	free(group_ids);

	return 0;
}

/* Check that a curve is one of our preferences. */
int
tls1_check_curve(SSL *s, const uint16_t curve_id)
{
	const uint16_t *curves;
	size_t curveslen, i;

	tls1_get_curvelist(s, 0, &curves, &curveslen);

	for (i = 0; i < curveslen; i++) {
		if (curves[i] == curve_id)
			return (1);
	}
	return (0);
}

int
tls1_get_shared_curve(SSL *s)
{
	size_t preflen, supplen, i, j;
	const uint16_t *pref, *supp;
	unsigned long server_pref;

	/* Cannot do anything on the client side. */
	if (s->server == 0)
		return (NID_undef);

	/* Return first preference shared curve. */
	server_pref = (s->internal->options & SSL_OP_CIPHER_SERVER_PREFERENCE);
	tls1_get_curvelist(s, (server_pref == 0), &pref, &preflen);
	tls1_get_curvelist(s, (server_pref != 0), &supp, &supplen);

	for (i = 0; i < preflen; i++) {
		for (j = 0; j < supplen; j++) {
			if (pref[i] == supp[j])
				return (tls1_ec_curve_id2nid(pref[i]));
		}
	}
	return (NID_undef);
}

/* For an EC key set TLS ID and required compression based on parameters. */
static int
tls1_set_ec_id(uint16_t *curve_id, uint8_t *comp_id, EC_KEY *ec)
{
	const EC_GROUP *grp;
	const EC_METHOD *meth;
	int is_prime = 0;
	int nid, id;

	if (ec == NULL)
		return (0);

	/* Determine if it is a prime field. */
	if ((grp = EC_KEY_get0_group(ec)) == NULL)
		return (0);
	if ((meth = EC_GROUP_method_of(grp)) == NULL)
		return (0);
	if (EC_METHOD_get_field_type(meth) == NID_X9_62_prime_field)
		is_prime = 1;

	/* Determine curve ID. */
	nid = EC_GROUP_get_curve_name(grp);
	id = tls1_ec_nid2curve_id(nid);

	/* If we have an ID set it, otherwise set arbitrary explicit curve. */
	if (id != 0)
		*curve_id = id;
	else
		*curve_id = is_prime ? 0xff01 : 0xff02;

	/* Specify the compression identifier. */
	if (comp_id != NULL) {
		if (EC_KEY_get0_public_key(ec) == NULL)
			return (0);

		if (EC_KEY_get_conv_form(ec) == POINT_CONVERSION_COMPRESSED) {
			*comp_id = is_prime ?
			    TLSEXT_ECPOINTFORMAT_ansiX962_compressed_prime :
			    TLSEXT_ECPOINTFORMAT_ansiX962_compressed_char2;
		} else {
			*comp_id = TLSEXT_ECPOINTFORMAT_uncompressed;
		}
	}
	return (1);
}

/* Check that an EC key is compatible with extensions. */
static int
tls1_check_ec_key(SSL *s, const uint16_t *curve_id, const uint8_t *comp_id)
{
	size_t curveslen, formatslen, i;
	const uint16_t *curves;
	const uint8_t *formats;

	/*
	 * Check point formats extension if present, otherwise everything
	 * is supported (see RFC4492).
	 */
	tls1_get_formatlist(s, 1, &formats, &formatslen);
	if (comp_id != NULL && formats != NULL) {
		for (i = 0; i < formatslen; i++) {
			if (formats[i] == *comp_id)
				break;
		}
		if (i == formatslen)
			return (0);
	}

	/*
	 * Check curve list if present, otherwise everything is supported.
	 */
	tls1_get_curvelist(s, 1, &curves, &curveslen);
	if (curve_id != NULL && curves != NULL) {
		for (i = 0; i < curveslen; i++) {
			if (curves[i] == *curve_id)
				break;
		}
		if (i == curveslen)
			return (0);
	}

	return (1);
}

/* Check EC server key is compatible with client extensions. */
int
tls1_check_ec_server_key(SSL *s)
{
	CERT_PKEY *cpk = s->cert->pkeys + SSL_PKEY_ECC;
	uint16_t curve_id;
	uint8_t comp_id;
	EVP_PKEY *pkey;
	int rv;

	if (cpk->x509 == NULL || cpk->privatekey == NULL)
		return (0);
	if ((pkey = X509_get_pubkey(cpk->x509)) == NULL)
		return (0);
	rv = tls1_set_ec_id(&curve_id, &comp_id, pkey->pkey.ec);
	EVP_PKEY_free(pkey);
	if (rv != 1)
		return (0);

	return tls1_check_ec_key(s, &curve_id, &comp_id);
}

/* Check EC temporary key is compatible with client extensions. */
int
tls1_check_ec_tmp_key(SSL *s)
{
	EC_KEY *ec = s->cert->ecdh_tmp;
	uint16_t curve_id;

	/* Need a shared curve. */
	if (tls1_get_shared_curve(s) != NID_undef)
		return (1);

	if (ec == NULL)
		return (0);

	if (tls1_set_ec_id(&curve_id, NULL, ec) != 1)
		return (0);

	return tls1_check_ec_key(s, &curve_id, NULL);
}

/*
 * List of supported signature algorithms and hashes. Should make this
 * customisable at some point, for now include everything we support.
 */

static unsigned char tls12_sigalgs[] = {
	TLSEXT_hash_sha512, TLSEXT_signature_rsa,
	TLSEXT_hash_sha512, TLSEXT_signature_ecdsa,
#ifndef OPENSSL_NO_GOST
	TLSEXT_hash_streebog_512, TLSEXT_signature_gostr12_512,
#endif

	TLSEXT_hash_sha384, TLSEXT_signature_rsa,
	TLSEXT_hash_sha384, TLSEXT_signature_ecdsa,

	TLSEXT_hash_sha256, TLSEXT_signature_rsa,
	TLSEXT_hash_sha256, TLSEXT_signature_ecdsa,

#ifndef OPENSSL_NO_GOST
	TLSEXT_hash_streebog_256, TLSEXT_signature_gostr12_256,
	TLSEXT_hash_gost94, TLSEXT_signature_gostr01,
#endif

	TLSEXT_hash_sha224, TLSEXT_signature_rsa,
	TLSEXT_hash_sha224, TLSEXT_signature_ecdsa,

	TLSEXT_hash_sha1, TLSEXT_signature_rsa,
	TLSEXT_hash_sha1, TLSEXT_signature_ecdsa,
};

void
tls12_get_req_sig_algs(SSL *s, unsigned char **sigalgs, size_t *sigalgs_len)
{
	*sigalgs = tls12_sigalgs;
	*sigalgs_len = sizeof(tls12_sigalgs);
}

unsigned char *
ssl_add_clienthello_tlsext(SSL *s, unsigned char *p, unsigned char *limit)
{
	size_t len;
	CBB cbb;

	if (p >= limit)
		return NULL;

	if (!CBB_init_fixed(&cbb, p, limit - p))
		return NULL;
	if (!tlsext_clienthello_build(s, &cbb)) {
		CBB_cleanup(&cbb);
		return NULL;
	}
	if (!CBB_finish(&cbb, NULL, &len)) {
		CBB_cleanup(&cbb);
		return NULL;
	}

	return (p + len);
}

unsigned char *
ssl_add_serverhello_tlsext(SSL *s, unsigned char *p, unsigned char *limit)
{
	size_t len;
	CBB cbb;

	if (p >= limit)
		return NULL;

	if (!CBB_init_fixed(&cbb, p, limit - p))
		return NULL;
	if (!tlsext_serverhello_build(s, &cbb)) {
		CBB_cleanup(&cbb);
		return NULL;
	}
	if (!CBB_finish(&cbb, NULL, &len)) {
		CBB_cleanup(&cbb);
		return NULL;
	}

	return (p + len);
}

int
ssl_parse_clienthello_tlsext(SSL *s, unsigned char **p, unsigned char *d,
    int n, int *al)
{
	unsigned short type;
	unsigned short size;
	unsigned short len;
	unsigned char *data = *p;
	unsigned char *end = d + n;
	CBS cbs;

	s->internal->servername_done = 0;
	s->tlsext_status_type = -1;
	S3I(s)->renegotiate_seen = 0;
	free(S3I(s)->alpn_selected);
	S3I(s)->alpn_selected = NULL;
	s->internal->srtp_profile = NULL;

	if (data == end)
		goto ri_check;

	if (end - data < 2)
		goto err;
	n2s(data, len);

	if (end - data != len)
		goto err;

	while (end - data >= 4) {
		n2s(data, type);
		n2s(data, size);

		if (end - data < size)
			goto err;

		if (s->internal->tlsext_debug_cb)
			s->internal->tlsext_debug_cb(s, 0, type, data, size,
			    s->internal->tlsext_debug_arg);

		CBS_init(&cbs, data, size);
		if (!tlsext_clienthello_parse_one(s, &cbs, type, al))
			return 0;

		data += size;
	}

	/* Spurious data on the end */
	if (data != end)
		goto err;

	*p = data;

ri_check:

	/* Need RI if renegotiating */

	if (!S3I(s)->renegotiate_seen && s->internal->renegotiate) {
		*al = SSL_AD_HANDSHAKE_FAILURE;
		SSLerror(s, SSL_R_UNSAFE_LEGACY_RENEGOTIATION_DISABLED);
		return 0;
	}

	return 1;

err:
	*al = SSL_AD_DECODE_ERROR;
	return 0;
}

int
ssl_parse_serverhello_tlsext(SSL *s, unsigned char **p, size_t n, int *al)
{
	unsigned short type;
	unsigned short size;
	unsigned short len;
	unsigned char *data = *p;
	unsigned char *end = *p + n;
	CBS cbs;

	S3I(s)->renegotiate_seen = 0;
	free(S3I(s)->alpn_selected);
	S3I(s)->alpn_selected = NULL;

	if (data == end)
		goto ri_check;

	if (end - data < 2)
		goto err;
	n2s(data, len);

	if (end - data != len)
		goto err;

	while (end - data >= 4) {
		n2s(data, type);
		n2s(data, size);

		if (end - data < size)
			goto err;

		if (s->internal->tlsext_debug_cb)
			s->internal->tlsext_debug_cb(s, 1, type, data, size,
			    s->internal->tlsext_debug_arg);

		CBS_init(&cbs, data, size);
		if (!tlsext_serverhello_parse_one(s, &cbs, type, al))
			return 0;

		data += size;

	}

	if (data != end) {
		*al = SSL_AD_DECODE_ERROR;
		return 0;
	}

	*p = data;

ri_check:

	/* Determine if we need to see RI. Strictly speaking if we want to
	 * avoid an attack we should *always* see RI even on initial server
	 * hello because the client doesn't see any renegotiation during an
	 * attack. However this would mean we could not connect to any server
	 * which doesn't support RI so for the immediate future tolerate RI
	 * absence on initial connect only.
	 */
	if (!S3I(s)->renegotiate_seen &&
	    !(s->internal->options & SSL_OP_LEGACY_SERVER_CONNECT)) {
		*al = SSL_AD_HANDSHAKE_FAILURE;
		SSLerror(s, SSL_R_UNSAFE_LEGACY_RENEGOTIATION_DISABLED);
		return 0;
	}

	return 1;

err:
	*al = SSL_AD_DECODE_ERROR;
	return 0;
}

int
ssl_check_clienthello_tlsext_early(SSL *s)
{
	int ret = SSL_TLSEXT_ERR_NOACK;
	int al = SSL_AD_UNRECOGNIZED_NAME;

	/* The handling of the ECPointFormats extension is done elsewhere, namely in
	 * ssl3_choose_cipher in s3_lib.c.
	 */
	/* The handling of the EllipticCurves extension is done elsewhere, namely in
	 * ssl3_choose_cipher in s3_lib.c.
	 */

	if (s->ctx != NULL && s->ctx->internal->tlsext_servername_callback != 0)
		ret = s->ctx->internal->tlsext_servername_callback(s, &al,
		    s->ctx->internal->tlsext_servername_arg);
	else if (s->initial_ctx != NULL && s->initial_ctx->internal->tlsext_servername_callback != 0)
		ret = s->initial_ctx->internal->tlsext_servername_callback(s, &al,
		    s->initial_ctx->internal->tlsext_servername_arg);

	switch (ret) {
	case SSL_TLSEXT_ERR_ALERT_FATAL:
		ssl3_send_alert(s, SSL3_AL_FATAL, al);
		return -1;
	case SSL_TLSEXT_ERR_ALERT_WARNING:
		ssl3_send_alert(s, SSL3_AL_WARNING, al);
		return 1;
	case SSL_TLSEXT_ERR_NOACK:
		s->internal->servername_done = 0;
	default:
		return 1;
	}
}

int
ssl_check_clienthello_tlsext_late(SSL *s)
{
	int ret = SSL_TLSEXT_ERR_OK;
	int al = 0;	/* XXX gcc3 */

	/* If status request then ask callback what to do.
 	 * Note: this must be called after servername callbacks in case
 	 * the certificate has changed, and must be called after the cipher
	 * has been chosen because this may influence which certificate is sent
 	 */
	if ((s->tlsext_status_type != -1) &&
	    s->ctx && s->ctx->internal->tlsext_status_cb) {
		int r;
		CERT_PKEY *certpkey;
		certpkey = ssl_get_server_send_pkey(s);
		/* If no certificate can't return certificate status */
		if (certpkey == NULL) {
			s->internal->tlsext_status_expected = 0;
			return 1;
		}
		/* Set current certificate to one we will use so
		 * SSL_get_certificate et al can pick it up.
		 */
		s->cert->key = certpkey;
		r = s->ctx->internal->tlsext_status_cb(s,
		    s->ctx->internal->tlsext_status_arg);
		switch (r) {
			/* We don't want to send a status request response */
		case SSL_TLSEXT_ERR_NOACK:
			s->internal->tlsext_status_expected = 0;
			break;
			/* status request response should be sent */
		case SSL_TLSEXT_ERR_OK:
			if (s->internal->tlsext_ocsp_resp)
				s->internal->tlsext_status_expected = 1;
			else
				s->internal->tlsext_status_expected = 0;
			break;
			/* something bad happened */
		case SSL_TLSEXT_ERR_ALERT_FATAL:
			ret = SSL_TLSEXT_ERR_ALERT_FATAL;
			al = SSL_AD_INTERNAL_ERROR;
			goto err;
		}
	} else
		s->internal->tlsext_status_expected = 0;

err:
	switch (ret) {
	case SSL_TLSEXT_ERR_ALERT_FATAL:
		ssl3_send_alert(s, SSL3_AL_FATAL, al);
		return -1;
	case SSL_TLSEXT_ERR_ALERT_WARNING:
		ssl3_send_alert(s, SSL3_AL_WARNING, al);
		return 1;
	default:
		return 1;
	}
}

int
ssl_check_serverhello_tlsext(SSL *s)
{
	int ret = SSL_TLSEXT_ERR_NOACK;
	int al = SSL_AD_UNRECOGNIZED_NAME;

	ret = SSL_TLSEXT_ERR_OK;

	if (s->ctx != NULL && s->ctx->internal->tlsext_servername_callback != 0)
		ret = s->ctx->internal->tlsext_servername_callback(s, &al,
		    s->ctx->internal->tlsext_servername_arg);
	else if (s->initial_ctx != NULL && s->initial_ctx->internal->tlsext_servername_callback != 0)
		ret = s->initial_ctx->internal->tlsext_servername_callback(s, &al,
		    s->initial_ctx->internal->tlsext_servername_arg);

	/* If we've requested certificate status and we wont get one
 	 * tell the callback
 	 */
	if ((s->tlsext_status_type != -1) && !(s->internal->tlsext_status_expected) &&
	    s->ctx && s->ctx->internal->tlsext_status_cb) {
		int r;
		/* Set resp to NULL, resplen to -1 so callback knows
 		 * there is no response.
 		 */
		free(s->internal->tlsext_ocsp_resp);
		s->internal->tlsext_ocsp_resp = NULL;
		s->internal->tlsext_ocsp_resplen = -1;
		r = s->ctx->internal->tlsext_status_cb(s,
		    s->ctx->internal->tlsext_status_arg);
		if (r == 0) {
			al = SSL_AD_BAD_CERTIFICATE_STATUS_RESPONSE;
			ret = SSL_TLSEXT_ERR_ALERT_FATAL;
		}
		if (r < 0) {
			al = SSL_AD_INTERNAL_ERROR;
			ret = SSL_TLSEXT_ERR_ALERT_FATAL;
		}
	}

	switch (ret) {
	case SSL_TLSEXT_ERR_ALERT_FATAL:
		ssl3_send_alert(s, SSL3_AL_FATAL, al);

		return -1;
	case SSL_TLSEXT_ERR_ALERT_WARNING:
		ssl3_send_alert(s, SSL3_AL_WARNING, al);

		return 1;
	case SSL_TLSEXT_ERR_NOACK:
		s->internal->servername_done = 0;
	default:
		return 1;
	}
}

/* Since the server cache lookup is done early on in the processing of the
 * ClientHello, and other operations depend on the result, we need to handle
 * any TLS session ticket extension at the same time.
 *
 *   session_id: points at the session ID in the ClientHello. This code will
 *       read past the end of this in order to parse out the session ticket
 *       extension, if any.
 *   len: the length of the session ID.
 *   limit: a pointer to the first byte after the ClientHello.
 *   ret: (output) on return, if a ticket was decrypted, then this is set to
 *       point to the resulting session.
 *
 * If s->internal->tls_session_secret_cb is set then we are expecting a pre-shared key
 * ciphersuite, in which case we have no use for session tickets and one will
 * never be decrypted, nor will s->internal->tlsext_ticket_expected be set to 1.
 *
 * Returns:
 *   -1: fatal error, either from parsing or decrypting the ticket.
 *    0: no ticket was found (or was ignored, based on settings).
 *    1: a zero length extension was found, indicating that the client supports
 *       session tickets but doesn't currently have one to offer.
 *    2: either s->internal->tls_session_secret_cb was set, or a ticket was offered but
 *       couldn't be decrypted because of a non-fatal error.
 *    3: a ticket was successfully decrypted and *ret was set.
 *
 * Side effects:
 *   Sets s->internal->tlsext_ticket_expected to 1 if the server will have to issue
 *   a new session ticket to the client because the client indicated support
 *   (and s->internal->tls_session_secret_cb is NULL) but the client either doesn't have
 *   a session ticket or we couldn't use the one it gave us, or if
 *   s->ctx->tlsext_ticket_key_cb asked to renew the client's ticket.
 *   Otherwise, s->internal->tlsext_ticket_expected is set to 0.
 */
int
tls1_process_ticket(SSL *s, const unsigned char *session, int session_len,
    const unsigned char *limit, SSL_SESSION **ret)
{
	/* Point after session ID in client hello */
	CBS session_id, cookie, cipher_list, compress_algo, extensions;

	*ret = NULL;
	s->internal->tlsext_ticket_expected = 0;

	/* If tickets disabled behave as if no ticket present
	 * to permit stateful resumption.
	 */
	if (SSL_get_options(s) & SSL_OP_NO_TICKET)
		return 0;
	if (!limit)
		return 0;

	if (limit < session)
		return -1;

	CBS_init(&session_id, session, limit - session);

	/* Skip past the session id */
	if (!CBS_skip(&session_id, session_len))
		return -1;

	/* Skip past DTLS cookie */
	if (SSL_IS_DTLS(s)) {
		if (!CBS_get_u8_length_prefixed(&session_id, &cookie))
			return -1;
	}

	/* Skip past cipher list */
	if (!CBS_get_u16_length_prefixed(&session_id, &cipher_list))
		return -1;

	/* Skip past compression algorithm list */
	if (!CBS_get_u8_length_prefixed(&session_id, &compress_algo))
		return -1;

	/* Now at start of extensions */
	if (CBS_len(&session_id) == 0)
		return 0;
	if (!CBS_get_u16_length_prefixed(&session_id, &extensions))
		return -1;

	while (CBS_len(&extensions) > 0) {
		CBS ext_data;
		uint16_t ext_type;

		if (!CBS_get_u16(&extensions, &ext_type) ||
		    !CBS_get_u16_length_prefixed(&extensions, &ext_data))
			return -1;

		if (ext_type == TLSEXT_TYPE_session_ticket) {
			int r;
			if (CBS_len(&ext_data) == 0) {
				/* The client will accept a ticket but doesn't
				 * currently have one. */
				s->internal->tlsext_ticket_expected = 1;
				return 1;
			}
			if (s->internal->tls_session_secret_cb) {
				/* Indicate that the ticket couldn't be
				 * decrypted rather than generating the session
				 * from ticket now, trigger abbreviated
				 * handshake based on external mechanism to
				 * calculate the master secret later. */
				return 2;
			}

			r = tls_decrypt_ticket(s, CBS_data(&ext_data),
			    CBS_len(&ext_data), session, session_len, ret);

			switch (r) {
			case 2: /* ticket couldn't be decrypted */
				s->internal->tlsext_ticket_expected = 1;
				return 2;
			case 3: /* ticket was decrypted */
				return r;
			case 4: /* ticket decrypted but need to renew */
				s->internal->tlsext_ticket_expected = 1;
				return 3;
			default: /* fatal error */
				return -1;
			}
		}
	}
	return 0;
}

/* tls_decrypt_ticket attempts to decrypt a session ticket.
 *
 *   etick: points to the body of the session ticket extension.
 *   eticklen: the length of the session tickets extenion.
 *   sess_id: points at the session ID.
 *   sesslen: the length of the session ID.
 *   psess: (output) on return, if a ticket was decrypted, then this is set to
 *       point to the resulting session.
 *
 * Returns:
 *   -1: fatal error, either from parsing or decrypting the ticket.
 *    2: the ticket couldn't be decrypted.
 *    3: a ticket was successfully decrypted and *psess was set.
 *    4: same as 3, but the ticket needs to be renewed.
 */
static int
tls_decrypt_ticket(SSL *s, const unsigned char *etick, int eticklen,
    const unsigned char *sess_id, int sesslen, SSL_SESSION **psess)
{
	SSL_SESSION *sess;
	unsigned char *sdec;
	const unsigned char *p;
	int slen, mlen, renew_ticket = 0;
	unsigned char tick_hmac[EVP_MAX_MD_SIZE];
	HMAC_CTX hctx;
	EVP_CIPHER_CTX ctx;
	SSL_CTX *tctx = s->initial_ctx;

	/*
	 * The API guarantees EVP_MAX_IV_LENGTH bytes of space for
	 * the iv to tlsext_ticket_key_cb().  Since the total space
	 * required for a session cookie is never less than this,
	 * this check isn't too strict.  The exact check comes later.
	 */
	if (eticklen < 16 + EVP_MAX_IV_LENGTH)
		return 2;

	/* Initialize session ticket encryption and HMAC contexts */
	HMAC_CTX_init(&hctx);
	EVP_CIPHER_CTX_init(&ctx);
	if (tctx->internal->tlsext_ticket_key_cb) {
		unsigned char *nctick = (unsigned char *)etick;
		int rv = tctx->internal->tlsext_ticket_key_cb(s,
		    nctick, nctick + 16, &ctx, &hctx, 0);
		if (rv < 0) {
			HMAC_CTX_cleanup(&hctx);
			EVP_CIPHER_CTX_cleanup(&ctx);
			return -1;
		}
		if (rv == 0) {
			HMAC_CTX_cleanup(&hctx);
			EVP_CIPHER_CTX_cleanup(&ctx);
			return 2;
		}
		if (rv == 2)
			renew_ticket = 1;
	} else {
		/* Check key name matches */
		if (timingsafe_memcmp(etick,
		    tctx->internal->tlsext_tick_key_name, 16))
			return 2;
		HMAC_Init_ex(&hctx, tctx->internal->tlsext_tick_hmac_key,
		    16, tlsext_tick_md(), NULL);
		EVP_DecryptInit_ex(&ctx, EVP_aes_128_cbc(), NULL,
		    tctx->internal->tlsext_tick_aes_key, etick + 16);
	}

	/*
	 * Attempt to process session ticket, first conduct sanity and
	 * integrity checks on ticket.
	 */
	mlen = HMAC_size(&hctx);
	if (mlen < 0) {
		HMAC_CTX_cleanup(&hctx);
		EVP_CIPHER_CTX_cleanup(&ctx);
		return -1;
	}

	/* Sanity check ticket length: must exceed keyname + IV + HMAC */
	if (eticklen <= 16 + EVP_CIPHER_CTX_iv_length(&ctx) + mlen) {
		HMAC_CTX_cleanup(&hctx);
		EVP_CIPHER_CTX_cleanup(&ctx);
		return 2;
	}
	eticklen -= mlen;

	/* Check HMAC of encrypted ticket */
	if (HMAC_Update(&hctx, etick, eticklen) <= 0 ||
	    HMAC_Final(&hctx, tick_hmac, NULL) <= 0) {
		HMAC_CTX_cleanup(&hctx);
		EVP_CIPHER_CTX_cleanup(&ctx);
		return -1;
	}

	HMAC_CTX_cleanup(&hctx);
	if (timingsafe_memcmp(tick_hmac, etick + eticklen, mlen)) {
		EVP_CIPHER_CTX_cleanup(&ctx);
		return 2;
	}

	/* Attempt to decrypt session data */
	/* Move p after IV to start of encrypted ticket, update length */
	p = etick + 16 + EVP_CIPHER_CTX_iv_length(&ctx);
	eticklen -= 16 + EVP_CIPHER_CTX_iv_length(&ctx);
	sdec = malloc(eticklen);
	if (sdec == NULL ||
	    EVP_DecryptUpdate(&ctx, sdec, &slen, p, eticklen) <= 0) {
		free(sdec);
		EVP_CIPHER_CTX_cleanup(&ctx);
		return -1;
	}
	if (EVP_DecryptFinal_ex(&ctx, sdec + slen, &mlen) <= 0) {
		free(sdec);
		EVP_CIPHER_CTX_cleanup(&ctx);
		return 2;
	}
	slen += mlen;
	EVP_CIPHER_CTX_cleanup(&ctx);
	p = sdec;

	sess = d2i_SSL_SESSION(NULL, &p, slen);
	free(sdec);
	if (sess) {
		/* The session ID, if non-empty, is used by some clients to
		 * detect that the ticket has been accepted. So we copy it to
		 * the session structure. If it is empty set length to zero
		 * as required by standard.
		 */
		if (sesslen)
			memcpy(sess->session_id, sess_id, sesslen);
		sess->session_id_length = sesslen;
		*psess = sess;
		if (renew_ticket)
			return 4;
		else
			return 3;
	}
	ERR_clear_error();
	/* For session parse failure, indicate that we need to send a new
	 * ticket. */
	return 2;
}

/* Tables to translate from NIDs to TLS v1.2 ids */

typedef struct {
	int nid;
	int id;
} tls12_lookup;

static tls12_lookup tls12_md[] = {
	{NID_md5, TLSEXT_hash_md5},
	{NID_sha1, TLSEXT_hash_sha1},
	{NID_sha224, TLSEXT_hash_sha224},
	{NID_sha256, TLSEXT_hash_sha256},
	{NID_sha384, TLSEXT_hash_sha384},
	{NID_sha512, TLSEXT_hash_sha512},
	{NID_id_GostR3411_94, TLSEXT_hash_gost94},
	{NID_id_tc26_gost3411_2012_256, TLSEXT_hash_streebog_256},
	{NID_id_tc26_gost3411_2012_512, TLSEXT_hash_streebog_512}
};

static tls12_lookup tls12_sig[] = {
	{EVP_PKEY_RSA, TLSEXT_signature_rsa},
	{EVP_PKEY_EC, TLSEXT_signature_ecdsa},
	{EVP_PKEY_GOSTR01, TLSEXT_signature_gostr01},
};

static int
tls12_find_id(int nid, tls12_lookup *table, size_t tlen)
{
	size_t i;
	for (i = 0; i < tlen; i++) {
		if (table[i].nid == nid)
			return table[i].id;
	}
	return -1;
}

int
tls12_get_sigandhash(unsigned char *p, const EVP_PKEY *pk, const EVP_MD *md)
{
	int sig_id, md_id;
	if (!md)
		return 0;
	md_id = tls12_find_id(EVP_MD_type(md), tls12_md,
	    sizeof(tls12_md) / sizeof(tls12_lookup));
	if (md_id == -1)
		return 0;
	sig_id = tls12_get_sigid(pk);
	if (sig_id == -1)
		return 0;
	p[0] = (unsigned char)md_id;
	p[1] = (unsigned char)sig_id;
	return 1;
}

int
tls12_get_sigid(const EVP_PKEY *pk)
{
	return tls12_find_id(pk->type, tls12_sig,
	    sizeof(tls12_sig) / sizeof(tls12_lookup));
}

const EVP_MD *
tls12_get_hash(unsigned char hash_alg)
{
	switch (hash_alg) {
	case TLSEXT_hash_sha1:
		return EVP_sha1();
	case TLSEXT_hash_sha224:
		return EVP_sha224();
	case TLSEXT_hash_sha256:
		return EVP_sha256();
	case TLSEXT_hash_sha384:
		return EVP_sha384();
	case TLSEXT_hash_sha512:
		return EVP_sha512();
#ifndef OPENSSL_NO_GOST
	case TLSEXT_hash_gost94:
		return EVP_gostr341194();
	case TLSEXT_hash_streebog_256:
		return EVP_streebog256();
	case TLSEXT_hash_streebog_512:
		return EVP_streebog512();
#endif
	default:
		return NULL;
	}
}

/* Set preferred digest for each key type */

int
tls1_process_sigalgs(SSL *s, CBS *cbs)
{
	const EVP_MD *md;
	CERT *c = s->cert;
	int idx;

	/* Extension ignored for inappropriate versions */
	if (!SSL_USE_SIGALGS(s))
		return 1;

	/* Should never happen */
	if (c == NULL)
		return 0;

	c->pkeys[SSL_PKEY_RSA_SIGN].digest = NULL;
	c->pkeys[SSL_PKEY_RSA_ENC].digest = NULL;
	c->pkeys[SSL_PKEY_ECC].digest = NULL;
	c->pkeys[SSL_PKEY_GOST01].digest = NULL;

	while (CBS_len(cbs) > 0) {
		uint8_t hash_alg, sig_alg;

		if (!CBS_get_u8(cbs, &hash_alg) || !CBS_get_u8(cbs, &sig_alg))
			return 0;

		switch (sig_alg) {
		case TLSEXT_signature_rsa:
			idx = SSL_PKEY_RSA_SIGN;
			break;
		case TLSEXT_signature_ecdsa:
			idx = SSL_PKEY_ECC;
			break;
		case TLSEXT_signature_gostr01:
		case TLSEXT_signature_gostr12_256:
		case TLSEXT_signature_gostr12_512:
			idx = SSL_PKEY_GOST01;
			break;
		default:
			continue;
		}

		if (c->pkeys[idx].digest == NULL) {
			md = tls12_get_hash(hash_alg);
			if (md) {
				c->pkeys[idx].digest = md;
				if (idx == SSL_PKEY_RSA_SIGN)
					c->pkeys[SSL_PKEY_RSA_ENC].digest = md;
			}
		}

	}

	/*
	 * Set any remaining keys to default values. NOTE: if alg is not
	 * supported it stays as NULL.
	 */
	if (!c->pkeys[SSL_PKEY_RSA_SIGN].digest) {
		c->pkeys[SSL_PKEY_RSA_SIGN].digest = EVP_sha1();
		c->pkeys[SSL_PKEY_RSA_ENC].digest = EVP_sha1();
	}
	if (!c->pkeys[SSL_PKEY_ECC].digest)
		c->pkeys[SSL_PKEY_ECC].digest = EVP_sha1();
#ifndef OPENSSL_NO_GOST
	if (!c->pkeys[SSL_PKEY_GOST01].digest)
		c->pkeys[SSL_PKEY_GOST01].digest = EVP_gostr341194();
#endif
	return 1;
}
