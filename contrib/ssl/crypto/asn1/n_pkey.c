/* $OpenBSD: n_pkey.c,v 1.31 2017/01/29 17:49:22 beck Exp $ */
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
#include <string.h>

#include <openssl/opensslconf.h>

#ifndef OPENSSL_NO_RSA
#include <openssl/asn1t.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/objects.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>

#ifndef OPENSSL_NO_RC4

typedef struct netscape_pkey_st {
	long version;
	X509_ALGOR *algor;
	ASN1_OCTET_STRING *private_key;
} NETSCAPE_PKEY;

typedef struct netscape_encrypted_pkey_st {
	ASN1_OCTET_STRING *os;
	/* This is the same structure as DigestInfo so use it:
	 * although this isn't really anything to do with
	 * digests.
	 */
	X509_SIG *enckey;
} NETSCAPE_ENCRYPTED_PKEY;


static const ASN1_AUX NETSCAPE_ENCRYPTED_PKEY_aux = {
	.flags = ASN1_AFLG_BROKEN,
};
static const ASN1_TEMPLATE NETSCAPE_ENCRYPTED_PKEY_seq_tt[] = {
	{
		.offset = offsetof(NETSCAPE_ENCRYPTED_PKEY, os),
		.field_name = "os",
		.item = &ASN1_OCTET_STRING_it,
	},
	{
		.offset = offsetof(NETSCAPE_ENCRYPTED_PKEY, enckey),
		.field_name = "enckey",
		.item = &X509_SIG_it,
	},
};

const ASN1_ITEM NETSCAPE_ENCRYPTED_PKEY_it = {
	.itype = ASN1_ITYPE_SEQUENCE,
	.utype = V_ASN1_SEQUENCE,
	.templates = NETSCAPE_ENCRYPTED_PKEY_seq_tt,
	.tcount = sizeof(NETSCAPE_ENCRYPTED_PKEY_seq_tt) / sizeof(ASN1_TEMPLATE),
	.funcs = &NETSCAPE_ENCRYPTED_PKEY_aux,
	.size = sizeof(NETSCAPE_ENCRYPTED_PKEY),
	.sname = "NETSCAPE_ENCRYPTED_PKEY",
};

NETSCAPE_ENCRYPTED_PKEY *NETSCAPE_ENCRYPTED_PKEY_new(void);
void NETSCAPE_ENCRYPTED_PKEY_free(NETSCAPE_ENCRYPTED_PKEY *a);
NETSCAPE_ENCRYPTED_PKEY *d2i_NETSCAPE_ENCRYPTED_PKEY(NETSCAPE_ENCRYPTED_PKEY **a, const unsigned char **in, long len);
int i2d_NETSCAPE_ENCRYPTED_PKEY(const NETSCAPE_ENCRYPTED_PKEY *a, unsigned char **out);

NETSCAPE_ENCRYPTED_PKEY *
d2i_NETSCAPE_ENCRYPTED_PKEY(NETSCAPE_ENCRYPTED_PKEY **a, const unsigned char **in, long len)
{
	return (NETSCAPE_ENCRYPTED_PKEY *)ASN1_item_d2i((ASN1_VALUE **)a, in, len,
	    &NETSCAPE_ENCRYPTED_PKEY_it);
}

int
i2d_NETSCAPE_ENCRYPTED_PKEY(const NETSCAPE_ENCRYPTED_PKEY *a, unsigned char **out)
{
	return ASN1_item_i2d((ASN1_VALUE *)a, out, &NETSCAPE_ENCRYPTED_PKEY_it);
}

NETSCAPE_ENCRYPTED_PKEY *
NETSCAPE_ENCRYPTED_PKEY_new(void)
{
	return (NETSCAPE_ENCRYPTED_PKEY *)ASN1_item_new(&NETSCAPE_ENCRYPTED_PKEY_it);
}

void
NETSCAPE_ENCRYPTED_PKEY_free(NETSCAPE_ENCRYPTED_PKEY *a)
{
	ASN1_item_free((ASN1_VALUE *)a, &NETSCAPE_ENCRYPTED_PKEY_it);
}

static const ASN1_TEMPLATE NETSCAPE_PKEY_seq_tt[] = {
	{
		.offset = offsetof(NETSCAPE_PKEY, version),
		.field_name = "version",
		.item = &LONG_it,
	},
	{
		.offset = offsetof(NETSCAPE_PKEY, algor),
		.field_name = "algor",
		.item = &X509_ALGOR_it,
	},
	{
		.offset = offsetof(NETSCAPE_PKEY, private_key),
		.field_name = "private_key",
		.item = &ASN1_OCTET_STRING_it,
	},
};

const ASN1_ITEM NETSCAPE_PKEY_it = {
	.itype = ASN1_ITYPE_SEQUENCE,
	.utype = V_ASN1_SEQUENCE,
	.templates = NETSCAPE_PKEY_seq_tt,
	.tcount = sizeof(NETSCAPE_PKEY_seq_tt) / sizeof(ASN1_TEMPLATE),
	.size = sizeof(NETSCAPE_PKEY),
	.sname = "NETSCAPE_PKEY",
};

NETSCAPE_PKEY *NETSCAPE_PKEY_new(void);
void NETSCAPE_PKEY_free(NETSCAPE_PKEY *a);
NETSCAPE_PKEY *d2i_NETSCAPE_PKEY(NETSCAPE_PKEY **a, const unsigned char **in, long len);
int i2d_NETSCAPE_PKEY(const NETSCAPE_PKEY *a, unsigned char **out);

NETSCAPE_PKEY *
d2i_NETSCAPE_PKEY(NETSCAPE_PKEY **a, const unsigned char **in, long len)
{
	return (NETSCAPE_PKEY *)ASN1_item_d2i((ASN1_VALUE **)a, in, len,
	    &NETSCAPE_PKEY_it);
}

int
i2d_NETSCAPE_PKEY(const NETSCAPE_PKEY *a, unsigned char **out)
{
	return ASN1_item_i2d((ASN1_VALUE *)a, out, &NETSCAPE_PKEY_it);
}

NETSCAPE_PKEY *
NETSCAPE_PKEY_new(void)
{
	return (NETSCAPE_PKEY *)ASN1_item_new(&NETSCAPE_PKEY_it);
}

void
NETSCAPE_PKEY_free(NETSCAPE_PKEY *a)
{
	ASN1_item_free((ASN1_VALUE *)a, &NETSCAPE_PKEY_it);
}

static RSA *d2i_RSA_NET_2(RSA **a, ASN1_OCTET_STRING *os,
    int (*cb)(char *buf, int len, const char *prompt, int verify), int sgckey);

int
i2d_Netscape_RSA(const RSA *a, unsigned char **pp,
    int (*cb)(char *buf, int len, const char *prompt, int verify))
{
	return i2d_RSA_NET(a, pp, cb, 0);
}

int
i2d_RSA_NET(const RSA *a, unsigned char **pp,
    int (*cb)(char *buf, int len, const char *prompt, int verify), int sgckey)
{
	int i, j, ret = 0;
	int rsalen, pkeylen, olen;
	NETSCAPE_PKEY *pkey = NULL;
	NETSCAPE_ENCRYPTED_PKEY *enckey = NULL;
	unsigned char buf[256], *zz;
	unsigned char key[EVP_MAX_KEY_LENGTH];
	EVP_CIPHER_CTX ctx;
	EVP_CIPHER_CTX_init(&ctx);

	if (a == NULL)
		return (0);

	if ((pkey = NETSCAPE_PKEY_new()) == NULL)
		goto err;
	if ((enckey = NETSCAPE_ENCRYPTED_PKEY_new()) == NULL)
		goto err;
	pkey->version = 0;

	pkey->algor->algorithm = OBJ_nid2obj(NID_rsaEncryption);
	if ((pkey->algor->parameter = ASN1_TYPE_new()) == NULL)
		goto err;
	pkey->algor->parameter->type = V_ASN1_NULL;

	rsalen = i2d_RSAPrivateKey(a, NULL);

	/* Fake some octet strings just for the initial length
	 * calculation.
 	 */
	pkey->private_key->length = rsalen;
	pkeylen = i2d_NETSCAPE_PKEY(pkey, NULL);
	enckey->enckey->digest->length = pkeylen;
	enckey->os->length = 11;	/* "private-key" */
	enckey->enckey->algor->algorithm = OBJ_nid2obj(NID_rc4);
	if ((enckey->enckey->algor->parameter = ASN1_TYPE_new()) == NULL)
		goto err;
	enckey->enckey->algor->parameter->type = V_ASN1_NULL;

	if (pp == NULL) {
		olen = i2d_NETSCAPE_ENCRYPTED_PKEY(enckey, NULL);
		NETSCAPE_PKEY_free(pkey);
		NETSCAPE_ENCRYPTED_PKEY_free(enckey);
		return olen;
	}

	/* Since its RC4 encrypted length is actual length */
	if ((zz = malloc(rsalen)) == NULL) {
		ASN1error(ERR_R_MALLOC_FAILURE);
		goto err;
	}

	pkey->private_key->data = zz;
	/* Write out private key encoding */
	i2d_RSAPrivateKey(a, &zz);

	if ((zz = malloc(pkeylen)) == NULL) {
		ASN1error(ERR_R_MALLOC_FAILURE);
		goto err;
	}

	if (!ASN1_STRING_set(enckey->os, "private-key", -1)) {
		ASN1error(ERR_R_MALLOC_FAILURE);
		goto err;
	}
	enckey->enckey->digest->data = zz;
	i2d_NETSCAPE_PKEY(pkey, &zz);

	/* Wipe the private key encoding */
	explicit_bzero(pkey->private_key->data, rsalen);

	if (cb == NULL)
		cb = EVP_read_pw_string;
	i = cb((char *)buf, sizeof(buf), "Enter Private Key password:", 1);
	if (i != 0) {
		ASN1error(ASN1_R_BAD_PASSWORD_READ);
		goto err;
	}
	i = strlen((char *)buf);
	/* If the key is used for SGC the algorithm is modified a little. */
	if (sgckey) {
		if (!EVP_Digest(buf, i, buf, NULL, EVP_md5(), NULL))
			goto err;
		memcpy(buf + 16, "SGCKEYSALT", 10);
		i = 26;
	}

	if (!EVP_BytesToKey(EVP_rc4(), EVP_md5(), NULL, buf, i,1, key, NULL))
		goto err;
	explicit_bzero(buf, sizeof(buf));

	/* Encrypt private key in place */
	zz = enckey->enckey->digest->data;
	if (!EVP_EncryptInit_ex(&ctx, EVP_rc4(), NULL, key, NULL))
		goto err;
	if (!EVP_EncryptUpdate(&ctx, zz, &i, zz, pkeylen))
		goto err;
	if (!EVP_EncryptFinal_ex(&ctx, zz + i, &j))
		goto err;

	ret = i2d_NETSCAPE_ENCRYPTED_PKEY(enckey, pp);
err:
	EVP_CIPHER_CTX_cleanup(&ctx);
	NETSCAPE_ENCRYPTED_PKEY_free(enckey);
	NETSCAPE_PKEY_free(pkey);
	return (ret);
}


RSA *
d2i_Netscape_RSA(RSA **a, const unsigned char **pp, long length,
    int (*cb)(char *buf, int len, const char *prompt, int verify))
{
	return d2i_RSA_NET(a, pp, length, cb, 0);
}

RSA *
d2i_RSA_NET(RSA **a, const unsigned char **pp, long length,
    int (*cb)(char *buf, int len, const char *prompt, int verify), int sgckey)
{
	RSA *ret = NULL;
	const unsigned char *p;
	NETSCAPE_ENCRYPTED_PKEY *enckey = NULL;

	p = *pp;

	enckey = d2i_NETSCAPE_ENCRYPTED_PKEY(NULL, &p, length);
	if (!enckey) {
		ASN1error(ASN1_R_DECODING_ERROR);
		return NULL;
	}

	/* XXX 11 == strlen("private-key") */
	if (enckey->os->length != 11 ||
	    memcmp("private-key", enckey->os->data, 11) != 0) {
		ASN1error(ASN1_R_PRIVATE_KEY_HEADER_MISSING);
		goto err;
	}
	if (OBJ_obj2nid(enckey->enckey->algor->algorithm) != NID_rc4) {
		ASN1error(ASN1_R_UNSUPPORTED_ENCRYPTION_ALGORITHM);
		goto err;
	}
	if (cb == NULL)
		cb = EVP_read_pw_string;
	if ((ret = d2i_RSA_NET_2(a, enckey->enckey->digest, cb,
	    sgckey)) == NULL)
		goto err;

	*pp = p;

err:
	NETSCAPE_ENCRYPTED_PKEY_free(enckey);
	return ret;

}

static RSA *
d2i_RSA_NET_2(RSA **a, ASN1_OCTET_STRING *os,
    int (*cb)(char *buf, int len, const char *prompt, int verify), int sgckey)
{
	NETSCAPE_PKEY *pkey = NULL;
	RSA *ret = NULL;
	int i, j;
	unsigned char buf[256];
	const unsigned char *zz;
	unsigned char key[EVP_MAX_KEY_LENGTH];
	EVP_CIPHER_CTX ctx;
	EVP_CIPHER_CTX_init(&ctx);

	i=cb((char *)buf, sizeof(buf), "Enter Private Key password:",0);
	if (i != 0) {
		ASN1error(ASN1_R_BAD_PASSWORD_READ);
		goto err;
	}

	i = strlen((char *)buf);
	if (sgckey){
		if (!EVP_Digest(buf, i, buf, NULL, EVP_md5(), NULL))
			goto err;
		memcpy(buf + 16, "SGCKEYSALT", 10);
		i = 26;
	}

	if (!EVP_BytesToKey(EVP_rc4(), EVP_md5(), NULL, buf, i,1, key, NULL))
		goto err;
	explicit_bzero(buf, sizeof(buf));

	if (!EVP_DecryptInit_ex(&ctx, EVP_rc4(), NULL, key, NULL))
		goto err;
	if (!EVP_DecryptUpdate(&ctx, os->data, &i, os->data, os->length))
		goto err;
	if (!EVP_DecryptFinal_ex(&ctx, &(os->data[i]), &j))
		goto err;
	os->length = i + j;

	zz = os->data;

	if ((pkey = d2i_NETSCAPE_PKEY(NULL, &zz, os->length)) == NULL) {
		ASN1error(ASN1_R_UNABLE_TO_DECODE_RSA_PRIVATE_KEY);
		goto err;
	}

	zz = pkey->private_key->data;
	if ((ret = d2i_RSAPrivateKey(a, &zz,
	    pkey->private_key->length)) == NULL) {
		ASN1error(ASN1_R_UNABLE_TO_DECODE_RSA_KEY);
		goto err;
	}

err:
	EVP_CIPHER_CTX_cleanup(&ctx);
	NETSCAPE_PKEY_free(pkey);
	return (ret);
}

#endif /* OPENSSL_NO_RC4 */

#endif
