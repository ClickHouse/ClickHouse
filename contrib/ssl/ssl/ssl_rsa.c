/* $OpenBSD: ssl_rsa.c,v 1.28 2017/02/07 02:08:38 beck Exp $ */
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

#include "ssl_locl.h"

#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/objects.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

static int ssl_set_cert(CERT *c, X509 *x509);
static int ssl_set_pkey(CERT *c, EVP_PKEY *pkey);
static int ssl_ctx_use_certificate_chain_bio(SSL_CTX *, BIO *);

int
SSL_use_certificate(SSL *ssl, X509 *x)
{
	if (x == NULL) {
		SSLerror(ssl, ERR_R_PASSED_NULL_PARAMETER);
		return (0);
	}
	if (!ssl_cert_inst(&ssl->cert)) {
		SSLerror(ssl, ERR_R_MALLOC_FAILURE);
		return (0);
	}
	return (ssl_set_cert(ssl->cert, x));
}

int
SSL_use_certificate_file(SSL *ssl, const char *file, int type)
{
	int j;
	BIO *in;
	int ret = 0;
	X509 *x = NULL;

	in = BIO_new(BIO_s_file_internal());
	if (in == NULL) {
		SSLerror(ssl, ERR_R_BUF_LIB);
		goto end;
	}

	if (BIO_read_filename(in, file) <= 0) {
		SSLerror(ssl, ERR_R_SYS_LIB);
		goto end;
	}
	if (type == SSL_FILETYPE_ASN1) {
		j = ERR_R_ASN1_LIB;
		x = d2i_X509_bio(in, NULL);
	} else if (type == SSL_FILETYPE_PEM) {
		j = ERR_R_PEM_LIB;
		x = PEM_read_bio_X509(in, NULL,
		    ssl->ctx->default_passwd_callback,
		    ssl->ctx->default_passwd_callback_userdata);
	} else {
		SSLerror(ssl, SSL_R_BAD_SSL_FILETYPE);
		goto end;
	}

	if (x == NULL) {
		SSLerror(ssl, j);
		goto end;
	}

	ret = SSL_use_certificate(ssl, x);
end:
	X509_free(x);
	BIO_free(in);
	return (ret);
}

int
SSL_use_certificate_ASN1(SSL *ssl, const unsigned char *d, int len)
{
	X509 *x;
	int ret;

	x = d2i_X509(NULL, &d,(long)len);
	if (x == NULL) {
		SSLerror(ssl, ERR_R_ASN1_LIB);
		return (0);
	}

	ret = SSL_use_certificate(ssl, x);
	X509_free(x);
	return (ret);
}

int
SSL_use_RSAPrivateKey(SSL *ssl, RSA *rsa)
{
	EVP_PKEY *pkey;
	int ret;

	if (rsa == NULL) {
		SSLerror(ssl, ERR_R_PASSED_NULL_PARAMETER);
		return (0);
	}
	if (!ssl_cert_inst(&ssl->cert)) {
		SSLerror(ssl, ERR_R_MALLOC_FAILURE);
		return (0);
	}
	if ((pkey = EVP_PKEY_new()) == NULL) {
		SSLerror(ssl, ERR_R_EVP_LIB);
		return (0);
	}

	RSA_up_ref(rsa);
	EVP_PKEY_assign_RSA(pkey, rsa);

	ret = ssl_set_pkey(ssl->cert, pkey);
	EVP_PKEY_free(pkey);
	return (ret);
}

static int
ssl_set_pkey(CERT *c, EVP_PKEY *pkey)
{
	int i;

	i = ssl_cert_type(NULL, pkey);
	if (i < 0) {
		SSLerrorx(SSL_R_UNKNOWN_CERTIFICATE_TYPE);
		return (0);
	}

	if (c->pkeys[i].x509 != NULL) {
		EVP_PKEY *pktmp;
		pktmp = X509_get_pubkey(c->pkeys[i].x509);
		EVP_PKEY_copy_parameters(pktmp, pkey);
		EVP_PKEY_free(pktmp);
		ERR_clear_error();

		/*
		 * Don't check the public/private key, this is mostly
		 * for smart cards.
		 */
		if ((pkey->type == EVP_PKEY_RSA) &&
			(RSA_flags(pkey->pkey.rsa) & RSA_METHOD_FLAG_NO_CHECK))
;
		else
		if (!X509_check_private_key(c->pkeys[i].x509, pkey)) {
			X509_free(c->pkeys[i].x509);
			c->pkeys[i].x509 = NULL;
			return 0;
		}
	}

	EVP_PKEY_free(c->pkeys[i].privatekey);
	CRYPTO_add(&pkey->references, 1, CRYPTO_LOCK_EVP_PKEY);
	c->pkeys[i].privatekey = pkey;
	c->key = &(c->pkeys[i]);

	c->valid = 0;
	return (1);
}

int
SSL_use_RSAPrivateKey_file(SSL *ssl, const char *file, int type)
{
	int j, ret = 0;
	BIO *in;
	RSA *rsa = NULL;

	in = BIO_new(BIO_s_file_internal());
	if (in == NULL) {
		SSLerror(ssl, ERR_R_BUF_LIB);
		goto end;
	}

	if (BIO_read_filename(in, file) <= 0) {
		SSLerror(ssl, ERR_R_SYS_LIB);
		goto end;
	}
	if (type == SSL_FILETYPE_ASN1) {
		j = ERR_R_ASN1_LIB;
		rsa = d2i_RSAPrivateKey_bio(in, NULL);
	} else if (type == SSL_FILETYPE_PEM) {
		j = ERR_R_PEM_LIB;
		rsa = PEM_read_bio_RSAPrivateKey(in, NULL,
		    ssl->ctx->default_passwd_callback,
		    ssl->ctx->default_passwd_callback_userdata);
	} else {
		SSLerror(ssl, SSL_R_BAD_SSL_FILETYPE);
		goto end;
	}
	if (rsa == NULL) {
		SSLerror(ssl, j);
		goto end;
	}
	ret = SSL_use_RSAPrivateKey(ssl, rsa);
	RSA_free(rsa);
end:
	BIO_free(in);
	return (ret);
}

int
SSL_use_RSAPrivateKey_ASN1(SSL *ssl, unsigned char *d, long len)
{
	int ret;
	const unsigned char *p;
	RSA *rsa;

	p = d;
	if ((rsa = d2i_RSAPrivateKey(NULL, &p,(long)len)) == NULL) {
		SSLerror(ssl, ERR_R_ASN1_LIB);
		return (0);
	}

	ret = SSL_use_RSAPrivateKey(ssl, rsa);
	RSA_free(rsa);
	return (ret);
}

int
SSL_use_PrivateKey(SSL *ssl, EVP_PKEY *pkey)
{
	int ret;

	if (pkey == NULL) {
		SSLerror(ssl, ERR_R_PASSED_NULL_PARAMETER);
		return (0);
	}
	if (!ssl_cert_inst(&ssl->cert)) {
		SSLerror(ssl, ERR_R_MALLOC_FAILURE);
		return (0);
	}
	ret = ssl_set_pkey(ssl->cert, pkey);
	return (ret);
}

int
SSL_use_PrivateKey_file(SSL *ssl, const char *file, int type)
{
	int j, ret = 0;
	BIO *in;
	EVP_PKEY *pkey = NULL;

	in = BIO_new(BIO_s_file_internal());
	if (in == NULL) {
		SSLerror(ssl, ERR_R_BUF_LIB);
		goto end;
	}

	if (BIO_read_filename(in, file) <= 0) {
		SSLerror(ssl, ERR_R_SYS_LIB);
		goto end;
	}
	if (type == SSL_FILETYPE_PEM) {
		j = ERR_R_PEM_LIB;
		pkey = PEM_read_bio_PrivateKey(in, NULL,
		    ssl->ctx->default_passwd_callback,
		    ssl->ctx->default_passwd_callback_userdata);
	} else if (type == SSL_FILETYPE_ASN1) {
		j = ERR_R_ASN1_LIB;
		pkey = d2i_PrivateKey_bio(in, NULL);
	} else {
		SSLerror(ssl, SSL_R_BAD_SSL_FILETYPE);
		goto end;
	}
	if (pkey == NULL) {
		SSLerror(ssl, j);
		goto end;
	}
	ret = SSL_use_PrivateKey(ssl, pkey);
	EVP_PKEY_free(pkey);
end:
	BIO_free(in);
	return (ret);
}

int
SSL_use_PrivateKey_ASN1(int type, SSL *ssl, const unsigned char *d, long len)
{
	int ret;
	const unsigned char *p;
	EVP_PKEY *pkey;

	p = d;
	if ((pkey = d2i_PrivateKey(type, NULL, &p,(long)len)) == NULL) {
		SSLerror(ssl, ERR_R_ASN1_LIB);
		return (0);
	}

	ret = SSL_use_PrivateKey(ssl, pkey);
	EVP_PKEY_free(pkey);
	return (ret);
}

int
SSL_CTX_use_certificate(SSL_CTX *ctx, X509 *x)
{
	if (x == NULL) {
		SSLerrorx(ERR_R_PASSED_NULL_PARAMETER);
		return (0);
	}
	if (!ssl_cert_inst(&ctx->internal->cert)) {
		SSLerrorx(ERR_R_MALLOC_FAILURE);
		return (0);
	}
	return (ssl_set_cert(ctx->internal->cert, x));
}

static int
ssl_set_cert(CERT *c, X509 *x)
{
	EVP_PKEY *pkey;
	int i;

	pkey = X509_get_pubkey(x);
	if (pkey == NULL) {
		SSLerrorx(SSL_R_X509_LIB);
		return (0);
	}

	i = ssl_cert_type(x, pkey);
	if (i < 0) {
		SSLerrorx(SSL_R_UNKNOWN_CERTIFICATE_TYPE);
		EVP_PKEY_free(pkey);
		return (0);
	}

	if (c->pkeys[i].privatekey != NULL) {
		EVP_PKEY_copy_parameters(pkey, c->pkeys[i].privatekey);
		ERR_clear_error();

		/*
		 * Don't check the public/private key, this is mostly
		 * for smart cards.
		 */
		if ((c->pkeys[i].privatekey->type == EVP_PKEY_RSA) &&
			(RSA_flags(c->pkeys[i].privatekey->pkey.rsa) &
		RSA_METHOD_FLAG_NO_CHECK))
;
		else
		if (!X509_check_private_key(x, c->pkeys[i].privatekey)) {
			/*
			 * don't fail for a cert/key mismatch, just free
			 * current private key (when switching to a different
			 * cert & key, first this function should be used,
			 * then ssl_set_pkey
			 */
			EVP_PKEY_free(c->pkeys[i].privatekey);
			c->pkeys[i].privatekey = NULL;
			/* clear error queue */
			ERR_clear_error();
		}
	}

	EVP_PKEY_free(pkey);

	X509_free(c->pkeys[i].x509);
	CRYPTO_add(&x->references, 1, CRYPTO_LOCK_X509);
	c->pkeys[i].x509 = x;
	c->key = &(c->pkeys[i]);

	c->valid = 0;
	return (1);
}

int
SSL_CTX_use_certificate_file(SSL_CTX *ctx, const char *file, int type)
{
	int j;
	BIO *in;
	int ret = 0;
	X509 *x = NULL;

	in = BIO_new(BIO_s_file_internal());
	if (in == NULL) {
		SSLerrorx(ERR_R_BUF_LIB);
		goto end;
	}

	if (BIO_read_filename(in, file) <= 0) {
		SSLerrorx(ERR_R_SYS_LIB);
		goto end;
	}
	if (type == SSL_FILETYPE_ASN1) {
		j = ERR_R_ASN1_LIB;
		x = d2i_X509_bio(in, NULL);
	} else if (type == SSL_FILETYPE_PEM) {
		j = ERR_R_PEM_LIB;
		x = PEM_read_bio_X509(in, NULL, ctx->default_passwd_callback,
		    ctx->default_passwd_callback_userdata);
	} else {
		SSLerrorx(SSL_R_BAD_SSL_FILETYPE);
		goto end;
	}

	if (x == NULL) {
		SSLerrorx(j);
		goto end;
	}

	ret = SSL_CTX_use_certificate(ctx, x);
end:
	X509_free(x);
	BIO_free(in);
	return (ret);
}

int
SSL_CTX_use_certificate_ASN1(SSL_CTX *ctx, int len, const unsigned char *d)
{
	X509 *x;
	int ret;

	x = d2i_X509(NULL, &d,(long)len);
	if (x == NULL) {
		SSLerrorx(ERR_R_ASN1_LIB);
		return (0);
	}

	ret = SSL_CTX_use_certificate(ctx, x);
	X509_free(x);
	return (ret);
}

int
SSL_CTX_use_RSAPrivateKey(SSL_CTX *ctx, RSA *rsa)
{
	int ret;
	EVP_PKEY *pkey;

	if (rsa == NULL) {
		SSLerrorx(ERR_R_PASSED_NULL_PARAMETER);
		return (0);
	}
	if (!ssl_cert_inst(&ctx->internal->cert)) {
		SSLerrorx(ERR_R_MALLOC_FAILURE);
		return (0);
	}
	if ((pkey = EVP_PKEY_new()) == NULL) {
		SSLerrorx(ERR_R_EVP_LIB);
		return (0);
	}

	RSA_up_ref(rsa);
	EVP_PKEY_assign_RSA(pkey, rsa);

	ret = ssl_set_pkey(ctx->internal->cert, pkey);
	EVP_PKEY_free(pkey);
	return (ret);
}

int
SSL_CTX_use_RSAPrivateKey_file(SSL_CTX *ctx, const char *file, int type)
{
	int j, ret = 0;
	BIO *in;
	RSA *rsa = NULL;

	in = BIO_new(BIO_s_file_internal());
	if (in == NULL) {
		SSLerrorx(ERR_R_BUF_LIB);
		goto end;
	}

	if (BIO_read_filename(in, file) <= 0) {
		SSLerrorx(ERR_R_SYS_LIB);
		goto end;
	}
	if (type == SSL_FILETYPE_ASN1) {
		j = ERR_R_ASN1_LIB;
		rsa = d2i_RSAPrivateKey_bio(in, NULL);
	} else if (type == SSL_FILETYPE_PEM) {
		j = ERR_R_PEM_LIB;
		rsa = PEM_read_bio_RSAPrivateKey(in, NULL,
		    ctx->default_passwd_callback,
		    ctx->default_passwd_callback_userdata);
	} else {
		SSLerrorx(SSL_R_BAD_SSL_FILETYPE);
		goto end;
	}
	if (rsa == NULL) {
		SSLerrorx(j);
		goto end;
	}
	ret = SSL_CTX_use_RSAPrivateKey(ctx, rsa);
	RSA_free(rsa);
end:
	BIO_free(in);
	return (ret);
}

int
SSL_CTX_use_RSAPrivateKey_ASN1(SSL_CTX *ctx, const unsigned char *d, long len)
{
	int ret;
	const unsigned char *p;
	RSA *rsa;

	p = d;
	if ((rsa = d2i_RSAPrivateKey(NULL, &p,(long)len)) == NULL) {
		SSLerrorx(ERR_R_ASN1_LIB);
		return (0);
	}

	ret = SSL_CTX_use_RSAPrivateKey(ctx, rsa);
	RSA_free(rsa);
	return (ret);
}

int
SSL_CTX_use_PrivateKey(SSL_CTX *ctx, EVP_PKEY *pkey)
{
	if (pkey == NULL) {
		SSLerrorx(ERR_R_PASSED_NULL_PARAMETER);
		return (0);
	}
	if (!ssl_cert_inst(&ctx->internal->cert)) {
		SSLerrorx(ERR_R_MALLOC_FAILURE);
		return (0);
	}
	return (ssl_set_pkey(ctx->internal->cert, pkey));
}

int
SSL_CTX_use_PrivateKey_file(SSL_CTX *ctx, const char *file, int type)
{
	int j, ret = 0;
	BIO *in;
	EVP_PKEY *pkey = NULL;

	in = BIO_new(BIO_s_file_internal());
	if (in == NULL) {
		SSLerrorx(ERR_R_BUF_LIB);
		goto end;
	}

	if (BIO_read_filename(in, file) <= 0) {
		SSLerrorx(ERR_R_SYS_LIB);
		goto end;
	}
	if (type == SSL_FILETYPE_PEM) {
		j = ERR_R_PEM_LIB;
		pkey = PEM_read_bio_PrivateKey(in, NULL,
		    ctx->default_passwd_callback,
		    ctx->default_passwd_callback_userdata);
	} else if (type == SSL_FILETYPE_ASN1) {
		j = ERR_R_ASN1_LIB;
		pkey = d2i_PrivateKey_bio(in, NULL);
	} else {
		SSLerrorx(SSL_R_BAD_SSL_FILETYPE);
		goto end;
	}
	if (pkey == NULL) {
		SSLerrorx(j);
		goto end;
	}
	ret = SSL_CTX_use_PrivateKey(ctx, pkey);
	EVP_PKEY_free(pkey);
end:
	BIO_free(in);
	return (ret);
}

int
SSL_CTX_use_PrivateKey_ASN1(int type, SSL_CTX *ctx, const unsigned char *d,
    long len)
{
	int ret;
	const unsigned char *p;
	EVP_PKEY *pkey;

	p = d;
	if ((pkey = d2i_PrivateKey(type, NULL, &p,(long)len)) == NULL) {
		SSLerrorx(ERR_R_ASN1_LIB);
		return (0);
	}

	ret = SSL_CTX_use_PrivateKey(ctx, pkey);
	EVP_PKEY_free(pkey);
	return (ret);
}


/*
 * Read a bio that contains our certificate in "PEM" format,
 * possibly followed by a sequence of CA certificates that should be
 * sent to the peer in the Certificate message.
 */
static int
ssl_ctx_use_certificate_chain_bio(SSL_CTX *ctx, BIO *in)
{
	int ret = 0;
	X509 *x = NULL;

	ERR_clear_error(); /* clear error stack for SSL_CTX_use_certificate() */

	x = PEM_read_bio_X509_AUX(in, NULL, ctx->default_passwd_callback,
	    ctx->default_passwd_callback_userdata);
	if (x == NULL) {
		SSLerrorx(ERR_R_PEM_LIB);
		goto end;
	}

	ret = SSL_CTX_use_certificate(ctx, x);

	if (ERR_peek_error() != 0)
		ret = 0;
	/* Key/certificate mismatch doesn't imply ret==0 ... */
	if (ret) {
		/*
		 * If we could set up our certificate, now proceed to
		 * the CA certificates.
		 */
		X509 *ca;
		int r;
		unsigned long err;

		sk_X509_pop_free(ctx->extra_certs, X509_free);
		ctx->extra_certs = NULL;

		while ((ca = PEM_read_bio_X509(in, NULL,
		    ctx->default_passwd_callback,
		    ctx->default_passwd_callback_userdata)) != NULL) {
			r = SSL_CTX_add_extra_chain_cert(ctx, ca);
			if (!r) {
				X509_free(ca);
				ret = 0;
				goto end;
			}
			/*
			 * Note that we must not free r if it was successfully
			 * added to the chain (while we must free the main
			 * certificate, since its reference count is increased
			 * by SSL_CTX_use_certificate).
			 */
		}

		/* When the while loop ends, it's usually just EOF. */
		err = ERR_peek_last_error();
		if (ERR_GET_LIB(err) == ERR_LIB_PEM &&
		    ERR_GET_REASON(err) == PEM_R_NO_START_LINE)
			ERR_clear_error();
		else
			ret = 0; /* some real error */
	}

end:
	X509_free(x);
	return (ret);
}

int
SSL_CTX_use_certificate_chain_file(SSL_CTX *ctx, const char *file)
{
	BIO *in;
	int ret = 0;

	in = BIO_new(BIO_s_file_internal());
	if (in == NULL) {
		SSLerrorx(ERR_R_BUF_LIB);
		goto end;
	}

	if (BIO_read_filename(in, file) <= 0) {
		SSLerrorx(ERR_R_SYS_LIB);
		goto end;
	}

	ret = ssl_ctx_use_certificate_chain_bio(ctx, in);

end:
	BIO_free(in);
	return (ret);
}

int
SSL_CTX_use_certificate_chain_mem(SSL_CTX *ctx, void *buf, int len)
{
	BIO *in;
	int ret = 0;

	in = BIO_new_mem_buf(buf, len);
	if (in == NULL) {
		SSLerrorx(ERR_R_BUF_LIB);
		goto end;
	}

	ret = ssl_ctx_use_certificate_chain_bio(ctx, in);

end:
	BIO_free(in);
	return (ret);
}
