/* $OpenBSD: t1_hash.c,v 1.2 2017/05/06 16:18:36 jsing Exp $ */
/*
 * Copyright (c) 2017 Joel Sing <jsing@openbsd.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "ssl_locl.h"

#include <openssl/ssl.h>

int
tls1_handshake_hash_init(SSL *s)
{
	const EVP_MD *md;
	long dlen;
	void *data;

	tls1_handshake_hash_free(s);

	if (!ssl_get_handshake_evp_md(s, &md)) {
		SSLerrorx(ERR_R_INTERNAL_ERROR);
		goto err;
	}

	if ((S3I(s)->handshake_hash = EVP_MD_CTX_create()) == NULL) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		goto err;
	}
	if (!EVP_DigestInit_ex(S3I(s)->handshake_hash, md, NULL)) {
		SSLerror(s, ERR_R_EVP_LIB);
		goto err;
	}

	dlen = BIO_get_mem_data(S3I(s)->handshake_buffer, &data);
	if (dlen <= 0) {
		SSLerror(s, SSL_R_BAD_HANDSHAKE_LENGTH);
		goto err;
	}
	if (!tls1_handshake_hash_update(s, data, dlen)) {
		SSLerror(s, ERR_R_EVP_LIB);
		goto err;
	}
		
	return 1;

 err:
	tls1_handshake_hash_free(s);

	return 0;
}

int
tls1_handshake_hash_update(SSL *s, const unsigned char *buf, size_t len)
{
	if (S3I(s)->handshake_hash == NULL)
		return 1;

	return EVP_DigestUpdate(S3I(s)->handshake_hash, buf, len);
}

int
tls1_handshake_hash_value(SSL *s, const unsigned char *out, size_t len,
    size_t *outlen)
{
	EVP_MD_CTX *mdctx = NULL;
	unsigned int mdlen;
	int ret = 0;

	if (EVP_MD_CTX_size(S3I(s)->handshake_hash) > len)
		goto err;

	if ((mdctx = EVP_MD_CTX_create()) == NULL) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		goto err;
	}
	if (!EVP_MD_CTX_copy_ex(mdctx, S3I(s)->handshake_hash)) {
		SSLerror(s, ERR_R_EVP_LIB);
		goto err;
	}
	if (!EVP_DigestFinal_ex(mdctx, (unsigned char *)out, &mdlen)) {
		SSLerror(s, ERR_R_EVP_LIB);
		goto err;
	}
	if (outlen != NULL)
		*outlen = mdlen;

	ret = 1;

 err:
	EVP_MD_CTX_destroy(mdctx);

	return (ret);
}

void
tls1_handshake_hash_free(SSL *s)
{
	EVP_MD_CTX_destroy(S3I(s)->handshake_hash);
	S3I(s)->handshake_hash = NULL;
}
