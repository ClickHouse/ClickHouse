/* $OpenBSD: e_chacha20poly1305.c,v 1.18 2017/08/28 17:48:02 jsing Exp $ */

/*
 * Copyright (c) 2015 Reyk Floter <reyk@openbsd.org>
 * Copyright (c) 2014, Google Inc.
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
 * OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <stdint.h>
#include <string.h>

#include <openssl/opensslconf.h>

#if !defined(OPENSSL_NO_CHACHA) && !defined(OPENSSL_NO_POLY1305)

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/chacha.h>
#include <openssl/poly1305.h>

#include "evp_locl.h"

#define POLY1305_TAG_LEN 16

#define CHACHA20_CONSTANT_LEN 4
#define CHACHA20_IV_LEN 8
#define CHACHA20_NONCE_LEN (CHACHA20_CONSTANT_LEN + CHACHA20_IV_LEN)

struct aead_chacha20_poly1305_ctx {
	unsigned char key[32];
	unsigned char tag_len;
};

static int
aead_chacha20_poly1305_init(EVP_AEAD_CTX *ctx, const unsigned char *key,
    size_t key_len, size_t tag_len)
{
	struct aead_chacha20_poly1305_ctx *c20_ctx;

	if (tag_len == 0)
		tag_len = POLY1305_TAG_LEN;

	if (tag_len > POLY1305_TAG_LEN) {
		EVPerror(EVP_R_TOO_LARGE);
		return 0;
	}

	/* Internal error - EVP_AEAD_CTX_init should catch this. */
	if (key_len != sizeof(c20_ctx->key))
		return 0;

	c20_ctx = malloc(sizeof(struct aead_chacha20_poly1305_ctx));
	if (c20_ctx == NULL)
		return 0;

	memcpy(&c20_ctx->key[0], key, key_len);
	c20_ctx->tag_len = tag_len;
	ctx->aead_state = c20_ctx;

	return 1;
}

static void
aead_chacha20_poly1305_cleanup(EVP_AEAD_CTX *ctx)
{
	struct aead_chacha20_poly1305_ctx *c20_ctx = ctx->aead_state;

	freezero(c20_ctx, sizeof(*c20_ctx));
}

static void
poly1305_update_with_length(poly1305_state *poly1305,
    const unsigned char *data, size_t data_len)
{
	size_t j = data_len;
	unsigned char length_bytes[8];
	unsigned i;

	for (i = 0; i < sizeof(length_bytes); i++) {
		length_bytes[i] = j;
		j >>= 8;
	}

	if (data != NULL)
		CRYPTO_poly1305_update(poly1305, data, data_len);
	CRYPTO_poly1305_update(poly1305, length_bytes, sizeof(length_bytes));
}

static void
poly1305_update_with_pad16(poly1305_state *poly1305,
    const unsigned char *data, size_t data_len)
{
	static const unsigned char zero_pad16[16];
	size_t pad_len;

	CRYPTO_poly1305_update(poly1305, data, data_len);

	/* pad16() is defined in RFC 7539 2.8.1. */
	if ((pad_len = data_len % 16) == 0)
		return;

	CRYPTO_poly1305_update(poly1305, zero_pad16, 16 - pad_len);
}

static int
aead_chacha20_poly1305_seal(const EVP_AEAD_CTX *ctx, unsigned char *out,
    size_t *out_len, size_t max_out_len, const unsigned char *nonce,
    size_t nonce_len, const unsigned char *in, size_t in_len,
    const unsigned char *ad, size_t ad_len)
{
	const struct aead_chacha20_poly1305_ctx *c20_ctx = ctx->aead_state;
	unsigned char poly1305_key[32];
	poly1305_state poly1305;
	const unsigned char *iv;
	const uint64_t in_len_64 = in_len;
	uint64_t ctr;

	/* The underlying ChaCha implementation may not overflow the block
	 * counter into the second counter word. Therefore we disallow
	 * individual operations that work on more than 2TB at a time.
	 * in_len_64 is needed because, on 32-bit platforms, size_t is only
	 * 32-bits and this produces a warning because it's always false.
	 * Casting to uint64_t inside the conditional is not sufficient to stop
	 * the warning. */
	if (in_len_64 >= (1ULL << 32) * 64 - 64) {
		EVPerror(EVP_R_TOO_LARGE);
		return 0;
	}

	if (max_out_len < in_len + c20_ctx->tag_len) {
		EVPerror(EVP_R_BUFFER_TOO_SMALL);
		return 0;
	}

	if (nonce_len != ctx->aead->nonce_len) {
		EVPerror(EVP_R_IV_TOO_LARGE);
		return 0;
	}

	ctr = (uint64_t)(nonce[0] | nonce[1] << 8 |
	    nonce[2] << 16 | nonce[3] << 24) << 32;
	iv = nonce + CHACHA20_CONSTANT_LEN;

	memset(poly1305_key, 0, sizeof(poly1305_key));
	CRYPTO_chacha_20(poly1305_key, poly1305_key,
	    sizeof(poly1305_key), c20_ctx->key, iv, ctr);

	CRYPTO_poly1305_init(&poly1305, poly1305_key);
	poly1305_update_with_pad16(&poly1305, ad, ad_len);
	CRYPTO_chacha_20(out, in, in_len, c20_ctx->key, iv, ctr + 1);
	poly1305_update_with_pad16(&poly1305, out, in_len);
	poly1305_update_with_length(&poly1305, NULL, ad_len);
	poly1305_update_with_length(&poly1305, NULL, in_len);

	if (c20_ctx->tag_len != POLY1305_TAG_LEN) {
		unsigned char tag[POLY1305_TAG_LEN];
		CRYPTO_poly1305_finish(&poly1305, tag);
		memcpy(out + in_len, tag, c20_ctx->tag_len);
		*out_len = in_len + c20_ctx->tag_len;
		return 1;
	}

	CRYPTO_poly1305_finish(&poly1305, out + in_len);
	*out_len = in_len + POLY1305_TAG_LEN;
	return 1;
}

static int
aead_chacha20_poly1305_open(const EVP_AEAD_CTX *ctx, unsigned char *out,
    size_t *out_len, size_t max_out_len, const unsigned char *nonce,
    size_t nonce_len, const unsigned char *in, size_t in_len,
    const unsigned char *ad, size_t ad_len)
{
	const struct aead_chacha20_poly1305_ctx *c20_ctx = ctx->aead_state;
	unsigned char mac[POLY1305_TAG_LEN];
	unsigned char poly1305_key[32];
	const unsigned char *iv = nonce;
	poly1305_state poly1305;
	const uint64_t in_len_64 = in_len;
	size_t plaintext_len;
	uint64_t ctr = 0;

	if (in_len < c20_ctx->tag_len) {
		EVPerror(EVP_R_BAD_DECRYPT);
		return 0;
	}

	/* The underlying ChaCha implementation may not overflow the block
	 * counter into the second counter word. Therefore we disallow
	 * individual operations that work on more than 2TB at a time.
	 * in_len_64 is needed because, on 32-bit platforms, size_t is only
	 * 32-bits and this produces a warning because it's always false.
	 * Casting to uint64_t inside the conditional is not sufficient to stop
	 * the warning. */
	if (in_len_64 >= (1ULL << 32) * 64 - 64) {
		EVPerror(EVP_R_TOO_LARGE);
		return 0;
	}

	if (nonce_len != ctx->aead->nonce_len) {
		EVPerror(EVP_R_IV_TOO_LARGE);
		return 0;
	}

	plaintext_len = in_len - c20_ctx->tag_len;

	if (max_out_len < plaintext_len) {
		EVPerror(EVP_R_BUFFER_TOO_SMALL);
		return 0;
	}

	ctr = (uint64_t)(nonce[0] | nonce[1] << 8 |
	    nonce[2] << 16 | nonce[3] << 24) << 32;
	iv = nonce + CHACHA20_CONSTANT_LEN;

	memset(poly1305_key, 0, sizeof(poly1305_key));
	CRYPTO_chacha_20(poly1305_key, poly1305_key,
	    sizeof(poly1305_key), c20_ctx->key, iv, ctr);

	CRYPTO_poly1305_init(&poly1305, poly1305_key);
	poly1305_update_with_pad16(&poly1305, ad, ad_len);
	poly1305_update_with_pad16(&poly1305, in, plaintext_len);
	poly1305_update_with_length(&poly1305, NULL, ad_len);
	poly1305_update_with_length(&poly1305, NULL, plaintext_len);

	CRYPTO_poly1305_finish(&poly1305, mac);

	if (timingsafe_memcmp(mac, in + plaintext_len, c20_ctx->tag_len) != 0) {
		EVPerror(EVP_R_BAD_DECRYPT);
		return 0;
	}

	CRYPTO_chacha_20(out, in, plaintext_len, c20_ctx->key, iv, ctr + 1);
	*out_len = plaintext_len;
	return 1;
}

/* RFC 7539 */
static const EVP_AEAD aead_chacha20_poly1305 = {
	.key_len = 32,
	.nonce_len = CHACHA20_NONCE_LEN,
	.overhead = POLY1305_TAG_LEN,
	.max_tag_len = POLY1305_TAG_LEN,

	.init = aead_chacha20_poly1305_init,
	.cleanup = aead_chacha20_poly1305_cleanup,
	.seal = aead_chacha20_poly1305_seal,
	.open = aead_chacha20_poly1305_open,
};

const EVP_AEAD *
EVP_aead_chacha20_poly1305()
{
	return &aead_chacha20_poly1305;
}

#endif  /* !OPENSSL_NO_CHACHA && !OPENSSL_NO_POLY1305 */
