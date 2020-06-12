/* $OpenBSD: tls_conninfo.c,v 1.16 2017/08/27 01:39:26 beck Exp $ */
/*
 * Copyright (c) 2015 Joel Sing <jsing@openbsd.org>
 * Copyright (c) 2015 Bob Beck <beck@openbsd.org>
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

#include <stdio.h>

#include <openssl/x509.h>

#include <tls.h>
#include "tls_internal.h"

int ASN1_time_tm_clamp_notafter(struct tm *tm);

int
tls_hex_string(const unsigned char *in, size_t inlen, char **out,
    size_t *outlen)
{
	static const char hex[] = "0123456789abcdef";
	size_t i, len;
	char *p;

	if (outlen != NULL)
		*outlen = 0;

	if (inlen >= SIZE_MAX)
		return (-1);
	if ((*out = reallocarray(NULL, inlen + 1, 2)) == NULL)
		return (-1);

	p = *out;
	len = 0;
	for (i = 0; i < inlen; i++) {
		p[len++] = hex[(in[i] >> 4) & 0x0f];
		p[len++] = hex[in[i] & 0x0f];
	}
	p[len++] = 0;

	if (outlen != NULL)
		*outlen = len;

	return (0);
}

static int
tls_get_peer_cert_hash(struct tls *ctx, char **hash)
{
	*hash = NULL;
	if (ctx->ssl_peer_cert == NULL)
		return (0);

	if (tls_cert_hash(ctx->ssl_peer_cert, hash) == -1) {
		tls_set_errorx(ctx, "unable to compute peer certificate hash - out of memory");
		*hash = NULL;
		return -1;
	}
	return 0;
}

static int
tls_get_peer_cert_issuer(struct tls *ctx,  char **issuer)
{
	X509_NAME *name = NULL;

	*issuer = NULL;
	if (ctx->ssl_peer_cert == NULL)
		return (-1);
	if ((name = X509_get_issuer_name(ctx->ssl_peer_cert)) == NULL)
		return (-1);
	*issuer = X509_NAME_oneline(name, 0, 0);
	if (*issuer == NULL)
		return (-1);
	return (0);
}

static int
tls_get_peer_cert_subject(struct tls *ctx, char **subject)
{
	X509_NAME *name = NULL;

	*subject = NULL;
	if (ctx->ssl_peer_cert == NULL)
		return (-1);
	if ((name = X509_get_subject_name(ctx->ssl_peer_cert)) == NULL)
		return (-1);
	*subject = X509_NAME_oneline(name, 0, 0);
	if (*subject == NULL)
		return (-1);
	return (0);
}

static int
tls_get_peer_cert_times(struct tls *ctx, time_t *notbefore,
    time_t *notafter)
{
	struct tm before_tm, after_tm;
	ASN1_TIME *before, *after;

	if (ctx->ssl_peer_cert == NULL)
		return (-1);

	memset(&before_tm, 0, sizeof(before_tm));
	memset(&after_tm, 0, sizeof(after_tm));

	if ((before = X509_get_notBefore(ctx->ssl_peer_cert)) == NULL)
		goto err;
	if ((after = X509_get_notAfter(ctx->ssl_peer_cert)) == NULL)
		goto err;
	if (ASN1_time_parse(before->data, before->length, &before_tm, 0) == -1)
		goto err;
	if (ASN1_time_parse(after->data, after->length, &after_tm, 0) == -1)
		goto err;
	if (!ASN1_time_tm_clamp_notafter(&after_tm))
		goto err;
	if ((*notbefore = timegm(&before_tm)) == -1)
		goto err;
	if ((*notafter = timegm(&after_tm)) == -1)
		goto err;

	return (0);

 err:
	return (-1);
}

static int
tls_get_peer_cert_info(struct tls *ctx)
{
	if (ctx->ssl_peer_cert == NULL)
		return (0);

	if (tls_get_peer_cert_hash(ctx, &ctx->conninfo->hash) == -1)
		goto err;
	if (tls_get_peer_cert_subject(ctx, &ctx->conninfo->subject) == -1)
		goto err;
	if (tls_get_peer_cert_issuer(ctx, &ctx->conninfo->issuer) == -1)
		goto err;
	if (tls_get_peer_cert_times(ctx, &ctx->conninfo->notbefore,
	    &ctx->conninfo->notafter) == -1)
		goto err;

	return (0);

 err:
	return (-1);
}

static int
tls_conninfo_alpn_proto(struct tls *ctx)
{
	const unsigned char *p;
	unsigned int len;

	free(ctx->conninfo->alpn);
	ctx->conninfo->alpn = NULL;

	SSL_get0_alpn_selected(ctx->ssl_conn, &p, &len);
	if (len > 0) {
		if ((ctx->conninfo->alpn = malloc(len + 1)) == NULL)
			return (-1);
		memcpy(ctx->conninfo->alpn, p, len);
		ctx->conninfo->alpn[len] = '\0';
	}

	return (0);
}

static int
tls_conninfo_cert_pem(struct tls *ctx)
{
	int i, rv = -1;
	BIO *membio = NULL;
	BUF_MEM *bptr = NULL;

	if (ctx->conninfo == NULL)
		goto err;
	if (ctx->ssl_peer_cert == NULL)
		return 0;
	if ((membio = BIO_new(BIO_s_mem()))== NULL)
		goto err;

	/*
	 * We have to write the peer cert out separately, because
	 * the certificate chain may or may not contain it.
	 */
	if (!PEM_write_bio_X509(membio, ctx->ssl_peer_cert))
		goto err;
	for (i = 0; i < sk_X509_num(ctx->ssl_peer_chain); i++) {
		X509 *chaincert = sk_X509_value(ctx->ssl_peer_chain, i);
		if (chaincert != ctx->ssl_peer_cert &&
		    !PEM_write_bio_X509(membio, chaincert))
			goto err;
	}

	BIO_get_mem_ptr(membio, &bptr);
	free(ctx->conninfo->peer_cert);
	ctx->conninfo->peer_cert_len = 0;
	if ((ctx->conninfo->peer_cert = malloc(bptr->length)) == NULL)
		goto err;
	ctx->conninfo->peer_cert_len = bptr->length;
	memcpy(ctx->conninfo->peer_cert, bptr->data,
	    ctx->conninfo->peer_cert_len);

	/* BIO_free() will kill BUF_MEM - because we have not set BIO_NOCLOSE */
	rv = 0;
 err:
	BIO_free(membio);
	return rv;
}

int
tls_conninfo_populate(struct tls *ctx)
{
	const char *tmp;

	tls_conninfo_free(ctx->conninfo);

	if ((ctx->conninfo = calloc(1, sizeof(struct tls_conninfo))) == NULL) {
		tls_set_errorx(ctx, "out of memory");
		goto err;
	}

	if (tls_conninfo_alpn_proto(ctx) == -1)
		goto err;

	if ((tmp = SSL_get_cipher(ctx->ssl_conn)) == NULL)
		goto err;
	ctx->conninfo->cipher = strdup(tmp);
	if (ctx->conninfo->cipher == NULL)
		goto err;

	if (ctx->servername != NULL) {
		if ((ctx->conninfo->servername =
		    strdup(ctx->servername)) == NULL)
			goto err;
	}

	if ((tmp = SSL_get_version(ctx->ssl_conn)) == NULL)
		goto err;
	ctx->conninfo->version = strdup(tmp);
	if (ctx->conninfo->version == NULL)
		goto err;

	if (tls_get_peer_cert_info(ctx) == -1)
		goto err;

	if (tls_conninfo_cert_pem(ctx) == -1)
		goto err;

	return (0);

 err:
	tls_conninfo_free(ctx->conninfo);
	ctx->conninfo = NULL;

	return (-1);
}

void
tls_conninfo_free(struct tls_conninfo *conninfo)
{
	if (conninfo == NULL)
		return;

	free(conninfo->alpn);
	conninfo->alpn = NULL;
	free(conninfo->cipher);
	conninfo->cipher = NULL;
	free(conninfo->servername);
	conninfo->servername = NULL;
	free(conninfo->version);
	conninfo->version = NULL;

	free(conninfo->hash);
	conninfo->hash = NULL;
	free(conninfo->issuer);
	conninfo->issuer = NULL;
	free(conninfo->subject);
	conninfo->subject = NULL;

	free(conninfo->peer_cert);
	conninfo->peer_cert = NULL;
	conninfo->peer_cert_len = 0;

	free(conninfo);
}

const char *
tls_conn_alpn_selected(struct tls *ctx)
{
	if (ctx->conninfo == NULL)
		return (NULL);
	return (ctx->conninfo->alpn);
}

const char *
tls_conn_cipher(struct tls *ctx)
{
	if (ctx->conninfo == NULL)
		return (NULL);
	return (ctx->conninfo->cipher);
}

const char *
tls_conn_servername(struct tls *ctx)
{
	if (ctx->conninfo == NULL)
		return (NULL);
	return (ctx->conninfo->servername);
}

const char *
tls_conn_version(struct tls *ctx)
{
	if (ctx->conninfo == NULL)
		return (NULL);
	return (ctx->conninfo->version);
}

