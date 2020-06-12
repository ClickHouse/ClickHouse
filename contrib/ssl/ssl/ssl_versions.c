/* $OpenBSD: ssl_versions.c,v 1.3 2017/05/06 20:37:25 jsing Exp $ */
/*
 * Copyright (c) 2016, 2017 Joel Sing <jsing@openbsd.org>
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

static int
ssl_clamp_version_range(uint16_t *min_ver, uint16_t *max_ver,
    uint16_t clamp_min, uint16_t clamp_max)
{
	if (clamp_min > clamp_max || *min_ver > *max_ver)
		return 0;
	if (clamp_max < *min_ver || clamp_min > *max_ver)
		return 0;

	if (*min_ver < clamp_min)
		*min_ver = clamp_min;
	if (*max_ver > clamp_max)
		*max_ver = clamp_max;

	return 1;
}

int
ssl_version_set_min(const SSL_METHOD *meth, uint16_t ver, uint16_t max_ver,
    uint16_t *out_ver)
{
	uint16_t min_version, max_version;

	if (ver == 0) {
		*out_ver = meth->internal->min_version;
		return 1;
	}

	min_version = ver;
	max_version = max_ver;

	if (!ssl_clamp_version_range(&min_version, &max_version,
	    meth->internal->min_version, meth->internal->max_version))
		return 0;

	*out_ver = min_version;
	
	return 1;
}

int
ssl_version_set_max(const SSL_METHOD *meth, uint16_t ver, uint16_t min_ver,
    uint16_t *out_ver)
{
	uint16_t min_version, max_version;

	if (ver == 0) {
		*out_ver = meth->internal->max_version;
		return 1;
	}

	min_version = min_ver;
	max_version = ver;

	if (!ssl_clamp_version_range(&min_version, &max_version,
	    meth->internal->min_version, meth->internal->max_version))
		return 0;

	*out_ver = max_version;
	
	return 1;
}

int
ssl_enabled_version_range(SSL *s, uint16_t *min_ver, uint16_t *max_ver)
{
	uint16_t min_version, max_version;

	/*
	 * The enabled versions have to be a contiguous range, which means we
	 * cannot enable and disable single versions at our whim, even though
	 * this is what the OpenSSL flags allow. The historical way this has
	 * been handled is by making a flag mean that all higher versions
	 * are disabled, if any version lower than the flag is enabled.
	 */

	min_version = 0;
	max_version = TLS1_2_VERSION;

	if ((s->internal->options & SSL_OP_NO_TLSv1) == 0)
		min_version = TLS1_VERSION;
	else if ((s->internal->options & SSL_OP_NO_TLSv1_1) == 0)
		min_version = TLS1_1_VERSION;
	else if ((s->internal->options & SSL_OP_NO_TLSv1_2) == 0)
		min_version = TLS1_2_VERSION;

	if ((s->internal->options & SSL_OP_NO_TLSv1_2) && min_version < TLS1_2_VERSION)
		max_version = TLS1_1_VERSION;
	if ((s->internal->options & SSL_OP_NO_TLSv1_1) && min_version < TLS1_1_VERSION)
		max_version = TLS1_VERSION;
	if ((s->internal->options & SSL_OP_NO_TLSv1) && min_version < TLS1_VERSION)
		max_version = 0;

	/* Everything has been disabled... */
	if (min_version == 0 || max_version == 0)
		return 0;

	/* Limit to configured version range. */
	if (!ssl_clamp_version_range(&min_version, &max_version,
	    s->internal->min_version, s->internal->max_version))
		return 0;

	if (min_ver != NULL)
		*min_ver = min_version;
	if (max_ver != NULL)
		*max_ver = max_version;

	return 1;
}

int
ssl_supported_version_range(SSL *s, uint16_t *min_ver, uint16_t *max_ver)
{
	uint16_t min_version, max_version;

	/* DTLS cannot currently be disabled... */
	if (SSL_IS_DTLS(s)) {
		min_version = max_version = DTLS1_VERSION;
		goto done;
	}

	if (!ssl_enabled_version_range(s, &min_version, &max_version))
		return 0;

	/* Limit to the versions supported by this method. */
	if (!ssl_clamp_version_range(&min_version, &max_version,
	    s->method->internal->min_version,
	    s->method->internal->max_version))
		return 0;

 done:
	if (min_ver != NULL)
		*min_ver = min_version;
	if (max_ver != NULL)
		*max_ver = max_version;

	return 1;
}

int
ssl_max_shared_version(SSL *s, uint16_t peer_ver, uint16_t *max_ver)
{
	uint16_t min_version, max_version, shared_version;

	*max_ver = 0;

	if (SSL_IS_DTLS(s)) {
		if (peer_ver >= DTLS1_VERSION) {
			*max_ver = DTLS1_VERSION;
			return 1;
		}
		return 0;
	}

	if (peer_ver >= TLS1_2_VERSION)
		shared_version = TLS1_2_VERSION;
	else if (peer_ver >= TLS1_1_VERSION)
		shared_version = TLS1_1_VERSION;
	else if (peer_ver >= TLS1_VERSION)
		shared_version = TLS1_VERSION;
	else
		return 0;

	if (!ssl_supported_version_range(s, &min_version, &max_version))
		return 0;

	if (shared_version < min_version)
		return 0;

	if (shared_version > max_version)
		shared_version = max_version;

	*max_ver = shared_version;

	return 1;
}

uint16_t
ssl_max_server_version(SSL *s)
{
	uint16_t max_version, min_version = 0;

	if (SSL_IS_DTLS(s))
		return (DTLS1_VERSION);

	if (!ssl_enabled_version_range(s, &min_version, &max_version))
		return 0;

	/*
	 * Limit to the versions supported by this method. The SSL method
	 * will be changed during version negotiation, as such we want to
	 * use the SSL method from the context.
	 */
	if (!ssl_clamp_version_range(&min_version, &max_version,
	    s->ctx->method->internal->min_version,
	    s->ctx->method->internal->max_version))
		return 0;

	return (max_version);
}
