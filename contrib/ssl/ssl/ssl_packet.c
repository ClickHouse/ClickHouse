/* $OpenBSD: ssl_packet.c,v 1.6 2017/05/06 16:18:36 jsing Exp $ */
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

#include "bytestring.h"

static int
ssl_is_sslv2_client_hello(CBS *header)
{
	uint16_t record_length;
	uint8_t message_type;
	CBS cbs;

	CBS_dup(header, &cbs);

	if (!CBS_get_u16(&cbs, &record_length) ||
	    !CBS_get_u8(&cbs, &message_type))
		return 0;

	/*
	 * The SSLv2 record length field uses variable length (2 or 3 byte)
	 * encoding. Given the size of a client hello, we expect/require the
	 * 2-byte form which is indicated by a one in the most significant bit.
	 */
	if ((record_length & 0x8000) == 0)
		return 0;
	if ((record_length & ~0x8000) < 3)
		return 0;
	if (message_type != SSL2_MT_CLIENT_HELLO)
		return 0;

	return 1;
}

static int
ssl_is_sslv3_handshake(CBS *header)
{
	uint16_t record_version;
	uint8_t record_type;
	CBS cbs;

	CBS_dup(header, &cbs);

	if (!CBS_get_u8(&cbs, &record_type) ||
	    !CBS_get_u16(&cbs, &record_version))
		return 0;

	if (record_type != SSL3_RT_HANDSHAKE)
		return 0;
	if ((record_version >> 8) != SSL3_VERSION_MAJOR)
		return 0;

	return 1;
}

static int
ssl_convert_sslv2_client_hello(SSL *s)
{
	CBB cbb, handshake, client_hello, cipher_suites, compression, session_id;
	CBS cbs, challenge, cipher_specs, session;
	uint16_t record_length, client_version, cipher_specs_length;
	uint16_t session_id_length, challenge_length;
	unsigned char *client_random = NULL, *data = NULL;
	size_t data_len, pad_len, len;
	uint32_t cipher_spec;
	uint8_t message_type;
	unsigned char *pad;
	int ret = -1;
	int n;

	memset(&cbb, 0, sizeof(cbb));

	CBS_init(&cbs, s->internal->packet, SSL3_RT_HEADER_LENGTH);

	if (!CBS_get_u16(&cbs, &record_length) ||
	    !CBS_get_u8(&cbs, &message_type) ||
	    !CBS_get_u16(&cbs, &client_version))
		return -1;

	/*
	 * The SSLv2 record length field uses variable length (2 or 3 byte)
	 * encoding. Given the size of a client hello, we expect/require the
	 * 2-byte form which is indicated by a one in the most significant bit.
	 * Also note that the record length value does not include the bytes
	 * used for the record length field.
	 */
	if ((record_length & 0x8000) == 0)
		return -1;
	record_length &= ~0x8000;
	if (record_length < SSL3_RT_HEADER_LENGTH - 2)
		return -1;
	if (message_type != SSL2_MT_CLIENT_HELLO)
		return -1;

	if (record_length < 9) {
		SSLerror(s, SSL_R_RECORD_LENGTH_MISMATCH);
		return -1;
	}
	if (record_length > 4096) {
		SSLerror(s, SSL_R_RECORD_TOO_LARGE);
		return -1;
	}

	n = ssl3_packet_extend(s, record_length + 2);
	if (n != record_length + 2)
		return n;

	tls1_finish_mac(s, s->internal->packet + 2,
	    s->internal->packet_length - 2);
	s->internal->mac_packet = 0;

	if (s->internal->msg_callback)
		s->internal->msg_callback(0, SSL2_VERSION, 0,
		    s->internal->packet + 2, s->internal->packet_length - 2, s,
		    s->internal->msg_callback_arg);

	/* Decode the SSLv2 record containing the client hello. */
	CBS_init(&cbs, s->internal->packet, s->internal->packet_length);

	if (!CBS_get_u16(&cbs, &record_length))
		return -1;
	if (!CBS_get_u8(&cbs, &message_type))
		return -1;
	if (!CBS_get_u16(&cbs, &client_version))
		return -1;
	if (!CBS_get_u16(&cbs, &cipher_specs_length))
		return -1;
	if (!CBS_get_u16(&cbs, &session_id_length))
		return -1;
	if (!CBS_get_u16(&cbs, &challenge_length))
		return -1;
	if (!CBS_get_bytes(&cbs, &cipher_specs, cipher_specs_length))
		return -1;
	if (!CBS_get_bytes(&cbs, &session, session_id_length))
		return -1;
	if (!CBS_get_bytes(&cbs, &challenge, challenge_length))
		return -1;
	if (CBS_len(&cbs) != 0) {
		SSLerror(s, SSL_R_RECORD_LENGTH_MISMATCH);
		return -1;
	}

	/*
	 * Convert SSLv2 challenge to SSLv3/TLS client random, by truncating or
	 * left-padding with zero bytes.
	 */
	if ((client_random = malloc(SSL3_RANDOM_SIZE)) == NULL)
		goto err;
	if (!CBB_init_fixed(&cbb, client_random, SSL3_RANDOM_SIZE))
		goto err;
	if ((len = CBS_len(&challenge)) > SSL3_RANDOM_SIZE)
		len = SSL3_RANDOM_SIZE;
	pad_len = SSL3_RANDOM_SIZE - len;
	if (!CBB_add_space(&cbb, &pad, pad_len))
		goto err;
	memset(pad, 0, pad_len);
	if (!CBB_add_bytes(&cbb, CBS_data(&challenge), len))
		goto err;
	if (!CBB_finish(&cbb, NULL, NULL))
		goto err;

	/* Build SSLv3/TLS record with client hello. */
	if (!CBB_init(&cbb, SSL3_RT_MAX_PLAIN_LENGTH))
		goto err;
	if (!CBB_add_u8(&cbb, SSL3_RT_HANDSHAKE))
		goto err;
	if (!CBB_add_u16(&cbb, 0x0301))
		goto err;
	if (!CBB_add_u16_length_prefixed(&cbb, &handshake))
		goto err;
	if (!CBB_add_u8(&handshake, SSL3_MT_CLIENT_HELLO))
		goto err;
	if (!CBB_add_u24_length_prefixed(&handshake, &client_hello))
		goto err;
	if (!CBB_add_u16(&client_hello, client_version))
		goto err;
	if (!CBB_add_bytes(&client_hello, client_random, SSL3_RANDOM_SIZE))
		goto err;
	if (!CBB_add_u8_length_prefixed(&client_hello, &session_id))
		goto err;
	if (!CBB_add_u16_length_prefixed(&client_hello, &cipher_suites))
		goto err;
	while (CBS_len(&cipher_specs) > 0) {
		if (!CBS_get_u24(&cipher_specs, &cipher_spec))
			goto err;
		if ((cipher_spec & 0xff0000) != 0)
			continue;
		if (!CBB_add_u16(&cipher_suites, cipher_spec & 0xffff))
			goto err;
	}
	if (!CBB_add_u8_length_prefixed(&client_hello, &compression))
		goto err;
	if (!CBB_add_u8(&compression, 0))
		goto err;
	if (!CBB_finish(&cbb, &data, &data_len))
		goto err;

	if (data_len > s->s3->rbuf.len)
		goto err;

	s->internal->packet = s->s3->rbuf.buf;
	s->internal->packet_length = data_len;
	memcpy(s->internal->packet, data, data_len);
	ret = 1;

 err:
	CBB_cleanup(&cbb);
	free(client_random);
	free(data);

	return (ret);
}

/*
 * Potentially do legacy processing on the first packet received by a TLS
 * server. We return 1 if we want SSLv3/TLS record processing to continue
 * normally, otherwise we must set an SSLerr and return -1.
 */
int
ssl_server_legacy_first_packet(SSL *s)
{
	uint16_t min_version;
	const char *data;
	CBS header;

	if (SSL_IS_DTLS(s))
		return 1;

	CBS_init(&header, s->internal->packet, SSL3_RT_HEADER_LENGTH);

	if (ssl_is_sslv3_handshake(&header) == 1)
		return 1;

	/* Only continue if this is not a version locked method. */
	if (s->method->internal->min_version == s->method->internal->max_version)
		return 1;

	if (ssl_is_sslv2_client_hello(&header) == 1) {
		/* Only permit SSLv2 client hellos if TLSv1.0 is enabled. */
		if (ssl_enabled_version_range(s, &min_version, NULL) != 1) {
			SSLerror(s, SSL_R_NO_PROTOCOLS_AVAILABLE);
			return -1;
		}
		if (min_version > TLS1_VERSION)
			return 1;

		if (ssl_convert_sslv2_client_hello(s) != 1) {
			SSLerror(s, SSL_R_BAD_PACKET_LENGTH);
			return -1;
		}

		return 1;
	}

	/* Ensure that we have SSL3_RT_HEADER_LENGTH (5 bytes) of the packet. */
	if (CBS_len(&header) != SSL3_RT_HEADER_LENGTH) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		return -1;
	}
	data = (const char *)CBS_data(&header);

	/* Is this a cleartext protocol? */
	if (strncmp("GET ", data, 4) == 0 ||
	    strncmp("POST ", data, 5) == 0 ||
	    strncmp("HEAD ", data, 5) == 0 ||
	    strncmp("PUT ", data, 4) == 0) {
		SSLerror(s, SSL_R_HTTP_REQUEST);
		return -1;
	}
	if (strncmp("CONNE", data, 5) == 0) {
		SSLerror(s, SSL_R_HTTPS_PROXY_REQUEST);
		return -1;
	}

	SSLerror(s, SSL_R_UNKNOWN_PROTOCOL);

	return -1;
}
