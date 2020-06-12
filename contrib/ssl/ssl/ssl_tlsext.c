/* $OpenBSD: ssl_tlsext.c,v 1.17.4.1 2017/12/09 13:43:25 jsing Exp $ */
/*
 * Copyright (c) 2016, 2017 Joel Sing <jsing@openbsd.org>
 * Copyright (c) 2017 Doug Hogan <doug@openbsd.org>
 * Copyright (c) 2017 Bob Beck <beck@openbsd.org>
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
#include <openssl/ocsp.h>

#include "ssl_locl.h"

#include "bytestring.h"
#include "ssl_tlsext.h"

/*
 * Supported Application-Layer Protocol Negotiation - RFC 7301
 */

int
tlsext_alpn_clienthello_needs(SSL *s)
{
	/* ALPN protos have been specified and this is the initial handshake */
	return s->internal->alpn_client_proto_list != NULL &&
	    S3I(s)->tmp.finish_md_len == 0;
}

int
tlsext_alpn_clienthello_build(SSL *s, CBB *cbb)
{
	CBB protolist;

	if (!CBB_add_u16_length_prefixed(cbb, &protolist))
		return 0;

	if (!CBB_add_bytes(&protolist, s->internal->alpn_client_proto_list,
	    s->internal->alpn_client_proto_list_len))
		return 0;

	if (!CBB_flush(cbb))
		return 0;

	return 1;
}

int
tlsext_alpn_clienthello_parse(SSL *s, CBS *cbs, int *alert)
{
	CBS proto_name_list, alpn;
	const unsigned char *selected;
	unsigned char selected_len;
	int r;

	if (!CBS_get_u16_length_prefixed(cbs, &alpn))
		goto err;
	if (CBS_len(&alpn) < 2)
		goto err;
	if (CBS_len(cbs) != 0)
		goto err;

	CBS_dup(&alpn, &proto_name_list);
	while (CBS_len(&proto_name_list) > 0) {
		CBS proto_name;

		if (!CBS_get_u8_length_prefixed(&proto_name_list, &proto_name))
			goto err;
		if (CBS_len(&proto_name) == 0)
			goto err;
	}

	if (s->ctx->internal->alpn_select_cb == NULL)
		return 1;

	r = s->ctx->internal->alpn_select_cb(s, &selected, &selected_len,
	    CBS_data(&alpn), CBS_len(&alpn),
	    s->ctx->internal->alpn_select_cb_arg);
	if (r == SSL_TLSEXT_ERR_OK) {
		free(S3I(s)->alpn_selected);
		if ((S3I(s)->alpn_selected = malloc(selected_len)) == NULL) {
			*alert = SSL_AD_INTERNAL_ERROR;
			return 0;
		}
		memcpy(S3I(s)->alpn_selected, selected, selected_len);
		S3I(s)->alpn_selected_len = selected_len;
	}

	return 1;

 err:
	*alert = SSL_AD_DECODE_ERROR;
	return 0;
}

int
tlsext_alpn_serverhello_needs(SSL *s)
{
	return S3I(s)->alpn_selected != NULL;
}

int
tlsext_alpn_serverhello_build(SSL *s, CBB *cbb)
{
	CBB list, selected;

	if (!CBB_add_u16_length_prefixed(cbb, &list))
		return 0;

	if (!CBB_add_u8_length_prefixed(&list, &selected))
		return 0;

	if (!CBB_add_bytes(&selected, S3I(s)->alpn_selected,
	    S3I(s)->alpn_selected_len))
		return 0;

	if (!CBB_flush(cbb))
		return 0;

	return 1;
}

int
tlsext_alpn_serverhello_parse(SSL *s, CBS *cbs, int *alert)
{
	CBS list, proto;

	if (s->internal->alpn_client_proto_list == NULL) {
		*alert = TLS1_AD_UNSUPPORTED_EXTENSION;
		return 0;
	}

	if (!CBS_get_u16_length_prefixed(cbs, &list))
		goto err;
	if (CBS_len(cbs) != 0)
		goto err;

	if (!CBS_get_u8_length_prefixed(&list, &proto))
		goto err;

	if (CBS_len(&list) != 0)
		goto err;
	if (CBS_len(&proto) == 0)
		goto err;

	if (!CBS_stow(&proto, &(S3I(s)->alpn_selected),
	    &(S3I(s)->alpn_selected_len)))
		goto err;

	return 1;

 err:
	*alert = TLS1_AD_DECODE_ERROR;
	return 0;
}

/*
 * Supported Elliptic Curves - RFC 4492 section 5.1.1
 */
int
tlsext_ec_clienthello_needs(SSL *s)
{
	return ssl_has_ecc_ciphers(s);
}

int
tlsext_ec_clienthello_build(SSL *s, CBB *cbb)
{
	CBB curvelist;
	size_t curves_len;
	int i;
	const uint16_t *curves;

	tls1_get_curvelist(s, 0, &curves, &curves_len);

	if (curves_len == 0) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		return 0;
	}

	if (!CBB_add_u16_length_prefixed(cbb, &curvelist))
		return 0;

	for (i = 0; i < curves_len; i++) {
		if (!CBB_add_u16(&curvelist, curves[i]))
			return 0;
	}

	if (!CBB_flush(cbb))
		return 0;

	return 1;
}

int
tlsext_ec_clienthello_parse(SSL *s, CBS *cbs, int *alert)
{
	CBS curvelist;
	size_t curves_len;

	if (!CBS_get_u16_length_prefixed(cbs, &curvelist))
		goto err;
	if (CBS_len(cbs) != 0)
		goto err;

	curves_len = CBS_len(&curvelist);
	if (curves_len == 0 || curves_len % 2 != 0)
		goto err;
	curves_len /= 2;

	if (!s->internal->hit) {
		int i;
		uint16_t *curves;

		if (SSI(s)->tlsext_supportedgroups != NULL)
			goto err;

		if ((curves = reallocarray(NULL, curves_len,
		    sizeof(uint16_t))) == NULL) {
			*alert = TLS1_AD_INTERNAL_ERROR;
			return 0;
		}

		for (i = 0; i < curves_len; i++) {
			if (!CBS_get_u16(&curvelist, &curves[i])) {
				free(curves);
				goto err;
			}
		}

		if (CBS_len(&curvelist) != 0) {
			free(curves);
			goto err;
		}

		SSI(s)->tlsext_supportedgroups = curves;
		SSI(s)->tlsext_supportedgroups_length = curves_len;
	}

	return 1;

 err:
	*alert = TLS1_AD_DECODE_ERROR;
	return 0;
}

/* This extension is never used by the server. */
int
tlsext_ec_serverhello_needs(SSL *s)
{
	return 0;
}

int
tlsext_ec_serverhello_build(SSL *s, CBB *cbb)
{
	return 0;
}

int
tlsext_ec_serverhello_parse(SSL *s, CBS *cbs, int *alert)
{
	/*
	 * Servers should not send this extension per the RFC.
	 *
	 * However, F5 sends it by mistake (case ID 492780) so we need to skip
	 * over it.  This bug is from at least 2014 but as of 2017, there
	 * are still large sites with this bug in production.
	 *
	 * https://devcentral.f5.com/questions/disable-supported-elliptic-curves-extension-from-server
	 */
	if (!CBS_skip(cbs, CBS_len(cbs))) {
		*alert = TLS1_AD_INTERNAL_ERROR;
		return 0;
	}

	return 1;
}

/*
 * Supported Point Formats Extension - RFC 4492 section 5.1.2
 */
static int
tlsext_ecpf_build(SSL *s, CBB *cbb)
{
	CBB ecpf;
	size_t formats_len;
	const uint8_t *formats;

	tls1_get_formatlist(s, 0, &formats, &formats_len);

	if (formats_len == 0) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		return 0;
	}

	if (!CBB_add_u8_length_prefixed(cbb, &ecpf))
		return 0;
	if (!CBB_add_bytes(&ecpf, formats, formats_len))
		return 0;
	if (!CBB_flush(cbb))
		return 0;

	return 1;
}

static int
tlsext_ecpf_parse(SSL *s, CBS *cbs, int *alert)
{
	CBS ecpf;

	if (!CBS_get_u8_length_prefixed(cbs, &ecpf))
		goto err;
	if (CBS_len(&ecpf) == 0)
		goto err;
	if (CBS_len(cbs) != 0)
		goto err;

	/* Must contain uncompressed (0) */
	if (!CBS_contains_zero_byte(&ecpf)) {
		SSLerror(s, SSL_R_TLS_INVALID_ECPOINTFORMAT_LIST);
		goto err;
	}

	if (!s->internal->hit) {
		if (!CBS_stow(&ecpf, &(SSI(s)->tlsext_ecpointformatlist),
		    &(SSI(s)->tlsext_ecpointformatlist_length)))
			goto err;
	}

	return 1;

 err:
	*alert = TLS1_AD_INTERNAL_ERROR;
	return 0;
}

int
tlsext_ecpf_clienthello_needs(SSL *s)
{
	return ssl_has_ecc_ciphers(s);
}

int
tlsext_ecpf_clienthello_build(SSL *s, CBB *cbb)
{
	return tlsext_ecpf_build(s, cbb);
}

int
tlsext_ecpf_clienthello_parse(SSL *s, CBS *cbs, int *alert)
{
	return tlsext_ecpf_parse(s, cbs, alert);
}

int
tlsext_ecpf_serverhello_needs(SSL *s)
{
	if (s->version == DTLS1_VERSION)
		return 0;

	return ssl_using_ecc_cipher(s);
}

int
tlsext_ecpf_serverhello_build(SSL *s, CBB *cbb)
{
	return tlsext_ecpf_build(s, cbb);
}

int
tlsext_ecpf_serverhello_parse(SSL *s, CBS *cbs, int *alert)
{
	return tlsext_ecpf_parse(s, cbs, alert);
}

/*
 * Renegotiation Indication - RFC 5746.
 */
int
tlsext_ri_clienthello_needs(SSL *s)
{
	return (s->internal->renegotiate);
}

int
tlsext_ri_clienthello_build(SSL *s, CBB *cbb)
{
	CBB reneg;

	if (!CBB_add_u8_length_prefixed(cbb, &reneg))
		return 0;
	if (!CBB_add_bytes(&reneg, S3I(s)->previous_client_finished,
	    S3I(s)->previous_client_finished_len))
		return 0;
	if (!CBB_flush(cbb))
		return 0;

	return 1;
}

int
tlsext_ri_clienthello_parse(SSL *s, CBS *cbs, int *alert)
{
	CBS reneg;

	if (!CBS_get_u8_length_prefixed(cbs, &reneg))
		goto err;
	if (CBS_len(cbs) != 0)
		goto err;

	if (!CBS_mem_equal(&reneg, S3I(s)->previous_client_finished,
	    S3I(s)->previous_client_finished_len)) {
		SSLerror(s, SSL_R_RENEGOTIATION_MISMATCH);
		*alert = SSL_AD_HANDSHAKE_FAILURE;
		return 0;
	}

	S3I(s)->renegotiate_seen = 1;
	S3I(s)->send_connection_binding = 1;

	return 1;

 err:
	SSLerror(s, SSL_R_RENEGOTIATION_ENCODING_ERR);
	*alert = SSL_AD_DECODE_ERROR;
	return 0;
}

int
tlsext_ri_serverhello_needs(SSL *s)
{
	return (S3I(s)->send_connection_binding);
}

int
tlsext_ri_serverhello_build(SSL *s, CBB *cbb)
{
	CBB reneg;

	if (!CBB_add_u8_length_prefixed(cbb, &reneg))
		return 0;
	if (!CBB_add_bytes(&reneg, S3I(s)->previous_client_finished,
	    S3I(s)->previous_client_finished_len))
		return 0;
	if (!CBB_add_bytes(&reneg, S3I(s)->previous_server_finished,
	    S3I(s)->previous_server_finished_len))
		return 0;
	if (!CBB_flush(cbb))
		return 0;

	return 1;
}

int
tlsext_ri_serverhello_parse(SSL *s, CBS *cbs, int *alert)
{
	CBS reneg, prev_client, prev_server;

	/*
	 * Ensure that the previous client and server values are both not
	 * present, or that they are both present.
	 */
	if ((S3I(s)->previous_client_finished_len == 0 &&
	    S3I(s)->previous_server_finished_len != 0) ||
	    (S3I(s)->previous_client_finished_len != 0 &&
	    S3I(s)->previous_server_finished_len == 0)) {
		*alert = TLS1_AD_INTERNAL_ERROR;
		return 0;
	}

	if (!CBS_get_u8_length_prefixed(cbs, &reneg))
		goto err;
	if (!CBS_get_bytes(&reneg, &prev_client,
	    S3I(s)->previous_client_finished_len))
		goto err;
	if (!CBS_get_bytes(&reneg, &prev_server,
	    S3I(s)->previous_server_finished_len))
		goto err;
	if (CBS_len(&reneg) != 0)
		goto err;
	if (CBS_len(cbs) != 0)
		goto err;

	if (!CBS_mem_equal(&prev_client, S3I(s)->previous_client_finished,
	    S3I(s)->previous_client_finished_len)) {
		SSLerror(s, SSL_R_RENEGOTIATION_MISMATCH);
		*alert = SSL_AD_HANDSHAKE_FAILURE;
		return 0;
	}
	if (!CBS_mem_equal(&prev_server, S3I(s)->previous_server_finished,
	    S3I(s)->previous_server_finished_len)) {
		SSLerror(s, SSL_R_RENEGOTIATION_MISMATCH);
		*alert = SSL_AD_HANDSHAKE_FAILURE;
		return 0;
	}

	S3I(s)->renegotiate_seen = 1;
	S3I(s)->send_connection_binding = 1;

	return 1;

 err:
	SSLerror(s, SSL_R_RENEGOTIATION_ENCODING_ERR);
	*alert = SSL_AD_DECODE_ERROR;
	return 0;
}

/*
 * Signature Algorithms - RFC 5246 section 7.4.1.4.1.
 */
int
tlsext_sigalgs_clienthello_needs(SSL *s)
{
	return (TLS1_get_client_version(s) >= TLS1_2_VERSION);
}

int
tlsext_sigalgs_clienthello_build(SSL *s, CBB *cbb)
{
	unsigned char *sigalgs_data;
	size_t sigalgs_len;
	CBB sigalgs;

	tls12_get_req_sig_algs(s, &sigalgs_data, &sigalgs_len);

	if (!CBB_add_u16_length_prefixed(cbb, &sigalgs))
		return 0;
	if (!CBB_add_bytes(&sigalgs, sigalgs_data, sigalgs_len))
		return 0;
	if (!CBB_flush(cbb))
		return 0;

	return 1;
}

int
tlsext_sigalgs_clienthello_parse(SSL *s, CBS *cbs, int *alert)
{
	CBS sigalgs;

	if (!CBS_get_u16_length_prefixed(cbs, &sigalgs))
		return 0;

	return tls1_process_sigalgs(s, &sigalgs);
}

int
tlsext_sigalgs_serverhello_needs(SSL *s)
{
	return 0;
}

int
tlsext_sigalgs_serverhello_build(SSL *s, CBB *cbb)
{
	return 0;
}

int
tlsext_sigalgs_serverhello_parse(SSL *s, CBS *cbs, int *alert)
{
	/* As per the RFC, servers must not send this extension. */
	return 0;
}

/*
 * Server Name Indication - RFC 6066, section 3.
 */
int
tlsext_sni_clienthello_needs(SSL *s)
{
	return (s->tlsext_hostname != NULL);
}

int
tlsext_sni_clienthello_build(SSL *s, CBB *cbb)
{
	CBB server_name_list, host_name;

	if (!CBB_add_u16_length_prefixed(cbb, &server_name_list))
		return 0;
	if (!CBB_add_u8(&server_name_list, TLSEXT_NAMETYPE_host_name))
		return 0;
	if (!CBB_add_u16_length_prefixed(&server_name_list, &host_name))
		return 0;
	if (!CBB_add_bytes(&host_name, (const uint8_t *)s->tlsext_hostname,
	    strlen(s->tlsext_hostname)))
		return 0;
	if (!CBB_flush(cbb))
		return 0;

	return 1;
}

int
tlsext_sni_clienthello_parse(SSL *s, CBS *cbs, int *alert)
{
	CBS server_name_list, host_name;
	uint8_t name_type;

	if (!CBS_get_u16_length_prefixed(cbs, &server_name_list))
		goto err;

	/*
	 * RFC 6066 section 3 forbids multiple host names with the same type.
	 * Additionally, only one type (host_name) is specified.
	 */
	if (!CBS_get_u8(&server_name_list, &name_type))
		goto err;
	if (name_type != TLSEXT_NAMETYPE_host_name)
		goto err;

	if (!CBS_get_u16_length_prefixed(&server_name_list, &host_name))
		goto err;
	if (CBS_len(&host_name) == 0 ||
	    CBS_len(&host_name) > TLSEXT_MAXLEN_host_name ||
	    CBS_contains_zero_byte(&host_name)) {
		*alert = TLS1_AD_UNRECOGNIZED_NAME;
		return 0;
	}

	if (s->internal->hit) {
		if (s->session->tlsext_hostname == NULL) {
			*alert = TLS1_AD_UNRECOGNIZED_NAME;
			return 0;
		}
		if (!CBS_mem_equal(&host_name, s->session->tlsext_hostname,
		    strlen(s->session->tlsext_hostname))) {
			*alert = TLS1_AD_UNRECOGNIZED_NAME;
			return 0;
		}
	} else {
		if (s->session->tlsext_hostname != NULL)
			goto err;
		if (!CBS_strdup(&host_name, &s->session->tlsext_hostname)) {
			*alert = TLS1_AD_INTERNAL_ERROR;
			return 0;
		}
	}

	if (CBS_len(&server_name_list) != 0)
		goto err;
	if (CBS_len(cbs) != 0)
		goto err;

	return 1;

 err:
	*alert = SSL_AD_DECODE_ERROR;
	return 0;
}

int
tlsext_sni_serverhello_needs(SSL *s)
{
	return (s->session->tlsext_hostname != NULL);
}

int
tlsext_sni_serverhello_build(SSL *s, CBB *cbb)
{
	return 1;
}

int
tlsext_sni_serverhello_parse(SSL *s, CBS *cbs, int *alert)
{
	if (s->tlsext_hostname == NULL || CBS_len(cbs) != 0) {
		*alert = TLS1_AD_UNRECOGNIZED_NAME;
		return 0;
	}

	if (s->internal->hit) {
		if (s->session->tlsext_hostname == NULL) {
			*alert = TLS1_AD_UNRECOGNIZED_NAME;
			return 0;
		}
		if (strcmp(s->tlsext_hostname,
		    s->session->tlsext_hostname) != 0) {
			*alert = TLS1_AD_UNRECOGNIZED_NAME;
			return 0;
		}
	} else {
		if (s->session->tlsext_hostname != NULL) {
			*alert = SSL_AD_DECODE_ERROR;
			return 0;
		}
		if ((s->session->tlsext_hostname =
		    strdup(s->tlsext_hostname)) == NULL) {
			*alert = TLS1_AD_INTERNAL_ERROR;
			return 0;
		}
	}

	return 1;
}


/*
 *Certificate Status Request - RFC 6066 section 8.
 */

int
tlsext_ocsp_clienthello_needs(SSL *s)
{
	return (s->tlsext_status_type == TLSEXT_STATUSTYPE_ocsp &&
	    s->version != DTLS1_VERSION);
}

int
tlsext_ocsp_clienthello_build(SSL *s, CBB *cbb)
{
	CBB respid_list, respid, exts;
	unsigned char *ext_data;
	size_t ext_len;
	int i;

	if (!CBB_add_u8(cbb, TLSEXT_STATUSTYPE_ocsp))
		return 0;
	if (!CBB_add_u16_length_prefixed(cbb, &respid_list))
		return 0;
	for (i = 0; i < sk_OCSP_RESPID_num(s->internal->tlsext_ocsp_ids); i++) {
		unsigned char *respid_data;
		OCSP_RESPID *id;
		size_t id_len;

		if ((id = sk_OCSP_RESPID_value(s->internal->tlsext_ocsp_ids,
		    i)) ==  NULL)
			return 0;
		if ((id_len = i2d_OCSP_RESPID(id, NULL)) == -1)
			return 0;
		if (!CBB_add_u16_length_prefixed(&respid_list, &respid))
			return 0;
		if (!CBB_add_space(&respid, &respid_data, id_len))
			return 0;
		if ((i2d_OCSP_RESPID(id, &respid_data)) != id_len)
			return 0;
	}
	if (!CBB_add_u16_length_prefixed(cbb, &exts))
		return 0;
	if ((ext_len = i2d_X509_EXTENSIONS(s->internal->tlsext_ocsp_exts,
	    NULL)) == -1)
		return 0;
	if (!CBB_add_space(&exts, &ext_data, ext_len))
		return 0;
	if ((i2d_X509_EXTENSIONS(s->internal->tlsext_ocsp_exts, &ext_data) !=
	    ext_len))
		return 0;
	if (!CBB_flush(cbb))
		return 0;
	return 1;
}

int
tlsext_ocsp_clienthello_parse(SSL *s, CBS *cbs, int *alert)
{
	int failure = SSL_AD_DECODE_ERROR;
	CBS respid_list, respid, exts;
	const unsigned char *p;
	uint8_t status_type;
	int ret = 0;

	if (!CBS_get_u8(cbs, &status_type))
		goto err;
	if (status_type != TLSEXT_STATUSTYPE_ocsp) {
		/* ignore unknown status types */
		s->tlsext_status_type = -1;

		if (!CBS_skip(cbs, CBS_len(cbs))) {
			*alert = TLS1_AD_INTERNAL_ERROR;
			return 0;
		}
		return 1;
	}
	s->tlsext_status_type = status_type;
	if (!CBS_get_u16_length_prefixed(cbs, &respid_list))
		goto err;

	/* XXX */
	sk_OCSP_RESPID_pop_free(s->internal->tlsext_ocsp_ids, OCSP_RESPID_free);
	s->internal->tlsext_ocsp_ids = NULL;
	if (CBS_len(&respid_list) > 0) {
		s->internal->tlsext_ocsp_ids = sk_OCSP_RESPID_new_null();
		if (s->internal->tlsext_ocsp_ids == NULL) {
			failure = SSL_AD_INTERNAL_ERROR;
			goto err;
		}
	}

	while (CBS_len(&respid_list) > 0) {
		OCSP_RESPID *id;

		if (!CBS_get_u16_length_prefixed(&respid_list, &respid))
			goto err;
		p = CBS_data(&respid);
		if ((id = d2i_OCSP_RESPID(NULL, &p, CBS_len(&respid))) == NULL)
			goto err;
		if (!sk_OCSP_RESPID_push(s->internal->tlsext_ocsp_ids, id)) {
			failure = SSL_AD_INTERNAL_ERROR;
			OCSP_RESPID_free(id);
			goto err;
		}
	}

	/* Read in request_extensions */
	if (!CBS_get_u16_length_prefixed(cbs, &exts))
		goto err;
	if (CBS_len(&exts) > 0) {
		sk_X509_EXTENSION_pop_free(s->internal->tlsext_ocsp_exts,
		    X509_EXTENSION_free);
		p = CBS_data(&exts);
		if ((s->internal->tlsext_ocsp_exts = d2i_X509_EXTENSIONS(NULL,
		    &p, CBS_len(&exts))) == NULL)
			goto err;
	}

	/* should be nothing left */
	if (CBS_len(cbs) > 0)
		goto err;

	ret = 1;
 err:
	if (ret == 0)
		*alert = failure;
	return ret;
}

int
tlsext_ocsp_serverhello_needs(SSL *s)
{
	return s->internal->tlsext_status_expected;
}

int
tlsext_ocsp_serverhello_build(SSL *s, CBB *cbb)
{
	return 1;
}

int
tlsext_ocsp_serverhello_parse(SSL *s, CBS *cbs, int *alert)
{
	if (s->tlsext_status_type == -1) {
		*alert = TLS1_AD_UNSUPPORTED_EXTENSION;
		return 0;
	}
	/* Set flag to expect CertificateStatus message */
	s->internal->tlsext_status_expected = 1;
	return 1;
}

/*
 * SessionTicket extension - RFC 5077 section 3.2
 */
int
tlsext_sessionticket_clienthello_needs(SSL *s)
{
	/*
	 * Send session ticket extension when enabled and not overridden.
	 *
	 * When renegotiating, send an empty session ticket to indicate support.
	 */
	if ((SSL_get_options(s) & SSL_OP_NO_TICKET) != 0)
		return 0;

	if (s->internal->new_session)
		return 1;

	if (s->internal->tlsext_session_ticket != NULL &&
	    s->internal->tlsext_session_ticket->data == NULL)
		return 0;

	return 1;
}

int
tlsext_sessionticket_clienthello_build(SSL *s, CBB *cbb)
{
	/*
	 * Signal that we support session tickets by sending an empty
	 * extension when renegotiating or no session found.
	 */
	if (s->internal->new_session || s->session == NULL)
		return 1;

	if (s->session->tlsext_tick != NULL) {
		/* Attempt to resume with an existing session ticket */
		if (!CBB_add_bytes(cbb, s->session->tlsext_tick,
		    s->session->tlsext_ticklen))
			return 0;

	} else if (s->internal->tlsext_session_ticket != NULL) {
		/*
		 * Attempt to resume with a custom provided session ticket set
		 * by SSL_set_session_ticket_ext().
		 */
		if (s->internal->tlsext_session_ticket->length > 0) {
			size_t ticklen = s->internal->tlsext_session_ticket->length;

			if ((s->session->tlsext_tick = malloc(ticklen)) == NULL)
				return 0;
			memcpy(s->session->tlsext_tick,
			    s->internal->tlsext_session_ticket->data,
			    ticklen);
			s->session->tlsext_ticklen = ticklen;

			if (!CBB_add_bytes(cbb, s->session->tlsext_tick,
			    s->session->tlsext_ticklen))
				return 0;
		}
	}

	if (!CBB_flush(cbb))
		return 0;

	return 1;
}

int
tlsext_sessionticket_clienthello_parse(SSL *s, CBS *cbs, int *alert)
{
	if (s->internal->tls_session_ticket_ext_cb) {
		if (!s->internal->tls_session_ticket_ext_cb(s, CBS_data(cbs),
		    (int)CBS_len(cbs),
		    s->internal->tls_session_ticket_ext_cb_arg)) {
			*alert = TLS1_AD_INTERNAL_ERROR;
			return 0;
		}
	}

	/* We need to signal that this was processed fully */
	if (!CBS_skip(cbs, CBS_len(cbs))) {
		*alert = TLS1_AD_INTERNAL_ERROR;
		return 0;
	}

	return 1;
}

int
tlsext_sessionticket_serverhello_needs(SSL *s)
{
	return (s->internal->tlsext_ticket_expected &&
	    !(SSL_get_options(s) & SSL_OP_NO_TICKET));
}

int
tlsext_sessionticket_serverhello_build(SSL *s, CBB *cbb)
{
	/* Empty ticket */

	return 1;
}

int
tlsext_sessionticket_serverhello_parse(SSL *s, CBS *cbs, int *alert)
{
	if (s->internal->tls_session_ticket_ext_cb) {
		if (!s->internal->tls_session_ticket_ext_cb(s, CBS_data(cbs),
		    (int)CBS_len(cbs),
		    s->internal->tls_session_ticket_ext_cb_arg)) {
			*alert = TLS1_AD_INTERNAL_ERROR;
			return 0;
		}
	}

	if ((SSL_get_options(s) & SSL_OP_NO_TICKET) != 0 || CBS_len(cbs) > 0) {
		*alert = TLS1_AD_UNSUPPORTED_EXTENSION;
		return 0;
	}

	s->internal->tlsext_ticket_expected = 1;

	return 1;
}

/*
 * DTLS extension for SRTP key establishment - RFC 5764
 */

#ifndef OPENSSL_NO_SRTP

int
tlsext_srtp_clienthello_needs(SSL *s)
{
	return SSL_IS_DTLS(s) && SSL_get_srtp_profiles(s) != NULL;
}

int
tlsext_srtp_clienthello_build(SSL *s, CBB *cbb)
{
	CBB profiles, mki;
	int ct, i;
	STACK_OF(SRTP_PROTECTION_PROFILE) *clnt = NULL;
	SRTP_PROTECTION_PROFILE *prof;

	if ((clnt = SSL_get_srtp_profiles(s)) == NULL) {
		SSLerror(s, SSL_R_EMPTY_SRTP_PROTECTION_PROFILE_LIST);
		return 0;
	}

	if ((ct = sk_SRTP_PROTECTION_PROFILE_num(clnt)) < 1) {
		SSLerror(s, SSL_R_EMPTY_SRTP_PROTECTION_PROFILE_LIST);
		return 0;
	}

	if (!CBB_add_u16_length_prefixed(cbb, &profiles))
		return 0;

	for (i = 0; i < ct; i++) {
		if ((prof = sk_SRTP_PROTECTION_PROFILE_value(clnt, i)) == NULL)
			return 0;
		if (!CBB_add_u16(&profiles, prof->id))
			return 0;
	}

	if (!CBB_add_u8_length_prefixed(cbb, &mki))
		return 0;

	if (!CBB_flush(cbb))
		return 0;

	return 1;
}

int
tlsext_srtp_clienthello_parse(SSL *s, CBS *cbs, int *alert)
{
	SRTP_PROTECTION_PROFILE *cprof, *sprof;
	STACK_OF(SRTP_PROTECTION_PROFILE) *clnt = NULL, *srvr;
	int i, j;
	int ret;
	uint16_t id;
	CBS profiles, mki;

	ret = 0;

	if (!CBS_get_u16_length_prefixed(cbs, &profiles))
		goto err;
	if (CBS_len(&profiles) == 0 || CBS_len(&profiles) % 2 != 0)
		goto err;

	if ((clnt = sk_SRTP_PROTECTION_PROFILE_new_null()) == NULL)
		goto err;

	while (CBS_len(&profiles) > 0) {
		if (!CBS_get_u16(&profiles, &id))
			goto err;

		if (!srtp_find_profile_by_num(id, &cprof)) {
			if (!sk_SRTP_PROTECTION_PROFILE_push(clnt, cprof))
				goto err;
		}
	}

	if (!CBS_get_u8_length_prefixed(cbs, &mki) || CBS_len(&mki) != 0) {
		SSLerror(s, SSL_R_BAD_SRTP_MKI_VALUE);
		*alert = SSL_AD_DECODE_ERROR;
		goto done;
	}
	if (CBS_len(cbs) != 0)
		goto err;

	/*
	 * Per RFC 5764 section 4.1.1
	 *
	 * Find the server preferred profile using the client's list.
	 *
	 * The server MUST send a profile if it sends the use_srtp
	 * extension.  If one is not found, it should fall back to the
	 * negotiated DTLS cipher suite or return a DTLS alert.
	 */
	if ((srvr = SSL_get_srtp_profiles(s)) == NULL)
		goto err;
	for (i = 0; i < sk_SRTP_PROTECTION_PROFILE_num(srvr); i++) {
		if ((sprof = sk_SRTP_PROTECTION_PROFILE_value(srvr, i))
		    == NULL)
			goto err;

		for (j = 0; j < sk_SRTP_PROTECTION_PROFILE_num(clnt); j++) {
			if ((cprof = sk_SRTP_PROTECTION_PROFILE_value(clnt, j))
			    == NULL)
				goto err;

			if (cprof->id == sprof->id) {
				s->internal->srtp_profile = sprof;
				ret = 1;
				goto done;
			}
		}
	}

	/* If we didn't find anything, fall back to the negotiated */
	ret = 1;
	goto done;

 err:
	SSLerror(s, SSL_R_BAD_SRTP_PROTECTION_PROFILE_LIST);
	*alert = SSL_AD_DECODE_ERROR;

 done:
	sk_SRTP_PROTECTION_PROFILE_free(clnt);
	return ret;
}

int
tlsext_srtp_serverhello_needs(SSL *s)
{
	return SSL_IS_DTLS(s) && SSL_get_selected_srtp_profile(s) != NULL;
}

int
tlsext_srtp_serverhello_build(SSL *s, CBB *cbb)
{
	SRTP_PROTECTION_PROFILE *profile;
	CBB srtp, mki;

	if (!CBB_add_u16_length_prefixed(cbb, &srtp))
		return 0;

	if ((profile = SSL_get_selected_srtp_profile(s)) == NULL)
		return 0;

	if (!CBB_add_u16(&srtp, profile->id))
		return 0;

	if (!CBB_add_u8_length_prefixed(cbb, &mki))
		return 0;

	if (!CBB_flush(cbb))
		return 0;

	return 1;
}

int
tlsext_srtp_serverhello_parse(SSL *s, CBS *cbs, int *alert)
{
	STACK_OF(SRTP_PROTECTION_PROFILE) *clnt;
	SRTP_PROTECTION_PROFILE *prof;
	int i;
	uint16_t id;
	CBS profile_ids, mki;

	if (!CBS_get_u16_length_prefixed(cbs, &profile_ids)) {
		SSLerror(s, SSL_R_BAD_SRTP_PROTECTION_PROFILE_LIST);
		goto err;
	}

	if (!CBS_get_u16(&profile_ids, &id) || CBS_len(&profile_ids) != 0) {
		SSLerror(s, SSL_R_BAD_SRTP_PROTECTION_PROFILE_LIST);
		goto err;
	}

	if (!CBS_get_u8_length_prefixed(cbs, &mki) || CBS_len(&mki) != 0) {
		SSLerror(s, SSL_R_BAD_SRTP_MKI_VALUE);
		*alert = SSL_AD_ILLEGAL_PARAMETER;
		return 0;
	}

	if ((clnt = SSL_get_srtp_profiles(s)) == NULL) {
		SSLerror(s, SSL_R_NO_SRTP_PROFILES);
		goto err;
	}

	for (i = 0; i < sk_SRTP_PROTECTION_PROFILE_num(clnt); i++) {
		if ((prof = sk_SRTP_PROTECTION_PROFILE_value(clnt, i))
		    == NULL) {
			SSLerror(s, SSL_R_NO_SRTP_PROFILES);
			goto err;
		}

		if (prof->id == id) {
			s->internal->srtp_profile = prof;
			return 1;
		}
	}

	SSLerror(s, SSL_R_BAD_SRTP_PROTECTION_PROFILE_LIST);
 err:
	*alert = SSL_AD_DECODE_ERROR;
	return 0;
}

#endif /* OPENSSL_NO_SRTP */

struct tls_extension {
	uint16_t type;
	int (*clienthello_needs)(SSL *s);
	int (*clienthello_build)(SSL *s, CBB *cbb);
	int (*clienthello_parse)(SSL *s, CBS *cbs, int *alert);
	int (*serverhello_needs)(SSL *s);
	int (*serverhello_build)(SSL *s, CBB *cbb);
	int (*serverhello_parse)(SSL *s, CBS *cbs, int *alert);
};

static struct tls_extension tls_extensions[] = {
	{
		.type = TLSEXT_TYPE_server_name,
		.clienthello_needs = tlsext_sni_clienthello_needs,
		.clienthello_build = tlsext_sni_clienthello_build,
		.clienthello_parse = tlsext_sni_clienthello_parse,
		.serverhello_needs = tlsext_sni_serverhello_needs,
		.serverhello_build = tlsext_sni_serverhello_build,
		.serverhello_parse = tlsext_sni_serverhello_parse,
	},
	{
		.type = TLSEXT_TYPE_renegotiate,
		.clienthello_needs = tlsext_ri_clienthello_needs,
		.clienthello_build = tlsext_ri_clienthello_build,
		.clienthello_parse = tlsext_ri_clienthello_parse,
		.serverhello_needs = tlsext_ri_serverhello_needs,
		.serverhello_build = tlsext_ri_serverhello_build,
		.serverhello_parse = tlsext_ri_serverhello_parse,
	},
	{
		.type = TLSEXT_TYPE_status_request,
		.clienthello_needs = tlsext_ocsp_clienthello_needs,
		.clienthello_build = tlsext_ocsp_clienthello_build,
		.clienthello_parse = tlsext_ocsp_clienthello_parse,
		.serverhello_needs = tlsext_ocsp_serverhello_needs,
		.serverhello_build = tlsext_ocsp_serverhello_build,
		.serverhello_parse = tlsext_ocsp_serverhello_parse,
	},
	{
		.type = TLSEXT_TYPE_ec_point_formats,
		.clienthello_needs = tlsext_ecpf_clienthello_needs,
		.clienthello_build = tlsext_ecpf_clienthello_build,
		.clienthello_parse = tlsext_ecpf_clienthello_parse,
		.serverhello_needs = tlsext_ecpf_serverhello_needs,
		.serverhello_build = tlsext_ecpf_serverhello_build,
		.serverhello_parse = tlsext_ecpf_serverhello_parse,
	},
	{
		.type = TLSEXT_TYPE_elliptic_curves,
		.clienthello_needs = tlsext_ec_clienthello_needs,
		.clienthello_build = tlsext_ec_clienthello_build,
		.clienthello_parse = tlsext_ec_clienthello_parse,
		.serverhello_needs = tlsext_ec_serverhello_needs,
		.serverhello_build = tlsext_ec_serverhello_build,
		.serverhello_parse = tlsext_ec_serverhello_parse,
	},
	{
		.type = TLSEXT_TYPE_session_ticket,
		.clienthello_needs = tlsext_sessionticket_clienthello_needs,
		.clienthello_build = tlsext_sessionticket_clienthello_build,
		.clienthello_parse = tlsext_sessionticket_clienthello_parse,
		.serverhello_needs = tlsext_sessionticket_serverhello_needs,
		.serverhello_build = tlsext_sessionticket_serverhello_build,
		.serverhello_parse = tlsext_sessionticket_serverhello_parse,
	},
	{
		.type = TLSEXT_TYPE_signature_algorithms,
		.clienthello_needs = tlsext_sigalgs_clienthello_needs,
		.clienthello_build = tlsext_sigalgs_clienthello_build,
		.clienthello_parse = tlsext_sigalgs_clienthello_parse,
		.serverhello_needs = tlsext_sigalgs_serverhello_needs,
		.serverhello_build = tlsext_sigalgs_serverhello_build,
		.serverhello_parse = tlsext_sigalgs_serverhello_parse,
	},
	{
		.type = TLSEXT_TYPE_application_layer_protocol_negotiation,
		.clienthello_needs = tlsext_alpn_clienthello_needs,
		.clienthello_build = tlsext_alpn_clienthello_build,
		.clienthello_parse = tlsext_alpn_clienthello_parse,
		.serverhello_needs = tlsext_alpn_serverhello_needs,
		.serverhello_build = tlsext_alpn_serverhello_build,
		.serverhello_parse = tlsext_alpn_serverhello_parse,
	},
#ifndef OPENSSL_NO_SRTP
	{
		.type = TLSEXT_TYPE_use_srtp,
		.clienthello_needs = tlsext_srtp_clienthello_needs,
		.clienthello_build = tlsext_srtp_clienthello_build,
		.clienthello_parse = tlsext_srtp_clienthello_parse,
		.serverhello_needs = tlsext_srtp_serverhello_needs,
		.serverhello_build = tlsext_srtp_serverhello_build,
		.serverhello_parse = tlsext_srtp_serverhello_parse,
	}
#endif /* OPENSSL_NO_SRTP */
};

#define N_TLS_EXTENSIONS (sizeof(tls_extensions) / sizeof(*tls_extensions))

int
tlsext_clienthello_build(SSL *s, CBB *cbb)
{
	CBB extensions, extension_data;
	struct tls_extension *tlsext;
	int extensions_present = 0;
	size_t i;

	if (!CBB_add_u16_length_prefixed(cbb, &extensions))
		return 0;

	for (i = 0; i < N_TLS_EXTENSIONS; i++) {
		tlsext = &tls_extensions[i];

		if (!tlsext->clienthello_needs(s))
			continue;

		if (!CBB_add_u16(&extensions, tlsext->type))
			return 0;
		if (!CBB_add_u16_length_prefixed(&extensions, &extension_data))
			return 0;
		if (!tls_extensions[i].clienthello_build(s, &extension_data))
			return 0;

		extensions_present = 1;
	}

	if (!extensions_present)
		CBB_discard_child(cbb);

	if (!CBB_flush(cbb))
		return 0;

	return 1;
}

int
tlsext_clienthello_parse_one(SSL *s, CBS *cbs, uint16_t type, int *alert)
{
	struct tls_extension *tlsext;
	size_t i;

	for (i = 0; i < N_TLS_EXTENSIONS; i++) {
		tlsext = &tls_extensions[i];

		if (tlsext->type != type)
			continue;
		if (!tlsext->clienthello_parse(s, cbs, alert))
			return 0;
		if (CBS_len(cbs) != 0) {
			*alert = SSL_AD_DECODE_ERROR;
			return 0;
		}

		return 1;
	}

	/* Not found. */
	return 2;
}

int
tlsext_serverhello_build(SSL *s, CBB *cbb)
{
	CBB extensions, extension_data;
	struct tls_extension *tlsext;
	int extensions_present = 0;
	size_t i;

	if (!CBB_add_u16_length_prefixed(cbb, &extensions))
		return 0;

	for (i = 0; i < N_TLS_EXTENSIONS; i++) {
		tlsext = &tls_extensions[i];

		if (!tlsext->serverhello_needs(s))
			continue;

		if (!CBB_add_u16(&extensions, tlsext->type))
			return 0;
		if (!CBB_add_u16_length_prefixed(&extensions, &extension_data))
			return 0;
		if (!tlsext->serverhello_build(s, &extension_data))
			return 0;

		extensions_present = 1;
	}

	if (!extensions_present)
		CBB_discard_child(cbb);

	if (!CBB_flush(cbb))
		return 0;

	return 1;
}

int
tlsext_serverhello_parse_one(SSL *s, CBS *cbs, uint16_t type, int *alert)
{
	struct tls_extension *tlsext;
	size_t i;

	for (i = 0; i < N_TLS_EXTENSIONS; i++) {
		tlsext = &tls_extensions[i];

		if (tlsext->type != type)
			continue;
		if (!tlsext->serverhello_parse(s, cbs, alert))
			return 0;
		if (CBS_len(cbs) != 0) {
			*alert = SSL_AD_DECODE_ERROR;
			return 0;
		}

		return 1;
	}

	/* Not found. */
	return 2;
}
