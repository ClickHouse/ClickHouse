/* $OpenBSD: d1_srvr.c,v 1.88 2017/05/07 04:22:24 beck Exp $ */
/*
 * DTLS implementation written by Nagendra Modadugu
 * (nagendra@cs.stanford.edu) for the OpenSSL project 2005.
 */
/* ====================================================================
 * Copyright (c) 1999-2007 The OpenSSL Project.  All rights reserved.
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
 *    for use in the OpenSSL Toolkit. (http://www.OpenSSL.org/)"
 *
 * 4. The names "OpenSSL Toolkit" and "OpenSSL Project" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For written permission, please contact
 *    openssl-core@OpenSSL.org.
 *
 * 5. Products derived from this software may not be called "OpenSSL"
 *    nor may "OpenSSL" appear in their names without prior written
 *    permission of the OpenSSL Project.
 *
 * 6. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by the OpenSSL Project
 *    for use in the OpenSSL Toolkit (http://www.OpenSSL.org/)"
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

#include <openssl/bn.h>
#include <openssl/buffer.h>
#include <openssl/dh.h>
#include <openssl/evp.h>
#include <openssl/md5.h>
#include <openssl/objects.h>
#include <openssl/x509.h>

static int dtls1_send_hello_verify_request(SSL *s);

static const SSL_METHOD_INTERNAL DTLSv1_server_method_internal_data = {
	.version = DTLS1_VERSION,
	.min_version = DTLS1_VERSION,
	.max_version = DTLS1_VERSION,
	.ssl_new = dtls1_new,
	.ssl_clear = dtls1_clear,
	.ssl_free = dtls1_free,
	.ssl_accept = dtls1_accept,
	.ssl_connect = ssl_undefined_function,
	.ssl_read = ssl3_read,
	.ssl_peek = ssl3_peek,
	.ssl_write = ssl3_write,
	.ssl_shutdown = dtls1_shutdown,
	.ssl_pending = ssl3_pending,
	.get_ssl_method = dtls1_get_server_method,
	.get_timeout = dtls1_default_timeout,
	.ssl_version = ssl_undefined_void_function,
	.ssl_renegotiate = ssl3_renegotiate,
	.ssl_renegotiate_check = ssl3_renegotiate_check,
	.ssl_get_message = dtls1_get_message,
	.ssl_read_bytes = dtls1_read_bytes,
	.ssl_write_bytes = dtls1_write_app_data_bytes,
	.ssl3_enc = &DTLSv1_enc_data,
};

static const SSL_METHOD DTLSv1_server_method_data = {
	.ssl_dispatch_alert = dtls1_dispatch_alert,
	.num_ciphers = ssl3_num_ciphers,
	.get_cipher = dtls1_get_cipher,
	.get_cipher_by_char = ssl3_get_cipher_by_char,
	.put_cipher_by_char = ssl3_put_cipher_by_char,
	.internal = &DTLSv1_server_method_internal_data,
};

const SSL_METHOD *
DTLSv1_server_method(void)
{
	return &DTLSv1_server_method_data;
}

const SSL_METHOD *
dtls1_get_server_method(int ver)
{
	if (ver == DTLS1_VERSION)
		return (DTLSv1_server_method());
	return (NULL);
}

int
dtls1_accept(SSL *s)
{
	void (*cb)(const SSL *ssl, int type, int val) = NULL;
	unsigned long alg_k;
	int ret = -1;
	int new_state, state, skip = 0;
	int listen;

	ERR_clear_error();
	errno = 0;

	if (s->internal->info_callback != NULL)
		cb = s->internal->info_callback;
	else if (s->ctx->internal->info_callback != NULL)
		cb = s->ctx->internal->info_callback;

	listen = D1I(s)->listen;

	/* init things to blank */
	s->internal->in_handshake++;
	if (!SSL_in_init(s) || SSL_in_before(s))
		SSL_clear(s);

	D1I(s)->listen = listen;

	if (s->cert == NULL) {
		SSLerror(s, SSL_R_NO_CERTIFICATE_SET);
		ret = -1;
		goto end;
	}

	for (;;) {
		state = S3I(s)->hs.state;

		switch (S3I(s)->hs.state) {
		case SSL_ST_RENEGOTIATE:
			s->internal->renegotiate = 1;
			/* S3I(s)->hs.state=SSL_ST_ACCEPT; */

		case SSL_ST_BEFORE:
		case SSL_ST_ACCEPT:
		case SSL_ST_BEFORE|SSL_ST_ACCEPT:
		case SSL_ST_OK|SSL_ST_ACCEPT:

			s->server = 1;
			if (cb != NULL)
				cb(s, SSL_CB_HANDSHAKE_START, 1);

			if ((s->version & 0xff00) != (DTLS1_VERSION & 0xff00)) {
				SSLerror(s, ERR_R_INTERNAL_ERROR);
				ret = -1;
				goto end;
			}
			s->internal->type = SSL_ST_ACCEPT;

			if (!ssl3_setup_init_buffer(s)) {
				ret = -1;
				goto end;
			}
			if (!ssl3_setup_buffers(s)) {
				ret = -1;
				goto end;
			}

			s->internal->init_num = 0;

			if (S3I(s)->hs.state != SSL_ST_RENEGOTIATE) {
				/* Ok, we now need to push on a buffering BIO so that
				 * the output is sent in a way that TCP likes :-)
				 * ...but not with SCTP :-)
				 */
				if (!ssl_init_wbio_buffer(s, 1)) {
					ret = -1;
					goto end;
				}

				if (!tls1_init_finished_mac(s)) {
					ret = -1;
					goto end;
				}

				S3I(s)->hs.state = SSL3_ST_SR_CLNT_HELLO_A;
				s->ctx->internal->stats.sess_accept++;
			} else {
				/* S3I(s)->hs.state == SSL_ST_RENEGOTIATE,
				 * we will just send a HelloRequest */
				s->ctx->internal->stats.sess_accept_renegotiate++;
				S3I(s)->hs.state = SSL3_ST_SW_HELLO_REQ_A;
			}

			break;

		case SSL3_ST_SW_HELLO_REQ_A:
		case SSL3_ST_SW_HELLO_REQ_B:

			s->internal->shutdown = 0;
			dtls1_clear_record_buffer(s);
			dtls1_start_timer(s);
			ret = ssl3_send_hello_request(s);
			if (ret <= 0)
				goto end;
			S3I(s)->hs.next_state = SSL3_ST_SR_CLNT_HELLO_A;
			S3I(s)->hs.state = SSL3_ST_SW_FLUSH;
			s->internal->init_num = 0;

			if (!tls1_init_finished_mac(s)) {
				ret = -1;
				goto end;
			}
			break;

		case SSL3_ST_SW_HELLO_REQ_C:
			S3I(s)->hs.state = SSL_ST_OK;
			break;

		case SSL3_ST_SR_CLNT_HELLO_A:
		case SSL3_ST_SR_CLNT_HELLO_B:
		case SSL3_ST_SR_CLNT_HELLO_C:

			s->internal->shutdown = 0;
			ret = ssl3_get_client_hello(s);
			if (ret <= 0)
				goto end;
			dtls1_stop_timer(s);

			if (ret == 1 && (SSL_get_options(s) & SSL_OP_COOKIE_EXCHANGE))
				S3I(s)->hs.state = DTLS1_ST_SW_HELLO_VERIFY_REQUEST_A;
			else
				S3I(s)->hs.state = SSL3_ST_SW_SRVR_HELLO_A;

			s->internal->init_num = 0;

			/* Reflect ClientHello sequence to remain stateless while listening */
			if (listen) {
				memcpy(S3I(s)->write_sequence, S3I(s)->read_sequence, sizeof(S3I(s)->write_sequence));
			}

			/* If we're just listening, stop here */
			if (listen && S3I(s)->hs.state == SSL3_ST_SW_SRVR_HELLO_A) {
				ret = 2;
				D1I(s)->listen = 0;
				/* Set expected sequence numbers
				 * to continue the handshake.
				 */
				D1I(s)->handshake_read_seq = 2;
				D1I(s)->handshake_write_seq = 1;
				D1I(s)->next_handshake_write_seq = 1;
				goto end;
			}

			break;

		case DTLS1_ST_SW_HELLO_VERIFY_REQUEST_A:
		case DTLS1_ST_SW_HELLO_VERIFY_REQUEST_B:

			ret = dtls1_send_hello_verify_request(s);
			if (ret <= 0)
				goto end;
			S3I(s)->hs.state = SSL3_ST_SW_FLUSH;
			S3I(s)->hs.next_state = SSL3_ST_SR_CLNT_HELLO_A;

			/* HelloVerifyRequest resets Finished MAC */
			if (!tls1_init_finished_mac(s)) {
				ret = -1;
				goto end;
			}
			break;


		case SSL3_ST_SW_SRVR_HELLO_A:
		case SSL3_ST_SW_SRVR_HELLO_B:
			s->internal->renegotiate = 2;
			dtls1_start_timer(s);
			ret = ssl3_send_server_hello(s);
			if (ret <= 0)
				goto end;

			if (s->internal->hit) {
				if (s->internal->tlsext_ticket_expected)
					S3I(s)->hs.state = SSL3_ST_SW_SESSION_TICKET_A;
				else
					S3I(s)->hs.state = SSL3_ST_SW_CHANGE_A;
			} else
				S3I(s)->hs.state = SSL3_ST_SW_CERT_A;
			s->internal->init_num = 0;
			break;

		case SSL3_ST_SW_CERT_A:
		case SSL3_ST_SW_CERT_B:
			/* Check if it is anon DH. */
			if (!(S3I(s)->hs.new_cipher->algorithm_auth &
			    SSL_aNULL)) {
				dtls1_start_timer(s);
				ret = ssl3_send_server_certificate(s);
				if (ret <= 0)
					goto end;
				if (s->internal->tlsext_status_expected)
					S3I(s)->hs.state = SSL3_ST_SW_CERT_STATUS_A;
				else
					S3I(s)->hs.state = SSL3_ST_SW_KEY_EXCH_A;
			} else {
				skip = 1;
				S3I(s)->hs.state = SSL3_ST_SW_KEY_EXCH_A;
			}
			s->internal->init_num = 0;
			break;

		case SSL3_ST_SW_KEY_EXCH_A:
		case SSL3_ST_SW_KEY_EXCH_B:
			alg_k = S3I(s)->hs.new_cipher->algorithm_mkey;

			/* Only send if using a DH key exchange. */
			if (alg_k & (SSL_kDHE|SSL_kECDHE)) {
				dtls1_start_timer(s);
				ret = ssl3_send_server_key_exchange(s);
				if (ret <= 0)
					goto end;
			} else
				skip = 1;

			S3I(s)->hs.state = SSL3_ST_SW_CERT_REQ_A;
			s->internal->init_num = 0;
			break;

		case SSL3_ST_SW_CERT_REQ_A:
		case SSL3_ST_SW_CERT_REQ_B:
			/*
			 * Determine whether or not we need to request a
			 * certificate.
			 *
			 * Do not request a certificate if:
			 *
			 * - We did not ask for it (SSL_VERIFY_PEER is unset).
			 *
			 * - SSL_VERIFY_CLIENT_ONCE is set and we are
			 *   renegotiating.
			 *
			 * - We are using an anonymous ciphersuites
			 *   (see section "Certificate request" in SSL 3 drafts
			 *   and in RFC 2246) ... except when the application
			 *   insists on verification (against the specs, but
			 *   s3_clnt.c accepts this for SSL 3).
			 */
			if (!(s->verify_mode & SSL_VERIFY_PEER) ||
			    ((s->session->peer != NULL) &&
			     (s->verify_mode & SSL_VERIFY_CLIENT_ONCE)) ||
			    ((S3I(s)->hs.new_cipher->algorithm_auth &
			     SSL_aNULL) && !(s->verify_mode &
			     SSL_VERIFY_FAIL_IF_NO_PEER_CERT))) {
				/* no cert request */
				skip = 1;
				S3I(s)->tmp.cert_request = 0;
				S3I(s)->hs.state = SSL3_ST_SW_SRVR_DONE_A;
			} else {
				S3I(s)->tmp.cert_request = 1;
				dtls1_start_timer(s);
				ret = ssl3_send_certificate_request(s);
				if (ret <= 0)
					goto end;
				S3I(s)->hs.state = SSL3_ST_SW_SRVR_DONE_A;
				s->internal->init_num = 0;
			}
			break;

		case SSL3_ST_SW_SRVR_DONE_A:
		case SSL3_ST_SW_SRVR_DONE_B:
			dtls1_start_timer(s);
			ret = ssl3_send_server_done(s);
			if (ret <= 0)
				goto end;
			S3I(s)->hs.next_state = SSL3_ST_SR_CERT_A;
			S3I(s)->hs.state = SSL3_ST_SW_FLUSH;
			s->internal->init_num = 0;
			break;

		case SSL3_ST_SW_FLUSH:
			s->internal->rwstate = SSL_WRITING;
			if (BIO_flush(s->wbio) <= 0) {
				/* If the write error was fatal, stop trying */
				if (!BIO_should_retry(s->wbio)) {
					s->internal->rwstate = SSL_NOTHING;
					S3I(s)->hs.state = S3I(s)->hs.next_state;
				}

				ret = -1;
				goto end;
			}
			s->internal->rwstate = SSL_NOTHING;
			S3I(s)->hs.state = S3I(s)->hs.next_state;
			break;

		case SSL3_ST_SR_CERT_A:
		case SSL3_ST_SR_CERT_B:
			if (S3I(s)->tmp.cert_request) {
				ret = ssl3_get_client_certificate(s);
				if (ret <= 0)
					goto end;
			}
			s->internal->init_num = 0;
			S3I(s)->hs.state = SSL3_ST_SR_KEY_EXCH_A;
			break;

		case SSL3_ST_SR_KEY_EXCH_A:
		case SSL3_ST_SR_KEY_EXCH_B:
			ret = ssl3_get_client_key_exchange(s);
			if (ret <= 0)
				goto end;

			S3I(s)->hs.state = SSL3_ST_SR_CERT_VRFY_A;
			s->internal->init_num = 0;

			if (ret == 2) {
				/* For the ECDH ciphersuites when
				 * the client sends its ECDH pub key in
				 * a certificate, the CertificateVerify
				 * message is not sent.
				 */
				S3I(s)->hs.state = SSL3_ST_SR_FINISHED_A;
				s->internal->init_num = 0;
			} else if (SSL_USE_SIGALGS(s)) {
				S3I(s)->hs.state = SSL3_ST_SR_CERT_VRFY_A;
				s->internal->init_num = 0;
				if (!s->session->peer)
					break;

				/*
				 * For sigalgs freeze the handshake buffer
				 * at this point and digest cached records.
				 */
				if (!S3I(s)->handshake_buffer) {
					SSLerror(s, ERR_R_INTERNAL_ERROR);
					ret = -1;
					goto end;
				}
				s->s3->flags |= TLS1_FLAGS_KEEP_HANDSHAKE;
				if (!tls1_digest_cached_records(s)) {
					ret = -1;
					goto end;
				}
			} else {
				S3I(s)->hs.state = SSL3_ST_SR_CERT_VRFY_A;
				s->internal->init_num = 0;

				/*
				 * We need to get hashes here so if there is
				 * a client cert, it can be verified.
				 */
				if (S3I(s)->handshake_buffer) {
					if (!tls1_digest_cached_records(s)) {
						ret = -1;
						goto end;
					}
				}
				if (!tls1_handshake_hash_value(s,
				    S3I(s)->tmp.cert_verify_md,
				    sizeof(S3I(s)->tmp.cert_verify_md),
				    NULL)) {
					ret = -1;
					goto end;
				}
			}
			break;

		case SSL3_ST_SR_CERT_VRFY_A:
		case SSL3_ST_SR_CERT_VRFY_B:

			D1I(s)->change_cipher_spec_ok = 1;
			/* we should decide if we expected this one */
			ret = ssl3_get_cert_verify(s);
			if (ret <= 0)
				goto end;
			S3I(s)->hs.state = SSL3_ST_SR_FINISHED_A;
			s->internal->init_num = 0;
			break;

		case SSL3_ST_SR_FINISHED_A:
		case SSL3_ST_SR_FINISHED_B:
			D1I(s)->change_cipher_spec_ok = 1;
			ret = ssl3_get_finished(s, SSL3_ST_SR_FINISHED_A,
			SSL3_ST_SR_FINISHED_B);
			if (ret <= 0)
				goto end;
			dtls1_stop_timer(s);
			if (s->internal->hit)
				S3I(s)->hs.state = SSL_ST_OK;
			else if (s->internal->tlsext_ticket_expected)
				S3I(s)->hs.state = SSL3_ST_SW_SESSION_TICKET_A;
			else
				S3I(s)->hs.state = SSL3_ST_SW_CHANGE_A;
			s->internal->init_num = 0;
			break;

		case SSL3_ST_SW_SESSION_TICKET_A:
		case SSL3_ST_SW_SESSION_TICKET_B:
			ret = ssl3_send_newsession_ticket(s);
			if (ret <= 0)
				goto end;
			S3I(s)->hs.state = SSL3_ST_SW_CHANGE_A;
			s->internal->init_num = 0;
			break;

		case SSL3_ST_SW_CERT_STATUS_A:
		case SSL3_ST_SW_CERT_STATUS_B:
			ret = ssl3_send_cert_status(s);
			if (ret <= 0)
				goto end;
			S3I(s)->hs.state = SSL3_ST_SW_KEY_EXCH_A;
			s->internal->init_num = 0;
			break;


		case SSL3_ST_SW_CHANGE_A:
		case SSL3_ST_SW_CHANGE_B:

			s->session->cipher = S3I(s)->hs.new_cipher;
			if (!tls1_setup_key_block(s)) {
				ret = -1;
				goto end;
			}

			ret = dtls1_send_change_cipher_spec(s,
			SSL3_ST_SW_CHANGE_A, SSL3_ST_SW_CHANGE_B);

			if (ret <= 0)
				goto end;


			S3I(s)->hs.state = SSL3_ST_SW_FINISHED_A;
			s->internal->init_num = 0;

			if (!tls1_change_cipher_state(s,
				SSL3_CHANGE_CIPHER_SERVER_WRITE)) {
				ret = -1;
				goto end;
			}

			dtls1_reset_seq_numbers(s, SSL3_CC_WRITE);
			break;

		case SSL3_ST_SW_FINISHED_A:
		case SSL3_ST_SW_FINISHED_B:
			ret = ssl3_send_finished(s,
			    SSL3_ST_SW_FINISHED_A, SSL3_ST_SW_FINISHED_B,
			    TLS_MD_SERVER_FINISH_CONST,
			    TLS_MD_SERVER_FINISH_CONST_SIZE);
			if (ret <= 0)
				goto end;
			S3I(s)->hs.state = SSL3_ST_SW_FLUSH;
			if (s->internal->hit) {
				S3I(s)->hs.next_state = SSL3_ST_SR_FINISHED_A;

			} else {
				S3I(s)->hs.next_state = SSL_ST_OK;
			}
			s->internal->init_num = 0;
			break;

		case SSL_ST_OK:
			/* clean a few things up */
			tls1_cleanup_key_block(s);

			/* remove buffering on output */
			ssl_free_wbio_buffer(s);

			s->internal->init_num = 0;

			if (s->internal->renegotiate == 2) /* skipped if we just sent a HelloRequest */
			{
				s->internal->renegotiate = 0;
				s->internal->new_session = 0;

				ssl_update_cache(s, SSL_SESS_CACHE_SERVER);

				s->ctx->internal->stats.sess_accept_good++;
				/* s->server=1; */
				s->internal->handshake_func = dtls1_accept;

				if (cb != NULL)
					cb(s, SSL_CB_HANDSHAKE_DONE, 1);
			}

			ret = 1;

			/* done handshaking, next message is client hello */
			D1I(s)->handshake_read_seq = 0;
			/* next message is server hello */
			D1I(s)->handshake_write_seq = 0;
			D1I(s)->next_handshake_write_seq = 0;
			goto end;
			/* break; */

		default:
			SSLerror(s, SSL_R_UNKNOWN_STATE);
			ret = -1;
			goto end;
			/* break; */
		}

		if (!S3I(s)->tmp.reuse_message && !skip) {
			if (s->internal->debug) {
				if ((ret = BIO_flush(s->wbio)) <= 0)
					goto end;
			}

			if ((cb != NULL) && (S3I(s)->hs.state != state)) {
				new_state = S3I(s)->hs.state;
				S3I(s)->hs.state = state;
				cb(s, SSL_CB_ACCEPT_LOOP, 1);
				S3I(s)->hs.state = new_state;
			}
		}
		skip = 0;
	}
end:
	/* BIO_flush(s->wbio); */

	s->internal->in_handshake--;

	if (cb != NULL)
		cb(s, SSL_CB_ACCEPT_EXIT, ret);

	return (ret);
}

int
dtls1_send_hello_verify_request(SSL *s)
{
	CBB cbb, verify, cookie;

	memset(&cbb, 0, sizeof(cbb));

	if (S3I(s)->hs.state == DTLS1_ST_SW_HELLO_VERIFY_REQUEST_A) {
		if (s->ctx->internal->app_gen_cookie_cb == NULL ||
		    s->ctx->internal->app_gen_cookie_cb(s, D1I(s)->cookie,
			&(D1I(s)->cookie_len)) == 0) {
			SSLerror(s, ERR_R_INTERNAL_ERROR);
			return 0;
		}

		if (!ssl3_handshake_msg_start_cbb(s, &cbb, &verify,
		    DTLS1_MT_HELLO_VERIFY_REQUEST))
			goto err;
		if (!CBB_add_u16(&verify, s->version))
			goto err;
		if (!CBB_add_u8_length_prefixed(&verify, &cookie))
			goto err;
		if (!CBB_add_bytes(&cookie, D1I(s)->cookie, D1I(s)->cookie_len))
			goto err;
		if (!ssl3_handshake_msg_finish_cbb(s, &cbb))
			goto err;

		S3I(s)->hs.state = DTLS1_ST_SW_HELLO_VERIFY_REQUEST_B;
	}

	/* S3I(s)->hs.state = DTLS1_ST_SW_HELLO_VERIFY_REQUEST_B */
	return (ssl3_handshake_write(s));

 err:
	CBB_cleanup(&cbb);

	return (-1);
}
