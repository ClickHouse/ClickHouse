/* $OpenBSD: ssl_srvr.c,v 1.22 2017/08/12 21:47:59 jsing Exp $ */
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
/* ====================================================================
 * Copyright 2002 Sun Microsystems, Inc. ALL RIGHTS RESERVED.
 *
 * Portions of the attached software ("Contribution") are developed by
 * SUN MICROSYSTEMS, INC., and are contributed to the OpenSSL project.
 *
 * The Contribution is licensed pursuant to the OpenSSL open source
 * license provided above.
 *
 * ECC cipher suite support in OpenSSL originally written by
 * Vipul Gupta and Sumit Gupta of Sun Microsystems Laboratories.
 *
 */
/* ====================================================================
 * Copyright 2005 Nokia. All rights reserved.
 *
 * The portions of the attached software ("Contribution") is developed by
 * Nokia Corporation and is licensed pursuant to the OpenSSL open source
 * license.
 *
 * The Contribution, originally written by Mika Kousa and Pasi Eronen of
 * Nokia Corporation, consists of the "PSK" (Pre-Shared Key) ciphersuites
 * support (see RFC 4279) to OpenSSL.
 *
 * No patent licenses or other rights except those expressly stated in
 * the OpenSSL open source license shall be deemed granted or received
 * expressly, by implication, estoppel, or otherwise.
 *
 * No assurances are provided by Nokia that the Contribution does not
 * infringe the patent or other intellectual property rights of any third
 * party or that the license provides you with all the necessary rights
 * to make use of the Contribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND. IN
 * ADDITION TO THE DISCLAIMERS INCLUDED IN THE LICENSE, NOKIA
 * SPECIFICALLY DISCLAIMS ANY LIABILITY FOR CLAIMS BROUGHT BY YOU OR ANY
 * OTHER ENTITY BASED ON INFRINGEMENT OF INTELLECTUAL PROPERTY RIGHTS OR
 * OTHERWISE.
 */

#include <stdio.h>

#include "ssl_locl.h"

#include <openssl/bn.h>
#include <openssl/buffer.h>
#include <openssl/curve25519.h>
#include <openssl/evp.h>
#include <openssl/dh.h>
#ifndef OPENSSL_NO_GOST
#include <openssl/gost.h>
#endif
#include <openssl/hmac.h>
#include <openssl/md5.h>
#include <openssl/objects.h>
#include <openssl/x509.h>

#include "bytestring.h"

int
ssl3_accept(SSL *s)
{
	unsigned long alg_k;
	void (*cb)(const SSL *ssl, int type, int val) = NULL;
	int ret = -1;
	int new_state, state, skip = 0;

	ERR_clear_error();
	errno = 0;

	if (s->internal->info_callback != NULL)
		cb = s->internal->info_callback;
	else if (s->ctx->internal->info_callback != NULL)
		cb = s->ctx->internal->info_callback;

	/* init things to blank */
	s->internal->in_handshake++;
	if (!SSL_in_init(s) || SSL_in_before(s))
		SSL_clear(s);

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

			if ((s->version >> 8) != 3) {
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
				/*
				 * Ok, we now need to push on a buffering BIO
				 * so that the output is sent in a way that
				 * TCP likes :-)
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
			} else if (!S3I(s)->send_connection_binding) {
				/*
				 * Server attempting to renegotiate with
				 * client that doesn't support secure
				 * renegotiation.
				 */
				SSLerror(s, SSL_R_UNSAFE_LEGACY_RENEGOTIATION_DISABLED);
				ssl3_send_alert(s, SSL3_AL_FATAL,
				    SSL_AD_HANDSHAKE_FAILURE);
				ret = -1;
				goto end;
			} else {
				/*
				 * S3I(s)->hs.state == SSL_ST_RENEGOTIATE,
				 * we will just send a HelloRequest
				 */
				s->ctx->internal->stats.sess_accept_renegotiate++;
				S3I(s)->hs.state = SSL3_ST_SW_HELLO_REQ_A;
			}
			break;

		case SSL3_ST_SW_HELLO_REQ_A:
		case SSL3_ST_SW_HELLO_REQ_B:

			s->internal->shutdown = 0;
			ret = ssl3_send_hello_request(s);
			if (ret <= 0)
				goto end;
			S3I(s)->hs.next_state = SSL3_ST_SW_HELLO_REQ_C;
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
			if (s->internal->rwstate != SSL_X509_LOOKUP) {
				ret = ssl3_get_client_hello(s);
				if (ret <= 0)
					goto end;
			}

			s->internal->renegotiate = 2;
			S3I(s)->hs.state = SSL3_ST_SW_SRVR_HELLO_A;
			s->internal->init_num = 0;
			break;

		case SSL3_ST_SW_SRVR_HELLO_A:
		case SSL3_ST_SW_SRVR_HELLO_B:
			ret = ssl3_send_server_hello(s);
			if (ret <= 0)
				goto end;
			if (s->internal->hit) {
				if (s->internal->tlsext_ticket_expected)
					S3I(s)->hs.state = SSL3_ST_SW_SESSION_TICKET_A;
				else
					S3I(s)->hs.state = SSL3_ST_SW_CHANGE_A;
			}
			else
				S3I(s)->hs.state = SSL3_ST_SW_CERT_A;
			s->internal->init_num = 0;
			break;

		case SSL3_ST_SW_CERT_A:
		case SSL3_ST_SW_CERT_B:
			/* Check if it is anon DH or anon ECDH. */
			if (!(S3I(s)->hs.new_cipher->algorithm_auth &
			    SSL_aNULL)) {
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

			/*
			 * Only send if using a DH key exchange.
			 *
			 * For ECC ciphersuites, we send a ServerKeyExchange
			 * message only if the cipher suite is ECDHE. In other
			 * cases, the server certificate contains the server's
			 * public key for key exchange.
			 */
			if (alg_k & (SSL_kDHE|SSL_kECDHE)) {
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
				/* No cert request */
				skip = 1;
				S3I(s)->tmp.cert_request = 0;
				S3I(s)->hs.state = SSL3_ST_SW_SRVR_DONE_A;
				if (S3I(s)->handshake_buffer) {
					if (!tls1_digest_cached_records(s)) {
						ret = -1;
						goto end;
					}
				}
			} else {
				S3I(s)->tmp.cert_request = 1;
				ret = ssl3_send_certificate_request(s);
				if (ret <= 0)
					goto end;
				S3I(s)->hs.state = SSL3_ST_SW_SRVR_DONE_A;
				s->internal->init_num = 0;
			}
			break;

		case SSL3_ST_SW_SRVR_DONE_A:
		case SSL3_ST_SW_SRVR_DONE_B:
			ret = ssl3_send_server_done(s);
			if (ret <= 0)
				goto end;
			S3I(s)->hs.next_state = SSL3_ST_SR_CERT_A;
			S3I(s)->hs.state = SSL3_ST_SW_FLUSH;
			s->internal->init_num = 0;
			break;

		case SSL3_ST_SW_FLUSH:

			/*
			 * This code originally checked to see if
			 * any data was pending using BIO_CTRL_INFO
			 * and then flushed. This caused problems
			 * as documented in PR#1939. The proposed
			 * fix doesn't completely resolve this issue
			 * as buggy implementations of BIO_CTRL_PENDING
			 * still exist. So instead we just flush
			 * unconditionally.
			 */

			s->internal->rwstate = SSL_WRITING;
			if (BIO_flush(s->wbio) <= 0) {
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
			alg_k = S3I(s)->hs.new_cipher->algorithm_mkey;
			if (ret == 2) {
				/*
				 * For the ECDH ciphersuites when
				 * the client sends its ECDH pub key in
				 * a certificate, the CertificateVerify
				 * message is not sent.
				 * Also for GOST ciphersuites when
				 * the client uses its key from the certificate
				 * for key exchange.
				 */
				S3I(s)->hs.state = SSL3_ST_SR_FINISHED_A;
				s->internal->init_num = 0;
			} else if (SSL_USE_SIGALGS(s) || (alg_k & SSL_kGOST)) {
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
			s->s3->flags |= SSL3_FLAGS_CCS_OK;

			/* we should decide if we expected this one */
			ret = ssl3_get_cert_verify(s);
			if (ret <= 0)
				goto end;

			S3I(s)->hs.state = SSL3_ST_SR_FINISHED_A;
			s->internal->init_num = 0;
			break;

		case SSL3_ST_SR_FINISHED_A:
		case SSL3_ST_SR_FINISHED_B:
			s->s3->flags |= SSL3_FLAGS_CCS_OK;
			ret = ssl3_get_finished(s, SSL3_ST_SR_FINISHED_A,
			    SSL3_ST_SR_FINISHED_B);
			if (ret <= 0)
				goto end;
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

			ret = ssl3_send_change_cipher_spec(s,
			    SSL3_ST_SW_CHANGE_A, SSL3_ST_SW_CHANGE_B);

			if (ret <= 0)
				goto end;
			S3I(s)->hs.state = SSL3_ST_SW_FINISHED_A;
			s->internal->init_num = 0;

			if (!tls1_change_cipher_state(
			    s, SSL3_CHANGE_CIPHER_SERVER_WRITE)) {
				ret = -1;
				goto end;
			}

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
			if (s->internal->hit)
				S3I(s)->hs.next_state = SSL3_ST_SR_FINISHED_A;
			else
				S3I(s)->hs.next_state = SSL_ST_OK;
			s->internal->init_num = 0;
			break;

		case SSL_ST_OK:
			/* clean a few things up */
			tls1_cleanup_key_block(s);

			BUF_MEM_free(s->internal->init_buf);
			s->internal->init_buf = NULL;

			/* remove buffering on output */
			ssl_free_wbio_buffer(s);

			s->internal->init_num = 0;

			/* skipped if we just sent a HelloRequest */
			if (s->internal->renegotiate == 2) {
				s->internal->renegotiate = 0;
				s->internal->new_session = 0;

				ssl_update_cache(s, SSL_SESS_CACHE_SERVER);

				s->ctx->internal->stats.sess_accept_good++;
				/* s->server=1; */
				s->internal->handshake_func = ssl3_accept;

				if (cb != NULL)
					cb(s, SSL_CB_HANDSHAKE_DONE, 1);
			}

			ret = 1;
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
ssl3_send_hello_request(SSL *s)
{
	CBB cbb, hello;

	memset(&cbb, 0, sizeof(cbb));

	if (S3I(s)->hs.state == SSL3_ST_SW_HELLO_REQ_A) {
		if (!ssl3_handshake_msg_start_cbb(s, &cbb, &hello,
		    SSL3_MT_HELLO_REQUEST))
			goto err;
		if (!ssl3_handshake_msg_finish_cbb(s, &cbb))
			goto err;

		S3I(s)->hs.state = SSL3_ST_SW_HELLO_REQ_B;
	}

	/* SSL3_ST_SW_HELLO_REQ_B */
	return (ssl3_handshake_write(s));

 err:
	CBB_cleanup(&cbb);

	return (-1);
}

int
ssl3_get_client_hello(SSL *s)
{
	CBS cbs, client_random, session_id, cookie, cipher_suites;
	CBS compression_methods;
	uint16_t client_version;
	uint8_t comp_method;
	int comp_null;
	int i, j, ok, al, ret = -1, cookie_valid = 0;
	long n;
	unsigned long id;
	unsigned char *p, *d;
	SSL_CIPHER *c;
	STACK_OF(SSL_CIPHER) *ciphers = NULL;
	unsigned long alg_k;
	const SSL_METHOD *method;
	uint16_t shared_version;
	unsigned char *end;

	/*
	 * We do this so that we will respond with our native type.
	 * If we are TLSv1 and we get SSLv3, we will respond with TLSv1,
	 * This down switching should be handled by a different method.
	 * If we are SSLv3, we will respond with SSLv3, even if prompted with
	 * TLSv1.
	 */
	if (S3I(s)->hs.state == SSL3_ST_SR_CLNT_HELLO_A) {
		S3I(s)->hs.state = SSL3_ST_SR_CLNT_HELLO_B;
	}

	s->internal->first_packet = 1;
	n = s->method->internal->ssl_get_message(s, SSL3_ST_SR_CLNT_HELLO_B,
	    SSL3_ST_SR_CLNT_HELLO_C, SSL3_MT_CLIENT_HELLO,
	    SSL3_RT_MAX_PLAIN_LENGTH, &ok);
	if (!ok)
		return ((int)n);
	s->internal->first_packet = 0;

	if (n < 0)
		goto err;

	d = p = (unsigned char *)s->internal->init_msg;
	end = d + n;

	CBS_init(&cbs, s->internal->init_msg, n);

	/*
	 * Use version from inside client hello, not from record header.
	 * (may differ: see RFC 2246, Appendix E, second paragraph)
	 */
	if (!CBS_get_u16(&cbs, &client_version))
		goto truncated;

	if (ssl_max_shared_version(s, client_version, &shared_version) != 1) {
		SSLerror(s, SSL_R_WRONG_VERSION_NUMBER);
		if ((s->client_version >> 8) == SSL3_VERSION_MAJOR &&
		    !s->internal->enc_write_ctx && !s->internal->write_hash) {
			/*
			 * Similar to ssl3_get_record, send alert using remote
			 * version number.
			 */
			s->version = s->client_version;
		}
		al = SSL_AD_PROTOCOL_VERSION;
		goto f_err;
	}
	s->client_version = client_version;
	s->version = shared_version;

	if ((method = tls1_get_server_method(shared_version)) == NULL)
		method = dtls1_get_server_method(shared_version);
	if (method == NULL) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		goto err;
	}
	s->method = method;

	if (!CBS_get_bytes(&cbs, &client_random, SSL3_RANDOM_SIZE))
		goto truncated;
	if (!CBS_get_u8_length_prefixed(&cbs, &session_id))
		goto truncated;

	/*
	 * If we require cookies (DTLS) and this ClientHello doesn't
	 * contain one, just return since we do not want to
	 * allocate any memory yet. So check cookie length...
	 */
	if (SSL_IS_DTLS(s)) {
		if (!CBS_get_u8_length_prefixed(&cbs, &cookie))
			goto truncated;
		if (SSL_get_options(s) & SSL_OP_COOKIE_EXCHANGE) {
			if (CBS_len(&cookie) == 0)
				return (1);
		}
	}

	if (!CBS_write_bytes(&client_random, s->s3->client_random,
	    sizeof(s->s3->client_random), NULL))
		goto err;

	s->internal->hit = 0;

	/*
	 * Versions before 0.9.7 always allow clients to resume sessions in
	 * renegotiation. 0.9.7 and later allow this by default, but optionally
	 * ignore resumption requests with flag
	 * SSL_OP_NO_SESSION_RESUMPTION_ON_RENEGOTIATION (it's a new flag
	 * rather than a change to default behavior so that applications
	 * relying on this for security won't even compile against older
	 * library versions).
	 *
	 * 1.0.1 and later also have a function SSL_renegotiate_abbreviated()
	 * to request renegotiation but not a new session (s->internal->new_session
	 * remains unset): for servers, this essentially just means that the
	 * SSL_OP_NO_SESSION_RESUMPTION_ON_RENEGOTIATION setting will be
	 * ignored.
	 */
	if ((s->internal->new_session && (s->internal->options &
	    SSL_OP_NO_SESSION_RESUMPTION_ON_RENEGOTIATION))) {
		if (!ssl_get_new_session(s, 1))
			goto err;
	} else {
		/* XXX - pass CBS through instead... */
		i = ssl_get_prev_session(s,
		    (unsigned char *)CBS_data(&session_id),
		    CBS_len(&session_id), end);
		if (i == 1) { /* previous session */
			s->internal->hit = 1;
		} else if (i == -1)
			goto err;
		else {
			/* i == 0 */
			if (!ssl_get_new_session(s, 1))
				goto err;
		}
	}

	if (SSL_IS_DTLS(s)) {
		/*
		 * The ClientHello may contain a cookie even if the HelloVerify
		 * message has not been sent - make sure that it does not cause
		 * an overflow.
		 */
		if (CBS_len(&cookie) > sizeof(D1I(s)->rcvd_cookie)) {
			al = SSL_AD_DECODE_ERROR;
			SSLerror(s, SSL_R_COOKIE_MISMATCH);
			goto f_err;
		}

		/* Verify the cookie if appropriate option is set. */
		if ((SSL_get_options(s) & SSL_OP_COOKIE_EXCHANGE) &&
		    CBS_len(&cookie) > 0) {
			size_t cookie_len;

			/* XXX - rcvd_cookie seems to only be used here... */
			if (!CBS_write_bytes(&cookie, D1I(s)->rcvd_cookie,
			    sizeof(D1I(s)->rcvd_cookie), &cookie_len))
				goto err;

			if (s->ctx->internal->app_verify_cookie_cb != NULL) {
				if (s->ctx->internal->app_verify_cookie_cb(s,
				    D1I(s)->rcvd_cookie, cookie_len) == 0) {
					al = SSL_AD_HANDSHAKE_FAILURE;
					SSLerror(s, SSL_R_COOKIE_MISMATCH);
					goto f_err;
				}
				/* else cookie verification succeeded */
			/* XXX - can d1->cookie_len > sizeof(rcvd_cookie) ? */
			} else if (timingsafe_memcmp(D1I(s)->rcvd_cookie,
			    D1I(s)->cookie, D1I(s)->cookie_len) != 0) {
				/* default verification */
				al = SSL_AD_HANDSHAKE_FAILURE;
				SSLerror(s, SSL_R_COOKIE_MISMATCH);
				goto f_err;
			}
			cookie_valid = 1;
		}
	}

	if (!CBS_get_u16_length_prefixed(&cbs, &cipher_suites))
		goto truncated;

	/* XXX - This logic seems wrong... */
	if (CBS_len(&cipher_suites) == 0 && CBS_len(&session_id) != 0) {
		/* we need a cipher if we are not resuming a session */
		al = SSL_AD_ILLEGAL_PARAMETER;
		SSLerror(s, SSL_R_NO_CIPHERS_SPECIFIED);
		goto f_err;
	}

	if (CBS_len(&cipher_suites) > 0) {
		if ((ciphers = ssl_bytes_to_cipher_list(s,
		    CBS_data(&cipher_suites), CBS_len(&cipher_suites))) == NULL)
			goto err;
	}

	/* If it is a hit, check that the cipher is in the list */
	if (s->internal->hit && CBS_len(&cipher_suites) > 0) {
		j = 0;
		id = s->session->cipher->id;

		for (i = 0; i < sk_SSL_CIPHER_num(ciphers); i++) {
			c = sk_SSL_CIPHER_value(ciphers, i);
			if (c->id == id) {
				j = 1;
				break;
			}
		}
		if (j == 0) {
			/*
			 * We need to have the cipher in the cipher
			 * list if we are asked to reuse it
			 */
			al = SSL_AD_ILLEGAL_PARAMETER;
			SSLerror(s, SSL_R_REQUIRED_CIPHER_MISSING);
			goto f_err;
		}
	}

	if (!CBS_get_u8_length_prefixed(&cbs, &compression_methods))
		goto truncated;

	comp_null = 0;
	while (CBS_len(&compression_methods) > 0) {
		if (!CBS_get_u8(&compression_methods, &comp_method))
			goto truncated;
		if (comp_method == 0)
			comp_null = 1;
	}
	if (comp_null == 0) {
		al = SSL_AD_DECODE_ERROR;
		SSLerror(s, SSL_R_NO_COMPRESSION_SPECIFIED);
		goto f_err;
	}

	p = (unsigned char *)CBS_data(&cbs);

	/* TLS extensions*/
	if (!ssl_parse_clienthello_tlsext(s, &p, d, n, &al)) {
		/* 'al' set by ssl_parse_clienthello_tlsext */
		SSLerror(s, SSL_R_PARSE_TLSEXT);
		goto f_err;
	}
	if (ssl_check_clienthello_tlsext_early(s) <= 0) {
		SSLerror(s, SSL_R_CLIENTHELLO_TLSEXT);
		goto err;
	}

	/*
	 * Check if we want to use external pre-shared secret for this
	 * handshake for not reused session only. We need to generate
	 * server_random before calling tls_session_secret_cb in order to allow
	 * SessionTicket processing to use it in key derivation.
	 */
	arc4random_buf(s->s3->server_random, SSL3_RANDOM_SIZE);

	if (!s->internal->hit && s->internal->tls_session_secret_cb) {
		SSL_CIPHER *pref_cipher = NULL;

		s->session->master_key_length = sizeof(s->session->master_key);
		if (s->internal->tls_session_secret_cb(s, s->session->master_key,
		    &s->session->master_key_length, ciphers, &pref_cipher,
		    s->internal->tls_session_secret_cb_arg)) {
			s->internal->hit = 1;
			s->session->ciphers = ciphers;
			s->session->verify_result = X509_V_OK;

			ciphers = NULL;

			/* check if some cipher was preferred by call back */
			pref_cipher = pref_cipher ? pref_cipher :
			    ssl3_choose_cipher(s, s->session->ciphers,
			    SSL_get_ciphers(s));
			if (pref_cipher == NULL) {
				al = SSL_AD_HANDSHAKE_FAILURE;
				SSLerror(s, SSL_R_NO_SHARED_CIPHER);
				goto f_err;
			}

			s->session->cipher = pref_cipher;

			sk_SSL_CIPHER_free(s->cipher_list);
			sk_SSL_CIPHER_free(s->internal->cipher_list_by_id);

			s->cipher_list = sk_SSL_CIPHER_dup(s->session->ciphers);
			s->internal->cipher_list_by_id =
			    sk_SSL_CIPHER_dup(s->session->ciphers);
		}
	}

	/*
	 * Given s->session->ciphers and SSL_get_ciphers, we must
	 * pick a cipher
	 */

	if (!s->internal->hit) {
		sk_SSL_CIPHER_free(s->session->ciphers);
		s->session->ciphers = ciphers;
		if (ciphers == NULL) {
			al = SSL_AD_ILLEGAL_PARAMETER;
			SSLerror(s, SSL_R_NO_CIPHERS_PASSED);
			goto f_err;
		}
		ciphers = NULL;
		c = ssl3_choose_cipher(s, s->session->ciphers,
		SSL_get_ciphers(s));

		if (c == NULL) {
			al = SSL_AD_HANDSHAKE_FAILURE;
			SSLerror(s, SSL_R_NO_SHARED_CIPHER);
			goto f_err;
		}
		S3I(s)->hs.new_cipher = c;
	} else {
		S3I(s)->hs.new_cipher = s->session->cipher;
	}

	if (!tls1_handshake_hash_init(s))
		goto err;

	alg_k = S3I(s)->hs.new_cipher->algorithm_mkey;
	if (!(SSL_USE_SIGALGS(s) || (alg_k & SSL_kGOST)) ||
	    !(s->verify_mode & SSL_VERIFY_PEER)) {
		if (!tls1_digest_cached_records(s)) {
			al = SSL_AD_INTERNAL_ERROR;
			goto f_err;
		}
	}

	/*
	 * We now have the following setup.
	 * client_random
	 * cipher_list 		- our prefered list of ciphers
	 * ciphers 		- the clients prefered list of ciphers
	 * compression		- basically ignored right now
	 * ssl version is set	- sslv3
	 * s->session		- The ssl session has been setup.
	 * s->internal->hit		- session reuse flag
	 * s->hs.new_cipher	- the new cipher to use.
	 */

	/* Handles TLS extensions that we couldn't check earlier */
	if (ssl_check_clienthello_tlsext_late(s) <= 0) {
		SSLerror(s, SSL_R_CLIENTHELLO_TLSEXT);
		goto err;
	}

	ret = cookie_valid ? 2 : 1;

	if (0) {
truncated:
		al = SSL_AD_DECODE_ERROR;
		SSLerror(s, SSL_R_BAD_PACKET_LENGTH);
f_err:
		ssl3_send_alert(s, SSL3_AL_FATAL, al);
	}
err:
	sk_SSL_CIPHER_free(ciphers);

	return (ret);
}

int
ssl3_send_server_hello(SSL *s)
{
	unsigned char *bufend;
	unsigned char *p, *d;
	CBB cbb, session_id;
	size_t outlen;
	int sl;

	memset(&cbb, 0, sizeof(cbb));

	bufend = (unsigned char *)s->internal->init_buf->data + SSL3_RT_MAX_PLAIN_LENGTH;

	if (S3I(s)->hs.state == SSL3_ST_SW_SRVR_HELLO_A) {
		d = p = ssl3_handshake_msg_start(s, SSL3_MT_SERVER_HELLO);

		if (!CBB_init_fixed(&cbb, p, bufend - p))
			goto err;

		if (!CBB_add_u16(&cbb, s->version))
			goto err;
		if (!CBB_add_bytes(&cbb, s->s3->server_random,
		    sizeof(s->s3->server_random)))
			goto err;

		/*
		 * There are several cases for the session ID to send
		 * back in the server hello:
		 *
		 * - For session reuse from the session cache,
		 *   we send back the old session ID.
		 * - If stateless session reuse (using a session ticket)
		 *   is successful, we send back the client's "session ID"
		 *   (which doesn't actually identify the session).
		 * - If it is a new session, we send back the new
		 *   session ID.
		 * - However, if we want the new session to be single-use,
		 *   we send back a 0-length session ID.
		 *
		 * s->internal->hit is non-zero in either case of session reuse,
		 * so the following won't overwrite an ID that we're supposed
		 * to send back.
		 */
		if (!(s->ctx->internal->session_cache_mode & SSL_SESS_CACHE_SERVER)
		    && !s->internal->hit)
			s->session->session_id_length = 0;

		sl = s->session->session_id_length;
		if (sl > (int)sizeof(s->session->session_id)) {
			SSLerror(s, ERR_R_INTERNAL_ERROR);
			goto err;
		}

		if (!CBB_add_u8_length_prefixed(&cbb, &session_id))
			goto err;
		if (!CBB_add_bytes(&session_id, s->session->session_id, sl))
			goto err;

		/* Cipher suite. */
		if (!CBB_add_u16(&cbb,
		    ssl3_cipher_get_value(S3I(s)->hs.new_cipher)))
			goto err;

		/* Compression method. */
		if (!CBB_add_u8(&cbb, 0))
			goto err;

		if (!CBB_finish(&cbb, NULL, &outlen))
			goto err;

		if ((p = ssl_add_serverhello_tlsext(s, p + outlen,
		    bufend)) == NULL) {
			SSLerror(s, ERR_R_INTERNAL_ERROR);
			goto err;
		}

		ssl3_handshake_msg_finish(s, p - d);
	}

	/* SSL3_ST_SW_SRVR_HELLO_B */
	return (ssl3_handshake_write(s));

 err:
	CBB_cleanup(&cbb);

	return (-1);
}

int
ssl3_send_server_done(SSL *s)
{
	CBB cbb, done;

	memset(&cbb, 0, sizeof(cbb));

	if (S3I(s)->hs.state == SSL3_ST_SW_SRVR_DONE_A) {
		if (!ssl3_handshake_msg_start_cbb(s, &cbb, &done,
		    SSL3_MT_SERVER_DONE))
			goto err;
		if (!ssl3_handshake_msg_finish_cbb(s, &cbb))
			goto err;

		S3I(s)->hs.state = SSL3_ST_SW_SRVR_DONE_B;
	}

	/* SSL3_ST_SW_SRVR_DONE_B */
	return (ssl3_handshake_write(s));

 err:
	CBB_cleanup(&cbb);

	return (-1);
}

int
ssl3_send_server_kex_dhe(SSL *s, CBB *cbb)
{
	CBB dh_p, dh_g, dh_Ys;
	DH *dh = NULL, *dhp;
	unsigned char *data;
	int al;

	if (s->cert->dh_tmp_auto != 0) {
		if ((dhp = ssl_get_auto_dh(s)) == NULL) {
			al = SSL_AD_INTERNAL_ERROR;
			SSLerror(s, ERR_R_INTERNAL_ERROR);
			goto f_err;
		}
	} else
		dhp = s->cert->dh_tmp;

	if (dhp == NULL && s->cert->dh_tmp_cb != NULL)
		dhp = s->cert->dh_tmp_cb(s, 0,
		    SSL_C_PKEYLENGTH(S3I(s)->hs.new_cipher));

	if (dhp == NULL) {
		al = SSL_AD_HANDSHAKE_FAILURE;
		SSLerror(s, SSL_R_MISSING_TMP_DH_KEY);
		goto f_err;
	}

	if (S3I(s)->tmp.dh != NULL) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		goto err;
	}

	if (s->cert->dh_tmp_auto != 0) {
		dh = dhp;
	} else if ((dh = DHparams_dup(dhp)) == NULL) {
		SSLerror(s, ERR_R_DH_LIB);
		goto err;
	}
	S3I(s)->tmp.dh = dh;
	if (!DH_generate_key(dh)) {
		SSLerror(s, ERR_R_DH_LIB);
		goto err;
	}

	/*
	 * Serialize the DH parameters and public key.
	 */
	if (!CBB_add_u16_length_prefixed(cbb, &dh_p))
		goto err;
	if (!CBB_add_space(&dh_p, &data, BN_num_bytes(dh->p)))
		goto err;
	BN_bn2bin(dh->p, data);

	if (!CBB_add_u16_length_prefixed(cbb, &dh_g))
		goto err;
	if (!CBB_add_space(&dh_g, &data, BN_num_bytes(dh->g)))
		goto err;
	BN_bn2bin(dh->g, data);

	if (!CBB_add_u16_length_prefixed(cbb, &dh_Ys))
		goto err;
	if (!CBB_add_space(&dh_Ys, &data, BN_num_bytes(dh->pub_key)))
		goto err;
	BN_bn2bin(dh->pub_key, data);

	if (!CBB_flush(cbb))
		goto err;

	return (1);

 f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, al);
 err:
	return (-1);
}

static int
ssl3_send_server_kex_ecdhe_ecp(SSL *s, int nid, CBB *cbb)
{
	const EC_GROUP *group;
	const EC_POINT *pubkey;
	unsigned char *data;
	int encoded_len = 0;
	int curve_id = 0;
	BN_CTX *bn_ctx = NULL;
	EC_KEY *ecdh;
	CBB ecpoint;
	int al;

	/*
	 * Only named curves are supported in ECDH ephemeral key exchanges.
	 * For supported named curves, curve_id is non-zero.
	 */
	if ((curve_id = tls1_ec_nid2curve_id(nid)) == 0) {
		SSLerror(s, SSL_R_UNSUPPORTED_ELLIPTIC_CURVE);
		goto err;
	}

	if (S3I(s)->tmp.ecdh != NULL) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		goto err;
	}

	if ((S3I(s)->tmp.ecdh = EC_KEY_new_by_curve_name(nid)) == NULL) {
		al = SSL_AD_HANDSHAKE_FAILURE;
		SSLerror(s, SSL_R_MISSING_TMP_ECDH_KEY);
		goto f_err;
	}
	ecdh = S3I(s)->tmp.ecdh;

	if (!EC_KEY_generate_key(ecdh)) {
		SSLerror(s, ERR_R_ECDH_LIB);
		goto err;
	}
	if ((group = EC_KEY_get0_group(ecdh)) == NULL ||
	    (pubkey = EC_KEY_get0_public_key(ecdh)) == NULL ||
	    EC_KEY_get0_private_key(ecdh) == NULL) {
		SSLerror(s, ERR_R_ECDH_LIB);
		goto err;
	}

	/*
	 * Encode the public key.
	 */
	encoded_len = EC_POINT_point2oct(group, pubkey,
	    POINT_CONVERSION_UNCOMPRESSED, NULL, 0, NULL);
	if (encoded_len == 0) {
		SSLerror(s, ERR_R_ECDH_LIB);
		goto err;
	}
	if ((bn_ctx = BN_CTX_new()) == NULL) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		goto err;
	}

	/*
	 * Only named curves are supported in ECDH ephemeral key exchanges.
	 * In this case the ServerKeyExchange message has:
	 * [1 byte CurveType], [2 byte CurveName]
	 * [1 byte length of encoded point], followed by
	 * the actual encoded point itself.
	 */
	if (!CBB_add_u8(cbb, NAMED_CURVE_TYPE))
		goto err;
	if (!CBB_add_u16(cbb, curve_id))
		goto err;
	if (!CBB_add_u8_length_prefixed(cbb, &ecpoint))
		goto err;
	if (!CBB_add_space(&ecpoint, &data, encoded_len))
		goto err;
	if (EC_POINT_point2oct(group, pubkey, POINT_CONVERSION_UNCOMPRESSED,
	    data, encoded_len, bn_ctx) == 0) {
		SSLerror(s, ERR_R_ECDH_LIB);
		goto err;
	}
	if (!CBB_flush(cbb))
		goto err;

	BN_CTX_free(bn_ctx);

	return (1);
	
 f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, al);
 err:
	BN_CTX_free(bn_ctx);

	return (-1);
}

static int
ssl3_send_server_kex_ecdhe_ecx(SSL *s, int nid, CBB *cbb)
{
	uint8_t *public_key = NULL;
	int curve_id;
	CBB ecpoint;
	int ret = -1;

	/* Generate an X25519 key pair. */
	if (S3I(s)->tmp.x25519 != NULL) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		goto err;
	}
	if ((S3I(s)->tmp.x25519 = malloc(X25519_KEY_LENGTH)) == NULL)
		goto err;
	if ((public_key = malloc(X25519_KEY_LENGTH)) == NULL)
		goto err;
	X25519_keypair(public_key, S3I(s)->tmp.x25519);

	/* Serialize public key. */
	if ((curve_id = tls1_ec_nid2curve_id(nid)) == 0) {
		SSLerror(s, SSL_R_UNSUPPORTED_ELLIPTIC_CURVE);
		goto err;
	}

	if (!CBB_add_u8(cbb, NAMED_CURVE_TYPE))
		goto err;
	if (!CBB_add_u16(cbb, curve_id))
		goto err;
	if (!CBB_add_u8_length_prefixed(cbb, &ecpoint))
		goto err;
	if (!CBB_add_bytes(&ecpoint, public_key, X25519_KEY_LENGTH))
		goto err;
	if (!CBB_flush(cbb))
		goto err;

	ret = 1;

 err:
	free(public_key);

	return (ret);
}

static int
ssl3_send_server_kex_ecdhe(SSL *s, CBB *cbb)
{
	int nid;

	nid = tls1_get_shared_curve(s);

	if (nid == NID_X25519)
		return ssl3_send_server_kex_ecdhe_ecx(s, nid, cbb);

	return ssl3_send_server_kex_ecdhe_ecp(s, nid, cbb);
}

int
ssl3_send_server_key_exchange(SSL *s)
{
	CBB cbb;
	unsigned char *params = NULL;
	size_t params_len;
	unsigned char *q;
	unsigned char md_buf[MD5_DIGEST_LENGTH + SHA_DIGEST_LENGTH];
	unsigned int u;
	EVP_PKEY *pkey;
	const EVP_MD *md = NULL;
	unsigned char *p, *d;
	int al, i, j, n, kn;
	unsigned long type;
	BUF_MEM *buf;
	EVP_MD_CTX md_ctx;

	memset(&cbb, 0, sizeof(cbb));

	EVP_MD_CTX_init(&md_ctx);
	if (S3I(s)->hs.state == SSL3_ST_SW_KEY_EXCH_A) {
		type = S3I(s)->hs.new_cipher->algorithm_mkey;

		buf = s->internal->init_buf;

		if (!CBB_init(&cbb, 0))
			goto err;

		if (type & SSL_kDHE) {
			if (ssl3_send_server_kex_dhe(s, &cbb) != 1)
				goto err;
		} else if (type & SSL_kECDHE) {
			if (ssl3_send_server_kex_ecdhe(s, &cbb) != 1)
				goto err;
		} else {
			al = SSL_AD_HANDSHAKE_FAILURE;
			SSLerror(s, SSL_R_UNKNOWN_KEY_EXCHANGE_TYPE);
			goto f_err;
		}

		if (!CBB_finish(&cbb, &params, &params_len))
			goto err;

		if (!(S3I(s)->hs.new_cipher->algorithm_auth & SSL_aNULL)) {
			if ((pkey = ssl_get_sign_pkey(
			    s, S3I(s)->hs.new_cipher, &md)) == NULL) {
				al = SSL_AD_DECODE_ERROR;
				goto f_err;
			}
			kn = EVP_PKEY_size(pkey);
		} else {
			pkey = NULL;
			kn = 0;
		}

		if (!BUF_MEM_grow_clean(buf, ssl3_handshake_msg_hdr_len(s) +
		    params_len + kn)) {
			SSLerror(s, ERR_LIB_BUF);
			goto err;
		}

		d = p = ssl3_handshake_msg_start(s,
		    SSL3_MT_SERVER_KEY_EXCHANGE);

		memcpy(p, params, params_len);

		free(params);
		params = NULL;

		n = params_len;
		p += params_len;

		/* not anonymous */
		if (pkey != NULL) {
			/*
			 * n is the length of the params, they start at &(d[4])
			 * and p points to the space at the end.
			 */
			if (pkey->type == EVP_PKEY_RSA && !SSL_USE_SIGALGS(s)) {
				q = md_buf;
				j = 0;
				if (!EVP_DigestInit_ex(&md_ctx, EVP_md5_sha1(),
				    NULL))
					goto err;
				EVP_DigestUpdate(&md_ctx, s->s3->client_random,
				    SSL3_RANDOM_SIZE);
				EVP_DigestUpdate(&md_ctx, s->s3->server_random,
				    SSL3_RANDOM_SIZE);
				EVP_DigestUpdate(&md_ctx, d, n);
				EVP_DigestFinal_ex(&md_ctx, q,
				    (unsigned int *)&i);
				q += i;
				j += i;
				if (RSA_sign(NID_md5_sha1, md_buf, j,
				    &(p[2]), &u, pkey->pkey.rsa) <= 0) {
					SSLerror(s, ERR_R_RSA_LIB);
					goto err;
				}
				s2n(u, p);
				n += u + 2;
			} else if (md) {
				/* Send signature algorithm. */
				if (SSL_USE_SIGALGS(s)) {
					if (!tls12_get_sigandhash(p, pkey, md)) {
						/* Should never happen */
						al = SSL_AD_INTERNAL_ERROR;
						SSLerror(s, ERR_R_INTERNAL_ERROR);
						goto f_err;
					}
					p += 2;
				}
				EVP_SignInit_ex(&md_ctx, md, NULL);
				EVP_SignUpdate(&md_ctx,
				    s->s3->client_random,
				    SSL3_RANDOM_SIZE);
				EVP_SignUpdate(&md_ctx,
				    s->s3->server_random,
				    SSL3_RANDOM_SIZE);
				EVP_SignUpdate(&md_ctx, d, n);
				if (!EVP_SignFinal(&md_ctx, &p[2],
					(unsigned int *)&i, pkey)) {
					SSLerror(s, ERR_R_EVP_LIB);
					goto err;
				}
				s2n(i, p);
				n += i + 2;
				if (SSL_USE_SIGALGS(s))
					n += 2;
			} else {
				/* Is this error check actually needed? */
				al = SSL_AD_HANDSHAKE_FAILURE;
				SSLerror(s, SSL_R_UNKNOWN_PKEY_TYPE);
				goto f_err;
			}
		}

		ssl3_handshake_msg_finish(s, n);
	}

	S3I(s)->hs.state = SSL3_ST_SW_KEY_EXCH_B;

	EVP_MD_CTX_cleanup(&md_ctx);

	return (ssl3_handshake_write(s));
	
 f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, al);
 err:
	free(params);
	EVP_MD_CTX_cleanup(&md_ctx);
	CBB_cleanup(&cbb);

	return (-1);
}

int
ssl3_send_certificate_request(SSL *s)
{
	CBB cbb, cert_request, cert_types, sigalgs, cert_auth, dn;
	STACK_OF(X509_NAME) *sk = NULL;
	X509_NAME *name;
	int i;

	/*
	 * Certificate Request - RFC 5246 section 7.4.4.
	 */

	memset(&cbb, 0, sizeof(cbb));

	if (S3I(s)->hs.state == SSL3_ST_SW_CERT_REQ_A) {
		if (!ssl3_handshake_msg_start_cbb(s, &cbb, &cert_request,
		    SSL3_MT_CERTIFICATE_REQUEST))
			goto err;

		if (!CBB_add_u8_length_prefixed(&cert_request, &cert_types))
			goto err;
		if (!ssl3_get_req_cert_types(s, &cert_types))
			goto err;

		if (SSL_USE_SIGALGS(s)) {
			unsigned char *sigalgs_data;
			size_t sigalgs_len;

			tls12_get_req_sig_algs(s, &sigalgs_data, &sigalgs_len);

			if (!CBB_add_u16_length_prefixed(&cert_request, &sigalgs))
				goto err;
			if (!CBB_add_bytes(&sigalgs, sigalgs_data, sigalgs_len))
				goto err;
		}

		if (!CBB_add_u16_length_prefixed(&cert_request, &cert_auth))
			goto err;

		sk = SSL_get_client_CA_list(s);
		for (i = 0; i < sk_X509_NAME_num(sk); i++) {
			unsigned char *name_data;
			size_t name_len;

			name = sk_X509_NAME_value(sk, i);
			name_len = i2d_X509_NAME(name, NULL);

			if (!CBB_add_u16_length_prefixed(&cert_auth, &dn))
				goto err;
			if (!CBB_add_space(&dn, &name_data, name_len))
				goto err;
			if (i2d_X509_NAME(name, &name_data) != name_len)
				goto err;
		}

		if (!ssl3_handshake_msg_finish_cbb(s, &cbb))
			goto err;

		S3I(s)->hs.state = SSL3_ST_SW_CERT_REQ_B;
	}

	/* SSL3_ST_SW_CERT_REQ_B */
	return (ssl3_handshake_write(s));

 err:
	CBB_cleanup(&cbb);

	return (-1);
}

static int
ssl3_get_client_kex_rsa(SSL *s, unsigned char *p, long n)
{
	unsigned char fakekey[SSL_MAX_MASTER_KEY_LENGTH];
	unsigned char *d;
	RSA *rsa = NULL;
	EVP_PKEY *pkey = NULL;
	int i, al;

	d = p;

	arc4random_buf(fakekey, sizeof(fakekey));
	fakekey[0] = s->client_version >> 8;
	fakekey[1] = s->client_version & 0xff;

	pkey = s->cert->pkeys[SSL_PKEY_RSA_ENC].privatekey;
	if ((pkey == NULL) || (pkey->type != EVP_PKEY_RSA) ||
	    (pkey->pkey.rsa == NULL)) {
		al = SSL_AD_HANDSHAKE_FAILURE;
		SSLerror(s, SSL_R_MISSING_RSA_CERTIFICATE);
		goto f_err;
	}
	rsa = pkey->pkey.rsa;

	if (2 > n)
		goto truncated;
	n2s(p, i);
	if (n != i + 2) {
		SSLerror(s, SSL_R_TLS_RSA_ENCRYPTED_VALUE_LENGTH_IS_WRONG);
		goto err;
	} else
		n = i;

	i = RSA_private_decrypt((int)n, p, p, rsa, RSA_PKCS1_PADDING);

	ERR_clear_error();

	al = -1;

	if (i != SSL_MAX_MASTER_KEY_LENGTH) {
		al = SSL_AD_DECODE_ERROR;
		/* SSLerror(s, SSL_R_BAD_RSA_DECRYPT); */
	}

	if (p - d + 2 > n)	/* needed in the SSL3 case */
		goto truncated;
	if ((al == -1) && !((p[0] == (s->client_version >> 8)) &&
	    (p[1] == (s->client_version & 0xff)))) {
		/*
		 * The premaster secret must contain the same version
		 * number as the ClientHello to detect version rollback
		 * attacks (strangely, the protocol does not offer such
		 * protection for DH ciphersuites).
		 * However, buggy clients exist that send the negotiated
		 * protocol version instead if the server does not
		 * support the requested protocol version.
		 * If SSL_OP_TLS_ROLLBACK_BUG is set, tolerate such
		 * clients.
		 */
		if (!((s->internal->options & SSL_OP_TLS_ROLLBACK_BUG) &&
		    (p[0] == (s->version >> 8)) &&
		    (p[1] == (s->version & 0xff)))) {
			al = SSL_AD_DECODE_ERROR;
			/* SSLerror(s, SSL_R_BAD_PROTOCOL_VERSION_NUMBER); */

			/*
			 * The Klima-Pokorny-Rosa extension of
			 * Bleichenbacher's attack
			 * (http://eprint.iacr.org/2003/052/) exploits
			 * the version number check as a "bad version
			 * oracle" -- an alert would reveal that the
			 * plaintext corresponding to some ciphertext
			 * made up by the adversary is properly
			 * formatted except that the version number is
			 * wrong.
			 * To avoid such attacks, we should treat this
			 * just like any other decryption error.
			 */
		}
	}

	if (al != -1) {
		/*
		 * Some decryption failure -- use random value instead
		 * as countermeasure against Bleichenbacher's attack
		 * on PKCS #1 v1.5 RSA padding (see RFC 2246,
		 * section 7.4.7.1).
		 */
		i = SSL_MAX_MASTER_KEY_LENGTH;
		p = fakekey;
	}

	s->session->master_key_length =
	    tls1_generate_master_secret(s,
	        s->session->master_key, p, i);

	explicit_bzero(p, i);

	return (1);
truncated:
	al = SSL_AD_DECODE_ERROR;
	SSLerror(s, SSL_R_BAD_PACKET_LENGTH);
f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, al);
err:
	return (-1);
}

static int
ssl3_get_client_kex_dhe(SSL *s, unsigned char *p, long n)
{
	BIGNUM *bn = NULL;
	int key_size, al;
	CBS cbs, dh_Yc;
	DH *dh;

	if (n < 0)
		goto err;

	CBS_init(&cbs, p, n);

	if (!CBS_get_u16_length_prefixed(&cbs, &dh_Yc))
		goto truncated;

	if (CBS_len(&cbs) != 0)
		goto truncated;

	if (S3I(s)->tmp.dh == NULL) {
		al = SSL_AD_HANDSHAKE_FAILURE;
		SSLerror(s, SSL_R_MISSING_TMP_DH_KEY);
		goto f_err;
	}
	dh = S3I(s)->tmp.dh;

	if ((bn = BN_bin2bn(CBS_data(&dh_Yc), CBS_len(&dh_Yc), NULL)) == NULL) {
		SSLerror(s, SSL_R_BN_LIB);
		goto err;
	}

	key_size = DH_compute_key(p, bn, dh);
	if (key_size <= 0) {
		SSLerror(s, ERR_R_DH_LIB);
		BN_clear_free(bn);
		goto err;
	}

	s->session->master_key_length =
	    tls1_generate_master_secret(
	        s, s->session->master_key, p, key_size);

	explicit_bzero(p, key_size);

	DH_free(S3I(s)->tmp.dh);
	S3I(s)->tmp.dh = NULL;

	BN_clear_free(bn);

	return (1);

 truncated:
	al = SSL_AD_DECODE_ERROR;
	SSLerror(s, SSL_R_BAD_PACKET_LENGTH);
 f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, al);
 err:
	return (-1);
}

static int
ssl3_get_client_kex_ecdhe_ecp(SSL *s, unsigned char *p, long n)
{
	EC_KEY *srvr_ecdh = NULL;
	EVP_PKEY *clnt_pub_pkey = NULL;
	EC_POINT *clnt_ecpoint = NULL;
	BN_CTX *bn_ctx = NULL;
	int i, al;

	int ret = 1;
	int key_size;
	const EC_KEY   *tkey;
	const EC_GROUP *group;
	const BIGNUM *priv_key;

	/* Initialize structures for server's ECDH key pair. */
	if ((srvr_ecdh = EC_KEY_new()) == NULL) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		goto err;
	}

	/*
	 * Use the ephemeral values we saved when
	 * generating the ServerKeyExchange message.
	 */
	tkey = S3I(s)->tmp.ecdh;

	group = EC_KEY_get0_group(tkey);
	priv_key = EC_KEY_get0_private_key(tkey);

	if (!EC_KEY_set_group(srvr_ecdh, group) ||
	    !EC_KEY_set_private_key(srvr_ecdh, priv_key)) {
		SSLerror(s, ERR_R_EC_LIB);
		goto err;
	}

	/* Let's get client's public key */
	if ((clnt_ecpoint = EC_POINT_new(group)) == NULL) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		goto err;
	}

	if (n == 0L) {
		/* Client Publickey was in Client Certificate */
		if (((clnt_pub_pkey = X509_get_pubkey(
		    s->session->peer)) == NULL) ||
		    (clnt_pub_pkey->type != EVP_PKEY_EC)) {
			/*
			 * XXX: For now, we do not support client
			 * authentication using ECDH certificates
			 * so this branch (n == 0L) of the code is
			 * never executed. When that support is
			 * added, we ought to ensure the key
			 * received in the certificate is
			 * authorized for key agreement.
			 * ECDH_compute_key implicitly checks that
			 * the two ECDH shares are for the same
			 * group.
			 */
			al = SSL_AD_HANDSHAKE_FAILURE;
			SSLerror(s, SSL_R_UNABLE_TO_DECODE_ECDH_CERTS);
			goto f_err;
		}

		if (EC_POINT_copy(clnt_ecpoint,
		    EC_KEY_get0_public_key(clnt_pub_pkey->pkey.ec))
		    == 0) {
			SSLerror(s, ERR_R_EC_LIB);
			goto err;
		}
		ret = 2; /* Skip certificate verify processing */
	} else {
		/*
		 * Get client's public key from encoded point
		 * in the ClientKeyExchange message.
		 */
		if ((bn_ctx = BN_CTX_new()) == NULL) {
			SSLerror(s, ERR_R_MALLOC_FAILURE);
			goto err;
		}

		/* Get encoded point length */
		i = *p;

		p += 1;
		if (n != 1 + i) {
			SSLerror(s, ERR_R_EC_LIB);
			goto err;
		}
		if (EC_POINT_oct2point(group,
			clnt_ecpoint, p, i, bn_ctx) == 0) {
			SSLerror(s, ERR_R_EC_LIB);
			goto err;
		}
		/*
		 * p is pointing to somewhere in the buffer
		 * currently, so set it to the start.
		 */
		p = (unsigned char *)s->internal->init_buf->data;
	}

	/* Compute the shared pre-master secret */
	key_size = ECDH_size(srvr_ecdh);
	if (key_size <= 0) {
		SSLerror(s, ERR_R_ECDH_LIB);
		goto err;
	}
	i = ECDH_compute_key(p, key_size, clnt_ecpoint, srvr_ecdh,
	    NULL);
	if (i <= 0) {
		SSLerror(s, ERR_R_ECDH_LIB);
		goto err;
	}

	EVP_PKEY_free(clnt_pub_pkey);
	EC_POINT_free(clnt_ecpoint);
	EC_KEY_free(srvr_ecdh);
	BN_CTX_free(bn_ctx);
	EC_KEY_free(S3I(s)->tmp.ecdh);
	S3I(s)->tmp.ecdh = NULL;

	/* Compute the master secret */
	s->session->master_key_length =
	    tls1_generate_master_secret(
		s, s->session->master_key, p, i);

	explicit_bzero(p, i);
	return (ret);

 f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, al);
 err:
	EVP_PKEY_free(clnt_pub_pkey);
	EC_POINT_free(clnt_ecpoint);
	EC_KEY_free(srvr_ecdh);
	BN_CTX_free(bn_ctx);
	return (-1);
}

static int
ssl3_get_client_kex_ecdhe_ecx(SSL *s, unsigned char *p, long n)
{
	uint8_t *shared_key = NULL;
	CBS cbs, ecpoint;
	int ret = -1;

	if (n < 0)
		goto err;

	CBS_init(&cbs, p, n);
	if (!CBS_get_u8_length_prefixed(&cbs, &ecpoint))
		goto err;
	if (CBS_len(&ecpoint) != X25519_KEY_LENGTH)
		goto err;

	if ((shared_key = malloc(X25519_KEY_LENGTH)) == NULL)
		goto err;
	if (!X25519(shared_key, S3I(s)->tmp.x25519, CBS_data(&ecpoint)))
		goto err;

	freezero(S3I(s)->tmp.x25519, X25519_KEY_LENGTH);
	S3I(s)->tmp.x25519 = NULL;

	s->session->master_key_length =
	    tls1_generate_master_secret(
		s, s->session->master_key, shared_key, X25519_KEY_LENGTH);

	ret = 1;

 err:
	freezero(shared_key, X25519_KEY_LENGTH);

	return (ret);
}

static int
ssl3_get_client_kex_ecdhe(SSL *s, unsigned char *p, long n)
{
        if (S3I(s)->tmp.x25519 != NULL)
		return ssl3_get_client_kex_ecdhe_ecx(s, p, n);

	return ssl3_get_client_kex_ecdhe_ecp(s, p, n);
}

static int
ssl3_get_client_kex_gost(SSL *s, unsigned char *p, long n)
{

	EVP_PKEY_CTX *pkey_ctx;
	EVP_PKEY *client_pub_pkey = NULL, *pk = NULL;
	unsigned char premaster_secret[32], *start;
	size_t outlen = 32, inlen;
	unsigned long alg_a;
	int Ttag, Tclass;
	long Tlen;
	int al;
	int ret = 0;

	/* Get our certificate private key*/
	alg_a = S3I(s)->hs.new_cipher->algorithm_auth;
	if (alg_a & SSL_aGOST01)
		pk = s->cert->pkeys[SSL_PKEY_GOST01].privatekey;

	pkey_ctx = EVP_PKEY_CTX_new(pk, NULL);
	EVP_PKEY_decrypt_init(pkey_ctx);
	/*
	 * If client certificate is present and is of the same type,
	 * maybe use it for key exchange.
	 * Don't mind errors from EVP_PKEY_derive_set_peer, because
	 * it is completely valid to use a client certificate for
	 * authorization only.
	 */
	client_pub_pkey = X509_get_pubkey(s->session->peer);
	if (client_pub_pkey) {
		if (EVP_PKEY_derive_set_peer(pkey_ctx,
		    client_pub_pkey) <= 0)
			ERR_clear_error();
	}
	if (2 > n)
		goto truncated;
	/* Decrypt session key */
	if (ASN1_get_object((const unsigned char **)&p, &Tlen, &Ttag,
	    &Tclass, n) != V_ASN1_CONSTRUCTED ||
	    Ttag != V_ASN1_SEQUENCE || Tclass != V_ASN1_UNIVERSAL) {
		SSLerror(s, SSL_R_DECRYPTION_FAILED);
		goto gerr;
	}
	start = p;
	inlen = Tlen;
	if (EVP_PKEY_decrypt(pkey_ctx, premaster_secret, &outlen,
	    start, inlen) <=0) {
		SSLerror(s, SSL_R_DECRYPTION_FAILED);
		goto gerr;
	}
	/* Generate master secret */
	s->session->master_key_length =
	    tls1_generate_master_secret(
		s, s->session->master_key, premaster_secret, 32);
	/* Check if pubkey from client certificate was used */
	if (EVP_PKEY_CTX_ctrl(pkey_ctx, -1, -1,
	    EVP_PKEY_CTRL_PEER_KEY, 2, NULL) > 0)
		ret = 2;
	else
		ret = 1;
 gerr:
	EVP_PKEY_free(client_pub_pkey);
	EVP_PKEY_CTX_free(pkey_ctx);
	if (ret)
		return (ret);
	else
		goto err;

 truncated:
	al = SSL_AD_DECODE_ERROR;
	SSLerror(s, SSL_R_BAD_PACKET_LENGTH);
	ssl3_send_alert(s, SSL3_AL_FATAL, al);
 err:
	return (-1);
}

int
ssl3_get_client_key_exchange(SSL *s)
{
	unsigned long alg_k;
	unsigned char *p;
	int al, ok;
	long n;

	/* 2048 maxlen is a guess.  How long a key does that permit? */
	n = s->method->internal->ssl_get_message(s, SSL3_ST_SR_KEY_EXCH_A,
	    SSL3_ST_SR_KEY_EXCH_B, SSL3_MT_CLIENT_KEY_EXCHANGE, 2048, &ok);
	if (!ok)
		return ((int)n);

	p = (unsigned char *)s->internal->init_msg;

	alg_k = S3I(s)->hs.new_cipher->algorithm_mkey;

	if (alg_k & SSL_kRSA) {
		if (ssl3_get_client_kex_rsa(s, p, n) != 1)
			goto err;
	} else if (alg_k & SSL_kDHE) {
		if (ssl3_get_client_kex_dhe(s, p, n) != 1)
			goto err;
	} else if (alg_k & SSL_kECDHE) {
		if (ssl3_get_client_kex_ecdhe(s, p, n) != 1)
			goto err;
	} else if (alg_k & SSL_kGOST) {
		if (ssl3_get_client_kex_gost(s, p, n) != 1)
			goto err;
	} else {
		al = SSL_AD_HANDSHAKE_FAILURE;
		SSLerror(s, SSL_R_UNKNOWN_CIPHER_TYPE);
		goto f_err;
	}

	return (1);

 f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, al);
 err:
	return (-1);
}

int
ssl3_get_cert_verify(SSL *s)
{
	EVP_PKEY *pkey = NULL;
	unsigned char *p;
	int al, ok, ret = 0;
	long n;
	int type = 0, i, j;
	X509 *peer;
	const EVP_MD *md = NULL;
	EVP_MD_CTX mctx;
	EVP_MD_CTX_init(&mctx);

	n = s->method->internal->ssl_get_message(s, SSL3_ST_SR_CERT_VRFY_A,
	    SSL3_ST_SR_CERT_VRFY_B, -1, SSL3_RT_MAX_PLAIN_LENGTH, &ok);
	if (!ok)
		return ((int)n);

	if (s->session->peer != NULL) {
		peer = s->session->peer;
		pkey = X509_get_pubkey(peer);
		type = X509_certificate_type(peer, pkey);
	} else {
		peer = NULL;
		pkey = NULL;
	}

	if (S3I(s)->tmp.message_type != SSL3_MT_CERTIFICATE_VERIFY) {
		S3I(s)->tmp.reuse_message = 1;
		if (peer != NULL) {
			al = SSL_AD_UNEXPECTED_MESSAGE;
			SSLerror(s, SSL_R_MISSING_VERIFY_MESSAGE);
			goto f_err;
		}
		ret = 1;
		goto end;
	}

	if (peer == NULL) {
		SSLerror(s, SSL_R_NO_CLIENT_CERT_RECEIVED);
		al = SSL_AD_UNEXPECTED_MESSAGE;
		goto f_err;
	}

	if (!(type & EVP_PKT_SIGN)) {
		SSLerror(s, SSL_R_SIGNATURE_FOR_NON_SIGNING_CERTIFICATE);
		al = SSL_AD_ILLEGAL_PARAMETER;
		goto f_err;
	}

	if (S3I(s)->change_cipher_spec) {
		SSLerror(s, SSL_R_CCS_RECEIVED_EARLY);
		al = SSL_AD_UNEXPECTED_MESSAGE;
		goto f_err;
	}

	/* we now have a signature that we need to verify */
	p = (unsigned char *)s->internal->init_msg;
	/*
	 * Check for broken implementations of GOST ciphersuites.
	 *
	 * If key is GOST and n is exactly 64, it is a bare
	 * signature without length field.
	 */
	if (n == 64 && (pkey->type == NID_id_GostR3410_94 ||
	    pkey->type == NID_id_GostR3410_2001) ) {
		i = 64;
	} else {
		if (SSL_USE_SIGALGS(s)) {
			int sigalg = tls12_get_sigid(pkey);
			/* Should never happen */
			if (sigalg == -1) {
				SSLerror(s, ERR_R_INTERNAL_ERROR);
				al = SSL_AD_INTERNAL_ERROR;
				goto f_err;
			}
			if (2 > n)
				goto truncated;
			/* Check key type is consistent with signature */
			if (sigalg != (int)p[1]) {
				SSLerror(s, SSL_R_WRONG_SIGNATURE_TYPE);
				al = SSL_AD_DECODE_ERROR;
				goto f_err;
			}
			md = tls12_get_hash(p[0]);
			if (md == NULL) {
				SSLerror(s, SSL_R_UNKNOWN_DIGEST);
				al = SSL_AD_DECODE_ERROR;
				goto f_err;
			}
			p += 2;
			n -= 2;
		}
		if (2 > n)
			goto truncated;
		n2s(p, i);
		n -= 2;
		if (i > n)
			goto truncated;
	}
	j = EVP_PKEY_size(pkey);
	if ((i > j) || (n > j) || (n <= 0)) {
		SSLerror(s, SSL_R_WRONG_SIGNATURE_SIZE);
		al = SSL_AD_DECODE_ERROR;
		goto f_err;
	}

	if (SSL_USE_SIGALGS(s)) {
		long hdatalen = 0;
		void *hdata;
		hdatalen = BIO_get_mem_data(S3I(s)->handshake_buffer, &hdata);
		if (hdatalen <= 0) {
			SSLerror(s, ERR_R_INTERNAL_ERROR);
			al = SSL_AD_INTERNAL_ERROR;
			goto f_err;
		}
		if (!EVP_VerifyInit_ex(&mctx, md, NULL) ||
		    !EVP_VerifyUpdate(&mctx, hdata, hdatalen)) {
			SSLerror(s, ERR_R_EVP_LIB);
			al = SSL_AD_INTERNAL_ERROR;
			goto f_err;
		}

		if (EVP_VerifyFinal(&mctx, p, i, pkey) <= 0) {
			al = SSL_AD_DECRYPT_ERROR;
			SSLerror(s, SSL_R_BAD_SIGNATURE);
			goto f_err;
		}
	} else
	if (pkey->type == EVP_PKEY_RSA) {
		i = RSA_verify(NID_md5_sha1, S3I(s)->tmp.cert_verify_md,
		    MD5_DIGEST_LENGTH + SHA_DIGEST_LENGTH, p, i,
		    pkey->pkey.rsa);
		if (i < 0) {
			al = SSL_AD_DECRYPT_ERROR;
			SSLerror(s, SSL_R_BAD_RSA_DECRYPT);
			goto f_err;
		}
		if (i == 0) {
			al = SSL_AD_DECRYPT_ERROR;
			SSLerror(s, SSL_R_BAD_RSA_SIGNATURE);
			goto f_err;
		}
	} else
	if (pkey->type == EVP_PKEY_EC) {
		j = ECDSA_verify(pkey->save_type,
		    &(S3I(s)->tmp.cert_verify_md[MD5_DIGEST_LENGTH]),
		    SHA_DIGEST_LENGTH, p, i, pkey->pkey.ec);
		if (j <= 0) {
			/* bad signature */
			al = SSL_AD_DECRYPT_ERROR;
			SSLerror(s, SSL_R_BAD_ECDSA_SIGNATURE);
			goto f_err;
		}
	} else
#ifndef OPENSSL_NO_GOST
	if (pkey->type == NID_id_GostR3410_94 ||
	    pkey->type == NID_id_GostR3410_2001) {
		long hdatalen = 0;
		void *hdata;
		unsigned char signature[128];
		unsigned int siglen = sizeof(signature);
		int nid;
		EVP_PKEY_CTX *pctx;

		hdatalen = BIO_get_mem_data(S3I(s)->handshake_buffer, &hdata);
		if (hdatalen <= 0) {
			SSLerror(s, ERR_R_INTERNAL_ERROR);
			al = SSL_AD_INTERNAL_ERROR;
			goto f_err;
		}
		if (!EVP_PKEY_get_default_digest_nid(pkey, &nid) ||
				!(md = EVP_get_digestbynid(nid))) {
			SSLerror(s, ERR_R_EVP_LIB);
			al = SSL_AD_INTERNAL_ERROR;
			goto f_err;
		}
		pctx = EVP_PKEY_CTX_new(pkey, NULL);
		if (!pctx) {
			SSLerror(s, ERR_R_EVP_LIB);
			al = SSL_AD_INTERNAL_ERROR;
			goto f_err;
		}
		if (!EVP_DigestInit_ex(&mctx, md, NULL) ||
		    !EVP_DigestUpdate(&mctx, hdata, hdatalen) ||
		    !EVP_DigestFinal(&mctx, signature, &siglen) ||
		    (EVP_PKEY_verify_init(pctx) <= 0) ||
		    (EVP_PKEY_CTX_set_signature_md(pctx, md) <= 0) ||
		    (EVP_PKEY_CTX_ctrl(pctx, -1, EVP_PKEY_OP_VERIFY,
				       EVP_PKEY_CTRL_GOST_SIG_FORMAT,
				       GOST_SIG_FORMAT_RS_LE,
				       NULL) <= 0)) {
			SSLerror(s, ERR_R_EVP_LIB);
			al = SSL_AD_INTERNAL_ERROR;
			EVP_PKEY_CTX_free(pctx);
			goto f_err;
		}

		if (EVP_PKEY_verify(pctx, p, i, signature, siglen) <= 0) {
			al = SSL_AD_DECRYPT_ERROR;
			SSLerror(s, SSL_R_BAD_SIGNATURE);
			EVP_PKEY_CTX_free(pctx);
			goto f_err;
		}

		EVP_PKEY_CTX_free(pctx);
	} else
#endif
	{
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		al = SSL_AD_UNSUPPORTED_CERTIFICATE;
		goto f_err;
	}


	ret = 1;
	if (0) {
truncated:
		al = SSL_AD_DECODE_ERROR;
		SSLerror(s, SSL_R_BAD_PACKET_LENGTH);
f_err:
		ssl3_send_alert(s, SSL3_AL_FATAL, al);
	}
end:
	if (S3I(s)->handshake_buffer) {
		BIO_free(S3I(s)->handshake_buffer);
		S3I(s)->handshake_buffer = NULL;
		s->s3->flags &= ~TLS1_FLAGS_KEEP_HANDSHAKE;
	}
	EVP_MD_CTX_cleanup(&mctx);
	EVP_PKEY_free(pkey);
	return (ret);
}

int
ssl3_get_client_certificate(SSL *s)
{
	CBS cbs, client_certs;
	int i, ok, al, ret = -1;
	X509 *x = NULL;
	long n;
	const unsigned char *q;
	STACK_OF(X509) *sk = NULL;

	n = s->method->internal->ssl_get_message(s, SSL3_ST_SR_CERT_A, SSL3_ST_SR_CERT_B,
	    -1, s->internal->max_cert_list, &ok);

	if (!ok)
		return ((int)n);

	if (S3I(s)->tmp.message_type == SSL3_MT_CLIENT_KEY_EXCHANGE) {
		if ((s->verify_mode & SSL_VERIFY_PEER) &&
		    (s->verify_mode & SSL_VERIFY_FAIL_IF_NO_PEER_CERT)) {
		    	SSLerror(s, SSL_R_PEER_DID_NOT_RETURN_A_CERTIFICATE);
			al = SSL_AD_HANDSHAKE_FAILURE;
			goto f_err;
		}
		/*
		 * If tls asked for a client cert,
		 * the client must return a 0 list.
		 */
		if (S3I(s)->tmp.cert_request) {
			SSLerror(s, SSL_R_TLS_PEER_DID_NOT_RESPOND_WITH_CERTIFICATE_LIST
			    );
			al = SSL_AD_UNEXPECTED_MESSAGE;
			goto f_err;
		}
		S3I(s)->tmp.reuse_message = 1;
		return (1);
	}

	if (S3I(s)->tmp.message_type != SSL3_MT_CERTIFICATE) {
		al = SSL_AD_UNEXPECTED_MESSAGE;
		SSLerror(s, SSL_R_WRONG_MESSAGE_TYPE);
		goto f_err;
	}

	if (n < 0)
		goto truncated;

	CBS_init(&cbs, s->internal->init_msg, n);

	if ((sk = sk_X509_new_null()) == NULL) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		goto err;
	}

	if (!CBS_get_u24_length_prefixed(&cbs, &client_certs) ||
	    CBS_len(&cbs) != 0)
		goto truncated;

	while (CBS_len(&client_certs) > 0) {
		CBS cert;

		if (!CBS_get_u24_length_prefixed(&client_certs, &cert)) {
			al = SSL_AD_DECODE_ERROR;
			SSLerror(s, SSL_R_CERT_LENGTH_MISMATCH);
			goto f_err;
		}

		q = CBS_data(&cert);
		x = d2i_X509(NULL, &q, CBS_len(&cert));
		if (x == NULL) {
			SSLerror(s, ERR_R_ASN1_LIB);
			goto err;
		}
		if (q != CBS_data(&cert) + CBS_len(&cert)) {
			al = SSL_AD_DECODE_ERROR;
			SSLerror(s, SSL_R_CERT_LENGTH_MISMATCH);
			goto f_err;
		}
		if (!sk_X509_push(sk, x)) {
			SSLerror(s, ERR_R_MALLOC_FAILURE);
			goto err;
		}
		x = NULL;
	}

	if (sk_X509_num(sk) <= 0) {
		/*
		 * TLS does not mind 0 certs returned.
		 * Fail for TLS only if we required a certificate.
		 */
		if ((s->verify_mode & SSL_VERIFY_PEER) &&
		    (s->verify_mode & SSL_VERIFY_FAIL_IF_NO_PEER_CERT)) {
			SSLerror(s, SSL_R_PEER_DID_NOT_RETURN_A_CERTIFICATE);
			al = SSL_AD_HANDSHAKE_FAILURE;
			goto f_err;
		}
		/* No client certificate so digest cached records */
		if (S3I(s)->handshake_buffer && !tls1_digest_cached_records(s)) {
			al = SSL_AD_INTERNAL_ERROR;
			goto f_err;
		}
	} else {
		i = ssl_verify_cert_chain(s, sk);
		if (i <= 0) {
			al = ssl_verify_alarm_type(s->verify_result);
			SSLerror(s, SSL_R_NO_CERTIFICATE_RETURNED);
			goto f_err;
		}
	}

	X509_free(s->session->peer);
	s->session->peer = sk_X509_shift(sk);
	s->session->verify_result = s->verify_result;

	/*
	 * With the current implementation, sess_cert will always be NULL
	 * when we arrive here
	 */
	if (SSI(s)->sess_cert == NULL) {
		SSI(s)->sess_cert = ssl_sess_cert_new();
		if (SSI(s)->sess_cert == NULL) {
			SSLerror(s, ERR_R_MALLOC_FAILURE);
			goto err;
		}
	}
	sk_X509_pop_free(SSI(s)->sess_cert->cert_chain, X509_free);
	SSI(s)->sess_cert->cert_chain = sk;

	/*
	 * Inconsistency alert: cert_chain does *not* include the
	 * peer's own certificate, while we do include it in s3_clnt.c
	 */

	sk = NULL;

	ret = 1;
	if (0) {
truncated:
		al = SSL_AD_DECODE_ERROR;
		SSLerror(s, SSL_R_BAD_PACKET_LENGTH);
f_err:
		ssl3_send_alert(s, SSL3_AL_FATAL, al);
	}
err:
	X509_free(x);
	sk_X509_pop_free(sk, X509_free);

	return (ret);
}

int
ssl3_send_server_certificate(SSL *s)
{
	CBB cbb, server_cert;
	X509 *x;

	/*
	 * Server Certificate - RFC 5246, section 7.4.2.
	 */

	memset(&cbb, 0, sizeof(cbb));

	if (S3I(s)->hs.state == SSL3_ST_SW_CERT_A) {
		if ((x = ssl_get_server_send_cert(s)) == NULL) {
			SSLerror(s, ERR_R_INTERNAL_ERROR);
			return (0);
		}

		if (!ssl3_handshake_msg_start_cbb(s, &cbb, &server_cert,
		    SSL3_MT_CERTIFICATE))
			goto err;
		if (!ssl3_output_cert_chain(s, &server_cert, x))
			goto err;
		if (!ssl3_handshake_msg_finish_cbb(s, &cbb))
			goto err;

		S3I(s)->hs.state = SSL3_ST_SW_CERT_B;
	}

	/* SSL3_ST_SW_CERT_B */
	return (ssl3_handshake_write(s));

 err:
	CBB_cleanup(&cbb);

	return (0);
}

/* send a new session ticket (not necessarily for a new session) */
int
ssl3_send_newsession_ticket(SSL *s)
{
	unsigned char *d, *p, *macstart;
	unsigned char *senc = NULL;
	const unsigned char *const_p;
	int len, slen_full, slen;
	SSL_SESSION *sess;
	unsigned int hlen;
	EVP_CIPHER_CTX ctx;
	HMAC_CTX hctx;
	SSL_CTX *tctx = s->initial_ctx;
	unsigned char iv[EVP_MAX_IV_LENGTH];
	unsigned char key_name[16];

	if (S3I(s)->hs.state == SSL3_ST_SW_SESSION_TICKET_A) {
		/* get session encoding length */
		slen_full = i2d_SSL_SESSION(s->session, NULL);
		/*
		 * Some length values are 16 bits, so forget it if session is
 		 * too long
 		 */
		if (slen_full > 0xFF00)
			goto err;
		senc = malloc(slen_full);
		if (!senc)
			goto err;
		p = senc;
		i2d_SSL_SESSION(s->session, &p);

		/*
		 * Create a fresh copy (not shared with other threads) to
		 * clean up
		 */
		const_p = senc;
		sess = d2i_SSL_SESSION(NULL, &const_p, slen_full);
		if (sess == NULL)
			goto err;

		/* ID is irrelevant for the ticket */
		sess->session_id_length = 0;

		slen = i2d_SSL_SESSION(sess, NULL);
		if (slen > slen_full) {
			/* shouldn't ever happen */
			goto err;
		}
		p = senc;
		i2d_SSL_SESSION(sess, &p);
		SSL_SESSION_free(sess);

		/*
		 * Grow buffer if need be: the length calculation is as
 		 * follows 1 (size of message name) + 3 (message length
 		 * bytes) + 4 (ticket lifetime hint) + 2 (ticket length) +
 		 * 16 (key name) + max_iv_len (iv length) +
 		 * session_length + max_enc_block_size (max encrypted session
 		 * length) + max_md_size (HMAC).
 		 */
		if (!BUF_MEM_grow(s->internal->init_buf, ssl3_handshake_msg_hdr_len(s) +
		    22 + EVP_MAX_IV_LENGTH + EVP_MAX_BLOCK_LENGTH +
		    EVP_MAX_MD_SIZE + slen))
			goto err;

		d = p = ssl3_handshake_msg_start(s, SSL3_MT_NEWSESSION_TICKET);

		EVP_CIPHER_CTX_init(&ctx);
		HMAC_CTX_init(&hctx);

		/*
		 * Initialize HMAC and cipher contexts. If callback present
		 * it does all the work otherwise use generated values
		 * from parent ctx.
		 */
		if (tctx->internal->tlsext_ticket_key_cb) {
			if (tctx->internal->tlsext_ticket_key_cb(s,
			    key_name, iv, &ctx, &hctx, 1) < 0) {
				EVP_CIPHER_CTX_cleanup(&ctx);
				goto err;
			}
		} else {
			arc4random_buf(iv, 16);
			EVP_EncryptInit_ex(&ctx, EVP_aes_128_cbc(), NULL,
			    tctx->internal->tlsext_tick_aes_key, iv);
			HMAC_Init_ex(&hctx, tctx->internal->tlsext_tick_hmac_key,
			    16, tlsext_tick_md(), NULL);
			memcpy(key_name, tctx->internal->tlsext_tick_key_name, 16);
		}

		/*
		 * Ticket lifetime hint (advisory only):
		 * We leave this unspecified for resumed session
		 * (for simplicity), and guess that tickets for new
		 * sessions will live as long as their sessions.
		 */
		l2n(s->internal->hit ? 0 : s->session->timeout, p);

		/* Skip ticket length for now */
		p += 2;
		/* Output key name */
		macstart = p;
		memcpy(p, key_name, 16);
		p += 16;
		/* output IV */
		memcpy(p, iv, EVP_CIPHER_CTX_iv_length(&ctx));
		p += EVP_CIPHER_CTX_iv_length(&ctx);
		/* Encrypt session data */
		EVP_EncryptUpdate(&ctx, p, &len, senc, slen);
		p += len;
		EVP_EncryptFinal_ex(&ctx, p, &len);
		p += len;
		EVP_CIPHER_CTX_cleanup(&ctx);

		HMAC_Update(&hctx, macstart, p - macstart);
		HMAC_Final(&hctx, p, &hlen);
		HMAC_CTX_cleanup(&hctx);
		p += hlen;

		/* Now write out lengths: p points to end of data written */
		/* Total length */
		len = p - d;

		/* Skip ticket lifetime hint. */
		p = d + 4;
		s2n(len - 6, p); /* Message length */

		ssl3_handshake_msg_finish(s, len);

		S3I(s)->hs.state = SSL3_ST_SW_SESSION_TICKET_B;

		freezero(senc, slen_full);
	}

	/* SSL3_ST_SW_SESSION_TICKET_B */
	return (ssl3_handshake_write(s));

 err:
	freezero(senc, slen_full);

	return (-1);
}

int
ssl3_send_cert_status(SSL *s)
{
	CBB cbb, certstatus, ocspresp;

	memset(&cbb, 0, sizeof(cbb));

	if (S3I(s)->hs.state == SSL3_ST_SW_CERT_STATUS_A) {
		if (!ssl3_handshake_msg_start_cbb(s, &cbb, &certstatus,
		    SSL3_MT_CERTIFICATE_STATUS))
			goto err;
		if (!CBB_add_u8(&certstatus, s->tlsext_status_type))
			goto err;
		if (!CBB_add_u24_length_prefixed(&certstatus, &ocspresp))
			goto err;
		if (!CBB_add_bytes(&ocspresp, s->internal->tlsext_ocsp_resp,
		    s->internal->tlsext_ocsp_resplen))
			goto err;
		if (!ssl3_handshake_msg_finish_cbb(s, &cbb))
			goto err;

		S3I(s)->hs.state = SSL3_ST_SW_CERT_STATUS_B;
	}

	/* SSL3_ST_SW_CERT_STATUS_B */
	return (ssl3_handshake_write(s));

 err:
	CBB_cleanup(&cbb);

	return (-1);
}
