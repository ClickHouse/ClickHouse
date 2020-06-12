/* $OpenBSD: ssl_clnt.c,v 1.17 2017/08/12 21:47:59 jsing Exp $ */
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

#include <limits.h>
#include <stdint.h>
#include <stdio.h>

#include "ssl_locl.h"

#include <openssl/bn.h>
#include <openssl/buffer.h>
#include <openssl/curve25519.h>
#include <openssl/dh.h>
#include <openssl/evp.h>
#include <openssl/md5.h>
#include <openssl/objects.h>

#ifndef OPENSSL_NO_ENGINE
#include <openssl/engine.h>
#endif
#ifndef OPENSSL_NO_GOST
#include <openssl/gost.h>
#endif

#include "bytestring.h"

static int ca_dn_cmp(const X509_NAME * const *a, const X509_NAME * const *b);

int
ssl3_connect(SSL *s)
{
	void   (*cb)(const SSL *ssl, int type, int val) = NULL;
	int	 ret = -1;
	int	 new_state, state, skip = 0;

	ERR_clear_error();
	errno = 0;

	if (s->internal->info_callback != NULL)
		cb = s->internal->info_callback;
	else if (s->ctx->internal->info_callback != NULL)
		cb = s->ctx->internal->info_callback;

	s->internal->in_handshake++;
	if (!SSL_in_init(s) || SSL_in_before(s))
		SSL_clear(s);

	for (;;) {
		state = S3I(s)->hs.state;

		switch (S3I(s)->hs.state) {
		case SSL_ST_RENEGOTIATE:
			s->internal->renegotiate = 1;
			S3I(s)->hs.state = SSL_ST_CONNECT;
			s->ctx->internal->stats.sess_connect_renegotiate++;
			/* break */
		case SSL_ST_BEFORE:
		case SSL_ST_CONNECT:
		case SSL_ST_BEFORE|SSL_ST_CONNECT:
		case SSL_ST_OK|SSL_ST_CONNECT:

			s->server = 0;
			if (cb != NULL)
				cb(s, SSL_CB_HANDSHAKE_START, 1);

			if ((s->version & 0xff00 ) != 0x0300) {
				SSLerror(s, ERR_R_INTERNAL_ERROR);
				ret = -1;
				goto end;
			}

			/* s->version=SSL3_VERSION; */
			s->internal->type = SSL_ST_CONNECT;

			if (!ssl3_setup_init_buffer(s)) {
				ret = -1;
				goto end;
			}
			if (!ssl3_setup_buffers(s)) {
				ret = -1;
				goto end;
			}
			if (!ssl_init_wbio_buffer(s, 0)) {
				ret = -1;
				goto end;
			}

			/* don't push the buffering BIO quite yet */

			if (!tls1_init_finished_mac(s)) {
				ret = -1;
				goto end;
			}

			S3I(s)->hs.state = SSL3_ST_CW_CLNT_HELLO_A;
			s->ctx->internal->stats.sess_connect++;
			s->internal->init_num = 0;
			break;

		case SSL3_ST_CW_CLNT_HELLO_A:
		case SSL3_ST_CW_CLNT_HELLO_B:

			s->internal->shutdown = 0;
			ret = ssl3_client_hello(s);
			if (ret <= 0)
				goto end;
			S3I(s)->hs.state = SSL3_ST_CR_SRVR_HELLO_A;
			s->internal->init_num = 0;

			/* turn on buffering for the next lot of output */
			if (s->bbio != s->wbio)
				s->wbio = BIO_push(s->bbio, s->wbio);

			break;

		case SSL3_ST_CR_SRVR_HELLO_A:
		case SSL3_ST_CR_SRVR_HELLO_B:
			ret = ssl3_get_server_hello(s);
			if (ret <= 0)
				goto end;

			if (s->internal->hit) {
				S3I(s)->hs.state = SSL3_ST_CR_FINISHED_A;
				if (s->internal->tlsext_ticket_expected) {
					/* receive renewed session ticket */
					S3I(s)->hs.state = SSL3_ST_CR_SESSION_TICKET_A;
				}
			} else
				S3I(s)->hs.state = SSL3_ST_CR_CERT_A;
			s->internal->init_num = 0;
			break;

		case SSL3_ST_CR_CERT_A:
		case SSL3_ST_CR_CERT_B:
			ret = ssl3_check_finished(s);
			if (ret <= 0)
				goto end;
			if (ret == 2) {
				s->internal->hit = 1;
				if (s->internal->tlsext_ticket_expected)
					S3I(s)->hs.state = SSL3_ST_CR_SESSION_TICKET_A;
				else
					S3I(s)->hs.state = SSL3_ST_CR_FINISHED_A;
				s->internal->init_num = 0;
				break;
			}
			/* Check if it is anon DH/ECDH. */
			if (!(S3I(s)->hs.new_cipher->algorithm_auth &
			    SSL_aNULL)) {
				ret = ssl3_get_server_certificate(s);
				if (ret <= 0)
					goto end;
				if (s->internal->tlsext_status_expected)
					S3I(s)->hs.state = SSL3_ST_CR_CERT_STATUS_A;
				else
					S3I(s)->hs.state = SSL3_ST_CR_KEY_EXCH_A;
			} else {
				skip = 1;
				S3I(s)->hs.state = SSL3_ST_CR_KEY_EXCH_A;
			}
			s->internal->init_num = 0;
			break;

		case SSL3_ST_CR_KEY_EXCH_A:
		case SSL3_ST_CR_KEY_EXCH_B:
			ret = ssl3_get_server_key_exchange(s);
			if (ret <= 0)
				goto end;
			S3I(s)->hs.state = SSL3_ST_CR_CERT_REQ_A;
			s->internal->init_num = 0;

			/*
			 * At this point we check that we have the
			 * required stuff from the server.
			 */
			if (!ssl3_check_cert_and_algorithm(s)) {
				ret = -1;
				goto end;
			}
			break;

		case SSL3_ST_CR_CERT_REQ_A:
		case SSL3_ST_CR_CERT_REQ_B:
			ret = ssl3_get_certificate_request(s);
			if (ret <= 0)
				goto end;
			S3I(s)->hs.state = SSL3_ST_CR_SRVR_DONE_A;
			s->internal->init_num = 0;
			break;

		case SSL3_ST_CR_SRVR_DONE_A:
		case SSL3_ST_CR_SRVR_DONE_B:
			ret = ssl3_get_server_done(s);
			if (ret <= 0)
				goto end;
			if (S3I(s)->tmp.cert_req)
				S3I(s)->hs.state = SSL3_ST_CW_CERT_A;
			else
				S3I(s)->hs.state = SSL3_ST_CW_KEY_EXCH_A;
			s->internal->init_num = 0;

			break;

		case SSL3_ST_CW_CERT_A:
		case SSL3_ST_CW_CERT_B:
		case SSL3_ST_CW_CERT_C:
		case SSL3_ST_CW_CERT_D:
			ret = ssl3_send_client_certificate(s);
			if (ret <= 0)
				goto end;
			S3I(s)->hs.state = SSL3_ST_CW_KEY_EXCH_A;
			s->internal->init_num = 0;
			break;

		case SSL3_ST_CW_KEY_EXCH_A:
		case SSL3_ST_CW_KEY_EXCH_B:
			ret = ssl3_send_client_key_exchange(s);
			if (ret <= 0)
				goto end;
			/*
			 * EAY EAY EAY need to check for DH fix cert
			 * sent back
			 */
			/*
			 * For TLS, cert_req is set to 2, so a cert chain
			 * of nothing is sent, but no verify packet is sent
			 */
			/*
			 * XXX: For now, we do not support client
			 * authentication in ECDH cipher suites with
			 * ECDH (rather than ECDSA) certificates.
			 * We need to skip the certificate verify
			 * message when client's ECDH public key is sent
			 * inside the client certificate.
			 */
			if (S3I(s)->tmp.cert_req == 1) {
				S3I(s)->hs.state = SSL3_ST_CW_CERT_VRFY_A;
			} else {
				S3I(s)->hs.state = SSL3_ST_CW_CHANGE_A;
				S3I(s)->change_cipher_spec = 0;
			}
			if (s->s3->flags & TLS1_FLAGS_SKIP_CERT_VERIFY) {
				S3I(s)->hs.state = SSL3_ST_CW_CHANGE_A;
				S3I(s)->change_cipher_spec = 0;
			}

			s->internal->init_num = 0;
			break;

		case SSL3_ST_CW_CERT_VRFY_A:
		case SSL3_ST_CW_CERT_VRFY_B:
			ret = ssl3_send_client_verify(s);
			if (ret <= 0)
				goto end;
			S3I(s)->hs.state = SSL3_ST_CW_CHANGE_A;
			s->internal->init_num = 0;
			S3I(s)->change_cipher_spec = 0;
			break;

		case SSL3_ST_CW_CHANGE_A:
		case SSL3_ST_CW_CHANGE_B:
			ret = ssl3_send_change_cipher_spec(s,
			    SSL3_ST_CW_CHANGE_A, SSL3_ST_CW_CHANGE_B);
			if (ret <= 0)
				goto end;

			S3I(s)->hs.state = SSL3_ST_CW_FINISHED_A;
			s->internal->init_num = 0;

			s->session->cipher = S3I(s)->hs.new_cipher;
			if (!tls1_setup_key_block(s)) {
				ret = -1;
				goto end;
			}

			if (!tls1_change_cipher_state(s,
			    SSL3_CHANGE_CIPHER_CLIENT_WRITE)) {
				ret = -1;
				goto end;
			}

			break;

		case SSL3_ST_CW_FINISHED_A:
		case SSL3_ST_CW_FINISHED_B:
			ret = ssl3_send_finished(s, SSL3_ST_CW_FINISHED_A,
			    SSL3_ST_CW_FINISHED_B,
			    TLS_MD_CLIENT_FINISH_CONST,
			    TLS_MD_CLIENT_FINISH_CONST_SIZE);
			if (ret <= 0)
				goto end;
			s->s3->flags |= SSL3_FLAGS_CCS_OK;
			S3I(s)->hs.state = SSL3_ST_CW_FLUSH;

			/* clear flags */
			s->s3->flags &= ~SSL3_FLAGS_POP_BUFFER;
			if (s->internal->hit) {
				S3I(s)->hs.next_state = SSL_ST_OK;
				if (s->s3->flags &
				    SSL3_FLAGS_DELAY_CLIENT_FINISHED) {
					S3I(s)->hs.state = SSL_ST_OK;
					s->s3->flags|=SSL3_FLAGS_POP_BUFFER;
					S3I(s)->delay_buf_pop_ret = 0;
				}
			} else {
				/* Allow NewSessionTicket if ticket expected */
				if (s->internal->tlsext_ticket_expected)
					S3I(s)->hs.next_state =
					    SSL3_ST_CR_SESSION_TICKET_A;
				else

				S3I(s)->hs.next_state = SSL3_ST_CR_FINISHED_A;
			}
			s->internal->init_num = 0;
			break;

		case SSL3_ST_CR_SESSION_TICKET_A:
		case SSL3_ST_CR_SESSION_TICKET_B:
			ret = ssl3_get_new_session_ticket(s);
			if (ret <= 0)
				goto end;
			S3I(s)->hs.state = SSL3_ST_CR_FINISHED_A;
			s->internal->init_num = 0;
			break;

		case SSL3_ST_CR_CERT_STATUS_A:
		case SSL3_ST_CR_CERT_STATUS_B:
			ret = ssl3_get_cert_status(s);
			if (ret <= 0)
				goto end;
			S3I(s)->hs.state = SSL3_ST_CR_KEY_EXCH_A;
			s->internal->init_num = 0;
			break;

		case SSL3_ST_CR_FINISHED_A:
		case SSL3_ST_CR_FINISHED_B:
			s->s3->flags |= SSL3_FLAGS_CCS_OK;
			ret = ssl3_get_finished(s, SSL3_ST_CR_FINISHED_A,
			    SSL3_ST_CR_FINISHED_B);
			if (ret <= 0)
				goto end;

			if (s->internal->hit)
				S3I(s)->hs.state = SSL3_ST_CW_CHANGE_A;
			else
				S3I(s)->hs.state = SSL_ST_OK;
			s->internal->init_num = 0;
			break;

		case SSL3_ST_CW_FLUSH:
			s->internal->rwstate = SSL_WRITING;
			if (BIO_flush(s->wbio) <= 0) {
				ret = -1;
				goto end;
			}
			s->internal->rwstate = SSL_NOTHING;
			S3I(s)->hs.state = S3I(s)->hs.next_state;
			break;

		case SSL_ST_OK:
			/* clean a few things up */
			tls1_cleanup_key_block(s);

			BUF_MEM_free(s->internal->init_buf);
			s->internal->init_buf = NULL;

			/*
			 * If we are not 'joining' the last two packets,
			 * remove the buffering now
			 */
			if (!(s->s3->flags & SSL3_FLAGS_POP_BUFFER))
				ssl_free_wbio_buffer(s);
			/* else do it later in ssl3_write */

			s->internal->init_num = 0;
			s->internal->renegotiate = 0;
			s->internal->new_session = 0;

			ssl_update_cache(s, SSL_SESS_CACHE_CLIENT);
			if (s->internal->hit)
				s->ctx->internal->stats.sess_hit++;

			ret = 1;
			/* s->server=0; */
			s->internal->handshake_func = ssl3_connect;
			s->ctx->internal->stats.sess_connect_good++;

			if (cb != NULL)
				cb(s, SSL_CB_HANDSHAKE_DONE, 1);

			goto end;
			/* break; */

		default:
			SSLerror(s, SSL_R_UNKNOWN_STATE);
			ret = -1;
			goto end;
			/* break; */
		}

		/* did we do anything */
		if (!S3I(s)->tmp.reuse_message && !skip) {
			if (s->internal->debug) {
				if ((ret = BIO_flush(s->wbio)) <= 0)
					goto end;
			}

			if ((cb != NULL) && (S3I(s)->hs.state != state)) {
				new_state = S3I(s)->hs.state;
				S3I(s)->hs.state = state;
				cb(s, SSL_CB_CONNECT_LOOP, 1);
				S3I(s)->hs.state = new_state;
			}
		}
		skip = 0;
	}

end:
	s->internal->in_handshake--;
	if (cb != NULL)
		cb(s, SSL_CB_CONNECT_EXIT, ret);

	return (ret);
}

int
ssl3_client_hello(SSL *s)
{
	unsigned char	*bufend, *p, *d;
	uint16_t	 max_version;
	size_t		 outlen;
	int		 i;

	bufend = (unsigned char *)s->internal->init_buf->data + SSL3_RT_MAX_PLAIN_LENGTH;

	if (S3I(s)->hs.state == SSL3_ST_CW_CLNT_HELLO_A) {
		SSL_SESSION *sess = s->session;

		if (ssl_supported_version_range(s, NULL, &max_version) != 1) {
			SSLerror(s, SSL_R_NO_PROTOCOLS_AVAILABLE);
			return (-1);
		}
		s->client_version = s->version = max_version;

		if ((sess == NULL) ||
		    (sess->ssl_version != s->version) ||
		    (!sess->session_id_length && !sess->tlsext_tick) ||
		    (sess->internal->not_resumable)) {
			if (!ssl_get_new_session(s, 0))
				goto err;
		}
		/* else use the pre-loaded session */

		/*
		 * If a DTLS ClientHello message is being resent after a
		 * HelloVerifyRequest, we must retain the original client
		 * random value.
		 */
		if (!SSL_IS_DTLS(s) || D1I(s)->send_cookie == 0)
			arc4random_buf(s->s3->client_random, SSL3_RANDOM_SIZE);

		d = p = ssl3_handshake_msg_start(s, SSL3_MT_CLIENT_HELLO);

		/*
		 * Version indicates the negotiated version: for example from
		 * an SSLv2/v3 compatible client hello). The client_version
		 * field is the maximum version we permit and it is also
		 * used in RSA encrypted premaster secrets. Some servers can
		 * choke if we initially report a higher version then
		 * renegotiate to a lower one in the premaster secret. This
		 * didn't happen with TLS 1.0 as most servers supported it
		 * but it can with TLS 1.1 or later if the server only supports
		 * 1.0.
		 *
		 * Possible scenario with previous logic:
		 * 	1. Client hello indicates TLS 1.2
		 * 	2. Server hello says TLS 1.0
		 *	3. RSA encrypted premaster secret uses 1.2.
		 * 	4. Handhaked proceeds using TLS 1.0.
		 *	5. Server sends hello request to renegotiate.
		 *	6. Client hello indicates TLS v1.0 as we now
		 *	   know that is maximum server supports.
		 *	7. Server chokes on RSA encrypted premaster secret
		 *	   containing version 1.0.
		 *
		 * For interoperability it should be OK to always use the
		 * maximum version we support in client hello and then rely
		 * on the checking of version to ensure the servers isn't
		 * being inconsistent: for example initially negotiating with
		 * TLS 1.0 and renegotiating with TLS 1.2. We do this by using
		 * client_version in client hello and not resetting it to
		 * the negotiated version.
		 */

		*(p++) = s->client_version >> 8;
		*(p++) = s->client_version & 0xff;

		/* Random stuff */
		memcpy(p, s->s3->client_random, SSL3_RANDOM_SIZE);
		p += SSL3_RANDOM_SIZE;

		/* Session ID */
		if (s->internal->new_session)
			i = 0;
		else
			i = s->session->session_id_length;
		*(p++) = i;
		if (i != 0) {
			if (i > (int)sizeof(s->session->session_id)) {
				SSLerror(s, ERR_R_INTERNAL_ERROR);
				goto err;
			}
			memcpy(p, s->session->session_id, i);
			p += i;
		}

		/* DTLS Cookie. */
		if (SSL_IS_DTLS(s)) {
			if (D1I(s)->cookie_len > sizeof(D1I(s)->cookie)) {
				SSLerror(s, ERR_R_INTERNAL_ERROR);
				goto err;
			}
			*(p++) = D1I(s)->cookie_len;
			memcpy(p, D1I(s)->cookie, D1I(s)->cookie_len);
			p += D1I(s)->cookie_len;
		}

		/* Ciphers supported */
		if (!ssl_cipher_list_to_bytes(s, SSL_get_ciphers(s), &p[2],
		    bufend - &p[2], &outlen))
			goto err;
		if (outlen == 0) {
			SSLerror(s, SSL_R_NO_CIPHERS_AVAILABLE);
			goto err;
		}
		s2n(outlen, p);
		p += outlen;

		/* add in (no) COMPRESSION */
		*(p++) = 1;
		*(p++) = 0; /* Add the NULL method */

		/* TLS extensions*/
		if ((p = ssl_add_clienthello_tlsext(s, p, bufend)) == NULL) {
			SSLerror(s, ERR_R_INTERNAL_ERROR);
			goto err;
		}

		ssl3_handshake_msg_finish(s, p - d);

		S3I(s)->hs.state = SSL3_ST_CW_CLNT_HELLO_B;
	}

	/* SSL3_ST_CW_CLNT_HELLO_B */
	return (ssl3_handshake_write(s));

err:
	return (-1);
}

int
ssl3_get_server_hello(SSL *s)
{
	CBS cbs, server_random, session_id;
	uint16_t server_version, cipher_suite;
	uint16_t min_version, max_version;
	uint8_t compression_method;
	STACK_OF(SSL_CIPHER) *sk;
	const SSL_CIPHER *cipher;
	const SSL_METHOD *method;
	unsigned char *p;
	unsigned long alg_k;
	size_t outlen;
	int i, al, ok;
	long n;

	s->internal->first_packet = 1;
	n = s->method->internal->ssl_get_message(s, SSL3_ST_CR_SRVR_HELLO_A,
	    SSL3_ST_CR_SRVR_HELLO_B, -1, 20000, /* ?? */ &ok);
	if (!ok)
		return ((int)n);
	s->internal->first_packet = 0;

	if (n < 0)
		goto truncated;

	CBS_init(&cbs, s->internal->init_msg, n);

	if (SSL_IS_DTLS(s)) {
		if (S3I(s)->tmp.message_type == DTLS1_MT_HELLO_VERIFY_REQUEST) {
			if (D1I(s)->send_cookie == 0) {
				S3I(s)->tmp.reuse_message = 1;
				return (1);
			} else {
				/* Already sent a cookie. */
				al = SSL_AD_UNEXPECTED_MESSAGE;
				SSLerror(s, SSL_R_BAD_MESSAGE_TYPE);
				goto f_err;
			}
		}
	}

	if (S3I(s)->tmp.message_type != SSL3_MT_SERVER_HELLO) {
		al = SSL_AD_UNEXPECTED_MESSAGE;
		SSLerror(s, SSL_R_BAD_MESSAGE_TYPE);
		goto f_err;
	}

	if (!CBS_get_u16(&cbs, &server_version))
		goto truncated;

	if (ssl_supported_version_range(s, &min_version, &max_version) != 1) {
		SSLerror(s, SSL_R_NO_PROTOCOLS_AVAILABLE);
		goto err;
	}

	if (server_version < min_version || server_version > max_version) {
		SSLerror(s, SSL_R_WRONG_SSL_VERSION);
		s->version = (s->version & 0xff00) | (server_version & 0xff);
		al = SSL_AD_PROTOCOL_VERSION;
		goto f_err;
	}
	s->version = server_version;

	if ((method = tls1_get_client_method(server_version)) == NULL)
		method = dtls1_get_client_method(server_version);
	if (method == NULL) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		goto err;
	}
	s->method = method;

	/* Server random. */
	if (!CBS_get_bytes(&cbs, &server_random, SSL3_RANDOM_SIZE))
		goto truncated;
	if (!CBS_write_bytes(&server_random, s->s3->server_random,
	    sizeof(s->s3->server_random), NULL))
		goto err;

	/* Session ID. */
	if (!CBS_get_u8_length_prefixed(&cbs, &session_id))
		goto truncated;

	if ((CBS_len(&session_id) > sizeof(s->session->session_id)) ||
	    (CBS_len(&session_id) > SSL3_SESSION_ID_SIZE)) {
		al = SSL_AD_ILLEGAL_PARAMETER;
		SSLerror(s, SSL_R_SSL3_SESSION_ID_TOO_LONG);
		goto f_err;
	}

	/* Cipher suite. */
	if (!CBS_get_u16(&cbs, &cipher_suite))
		goto truncated;

	/*
	 * Check if we want to resume the session based on external
	 * pre-shared secret.
	 */
	if (s->internal->tls_session_secret_cb) {
		SSL_CIPHER *pref_cipher = NULL;
		s->session->master_key_length = sizeof(s->session->master_key);
		if (s->internal->tls_session_secret_cb(s, s->session->master_key,
		    &s->session->master_key_length, NULL, &pref_cipher,
		    s->internal->tls_session_secret_cb_arg)) {
			s->session->cipher = pref_cipher ? pref_cipher :
			    ssl3_get_cipher_by_value(cipher_suite);
			s->s3->flags |= SSL3_FLAGS_CCS_OK;
		}
	}

	if (s->session->session_id_length != 0 &&
	    CBS_mem_equal(&session_id, s->session->session_id,
		s->session->session_id_length)) {
		if (s->sid_ctx_length != s->session->sid_ctx_length ||
		    timingsafe_memcmp(s->session->sid_ctx,
		    s->sid_ctx, s->sid_ctx_length) != 0) {
			/* actually a client application bug */
			al = SSL_AD_ILLEGAL_PARAMETER;
			SSLerror(s, SSL_R_ATTEMPT_TO_REUSE_SESSION_IN_DIFFERENT_CONTEXT);
			goto f_err;
		}
		s->s3->flags |= SSL3_FLAGS_CCS_OK;
		s->internal->hit = 1;
	} else {
		/* a miss or crap from the other end */

		/* If we were trying for session-id reuse, make a new
		 * SSL_SESSION so we don't stuff up other people */
		s->internal->hit = 0;
		if (s->session->session_id_length > 0) {
			if (!ssl_get_new_session(s, 0)) {
				al = SSL_AD_INTERNAL_ERROR;
				goto f_err;
			}
		}

		/*
		 * XXX - improve the handling for the case where there is a
		 * zero length session identifier.
		 */
		if (!CBS_write_bytes(&session_id, s->session->session_id,
		    sizeof(s->session->session_id), &outlen))
			goto err;
		s->session->session_id_length = outlen;

		s->session->ssl_version = s->version;
	}

	if ((cipher = ssl3_get_cipher_by_value(cipher_suite)) == NULL) {
		al = SSL_AD_ILLEGAL_PARAMETER;
		SSLerror(s, SSL_R_UNKNOWN_CIPHER_RETURNED);
		goto f_err;
	}

	/* TLS v1.2 only ciphersuites require v1.2 or later. */
	if ((cipher->algorithm_ssl & SSL_TLSV1_2) &&
	    (TLS1_get_version(s) < TLS1_2_VERSION)) {
		al = SSL_AD_ILLEGAL_PARAMETER;
		SSLerror(s, SSL_R_WRONG_CIPHER_RETURNED);
		goto f_err;
	}

	sk = ssl_get_ciphers_by_id(s);
	i = sk_SSL_CIPHER_find(sk, cipher);
	if (i < 0) {
		/* we did not say we would use this cipher */
		al = SSL_AD_ILLEGAL_PARAMETER;
		SSLerror(s, SSL_R_WRONG_CIPHER_RETURNED);
		goto f_err;
	}

	/*
	 * Depending on the session caching (internal/external), the cipher
	 * and/or cipher_id values may not be set. Make sure that
	 * cipher_id is set and use it for comparison.
	 */
	if (s->session->cipher)
		s->session->cipher_id = s->session->cipher->id;
	if (s->internal->hit && (s->session->cipher_id != cipher->id)) {
		al = SSL_AD_ILLEGAL_PARAMETER;
		SSLerror(s, SSL_R_OLD_SESSION_CIPHER_NOT_RETURNED);
		goto f_err;
	}
	S3I(s)->hs.new_cipher = cipher;

	if (!tls1_handshake_hash_init(s))
		goto err;

	/*
	 * Don't digest cached records if no sigalgs: we may need them for
	 * client authentication.
	 */
	alg_k = S3I(s)->hs.new_cipher->algorithm_mkey;
	if (!(SSL_USE_SIGALGS(s) || (alg_k & SSL_kGOST)) &&
	    !tls1_digest_cached_records(s)) {
		al = SSL_AD_INTERNAL_ERROR;
		goto f_err;
	}

	if (!CBS_get_u8(&cbs, &compression_method))
		goto truncated;

	if (compression_method != 0) {
		al = SSL_AD_ILLEGAL_PARAMETER;
		SSLerror(s, SSL_R_UNSUPPORTED_COMPRESSION_ALGORITHM);
		goto f_err;
	}

	/* TLS extensions. */
	p = (unsigned char *)CBS_data(&cbs);
	if (!ssl_parse_serverhello_tlsext(s, &p, CBS_len(&cbs), &al)) {
		/* 'al' set by ssl_parse_serverhello_tlsext */
		SSLerror(s, SSL_R_PARSE_TLSEXT);
		goto f_err;
	}
	if (ssl_check_serverhello_tlsext(s) <= 0) {
		SSLerror(s, SSL_R_SERVERHELLO_TLSEXT);
		goto err;
	}

	/* See if any data remains... */
	if (p - CBS_data(&cbs) != CBS_len(&cbs))
		goto truncated;

	return (1);

truncated:
	/* wrong packet length */
	al = SSL_AD_DECODE_ERROR;
	SSLerror(s, SSL_R_BAD_PACKET_LENGTH);
f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, al);
err:
	return (-1);
}

int
ssl3_get_server_certificate(SSL *s)
{
	int			 al, i, ok, ret = -1;
	long			 n;
	CBS			 cbs, cert_list;
	X509			*x = NULL;
	const unsigned char	*q;
	STACK_OF(X509)		*sk = NULL;
	SESS_CERT		*sc;
	EVP_PKEY		*pkey = NULL;

	n = s->method->internal->ssl_get_message(s, SSL3_ST_CR_CERT_A,
	    SSL3_ST_CR_CERT_B, -1, s->internal->max_cert_list, &ok);

	if (!ok)
		return ((int)n);

	if (S3I(s)->tmp.message_type == SSL3_MT_SERVER_KEY_EXCHANGE) {
		S3I(s)->tmp.reuse_message = 1;
		return (1);
	}

	if (S3I(s)->tmp.message_type != SSL3_MT_CERTIFICATE) {
		al = SSL_AD_UNEXPECTED_MESSAGE;
		SSLerror(s, SSL_R_BAD_MESSAGE_TYPE);
		goto f_err;
	}


	if ((sk = sk_X509_new_null()) == NULL) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		goto err;
	}

	if (n < 0)
		goto truncated;

	CBS_init(&cbs, s->internal->init_msg, n);
	if (CBS_len(&cbs) < 3)
		goto truncated;

	if (!CBS_get_u24_length_prefixed(&cbs, &cert_list) ||
	    CBS_len(&cbs) != 0) {
		al = SSL_AD_DECODE_ERROR;
		SSLerror(s, SSL_R_LENGTH_MISMATCH);
		goto f_err;
	}

	while (CBS_len(&cert_list) > 0) {
		CBS cert;

		if (CBS_len(&cert_list) < 3)
			goto truncated;
		if (!CBS_get_u24_length_prefixed(&cert_list, &cert)) {
			al = SSL_AD_DECODE_ERROR;
			SSLerror(s, SSL_R_CERT_LENGTH_MISMATCH);
			goto f_err;
		}

		q = CBS_data(&cert);
		x = d2i_X509(NULL, &q, CBS_len(&cert));
		if (x == NULL) {
			al = SSL_AD_BAD_CERTIFICATE;
			SSLerror(s, ERR_R_ASN1_LIB);
			goto f_err;
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

	i = ssl_verify_cert_chain(s, sk);
	if ((s->verify_mode != SSL_VERIFY_NONE) && (i <= 0)) {
		al = ssl_verify_alarm_type(s->verify_result);
		SSLerror(s, SSL_R_CERTIFICATE_VERIFY_FAILED);
		goto f_err;

	}
	ERR_clear_error(); /* but we keep s->verify_result */

	sc = ssl_sess_cert_new();
	if (sc == NULL)
		goto err;
	ssl_sess_cert_free(SSI(s)->sess_cert);
	SSI(s)->sess_cert = sc;

	sc->cert_chain = sk;
	/*
	 * Inconsistency alert: cert_chain does include the peer's
	 * certificate, which we don't include in s3_srvr.c
	 */
	x = sk_X509_value(sk, 0);
	sk = NULL;
	/* VRS 19990621: possible memory leak; sk=null ==> !sk_pop_free() @end*/

	pkey = X509_get_pubkey(x);

	if (pkey == NULL || EVP_PKEY_missing_parameters(pkey)) {
		x = NULL;
		al = SSL3_AL_FATAL;
		SSLerror(s, SSL_R_UNABLE_TO_FIND_PUBLIC_KEY_PARAMETERS);
		goto f_err;
	}

	i = ssl_cert_type(x, pkey);
	if (i < 0) {
		x = NULL;
		al = SSL3_AL_FATAL;
		SSLerror(s, SSL_R_UNKNOWN_CERTIFICATE_TYPE);
		goto f_err;
	}

	sc->peer_cert_type = i;
	CRYPTO_add(&x->references, 1, CRYPTO_LOCK_X509);
	/*
	 * Why would the following ever happen?
	 * We just created sc a couple of lines ago.
	 */
	X509_free(sc->peer_pkeys[i].x509);
	sc->peer_pkeys[i].x509 = x;
	sc->peer_key = &(sc->peer_pkeys[i]);

	X509_free(s->session->peer);
	CRYPTO_add(&x->references, 1, CRYPTO_LOCK_X509);
	s->session->peer = x;
	s->session->verify_result = s->verify_result;

	x = NULL;
	ret = 1;

	if (0) {
truncated:
		/* wrong packet length */
		al = SSL_AD_DECODE_ERROR;
		SSLerror(s, SSL_R_BAD_PACKET_LENGTH);
f_err:
		ssl3_send_alert(s, SSL3_AL_FATAL, al);
	}
err:
	EVP_PKEY_free(pkey);
	X509_free(x);
	sk_X509_pop_free(sk, X509_free);

	return (ret);
}

static int
ssl3_get_server_kex_dhe(SSL *s, EVP_PKEY **pkey, unsigned char **pp, long *nn)
{
	CBS cbs, dhp, dhg, dhpk;
	BN_CTX *bn_ctx = NULL;
	SESS_CERT *sc = NULL;
	DH *dh = NULL;
	long alg_a;
	int al;

	alg_a = S3I(s)->hs.new_cipher->algorithm_auth;
	sc = SSI(s)->sess_cert;

	if (*nn < 0)
		goto err;

	CBS_init(&cbs, *pp, *nn);

	if ((dh = DH_new()) == NULL) {
		SSLerror(s, ERR_R_DH_LIB);
		goto err;
	}

	if (!CBS_get_u16_length_prefixed(&cbs, &dhp))
		goto truncated;
	if ((dh->p = BN_bin2bn(CBS_data(&dhp), CBS_len(&dhp), NULL)) == NULL) {
		SSLerror(s, ERR_R_BN_LIB);
		goto err;
	}

	if (!CBS_get_u16_length_prefixed(&cbs, &dhg))
		goto truncated;
	if ((dh->g = BN_bin2bn(CBS_data(&dhg), CBS_len(&dhg), NULL)) == NULL) {
		SSLerror(s, ERR_R_BN_LIB);
		goto err;
	}

	if (!CBS_get_u16_length_prefixed(&cbs, &dhpk))
		goto truncated;
	if ((dh->pub_key = BN_bin2bn(CBS_data(&dhpk), CBS_len(&dhpk),
	    NULL)) == NULL) {
		SSLerror(s, ERR_R_BN_LIB);
		goto err;
	}

	/*
	 * Check the strength of the DH key just constructed.
	 * Discard keys weaker than 1024 bits.
	 */
	if (DH_size(dh) < 1024 / 8) {
		SSLerror(s, SSL_R_BAD_DH_P_LENGTH);
		goto err;
	}

	if (alg_a & SSL_aRSA)
		*pkey = X509_get_pubkey(sc->peer_pkeys[SSL_PKEY_RSA_ENC].x509);
	else
		/* XXX - Anonymous DH, so no certificate or pkey. */
		*pkey = NULL;

	sc->peer_dh_tmp = dh;

	*nn = CBS_len(&cbs);
	*pp = (unsigned char *)CBS_data(&cbs);

	return (1);

 truncated:
	al = SSL_AD_DECODE_ERROR;
	SSLerror(s, SSL_R_BAD_PACKET_LENGTH);
	ssl3_send_alert(s, SSL3_AL_FATAL, al);

 err:
	DH_free(dh);
	BN_CTX_free(bn_ctx);

	return (-1);
}

static int
ssl3_get_server_kex_ecdhe_ecp(SSL *s, SESS_CERT *sc, int nid, CBS *public)
{
	const EC_GROUP *group;
	EC_GROUP *ngroup = NULL;
	EC_POINT *point = NULL;
	BN_CTX *bn_ctx = NULL;
	EC_KEY *ecdh = NULL;
	int ret = -1;

	/*
	 * Extract the server's ephemeral ECDH public key.
	 */

	if ((ecdh = EC_KEY_new()) == NULL) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		goto err;
	}

	if ((ngroup = EC_GROUP_new_by_curve_name(nid)) == NULL) {
		SSLerror(s, ERR_R_EC_LIB);
		goto err;
	}
	if (EC_KEY_set_group(ecdh, ngroup) == 0) {
		SSLerror(s, ERR_R_EC_LIB);
		goto err;
	}

	group = EC_KEY_get0_group(ecdh);

	if ((point = EC_POINT_new(group)) == NULL ||
	    (bn_ctx = BN_CTX_new()) == NULL) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		goto err;
	}

	if (EC_POINT_oct2point(group, point, CBS_data(public),
	    CBS_len(public), bn_ctx) == 0) {
		SSLerror(s, SSL_R_BAD_ECPOINT);
		ssl3_send_alert(s, SSL3_AL_FATAL, SSL_AD_DECODE_ERROR);
		goto err;
	}

	EC_KEY_set_public_key(ecdh, point);
	sc->peer_ecdh_tmp = ecdh;
	ecdh = NULL;

	ret = 1;

 err:
	BN_CTX_free(bn_ctx);
	EC_GROUP_free(ngroup);
	EC_POINT_free(point);
	EC_KEY_free(ecdh);

	return (ret);
}

static int
ssl3_get_server_kex_ecdhe_ecx(SSL *s, SESS_CERT *sc, int nid, CBS *public)
{
	size_t outlen;

	if (nid != NID_X25519) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		goto err;
	}

	if (CBS_len(public) != X25519_KEY_LENGTH) {
		SSLerror(s, SSL_R_BAD_ECPOINT);
		ssl3_send_alert(s, SSL3_AL_FATAL, SSL_AD_DECODE_ERROR);
		goto err;
	}

	if (!CBS_stow(public, &sc->peer_x25519_tmp, &outlen)) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		goto err;
	}

	return (1);

 err:
	return (-1);
}

static int
ssl3_get_server_kex_ecdhe(SSL *s, EVP_PKEY **pkey, unsigned char **pp, long *nn)
{
	CBS cbs, public;
	uint8_t curve_type;
	uint16_t curve_id;
	SESS_CERT *sc;
	long alg_a;
	int nid;
	int al;

	alg_a = S3I(s)->hs.new_cipher->algorithm_auth;
	sc = SSI(s)->sess_cert;

	if (*nn < 0)
		goto err;

	CBS_init(&cbs, *pp, *nn);

	/* Only named curves are supported. */
	if (!CBS_get_u8(&cbs, &curve_type) ||
	    curve_type != NAMED_CURVE_TYPE ||
	    !CBS_get_u16(&cbs, &curve_id)) {
		al = SSL_AD_DECODE_ERROR;
		SSLerror(s, SSL_R_LENGTH_TOO_SHORT);
		goto f_err;
	}

	/*
	 * Check that the curve is one of our preferences - if it is not,
	 * the server has sent us an invalid curve.
	 */
	if (tls1_check_curve(s, curve_id) != 1) {
		al = SSL_AD_DECODE_ERROR;
		SSLerror(s, SSL_R_WRONG_CURVE);
		goto f_err;
	}

	if ((nid = tls1_ec_curve_id2nid(curve_id)) == 0) {
		al = SSL_AD_INTERNAL_ERROR;
		SSLerror(s, SSL_R_UNABLE_TO_FIND_ECDH_PARAMETERS);
		goto f_err;
	}

	if (!CBS_get_u8_length_prefixed(&cbs, &public))
		goto truncated;

	if (nid == NID_X25519) {
		if (ssl3_get_server_kex_ecdhe_ecx(s, sc, nid, &public) != 1)
			goto err;
	} else {
		if (ssl3_get_server_kex_ecdhe_ecp(s, sc, nid, &public) != 1)
			goto err;
	}

	/*
	 * The ECC/TLS specification does not mention the use of DSA to sign
	 * ECParameters in the server key exchange message. We do support RSA
	 * and ECDSA.
	 */
	if (alg_a & SSL_aRSA)
		*pkey = X509_get_pubkey(sc->peer_pkeys[SSL_PKEY_RSA_ENC].x509);
	else if (alg_a & SSL_aECDSA)
		*pkey = X509_get_pubkey(sc->peer_pkeys[SSL_PKEY_ECC].x509);
	else
		/* XXX - Anonymous ECDH, so no certificate or pkey. */
		*pkey = NULL;

	*nn = CBS_len(&cbs);
	*pp = (unsigned char *)CBS_data(&cbs);

	return (1);

 truncated:
	al = SSL_AD_DECODE_ERROR;
	SSLerror(s, SSL_R_BAD_PACKET_LENGTH);

 f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, al);

 err:
	return (-1);
}

int
ssl3_get_server_key_exchange(SSL *s)
{
	unsigned char	*q, md_buf[EVP_MAX_MD_SIZE*2];
	EVP_MD_CTX	 md_ctx;
	unsigned char	*param, *p;
	int		 al, i, j, param_len, ok;
	long		 n, alg_k, alg_a;
	EVP_PKEY	*pkey = NULL;
	const		 EVP_MD *md = NULL;
	RSA		*rsa = NULL;

	alg_k = S3I(s)->hs.new_cipher->algorithm_mkey;
	alg_a = S3I(s)->hs.new_cipher->algorithm_auth;

	/*
	 * Use same message size as in ssl3_get_certificate_request()
	 * as ServerKeyExchange message may be skipped.
	 */
	n = s->method->internal->ssl_get_message(s, SSL3_ST_CR_KEY_EXCH_A,
	    SSL3_ST_CR_KEY_EXCH_B, -1, s->internal->max_cert_list, &ok);
	if (!ok)
		return ((int)n);

	EVP_MD_CTX_init(&md_ctx);

	if (S3I(s)->tmp.message_type != SSL3_MT_SERVER_KEY_EXCHANGE) {
		/*
		 * Do not skip server key exchange if this cipher suite uses
		 * ephemeral keys.
		 */
		if (alg_k & (SSL_kDHE|SSL_kECDHE)) {
			SSLerror(s, SSL_R_UNEXPECTED_MESSAGE);
			al = SSL_AD_UNEXPECTED_MESSAGE;
			goto f_err;
		}

		S3I(s)->tmp.reuse_message = 1;
		EVP_MD_CTX_cleanup(&md_ctx);
		return (1);
	}

	if (SSI(s)->sess_cert != NULL) {
		DH_free(SSI(s)->sess_cert->peer_dh_tmp);
		SSI(s)->sess_cert->peer_dh_tmp = NULL;

		EC_KEY_free(SSI(s)->sess_cert->peer_ecdh_tmp);
		SSI(s)->sess_cert->peer_ecdh_tmp = NULL;

		free(SSI(s)->sess_cert->peer_x25519_tmp);
		SSI(s)->sess_cert->peer_x25519_tmp = NULL;
	} else {
		SSI(s)->sess_cert = ssl_sess_cert_new();
		if (SSI(s)->sess_cert == NULL)
			goto err;
	}

	param = p = (unsigned char *)s->internal->init_msg;
	param_len = n;

	if (alg_k & SSL_kDHE) {
		if (ssl3_get_server_kex_dhe(s, &pkey, &p, &n) != 1)
			goto err;
	} else if (alg_k & SSL_kECDHE) {
		if (ssl3_get_server_kex_ecdhe(s, &pkey, &p, &n) != 1)
			goto err;
	} else if (alg_k != 0) {
		al = SSL_AD_UNEXPECTED_MESSAGE;
		SSLerror(s, SSL_R_UNEXPECTED_MESSAGE);
			goto f_err;
	}

	param_len = param_len - n;

	/* if it was signed, check the signature */
	if (pkey != NULL) {
		if (SSL_USE_SIGALGS(s)) {
			int sigalg = tls12_get_sigid(pkey);
			/* Should never happen */
			if (sigalg == -1) {
				SSLerror(s, ERR_R_INTERNAL_ERROR);
				goto err;
			}
			/*
			 * Check key type is consistent
			 * with signature
			 */
			if (2 > n)
				goto truncated;
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
		} else
			md = EVP_sha1();

		if (2 > n)
			goto truncated;
		n2s(p, i);
		n -= 2;
		j = EVP_PKEY_size(pkey);

		if (i != n || n > j) {
			/* wrong packet length */
			al = SSL_AD_DECODE_ERROR;
			SSLerror(s, SSL_R_WRONG_SIGNATURE_LENGTH);
			goto f_err;
		}

		if (pkey->type == EVP_PKEY_RSA && !SSL_USE_SIGALGS(s)) {
			j = 0;
			q = md_buf;
			if (!EVP_DigestInit_ex(&md_ctx, EVP_md5_sha1(), NULL)) {
				al = SSL_AD_INTERNAL_ERROR;
				goto f_err;
			}
			EVP_DigestUpdate(&md_ctx, s->s3->client_random,
			    SSL3_RANDOM_SIZE);
			EVP_DigestUpdate(&md_ctx, s->s3->server_random,
			    SSL3_RANDOM_SIZE);
			EVP_DigestUpdate(&md_ctx, param, param_len);
			EVP_DigestFinal_ex(&md_ctx, q, (unsigned int *)&i);
			q += i;
			j += i;
			i = RSA_verify(NID_md5_sha1, md_buf, j,
			    p, n, pkey->pkey.rsa);
			if (i < 0) {
				al = SSL_AD_DECRYPT_ERROR;
				SSLerror(s, SSL_R_BAD_RSA_DECRYPT);
				goto f_err;
			}
			if (i == 0) {
				/* bad signature */
				al = SSL_AD_DECRYPT_ERROR;
				SSLerror(s, SSL_R_BAD_SIGNATURE);
				goto f_err;
			}
		} else {
			EVP_VerifyInit_ex(&md_ctx, md, NULL);
			EVP_VerifyUpdate(&md_ctx, s->s3->client_random,
			    SSL3_RANDOM_SIZE);
			EVP_VerifyUpdate(&md_ctx, s->s3->server_random,
			    SSL3_RANDOM_SIZE);
			EVP_VerifyUpdate(&md_ctx, param, param_len);
			if (EVP_VerifyFinal(&md_ctx, p,(int)n, pkey) <= 0) {
				/* bad signature */
				al = SSL_AD_DECRYPT_ERROR;
				SSLerror(s, SSL_R_BAD_SIGNATURE);
				goto f_err;
			}
		}
	} else {
		/* aNULL does not need public keys. */
		if (!(alg_a & SSL_aNULL)) {
			SSLerror(s, ERR_R_INTERNAL_ERROR);
			goto err;
		}
		/* still data left over */
		if (n != 0) {
			al = SSL_AD_DECODE_ERROR;
			SSLerror(s, SSL_R_EXTRA_DATA_IN_MESSAGE);
			goto f_err;
		}
	}

	EVP_PKEY_free(pkey);
	EVP_MD_CTX_cleanup(&md_ctx);

	return (1);

 truncated:
	/* wrong packet length */
	al = SSL_AD_DECODE_ERROR;
	SSLerror(s, SSL_R_BAD_PACKET_LENGTH);

 f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, al);

 err:
	EVP_PKEY_free(pkey);
	RSA_free(rsa);
	EVP_MD_CTX_cleanup(&md_ctx);

	return (-1);
}

int
ssl3_get_certificate_request(SSL *s)
{
	int			 ok, ret = 0;
	long		 	 n;
	uint8_t			 ctype_num;
	CBS			 cert_request, ctypes, rdn_list;
	X509_NAME		*xn = NULL;
	const unsigned char	*q;
	STACK_OF(X509_NAME)	*ca_sk = NULL;

	n = s->method->internal->ssl_get_message(s, SSL3_ST_CR_CERT_REQ_A,
	    SSL3_ST_CR_CERT_REQ_B, -1, s->internal->max_cert_list, &ok);

	if (!ok)
		return ((int)n);

	S3I(s)->tmp.cert_req = 0;

	if (S3I(s)->tmp.message_type == SSL3_MT_SERVER_DONE) {
		S3I(s)->tmp.reuse_message = 1;
		/*
		 * If we get here we don't need any cached handshake records
		 * as we wont be doing client auth.
		 */
		if (S3I(s)->handshake_buffer) {
			if (!tls1_digest_cached_records(s))
				goto err;
		}
		return (1);
	}

	if (S3I(s)->tmp.message_type != SSL3_MT_CERTIFICATE_REQUEST) {
		ssl3_send_alert(s, SSL3_AL_FATAL, SSL_AD_UNEXPECTED_MESSAGE);
		SSLerror(s, SSL_R_WRONG_MESSAGE_TYPE);
		goto err;
	}

	/* TLS does not like anon-DH with client cert */
	if (S3I(s)->hs.new_cipher->algorithm_auth & SSL_aNULL) {
		ssl3_send_alert(s, SSL3_AL_FATAL, SSL_AD_UNEXPECTED_MESSAGE);
		SSLerror(s, SSL_R_TLS_CLIENT_CERT_REQ_WITH_ANON_CIPHER);
		goto err;
	}

	if (n < 0)
		goto truncated;
	CBS_init(&cert_request, s->internal->init_msg, n);

	if ((ca_sk = sk_X509_NAME_new(ca_dn_cmp)) == NULL) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		goto err;
	}

	/* get the certificate types */
	if (!CBS_get_u8(&cert_request, &ctype_num))
		goto truncated;

	if (ctype_num > SSL3_CT_NUMBER)
		ctype_num = SSL3_CT_NUMBER;
	if (!CBS_get_bytes(&cert_request, &ctypes, ctype_num) ||
	    !CBS_write_bytes(&ctypes, (uint8_t *)S3I(s)->tmp.ctype,
	    sizeof(S3I(s)->tmp.ctype), NULL)) {
		SSLerror(s, SSL_R_DATA_LENGTH_TOO_LONG);
		goto err;
	}

	if (SSL_USE_SIGALGS(s)) {
		CBS sigalgs;

		if (CBS_len(&cert_request) < 2) {
			SSLerror(s, SSL_R_DATA_LENGTH_TOO_LONG);
			goto err;
		}

		/* Check we have enough room for signature algorithms and
		 * following length value.
		 */
		if (!CBS_get_u16_length_prefixed(&cert_request, &sigalgs)) {
			ssl3_send_alert(s, SSL3_AL_FATAL, SSL_AD_DECODE_ERROR);
			SSLerror(s, SSL_R_DATA_LENGTH_TOO_LONG);
			goto err;
		}
		if (!tls1_process_sigalgs(s, &sigalgs)) {
			ssl3_send_alert(s, SSL3_AL_FATAL, SSL_AD_DECODE_ERROR);
			SSLerror(s, SSL_R_SIGNATURE_ALGORITHMS_ERROR);
			goto err;
		}
	}

	/* get the CA RDNs */
	if (CBS_len(&cert_request) < 2) {
		SSLerror(s, SSL_R_DATA_LENGTH_TOO_LONG);
		goto err;
	}

	if (!CBS_get_u16_length_prefixed(&cert_request, &rdn_list) ||
	    CBS_len(&cert_request) != 0) {
		ssl3_send_alert(s, SSL3_AL_FATAL, SSL_AD_DECODE_ERROR);
		SSLerror(s, SSL_R_LENGTH_MISMATCH);
		goto err;
	}

	while (CBS_len(&rdn_list) > 0) {
		CBS rdn;

		if (CBS_len(&rdn_list) < 2) {
			SSLerror(s, SSL_R_DATA_LENGTH_TOO_LONG);
			goto err;
		}

		if (!CBS_get_u16_length_prefixed(&rdn_list, &rdn)) {
			ssl3_send_alert(s, SSL3_AL_FATAL, SSL_AD_DECODE_ERROR);
			SSLerror(s, SSL_R_CA_DN_TOO_LONG);
			goto err;
		}

		q = CBS_data(&rdn);
		if ((xn = d2i_X509_NAME(NULL, &q, CBS_len(&rdn))) == NULL) {
			ssl3_send_alert(s, SSL3_AL_FATAL,
			    SSL_AD_DECODE_ERROR);
			SSLerror(s, ERR_R_ASN1_LIB);
			goto err;
		}

		if (q != CBS_data(&rdn) + CBS_len(&rdn)) {
			ssl3_send_alert(s, SSL3_AL_FATAL, SSL_AD_DECODE_ERROR);
			SSLerror(s, SSL_R_CA_DN_LENGTH_MISMATCH);
			goto err;
		}
		if (!sk_X509_NAME_push(ca_sk, xn)) {
			SSLerror(s, ERR_R_MALLOC_FAILURE);
			goto err;
		}
		xn = NULL;	/* avoid free in err block */
	}

	/* we should setup a certificate to return.... */
	S3I(s)->tmp.cert_req = 1;
	S3I(s)->tmp.ctype_num = ctype_num;
	sk_X509_NAME_pop_free(S3I(s)->tmp.ca_names, X509_NAME_free);
	S3I(s)->tmp.ca_names = ca_sk;
	ca_sk = NULL;

	ret = 1;
	if (0) {
truncated:
		SSLerror(s, SSL_R_BAD_PACKET_LENGTH);
	}
err:
	X509_NAME_free(xn);
	sk_X509_NAME_pop_free(ca_sk, X509_NAME_free);
	return (ret);
}

static int
ca_dn_cmp(const X509_NAME * const *a, const X509_NAME * const *b)
{
	return (X509_NAME_cmp(*a, *b));
}

int
ssl3_get_new_session_ticket(SSL *s)
{
	int			 ok, al, ret = 0;
	uint32_t		 lifetime_hint;
	long			 n;
	CBS			 cbs, session_ticket;

	n = s->method->internal->ssl_get_message(s, SSL3_ST_CR_SESSION_TICKET_A,
	    SSL3_ST_CR_SESSION_TICKET_B, -1, 16384, &ok);
	if (!ok)
		return ((int)n);

	if (S3I(s)->tmp.message_type == SSL3_MT_FINISHED) {
		S3I(s)->tmp.reuse_message = 1;
		return (1);
	}
	if (S3I(s)->tmp.message_type != SSL3_MT_NEWSESSION_TICKET) {
		al = SSL_AD_UNEXPECTED_MESSAGE;
		SSLerror(s, SSL_R_BAD_MESSAGE_TYPE);
		goto f_err;
	}

	if (n < 0) {
		al = SSL_AD_DECODE_ERROR;
		SSLerror(s, SSL_R_LENGTH_MISMATCH);
		goto f_err;
	}

	CBS_init(&cbs, s->internal->init_msg, n);
	if (!CBS_get_u32(&cbs, &lifetime_hint) ||
#if UINT32_MAX > LONG_MAX
	    lifetime_hint > LONG_MAX ||
#endif
	    !CBS_get_u16_length_prefixed(&cbs, &session_ticket) ||
	    CBS_len(&cbs) != 0) {
		al = SSL_AD_DECODE_ERROR;
		SSLerror(s, SSL_R_LENGTH_MISMATCH);
		goto f_err;
	}
	s->session->tlsext_tick_lifetime_hint = (long)lifetime_hint;

	if (!CBS_stow(&session_ticket, &s->session->tlsext_tick,
	    &s->session->tlsext_ticklen)) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		goto err;
	}

	/*
	 * There are two ways to detect a resumed ticket sesion.
	 * One is to set an appropriate session ID and then the server
	 * must return a match in ServerHello. This allows the normal
	 * client session ID matching to work and we know much
	 * earlier that the ticket has been accepted.
	 *
	 * The other way is to set zero length session ID when the
	 * ticket is presented and rely on the handshake to determine
	 * session resumption.
	 *
	 * We choose the former approach because this fits in with
	 * assumptions elsewhere in OpenSSL. The session ID is set
	 * to the SHA256 (or SHA1 is SHA256 is disabled) hash of the
	 * ticket.
	 */
	EVP_Digest(CBS_data(&session_ticket), CBS_len(&session_ticket),
	    s->session->session_id, &s->session->session_id_length,
	    EVP_sha256(), NULL);
	ret = 1;
	return (ret);
f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, al);
err:
	return (-1);
}

int
ssl3_get_cert_status(SSL *s)
{
	CBS			 cert_status, response;
	size_t			 stow_len;
	int			 ok, al;
	long			 n;
	uint8_t			 status_type;

	n = s->method->internal->ssl_get_message(s, SSL3_ST_CR_CERT_STATUS_A,
	    SSL3_ST_CR_CERT_STATUS_B, SSL3_MT_CERTIFICATE_STATUS,
	    16384, &ok);

	if (!ok)
		return ((int)n);

	if (n < 0) {
		/* need at least status type + length */
		al = SSL_AD_DECODE_ERROR;
		SSLerror(s, SSL_R_LENGTH_MISMATCH);
		goto f_err;
	}

	CBS_init(&cert_status, s->internal->init_msg, n);
	if (!CBS_get_u8(&cert_status, &status_type) ||
	    CBS_len(&cert_status) < 3) {
		/* need at least status type + length */
		al = SSL_AD_DECODE_ERROR;
		SSLerror(s, SSL_R_LENGTH_MISMATCH);
		goto f_err;
	}

	if (status_type != TLSEXT_STATUSTYPE_ocsp) {
		al = SSL_AD_DECODE_ERROR;
		SSLerror(s, SSL_R_UNSUPPORTED_STATUS_TYPE);
		goto f_err;
	}

	if (!CBS_get_u24_length_prefixed(&cert_status, &response) ||
	    CBS_len(&cert_status) != 0) {
		al = SSL_AD_DECODE_ERROR;
		SSLerror(s, SSL_R_LENGTH_MISMATCH);
		goto f_err;
	}

	if (!CBS_stow(&response, &s->internal->tlsext_ocsp_resp,
	    &stow_len) || stow_len > INT_MAX) {
		s->internal->tlsext_ocsp_resplen = 0;
 		al = SSL_AD_INTERNAL_ERROR;
 		SSLerror(s, ERR_R_MALLOC_FAILURE);
 		goto f_err;
 	}
	s->internal->tlsext_ocsp_resplen = (int)stow_len;

	if (s->ctx->internal->tlsext_status_cb) {
		int ret;
		ret = s->ctx->internal->tlsext_status_cb(s,
		    s->ctx->internal->tlsext_status_arg);
		if (ret == 0) {
			al = SSL_AD_BAD_CERTIFICATE_STATUS_RESPONSE;
			SSLerror(s, SSL_R_INVALID_STATUS_RESPONSE);
			goto f_err;
		}
		if (ret < 0) {
			al = SSL_AD_INTERNAL_ERROR;
			SSLerror(s, ERR_R_MALLOC_FAILURE);
			goto f_err;
		}
	}
	return (1);
f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, al);
	return (-1);
}

int
ssl3_get_server_done(SSL *s)
{
	int	ok, ret = 0;
	long	n;

	n = s->method->internal->ssl_get_message(s, SSL3_ST_CR_SRVR_DONE_A,
	    SSL3_ST_CR_SRVR_DONE_B, SSL3_MT_SERVER_DONE,
	    30, /* should be very small, like 0 :-) */ &ok);

	if (!ok)
		return ((int)n);
	if (n > 0) {
		/* should contain no data */
		ssl3_send_alert(s, SSL3_AL_FATAL, SSL_AD_DECODE_ERROR);
		SSLerror(s, SSL_R_LENGTH_MISMATCH);
		return (-1);
	}
	ret = 1;
	return (ret);
}

static int
ssl3_send_client_kex_rsa(SSL *s, SESS_CERT *sess_cert, CBB *cbb)
{
	unsigned char pms[SSL_MAX_MASTER_KEY_LENGTH];
	unsigned char *enc_pms = NULL;
	EVP_PKEY *pkey = NULL;
	int ret = -1;
	int enc_len;
	CBB epms;

	/*
	 * RSA-Encrypted Premaster Secret Message - RFC 5246 section 7.4.7.1.
	 */

	pkey = X509_get_pubkey(sess_cert->peer_pkeys[SSL_PKEY_RSA_ENC].x509);
	if (pkey == NULL || pkey->type != EVP_PKEY_RSA ||
	    pkey->pkey.rsa == NULL) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		goto err;
	}

	pms[0] = s->client_version >> 8;
	pms[1] = s->client_version & 0xff;
	arc4random_buf(&pms[2], sizeof(pms) - 2);

	if ((enc_pms = malloc(RSA_size(pkey->pkey.rsa))) == NULL) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		goto err;
	}

	enc_len = RSA_public_encrypt(sizeof(pms), pms, enc_pms, pkey->pkey.rsa,
	    RSA_PKCS1_PADDING);
	if (enc_len <= 0) {
		SSLerror(s, SSL_R_BAD_RSA_ENCRYPT);
		goto err;
	}

	if (!CBB_add_u16_length_prefixed(cbb, &epms))
		goto err;
	if (!CBB_add_bytes(&epms, enc_pms, enc_len))
		goto err;
	if (!CBB_flush(cbb))
		goto err;

	s->session->master_key_length =
	    tls1_generate_master_secret(s,
		s->session->master_key, pms, sizeof(pms));

	ret = 1;

err:
	explicit_bzero(pms, sizeof(pms));
	EVP_PKEY_free(pkey);
	free(enc_pms);

	return (ret);
}

static int
ssl3_send_client_kex_dhe(SSL *s, SESS_CERT *sess_cert, CBB *cbb)
{
	DH *dh_srvr = NULL, *dh_clnt = NULL;
	unsigned char *key = NULL;
	int key_size = 0, key_len;
	unsigned char *data;
	int ret = -1;
	CBB dh_Yc;

	/* Ensure that we have an ephemeral key for DHE. */
	if (sess_cert->peer_dh_tmp == NULL) {
		ssl3_send_alert(s, SSL3_AL_FATAL, SSL_AD_HANDSHAKE_FAILURE);
		SSLerror(s, SSL_R_UNABLE_TO_FIND_DH_PARAMETERS);
		goto err;
	}
	dh_srvr = sess_cert->peer_dh_tmp;

	/* Generate a new random key. */
	if ((dh_clnt = DHparams_dup(dh_srvr)) == NULL) {
		SSLerror(s, ERR_R_DH_LIB);
		goto err;
	}
	if (!DH_generate_key(dh_clnt)) {
		SSLerror(s, ERR_R_DH_LIB);
		goto err;
	}
	key_size = DH_size(dh_clnt);
	if ((key = malloc(key_size)) == NULL) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		goto err;
	}
	key_len = DH_compute_key(key, dh_srvr->pub_key, dh_clnt);
	if (key_len <= 0) {
		SSLerror(s, ERR_R_DH_LIB);
		goto err;
	}

	/* Generate master key from the result. */
	s->session->master_key_length =
	    tls1_generate_master_secret(s,
		s->session->master_key, key, key_len);

	if (!CBB_add_u16_length_prefixed(cbb, &dh_Yc))
		goto err;
	if (!CBB_add_space(&dh_Yc, &data, BN_num_bytes(dh_clnt->pub_key)))
		goto err;
	BN_bn2bin(dh_clnt->pub_key, data);
	if (!CBB_flush(cbb))
		goto err;

	ret = 1;

err:
	DH_free(dh_clnt);
	freezero(key, key_size);

	return (ret);
}

static int
ssl3_send_client_kex_ecdhe_ecp(SSL *s, SESS_CERT *sc, CBB *cbb)
{
	const EC_GROUP *group = NULL;
	const EC_POINT *point = NULL;
	EC_KEY *ecdh = NULL;
	BN_CTX *bn_ctx = NULL;
	unsigned char *key = NULL;
	unsigned char *data;
	size_t encoded_len;
	int key_size = 0, key_len;
	int ret = -1;
	CBB ecpoint;

	if ((group = EC_KEY_get0_group(sc->peer_ecdh_tmp)) == NULL ||
	    (point = EC_KEY_get0_public_key(sc->peer_ecdh_tmp)) == NULL) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		goto err;
	}

	if ((ecdh = EC_KEY_new()) == NULL) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		goto err;
	}

	if (!EC_KEY_set_group(ecdh, group)) {
		SSLerror(s, ERR_R_EC_LIB);
		goto err;
	}

	/* Generate a new ECDH key pair. */
	if (!(EC_KEY_generate_key(ecdh))) {
		SSLerror(s, ERR_R_ECDH_LIB);
		goto err;
	}
	if ((key_size = ECDH_size(ecdh)) <= 0) {
		SSLerror(s, ERR_R_ECDH_LIB);
		goto err;
	}
	if ((key = malloc(key_size)) == NULL) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
	}
	key_len = ECDH_compute_key(key, key_size, point, ecdh, NULL);
	if (key_len <= 0) {
		SSLerror(s, ERR_R_ECDH_LIB);
		goto err;
	}

	/* Generate master key from the result. */
	s->session->master_key_length =
	    tls1_generate_master_secret(s,
		s->session->master_key, key, key_len);

	encoded_len = EC_POINT_point2oct(group, EC_KEY_get0_public_key(ecdh),
	    POINT_CONVERSION_UNCOMPRESSED, NULL, 0, NULL);
	if (encoded_len == 0) {
		SSLerror(s, ERR_R_ECDH_LIB);
		goto err;
	}

	if ((bn_ctx = BN_CTX_new()) == NULL) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		goto err;
	}

	/* Encode the public key. */
	if (!CBB_add_u8_length_prefixed(cbb, &ecpoint))
		goto err;
	if (!CBB_add_space(&ecpoint, &data, encoded_len))
		goto err;
	if (EC_POINT_point2oct(group, EC_KEY_get0_public_key(ecdh),
	    POINT_CONVERSION_UNCOMPRESSED, data, encoded_len,
	    bn_ctx) == 0)
		goto err;
	if (!CBB_flush(cbb))
		goto err;

	ret = 1;

 err:
	freezero(key, key_size);

	BN_CTX_free(bn_ctx);
	EC_KEY_free(ecdh);

	return (ret);
}

static int
ssl3_send_client_kex_ecdhe_ecx(SSL *s, SESS_CERT *sc, CBB *cbb)
{
	uint8_t *public_key = NULL, *private_key = NULL, *shared_key = NULL;
	int ret = -1;
	CBB ecpoint;

	/* Generate X25519 key pair and derive shared key. */
	if ((public_key = malloc(X25519_KEY_LENGTH)) == NULL)
		goto err;
	if ((private_key = malloc(X25519_KEY_LENGTH)) == NULL)
		goto err;
	if ((shared_key = malloc(X25519_KEY_LENGTH)) == NULL)
		goto err;
	X25519_keypair(public_key, private_key);
	if (!X25519(shared_key, private_key, sc->peer_x25519_tmp))
		goto err;

	/* Serialize the public key. */
	if (!CBB_add_u8_length_prefixed(cbb, &ecpoint))
		goto err;
	if (!CBB_add_bytes(&ecpoint, public_key, X25519_KEY_LENGTH))
		goto err;
	if (!CBB_flush(cbb))
		goto err;

	/* Generate master key from the result. */
	s->session->master_key_length =
	    tls1_generate_master_secret(s,
		s->session->master_key, shared_key, X25519_KEY_LENGTH);

	ret = 1;

 err:
	free(public_key);
	freezero(private_key, X25519_KEY_LENGTH);
	freezero(shared_key, X25519_KEY_LENGTH);

	return (ret);
}

static int
ssl3_send_client_kex_ecdhe(SSL *s, SESS_CERT *sc, CBB *cbb)
{
	if (sc->peer_x25519_tmp != NULL) {
		if (ssl3_send_client_kex_ecdhe_ecx(s, sc, cbb) != 1)
			goto err;
	} else if (sc->peer_ecdh_tmp != NULL) {
		if (ssl3_send_client_kex_ecdhe_ecp(s, sc, cbb) != 1)
			goto err;
	} else {
		ssl3_send_alert(s, SSL3_AL_FATAL, SSL_AD_HANDSHAKE_FAILURE);
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		goto err;
	}

	return (1);

 err:
	return (-1);
}

static int
ssl3_send_client_kex_gost(SSL *s, SESS_CERT *sess_cert, CBB *cbb)
{
	unsigned char premaster_secret[32], shared_ukm[32], tmp[256];
	EVP_PKEY *pub_key = NULL;
	EVP_PKEY_CTX *pkey_ctx;
	X509 *peer_cert;
	size_t msglen;
	unsigned int md_len;
	EVP_MD_CTX *ukm_hash;
	int ret = -1;
	int nid;
	CBB gostblob;

	/* Get server sertificate PKEY and create ctx from it */
	peer_cert = sess_cert->peer_pkeys[SSL_PKEY_GOST01].x509;
	if (peer_cert == NULL) {
		SSLerror(s, SSL_R_NO_GOST_CERTIFICATE_SENT_BY_PEER);
		goto err;
	}

	pub_key = X509_get_pubkey(peer_cert);
	pkey_ctx = EVP_PKEY_CTX_new(pub_key, NULL);

	/*
	 * If we have send a certificate, and certificate key parameters match
	 * those of server certificate, use certificate key for key exchange.
	 * Otherwise, generate ephemeral key pair.
	 */
	EVP_PKEY_encrypt_init(pkey_ctx);

	/* Generate session key. */
	arc4random_buf(premaster_secret, 32);

	/*
	 * If we have client certificate, use its secret as peer key.
	 */
	if (S3I(s)->tmp.cert_req && s->cert->key->privatekey) {
		if (EVP_PKEY_derive_set_peer(pkey_ctx,
		    s->cert->key->privatekey) <=0) {
			/*
			 * If there was an error - just ignore it.
			 * Ephemeral key would be used.
			 */
			ERR_clear_error();
		}
	}

	/*
	 * Compute shared IV and store it in algorithm-specific context data.
	 */
	ukm_hash = EVP_MD_CTX_create();
	if (ukm_hash == NULL) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		goto err;
	}

	if (ssl_get_algorithm2(s) & SSL_HANDSHAKE_MAC_GOST94)
		nid = NID_id_GostR3411_94;
	else
		nid = NID_id_tc26_gost3411_2012_256;
	if (!EVP_DigestInit(ukm_hash, EVP_get_digestbynid(nid)))
		goto err;
	EVP_DigestUpdate(ukm_hash, s->s3->client_random, SSL3_RANDOM_SIZE);
	EVP_DigestUpdate(ukm_hash, s->s3->server_random, SSL3_RANDOM_SIZE);
	EVP_DigestFinal_ex(ukm_hash, shared_ukm, &md_len);
	EVP_MD_CTX_destroy(ukm_hash);
	if (EVP_PKEY_CTX_ctrl(pkey_ctx, -1, EVP_PKEY_OP_ENCRYPT,
	    EVP_PKEY_CTRL_SET_IV, 8, shared_ukm) < 0) {
		SSLerror(s, SSL_R_LIBRARY_BUG);
		goto err;
	}

	/*
	 * Make GOST keytransport blob message, encapsulate it into sequence.
	 */
	msglen = 255;
	if (EVP_PKEY_encrypt(pkey_ctx, tmp, &msglen, premaster_secret,
	    32) < 0) {
		SSLerror(s, SSL_R_LIBRARY_BUG);
		goto err;
	}

	if (!CBB_add_asn1(cbb, &gostblob, CBS_ASN1_SEQUENCE))
		goto err;
	if (!CBB_add_bytes(&gostblob, tmp, msglen))
		goto err;
	if (!CBB_flush(cbb))
		goto err;

	/* Check if pubkey from client certificate was used. */
	if (EVP_PKEY_CTX_ctrl(pkey_ctx, -1, -1, EVP_PKEY_CTRL_PEER_KEY, 2,
	    NULL) > 0) {
		/* Set flag "skip certificate verify". */
		s->s3->flags |= TLS1_FLAGS_SKIP_CERT_VERIFY;
	}
	EVP_PKEY_CTX_free(pkey_ctx);
	s->session->master_key_length =
	    tls1_generate_master_secret(s,
		s->session->master_key, premaster_secret, 32);

	ret = 1;

 err:
	explicit_bzero(premaster_secret, sizeof(premaster_secret));
	EVP_PKEY_free(pub_key);

	return (ret);
}

int
ssl3_send_client_key_exchange(SSL *s)
{
	SESS_CERT *sess_cert;
	unsigned long alg_k;
	CBB cbb, kex;

	memset(&cbb, 0, sizeof(cbb));

	if (S3I(s)->hs.state == SSL3_ST_CW_KEY_EXCH_A) {
		alg_k = S3I(s)->hs.new_cipher->algorithm_mkey;

		if ((sess_cert = SSI(s)->sess_cert) == NULL) {
			ssl3_send_alert(s, SSL3_AL_FATAL,
			    SSL_AD_UNEXPECTED_MESSAGE);
			SSLerror(s, ERR_R_INTERNAL_ERROR);
			goto err;
		}

		if (!ssl3_handshake_msg_start_cbb(s, &cbb, &kex,
		    SSL3_MT_CLIENT_KEY_EXCHANGE))
			goto err;

		if (alg_k & SSL_kRSA) {
			if (ssl3_send_client_kex_rsa(s, sess_cert, &kex) != 1)
				goto err;
		} else if (alg_k & SSL_kDHE) {
			if (ssl3_send_client_kex_dhe(s, sess_cert, &kex) != 1)
				goto err;
		} else if (alg_k & SSL_kECDHE) {
			if (ssl3_send_client_kex_ecdhe(s, sess_cert, &kex) != 1)
				goto err;
		} else if (alg_k & SSL_kGOST) {
			if (ssl3_send_client_kex_gost(s, sess_cert, &kex) != 1)
				goto err;
		} else {
			ssl3_send_alert(s, SSL3_AL_FATAL,
			    SSL_AD_HANDSHAKE_FAILURE);
			SSLerror(s, ERR_R_INTERNAL_ERROR);
			goto err;
		}

		if (!ssl3_handshake_msg_finish_cbb(s, &cbb))
			goto err;

		S3I(s)->hs.state = SSL3_ST_CW_KEY_EXCH_B;
	}

	/* SSL3_ST_CW_KEY_EXCH_B */
	return (ssl3_handshake_write(s));

err:
	CBB_cleanup(&cbb);

	return (-1);
}

int
ssl3_send_client_verify(SSL *s)
{
	unsigned char	*p;
	unsigned char	 data[MD5_DIGEST_LENGTH + SHA_DIGEST_LENGTH];
	EVP_PKEY	*pkey;
	EVP_PKEY_CTX	*pctx = NULL;
	EVP_MD_CTX	 mctx;
	unsigned	 u = 0;
	unsigned long	 n;
	int		 j;

	EVP_MD_CTX_init(&mctx);

	if (S3I(s)->hs.state == SSL3_ST_CW_CERT_VRFY_A) {
		p = ssl3_handshake_msg_start(s, SSL3_MT_CERTIFICATE_VERIFY);

		/*
		 * Create context from key and test if sha1 is allowed as
		 * digest.
		 */
		pkey = s->cert->key->privatekey;
		pctx = EVP_PKEY_CTX_new(pkey, NULL);
		EVP_PKEY_sign_init(pctx);

		/* XXX - is this needed? */
		if (EVP_PKEY_CTX_set_signature_md(pctx, EVP_sha1()) <= 0)
			ERR_clear_error();

		if (!SSL_USE_SIGALGS(s)) {
			if (S3I(s)->handshake_buffer) {
				if (!tls1_digest_cached_records(s))
					goto err;
			}
			if (!tls1_handshake_hash_value(s, data, sizeof(data),
			    NULL))
				goto err;
		}

		/*
		 * For TLS v1.2 send signature algorithm and signature
		 * using agreed digest and cached handshake records.
		 */
		if (SSL_USE_SIGALGS(s)) {
			long hdatalen = 0;
			void *hdata;
			const EVP_MD *md = s->cert->key->digest;
			hdatalen = BIO_get_mem_data(S3I(s)->handshake_buffer,
			    &hdata);
			if (hdatalen <= 0 ||
			    !tls12_get_sigandhash(p, pkey, md)) {
				SSLerror(s, ERR_R_INTERNAL_ERROR);
				goto err;
			}
			p += 2;
			if (!EVP_SignInit_ex(&mctx, md, NULL) ||
			    !EVP_SignUpdate(&mctx, hdata, hdatalen) ||
			    !EVP_SignFinal(&mctx, p + 2, &u, pkey)) {
				SSLerror(s, ERR_R_EVP_LIB);
				goto err;
			}
			s2n(u, p);
			n = u + 4;
			if (!tls1_digest_cached_records(s))
				goto err;
		} else if (pkey->type == EVP_PKEY_RSA) {
			if (RSA_sign(NID_md5_sha1, data,
			    MD5_DIGEST_LENGTH + SHA_DIGEST_LENGTH, &(p[2]),
			    &u, pkey->pkey.rsa) <= 0 ) {
				SSLerror(s, ERR_R_RSA_LIB);
				goto err;
			}
			s2n(u, p);
			n = u + 2;
		} else if (pkey->type == EVP_PKEY_EC) {
			if (!ECDSA_sign(pkey->save_type,
			    &(data[MD5_DIGEST_LENGTH]),
			    SHA_DIGEST_LENGTH, &(p[2]),
			    (unsigned int *)&j, pkey->pkey.ec)) {
				SSLerror(s, ERR_R_ECDSA_LIB);
				goto err;
			}
			s2n(j, p);
			n = j + 2;
#ifndef OPENSSL_NO_GOST
		} else if (pkey->type == NID_id_GostR3410_94 ||
			   pkey->type == NID_id_GostR3410_2001) {
			unsigned char signbuf[128];
			long hdatalen = 0;
			void *hdata;
			const EVP_MD *md;
			int nid;
			size_t sigsize;

			hdatalen = BIO_get_mem_data(S3I(s)->handshake_buffer, &hdata);
			if (hdatalen <= 0) {
				SSLerror(s, ERR_R_INTERNAL_ERROR);
				goto err;
			}
			if (!EVP_PKEY_get_default_digest_nid(pkey, &nid) ||
			    !(md = EVP_get_digestbynid(nid))) {
				SSLerror(s, ERR_R_EVP_LIB);
				goto err;
			}
			if (!EVP_DigestInit_ex(&mctx, md, NULL) ||
			    !EVP_DigestUpdate(&mctx, hdata, hdatalen) ||
			    !EVP_DigestFinal(&mctx, signbuf, &u) ||
			    (EVP_PKEY_CTX_set_signature_md(pctx, md) <= 0) ||
			    (EVP_PKEY_CTX_ctrl(pctx, -1, EVP_PKEY_OP_SIGN,
					       EVP_PKEY_CTRL_GOST_SIG_FORMAT,
					       GOST_SIG_FORMAT_RS_LE,
					       NULL) <= 0) ||
			    (EVP_PKEY_sign(pctx, &(p[2]), &sigsize,
					   signbuf, u) <= 0)) {
				SSLerror(s, ERR_R_EVP_LIB);
				goto err;
			}
			if (!tls1_digest_cached_records(s))
				goto err;
			j = sigsize;
			s2n(j, p);
			n = j + 2;
#endif
		} else {
			SSLerror(s, ERR_R_INTERNAL_ERROR);
			goto err;
		}

		S3I(s)->hs.state = SSL3_ST_CW_CERT_VRFY_B;

		ssl3_handshake_msg_finish(s, n);
	}

	EVP_MD_CTX_cleanup(&mctx);
	EVP_PKEY_CTX_free(pctx);

	return (ssl3_handshake_write(s));

err:
	EVP_MD_CTX_cleanup(&mctx);
	EVP_PKEY_CTX_free(pctx);
	return (-1);
}

int
ssl3_send_client_certificate(SSL *s)
{
	EVP_PKEY *pkey = NULL;
	X509 *x509 = NULL;
	CBB cbb, client_cert;
	int i;

	memset(&cbb, 0, sizeof(cbb));

	if (S3I(s)->hs.state == SSL3_ST_CW_CERT_A) {
		if ((s->cert == NULL) || (s->cert->key->x509 == NULL) ||
		    (s->cert->key->privatekey == NULL))
			S3I(s)->hs.state = SSL3_ST_CW_CERT_B;
		else
			S3I(s)->hs.state = SSL3_ST_CW_CERT_C;
	}

	/* We need to get a client cert */
	if (S3I(s)->hs.state == SSL3_ST_CW_CERT_B) {
		/*
		 * If we get an error, we need to
		 * ssl->rwstate=SSL_X509_LOOKUP; return(-1);
		 * We then get retied later
		 */
		i = ssl_do_client_cert_cb(s, &x509, &pkey);
		if (i < 0) {
			s->internal->rwstate = SSL_X509_LOOKUP;
			return (-1);
		}
		s->internal->rwstate = SSL_NOTHING;
		if ((i == 1) && (pkey != NULL) && (x509 != NULL)) {
			S3I(s)->hs.state = SSL3_ST_CW_CERT_B;
			if (!SSL_use_certificate(s, x509) ||
			    !SSL_use_PrivateKey(s, pkey))
				i = 0;
		} else if (i == 1) {
			i = 0;
			SSLerror(s, SSL_R_BAD_DATA_RETURNED_BY_CALLBACK);
		}

		X509_free(x509);
		EVP_PKEY_free(pkey);
		if (i == 0)
			S3I(s)->tmp.cert_req = 2;

		/* Ok, we have a cert */
		S3I(s)->hs.state = SSL3_ST_CW_CERT_C;
	}

	if (S3I(s)->hs.state == SSL3_ST_CW_CERT_C) {
		if (!ssl3_handshake_msg_start_cbb(s, &cbb, &client_cert,
		    SSL3_MT_CERTIFICATE))
			goto err;
		if (!ssl3_output_cert_chain(s, &client_cert,
		    (S3I(s)->tmp.cert_req == 2) ? NULL : s->cert->key->x509))
			goto err;
		if (!ssl3_handshake_msg_finish_cbb(s, &cbb))
			goto err;

		S3I(s)->hs.state = SSL3_ST_CW_CERT_D;
	}

	/* SSL3_ST_CW_CERT_D */
	return (ssl3_handshake_write(s));

 err:
	CBB_cleanup(&cbb);

	return (0);
}

#define has_bits(i,m)	(((i)&(m)) == (m))

int
ssl3_check_cert_and_algorithm(SSL *s)
{
	int		 i, idx;
	long		 alg_k, alg_a;
	EVP_PKEY	*pkey = NULL;
	SESS_CERT	*sc;
	DH		*dh;

	alg_k = S3I(s)->hs.new_cipher->algorithm_mkey;
	alg_a = S3I(s)->hs.new_cipher->algorithm_auth;

	/* We don't have a certificate. */
	if (alg_a & SSL_aNULL)
		return (1);

	sc = SSI(s)->sess_cert;
	if (sc == NULL) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		goto err;
	}
	dh = SSI(s)->sess_cert->peer_dh_tmp;

	/* This is the passed certificate. */

	idx = sc->peer_cert_type;
	if (idx == SSL_PKEY_ECC) {
		if (ssl_check_srvr_ecc_cert_and_alg(
		    sc->peer_pkeys[idx].x509, s) == 0) {
			/* check failed */
			SSLerror(s, SSL_R_BAD_ECC_CERT);
			goto f_err;
		} else {
			return (1);
		}
	}
	pkey = X509_get_pubkey(sc->peer_pkeys[idx].x509);
	i = X509_certificate_type(sc->peer_pkeys[idx].x509, pkey);
	EVP_PKEY_free(pkey);

	/* Check that we have a certificate if we require one. */
	if ((alg_a & SSL_aRSA) && !has_bits(i, EVP_PK_RSA|EVP_PKT_SIGN)) {
		SSLerror(s, SSL_R_MISSING_RSA_SIGNING_CERT);
		goto f_err;
	}
	if ((alg_k & SSL_kRSA) && !has_bits(i, EVP_PK_RSA|EVP_PKT_ENC)) {
		SSLerror(s, SSL_R_MISSING_RSA_ENCRYPTING_CERT);
		goto f_err;
	}
	if ((alg_k & SSL_kDHE) &&
	    !(has_bits(i, EVP_PK_DH|EVP_PKT_EXCH) || (dh != NULL))) {
		SSLerror(s, SSL_R_MISSING_DH_KEY);
		goto f_err;
	}

	return (1);
f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, SSL_AD_HANDSHAKE_FAILURE);
err:
	return (0);
}

/*
 * Check to see if handshake is full or resumed. Usually this is just a
 * case of checking to see if a cache hit has occurred. In the case of
 * session tickets we have to check the next message to be sure.
 */

int
ssl3_check_finished(SSL *s)
{
	int	ok;
	long	n;

	/* If we have no ticket it cannot be a resumed session. */
	if (!s->session->tlsext_tick)
		return (1);
	/* this function is called when we really expect a Certificate
	 * message, so permit appropriate message length */
	n = s->method->internal->ssl_get_message(s, SSL3_ST_CR_CERT_A,
	    SSL3_ST_CR_CERT_B, -1, s->internal->max_cert_list, &ok);
	if (!ok)
		return ((int)n);
	S3I(s)->tmp.reuse_message = 1;
	if ((S3I(s)->tmp.message_type == SSL3_MT_FINISHED) ||
	    (S3I(s)->tmp.message_type == SSL3_MT_NEWSESSION_TICKET))
		return (2);

	return (1);
}

int
ssl_do_client_cert_cb(SSL *s, X509 **px509, EVP_PKEY **ppkey)
{
	int	i = 0;

#ifndef OPENSSL_NO_ENGINE
	if (s->ctx->internal->client_cert_engine) {
		i = ENGINE_load_ssl_client_cert(
		    s->ctx->internal->client_cert_engine, s,
		    SSL_get_client_CA_list(s), px509, ppkey, NULL, NULL, NULL);
		if (i != 0)
			return (i);
	}
#endif
	if (s->ctx->internal->client_cert_cb)
		i = s->ctx->internal->client_cert_cb(s, px509, ppkey);
	return (i);
}
