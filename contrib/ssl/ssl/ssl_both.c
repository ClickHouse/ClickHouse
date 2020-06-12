/* $OpenBSD: ssl_both.c,v 1.10 2017/08/12 02:55:22 jsing Exp $ */
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
 * Copyright (c) 1998-2002 The OpenSSL Project.  All rights reserved.
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
 * ECC cipher suite support in OpenSSL originally developed by
 * SUN MICROSYSTEMS, INC., and contributed to the OpenSSL project.
 */

#include <limits.h>
#include <stdio.h>
#include <string.h>

#include "ssl_locl.h"

#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <openssl/objects.h>
#include <openssl/x509.h>

#include "bytestring.h"

/*
 * Send s->internal->init_buf in records of type 'type' (SSL3_RT_HANDSHAKE or
 * SSL3_RT_CHANGE_CIPHER_SPEC).
 */
int
ssl3_do_write(SSL *s, int type)
{
	int ret;

	ret = ssl3_write_bytes(s, type, &s->internal->init_buf->data[s->internal->init_off],
	    s->internal->init_num);
	if (ret < 0)
		return (-1);

	if (type == SSL3_RT_HANDSHAKE)
		/*
		 * Should not be done for 'Hello Request's, but in that case
		 * we'll ignore the result anyway.
		 */
		tls1_finish_mac(s,
		    (unsigned char *)&s->internal->init_buf->data[s->internal->init_off], ret);

	if (ret == s->internal->init_num) {
		if (s->internal->msg_callback)
			s->internal->msg_callback(1, s->version, type, s->internal->init_buf->data,
			    (size_t)(s->internal->init_off + s->internal->init_num), s,
			    s->internal->msg_callback_arg);
		return (1);
	}

	s->internal->init_off += ret;
	s->internal->init_num -= ret;

	return (0);
}

int
ssl3_send_finished(SSL *s, int a, int b, const char *sender, int slen)
{
	CBB cbb, finished;
	int md_len;

	memset(&cbb, 0, sizeof(cbb));

	if (S3I(s)->hs.state == a) {
		md_len = TLS1_FINISH_MAC_LENGTH;
		OPENSSL_assert(md_len <= EVP_MAX_MD_SIZE);

		if (tls1_final_finish_mac(s, sender, slen,
		    S3I(s)->tmp.finish_md) != md_len)
			return (0);
		S3I(s)->tmp.finish_md_len = md_len;

		/* Copy finished so we can use it for renegotiation checks. */
		if (s->internal->type == SSL_ST_CONNECT) {
			memcpy(S3I(s)->previous_client_finished,
			    S3I(s)->tmp.finish_md, md_len);
			S3I(s)->previous_client_finished_len = md_len;
		} else {
			memcpy(S3I(s)->previous_server_finished,
			    S3I(s)->tmp.finish_md, md_len);
			S3I(s)->previous_server_finished_len = md_len;
		}

		if (!ssl3_handshake_msg_start_cbb(s, &cbb, &finished,
		    SSL3_MT_FINISHED))
                        goto err;
		if (!CBB_add_bytes(&finished, S3I(s)->tmp.finish_md, md_len))
			goto err;
		if (!ssl3_handshake_msg_finish_cbb(s, &cbb))
			goto err;

		S3I(s)->hs.state = b;
	}

	return (ssl3_handshake_write(s));

 err:
	CBB_cleanup(&cbb);

	return (-1);
}

/*
 * ssl3_take_mac calculates the Finished MAC for the handshakes messages seen
 * so far.
 */
static void
ssl3_take_mac(SSL *s)
{
	const char *sender;
	int slen;

	/*
	 * If no new cipher setup return immediately: other functions will
	 * set the appropriate error.
	 */
	if (S3I(s)->hs.new_cipher == NULL)
		return;

	if (S3I(s)->hs.state & SSL_ST_CONNECT) {
		sender = TLS_MD_SERVER_FINISH_CONST;
		slen = TLS_MD_SERVER_FINISH_CONST_SIZE;
	} else {
		sender = TLS_MD_CLIENT_FINISH_CONST;
		slen = TLS_MD_CLIENT_FINISH_CONST_SIZE;
	}

	S3I(s)->tmp.peer_finish_md_len =
	    tls1_final_finish_mac(s, sender, slen,
		S3I(s)->tmp.peer_finish_md);
}

int
ssl3_get_finished(SSL *s, int a, int b)
{
	int al, ok, md_len;
	long n;
	CBS cbs;

	/* should actually be 36+4 :-) */
	n = s->method->internal->ssl_get_message(s, a, b, SSL3_MT_FINISHED, 64, &ok);
	if (!ok)
		return ((int)n);

	/* If this occurs, we have missed a message */
	if (!S3I(s)->change_cipher_spec) {
		al = SSL_AD_UNEXPECTED_MESSAGE;
		SSLerror(s, SSL_R_GOT_A_FIN_BEFORE_A_CCS);
		goto f_err;
	}
	S3I(s)->change_cipher_spec = 0;

	md_len = TLS1_FINISH_MAC_LENGTH;

	if (n < 0) {
		al = SSL_AD_DECODE_ERROR;
		SSLerror(s, SSL_R_BAD_DIGEST_LENGTH);
		goto f_err;
	}

	CBS_init(&cbs, s->internal->init_msg, n);

	if (S3I(s)->tmp.peer_finish_md_len != md_len ||
	    CBS_len(&cbs) != md_len) {
		al = SSL_AD_DECODE_ERROR;
		SSLerror(s, SSL_R_BAD_DIGEST_LENGTH);
		goto f_err;
	}

	if (!CBS_mem_equal(&cbs, S3I(s)->tmp.peer_finish_md, CBS_len(&cbs))) {
		al = SSL_AD_DECRYPT_ERROR;
		SSLerror(s, SSL_R_DIGEST_CHECK_FAILED);
		goto f_err;
	}

	/* Copy finished so we can use it for renegotiation checks. */
	OPENSSL_assert(md_len <= EVP_MAX_MD_SIZE);
	if (s->internal->type == SSL_ST_ACCEPT) {
		memcpy(S3I(s)->previous_client_finished,
		    S3I(s)->tmp.peer_finish_md, md_len);
		S3I(s)->previous_client_finished_len = md_len;
	} else {
		memcpy(S3I(s)->previous_server_finished,
		    S3I(s)->tmp.peer_finish_md, md_len);
		S3I(s)->previous_server_finished_len = md_len;
	}

	return (1);
f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, al);
	return (0);
}

/* for these 2 messages, we need to
 * ssl->enc_read_ctx			re-init
 * ssl->s3->internal->read_sequence		zero
 * ssl->s3->internal->read_mac_secret		re-init
 * ssl->session->read_sym_enc		assign
 * ssl->session->read_hash		assign
 */
int
ssl3_send_change_cipher_spec(SSL *s, int a, int b)
{
	unsigned char *p;

	if (S3I(s)->hs.state == a) {
		p = (unsigned char *)s->internal->init_buf->data;
		*p = SSL3_MT_CCS;
		s->internal->init_num = 1;
		s->internal->init_off = 0;

		S3I(s)->hs.state = b;
	}

	/* SSL3_ST_CW_CHANGE_B */
	return (ssl3_do_write(s, SSL3_RT_CHANGE_CIPHER_SPEC));
}

static int
ssl3_add_cert(CBB *cbb, X509 *x)
{
	unsigned char *data;
	int cert_len;
	int ret = 0;
	CBB cert;

	if ((cert_len = i2d_X509(x, NULL)) < 0)
		goto err;

	if (!CBB_add_u24_length_prefixed(cbb, &cert))
		goto err;
	if (!CBB_add_space(&cert, &data, cert_len))
		goto err;
	if (i2d_X509(x, &data) < 0)
		goto err;
	if (!CBB_flush(cbb))
		goto err;

	ret = 1;

 err:
	return (ret);
}

int
ssl3_output_cert_chain(SSL *s, CBB *cbb, X509 *x)
{
	int no_chain = 0;
	CBB cert_list;
	int ret = 0;
	int i;

	if (!CBB_add_u24_length_prefixed(cbb, &cert_list))
		goto err;

	if ((s->internal->mode & SSL_MODE_NO_AUTO_CHAIN) || s->ctx->extra_certs)
		no_chain = 1;

	/* TLSv1 sends a chain with nothing in it, instead of an alert. */
	if (x != NULL) {
		if (no_chain) {
			if (!ssl3_add_cert(&cert_list, x))
				goto err;
		} else {
			X509_STORE_CTX xs_ctx;

			if (!X509_STORE_CTX_init(&xs_ctx, s->ctx->cert_store,
			    x, NULL)) {
				SSLerror(s, ERR_R_X509_LIB);
				goto err;
			}
			X509_verify_cert(&xs_ctx);

			/* Don't leave errors in the queue. */
			ERR_clear_error();
			for (i = 0; i < sk_X509_num(xs_ctx.chain); i++) {
				x = sk_X509_value(xs_ctx.chain, i);
				if (!ssl3_add_cert(&cert_list, x)) {
					X509_STORE_CTX_cleanup(&xs_ctx);
					goto err;
				}
			}
			X509_STORE_CTX_cleanup(&xs_ctx);
		}
	}

	/* Thawte special :-) */
	for (i = 0; i < sk_X509_num(s->ctx->extra_certs); i++) {
		x = sk_X509_value(s->ctx->extra_certs, i);
		if (!ssl3_add_cert(&cert_list, x))
			goto err;
	}

	if (!CBB_flush(cbb))
		goto err;

	ret = 1;

 err:
	return (ret);
}

/*
 * Obtain handshake message of message type 'mt' (any if mt == -1),
 * maximum acceptable body length 'max'.
 * The first four bytes (msg_type and length) are read in state 'st1',
 * the body is read in state 'stn'.
 */
long
ssl3_get_message(SSL *s, int st1, int stn, int mt, long max, int *ok)
{
	unsigned char *p;
	uint32_t l;
	long n;
	int i, al;
	CBS cbs;
	uint8_t u8;

	if (S3I(s)->tmp.reuse_message) {
		S3I(s)->tmp.reuse_message = 0;
		if ((mt >= 0) && (S3I(s)->tmp.message_type != mt)) {
			al = SSL_AD_UNEXPECTED_MESSAGE;
			SSLerror(s, SSL_R_UNEXPECTED_MESSAGE);
			goto f_err;
		}
		*ok = 1;
		s->internal->init_msg = s->internal->init_buf->data + 4;
		s->internal->init_num = (int)S3I(s)->tmp.message_size;
		return s->internal->init_num;
	}

	p = (unsigned char *)s->internal->init_buf->data;

	/* s->internal->init_num < 4 */
	if (S3I(s)->hs.state == st1) {
		int skip_message;

		do {
			while (s->internal->init_num < 4) {
				i = s->method->internal->ssl_read_bytes(s,
				    SSL3_RT_HANDSHAKE, &p[s->internal->init_num],
				    4 - s->internal->init_num, 0);
				if (i <= 0) {
					s->internal->rwstate = SSL_READING;
					*ok = 0;
					return i;
				}
				s->internal->init_num += i;
			}

			skip_message = 0;
			if (!s->server && p[0] == SSL3_MT_HELLO_REQUEST) {
				/*
				 * The server may always send 'Hello Request'
				 * messages -- we are doing a handshake anyway
				 * now, so ignore them if their format is
				 * correct.  Does not count for 'Finished' MAC.
				 */
				if (p[1] == 0 && p[2] == 0 &&p[3] == 0) {
					s->internal->init_num = 0;
					skip_message = 1;

					if (s->internal->msg_callback)
						s->internal->msg_callback(0, s->version,
						    SSL3_RT_HANDSHAKE, p, 4, s,
						    s->internal->msg_callback_arg);
				}
			}
		} while (skip_message);

		/* s->internal->init_num == 4 */

		if ((mt >= 0) && (*p != mt)) {
			al = SSL_AD_UNEXPECTED_MESSAGE;
			SSLerror(s, SSL_R_UNEXPECTED_MESSAGE);
			goto f_err;
		}

		CBS_init(&cbs, p, 4);
		if (!CBS_get_u8(&cbs, &u8) ||
		    !CBS_get_u24(&cbs, &l)) {
			SSLerror(s, ERR_R_BUF_LIB);
			goto err;
		}
		S3I(s)->tmp.message_type = u8;

		if (l > (unsigned long)max) {
			al = SSL_AD_ILLEGAL_PARAMETER;
			SSLerror(s, SSL_R_EXCESSIVE_MESSAGE_SIZE);
			goto f_err;
		}
		if (l && !BUF_MEM_grow_clean(s->internal->init_buf, l + 4)) {
			SSLerror(s, ERR_R_BUF_LIB);
			goto err;
		}
		S3I(s)->tmp.message_size = l;
		S3I(s)->hs.state = stn;

		s->internal->init_msg = s->internal->init_buf->data + 4;
		s->internal->init_num = 0;
	}

	/* next state (stn) */
	p = s->internal->init_msg;
	n = S3I(s)->tmp.message_size - s->internal->init_num;
	while (n > 0) {
		i = s->method->internal->ssl_read_bytes(s, SSL3_RT_HANDSHAKE,
		    &p[s->internal->init_num], n, 0);
		if (i <= 0) {
			s->internal->rwstate = SSL_READING;
			*ok = 0;
			return i;
		}
		s->internal->init_num += i;
		n -= i;
	}

	/* If receiving Finished, record MAC of prior handshake messages for
	 * Finished verification. */
	if (*s->internal->init_buf->data == SSL3_MT_FINISHED)
		ssl3_take_mac(s);

	/* Feed this message into MAC computation. */
	if (s->internal->mac_packet) {
		tls1_finish_mac(s, (unsigned char *)s->internal->init_buf->data,
		    s->internal->init_num + 4);

		if (s->internal->msg_callback)
			s->internal->msg_callback(0, s->version,
			    SSL3_RT_HANDSHAKE, s->internal->init_buf->data,
			    (size_t)s->internal->init_num + 4, s,
			    s->internal->msg_callback_arg);
	}

	*ok = 1;
	return (s->internal->init_num);

f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, al);
err:
	*ok = 0;
	return (-1);
}

int
ssl_cert_type(X509 *x, EVP_PKEY *pkey)
{
	EVP_PKEY *pk;
	int ret = -1, i;

	if (pkey == NULL)
		pk = X509_get_pubkey(x);
	else
		pk = pkey;
	if (pk == NULL)
		goto err;

	i = pk->type;
	if (i == EVP_PKEY_RSA) {
		ret = SSL_PKEY_RSA_ENC;
	} else if (i == EVP_PKEY_EC) {
		ret = SSL_PKEY_ECC;
	} else if (i == NID_id_GostR3410_2001 ||
	    i == NID_id_GostR3410_2001_cc) {
		ret = SSL_PKEY_GOST01;
	}

err:
	if (!pkey)
		EVP_PKEY_free(pk);
	return (ret);
}

int
ssl_verify_alarm_type(long type)
{
	int al;

	switch (type) {
	case X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT:
	case X509_V_ERR_UNABLE_TO_GET_CRL:
	case X509_V_ERR_UNABLE_TO_GET_CRL_ISSUER:
		al = SSL_AD_UNKNOWN_CA;
		break;
	case X509_V_ERR_UNABLE_TO_DECRYPT_CERT_SIGNATURE:
	case X509_V_ERR_UNABLE_TO_DECRYPT_CRL_SIGNATURE:
	case X509_V_ERR_UNABLE_TO_DECODE_ISSUER_PUBLIC_KEY:
	case X509_V_ERR_ERROR_IN_CERT_NOT_BEFORE_FIELD:
	case X509_V_ERR_ERROR_IN_CERT_NOT_AFTER_FIELD:
	case X509_V_ERR_ERROR_IN_CRL_LAST_UPDATE_FIELD:
	case X509_V_ERR_ERROR_IN_CRL_NEXT_UPDATE_FIELD:
	case X509_V_ERR_CERT_NOT_YET_VALID:
	case X509_V_ERR_CRL_NOT_YET_VALID:
	case X509_V_ERR_CERT_UNTRUSTED:
	case X509_V_ERR_CERT_REJECTED:
		al = SSL_AD_BAD_CERTIFICATE;
		break;
	case X509_V_ERR_CERT_SIGNATURE_FAILURE:
	case X509_V_ERR_CRL_SIGNATURE_FAILURE:
		al = SSL_AD_DECRYPT_ERROR;
		break;
	case X509_V_ERR_CERT_HAS_EXPIRED:
	case X509_V_ERR_CRL_HAS_EXPIRED:
		al = SSL_AD_CERTIFICATE_EXPIRED;
		break;
	case X509_V_ERR_CERT_REVOKED:
		al = SSL_AD_CERTIFICATE_REVOKED;
		break;
	case X509_V_ERR_OUT_OF_MEM:
		al = SSL_AD_INTERNAL_ERROR;
		break;
	case X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT:
	case X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN:
	case X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY:
	case X509_V_ERR_UNABLE_TO_VERIFY_LEAF_SIGNATURE:
	case X509_V_ERR_CERT_CHAIN_TOO_LONG:
	case X509_V_ERR_PATH_LENGTH_EXCEEDED:
	case X509_V_ERR_INVALID_CA:
		al = SSL_AD_UNKNOWN_CA;
		break;
	case X509_V_ERR_APPLICATION_VERIFICATION:
		al = SSL_AD_HANDSHAKE_FAILURE;
		break;
	case X509_V_ERR_INVALID_PURPOSE:
		al = SSL_AD_UNSUPPORTED_CERTIFICATE;
		break;
	default:
		al = SSL_AD_CERTIFICATE_UNKNOWN;
		break;
	}
	return (al);
}

int
ssl3_setup_init_buffer(SSL *s)
{
	BUF_MEM *buf = NULL;

	if (s->internal->init_buf != NULL)
		return (1);

	if ((buf = BUF_MEM_new()) == NULL)
		goto err;
	if (!BUF_MEM_grow(buf, SSL3_RT_MAX_PLAIN_LENGTH))
		goto err;

	s->internal->init_buf = buf;
	return (1);

err:
	BUF_MEM_free(buf);
	return (0);
}

int
ssl3_setup_read_buffer(SSL *s)
{
	unsigned char *p;
	size_t len, align, headerlen;

	if (SSL_IS_DTLS(s))
		headerlen = DTLS1_RT_HEADER_LENGTH;
	else
		headerlen = SSL3_RT_HEADER_LENGTH;

	align = (-SSL3_RT_HEADER_LENGTH) & (SSL3_ALIGN_PAYLOAD - 1);

	if (s->s3->rbuf.buf == NULL) {
		len = SSL3_RT_MAX_PLAIN_LENGTH +
		    SSL3_RT_MAX_ENCRYPTED_OVERHEAD + headerlen + align;
		if ((p = malloc(len)) == NULL)
			goto err;
		s->s3->rbuf.buf = p;
		s->s3->rbuf.len = len;
	}

	s->internal->packet = &(s->s3->rbuf.buf[0]);
	return 1;

err:
	SSLerror(s, ERR_R_MALLOC_FAILURE);
	return 0;
}

int
ssl3_setup_write_buffer(SSL *s)
{
	unsigned char *p;
	size_t len, align, headerlen;

	if (SSL_IS_DTLS(s))
		headerlen = DTLS1_RT_HEADER_LENGTH + 1;
	else
		headerlen = SSL3_RT_HEADER_LENGTH;

	align = (-SSL3_RT_HEADER_LENGTH) & (SSL3_ALIGN_PAYLOAD - 1);

	if (s->s3->wbuf.buf == NULL) {
		len = s->max_send_fragment +
		    SSL3_RT_SEND_MAX_ENCRYPTED_OVERHEAD + headerlen + align;
		if (!(s->internal->options & SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS))
			len += headerlen + align +
			    SSL3_RT_SEND_MAX_ENCRYPTED_OVERHEAD;

		if ((p = malloc(len)) == NULL)
			goto err;
		s->s3->wbuf.buf = p;
		s->s3->wbuf.len = len;
	}

	return 1;

err:
	SSLerror(s, ERR_R_MALLOC_FAILURE);
	return 0;
}

int
ssl3_setup_buffers(SSL *s)
{
	if (!ssl3_setup_read_buffer(s))
		return 0;
	if (!ssl3_setup_write_buffer(s))
		return 0;
	return 1;
}

int
ssl3_release_write_buffer(SSL *s)
{
	free(s->s3->wbuf.buf);
	s->s3->wbuf.buf = NULL;
	return 1;
}

int
ssl3_release_read_buffer(SSL *s)
{
	free(s->s3->rbuf.buf);
	s->s3->rbuf.buf = NULL;
	return 1;
}
