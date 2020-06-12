/* $OpenBSD: ssl_lib.c,v 1.170 2017/08/30 16:24:21 jsing Exp $ */
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
 * ECC cipher suite support in OpenSSL originally developed by
 * SUN MICROSYSTEMS, INC., and contributed to the OpenSSL project.
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
#include <openssl/dh.h>
#include <openssl/lhash.h>
#include <openssl/objects.h>
#include <openssl/ocsp.h>
#include <openssl/x509v3.h>

#ifndef OPENSSL_NO_ENGINE
#include <openssl/engine.h>
#endif

#include "bytestring.h"

const char *SSL_version_str = OPENSSL_VERSION_TEXT;

int
SSL_clear(SSL *s)
{
	if (s->method == NULL) {
		SSLerror(s, SSL_R_NO_METHOD_SPECIFIED);
		return (0);
	}

	if (ssl_clear_bad_session(s)) {
		SSL_SESSION_free(s->session);
		s->session = NULL;
	}

	s->error = 0;
	s->internal->hit = 0;
	s->internal->shutdown = 0;

	if (s->internal->renegotiate) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		return (0);
	}

	s->internal->type = 0;

	s->version = s->method->internal->version;
	s->client_version = s->version;
	s->internal->rwstate = SSL_NOTHING;
	s->internal->rstate = SSL_ST_READ_HEADER;

	BUF_MEM_free(s->internal->init_buf);
	s->internal->init_buf = NULL;

	ssl_clear_cipher_ctx(s);
	ssl_clear_hash_ctx(&s->read_hash);
	ssl_clear_hash_ctx(&s->internal->write_hash);

	s->internal->first_packet = 0;

	/*
	 * Check to see if we were changed into a different method, if
	 * so, revert back if we are not doing session-id reuse.
	 */
	if (!s->internal->in_handshake && (s->session == NULL) &&
	    (s->method != s->ctx->method)) {
		s->method->internal->ssl_free(s);
		s->method = s->ctx->method;
		if (!s->method->internal->ssl_new(s))
			return (0);
	} else
		s->method->internal->ssl_clear(s);

	S3I(s)->hs.state = SSL_ST_BEFORE|((s->server) ? SSL_ST_ACCEPT : SSL_ST_CONNECT);

	return (1);
}

/* Used to change an SSL_CTXs default SSL method type */
int
SSL_CTX_set_ssl_version(SSL_CTX *ctx, const SSL_METHOD *meth)
{
	STACK_OF(SSL_CIPHER)	*sk;

	ctx->method = meth;

	sk = ssl_create_cipher_list(ctx->method, &(ctx->cipher_list),
	    &(ctx->internal->cipher_list_by_id), SSL_DEFAULT_CIPHER_LIST);
	if ((sk == NULL) || (sk_SSL_CIPHER_num(sk) <= 0)) {
		SSLerrorx(SSL_R_SSL_LIBRARY_HAS_NO_CIPHERS);
		return (0);
	}
	return (1);
}

SSL *
SSL_new(SSL_CTX *ctx)
{
	SSL	*s;

	if (ctx == NULL) {
		SSLerrorx(SSL_R_NULL_SSL_CTX);
		return (NULL);
	}
	if (ctx->method == NULL) {
		SSLerrorx(SSL_R_SSL_CTX_HAS_NO_DEFAULT_SSL_VERSION);
		return (NULL);
	}

	if ((s = calloc(1, sizeof(*s))) == NULL) {
		SSLerrorx(ERR_R_MALLOC_FAILURE);
		return (NULL);
	}
	if ((s->internal = calloc(1, sizeof(*s->internal))) == NULL) {
		free(s);
		SSLerrorx(ERR_R_MALLOC_FAILURE);
		return (NULL);
	}

	s->internal->min_version = ctx->internal->min_version;
	s->internal->max_version = ctx->internal->max_version;

	s->internal->options = ctx->internal->options;
	s->internal->mode = ctx->internal->mode;
	s->internal->max_cert_list = ctx->internal->max_cert_list;

	if (ctx->internal->cert != NULL) {
		/*
		 * Earlier library versions used to copy the pointer to
		 * the CERT, not its contents; only when setting new
		 * parameters for the per-SSL copy, ssl_cert_new would be
		 * called (and the direct reference to the per-SSL_CTX
		 * settings would be lost, but those still were indirectly
		 * accessed for various purposes, and for that reason they
		 * used to be known as s->ctx->default_cert).
		 * Now we don't look at the SSL_CTX's CERT after having
		 * duplicated it once.
		*/
		s->cert = ssl_cert_dup(ctx->internal->cert);
		if (s->cert == NULL)
			goto err;
	} else
		s->cert=NULL; /* Cannot really happen (see SSL_CTX_new) */

	s->internal->read_ahead = ctx->internal->read_ahead;
	s->internal->msg_callback = ctx->internal->msg_callback;
	s->internal->msg_callback_arg = ctx->internal->msg_callback_arg;
	s->verify_mode = ctx->verify_mode;
	s->sid_ctx_length = ctx->sid_ctx_length;
	OPENSSL_assert(s->sid_ctx_length <= sizeof s->sid_ctx);
	memcpy(&s->sid_ctx, &ctx->sid_ctx, sizeof(s->sid_ctx));
	s->internal->verify_callback = ctx->internal->default_verify_callback;
	s->internal->generate_session_id = ctx->internal->generate_session_id;

	s->param = X509_VERIFY_PARAM_new();
	if (!s->param)
		goto err;
	X509_VERIFY_PARAM_inherit(s->param, ctx->param);
	s->internal->quiet_shutdown = ctx->internal->quiet_shutdown;
	s->max_send_fragment = ctx->internal->max_send_fragment;

	CRYPTO_add(&ctx->references, 1, CRYPTO_LOCK_SSL_CTX);
	s->ctx = ctx;
	s->internal->tlsext_debug_cb = 0;
	s->internal->tlsext_debug_arg = NULL;
	s->internal->tlsext_ticket_expected = 0;
	s->tlsext_status_type = -1;
	s->internal->tlsext_status_expected = 0;
	s->internal->tlsext_ocsp_ids = NULL;
	s->internal->tlsext_ocsp_exts = NULL;
	s->internal->tlsext_ocsp_resp = NULL;
	s->internal->tlsext_ocsp_resplen = -1;
	CRYPTO_add(&ctx->references, 1, CRYPTO_LOCK_SSL_CTX);
	s->initial_ctx = ctx;

	if (ctx->internal->tlsext_ecpointformatlist != NULL) {
		s->internal->tlsext_ecpointformatlist =
		    calloc(ctx->internal->tlsext_ecpointformatlist_length,
			sizeof(ctx->internal->tlsext_ecpointformatlist[0]));
		if (s->internal->tlsext_ecpointformatlist == NULL)
			goto err;
		memcpy(s->internal->tlsext_ecpointformatlist,
		    ctx->internal->tlsext_ecpointformatlist,
		    ctx->internal->tlsext_ecpointformatlist_length *
		    sizeof(ctx->internal->tlsext_ecpointformatlist[0]));
		s->internal->tlsext_ecpointformatlist_length =
		    ctx->internal->tlsext_ecpointformatlist_length;
	}
	if (ctx->internal->tlsext_supportedgroups != NULL) {
		s->internal->tlsext_supportedgroups =
		    calloc(ctx->internal->tlsext_supportedgroups_length,
			sizeof(ctx->internal->tlsext_supportedgroups));
		if (s->internal->tlsext_supportedgroups == NULL)
			goto err;
		memcpy(s->internal->tlsext_supportedgroups,
		    ctx->internal->tlsext_supportedgroups,
		    ctx->internal->tlsext_supportedgroups_length *
		    sizeof(ctx->internal->tlsext_supportedgroups[0]));
		s->internal->tlsext_supportedgroups_length =
		    ctx->internal->tlsext_supportedgroups_length;
	}

	if (s->ctx->internal->alpn_client_proto_list != NULL) {
		s->internal->alpn_client_proto_list =
		    malloc(s->ctx->internal->alpn_client_proto_list_len);
		if (s->internal->alpn_client_proto_list == NULL)
			goto err;
		memcpy(s->internal->alpn_client_proto_list,
		    s->ctx->internal->alpn_client_proto_list,
		    s->ctx->internal->alpn_client_proto_list_len);
		s->internal->alpn_client_proto_list_len =
		    s->ctx->internal->alpn_client_proto_list_len;
	}

	s->verify_result = X509_V_OK;

	s->method = ctx->method;

	if (!s->method->internal->ssl_new(s))
		goto err;

	s->references = 1;
	s->server = (ctx->method->internal->ssl_accept == ssl_undefined_function) ? 0 : 1;

	SSL_clear(s);

	CRYPTO_new_ex_data(CRYPTO_EX_INDEX_SSL, s, &s->internal->ex_data);

	return (s);

 err:
	SSL_free(s);
	SSLerrorx(ERR_R_MALLOC_FAILURE);
	return (NULL);
}

int
SSL_CTX_set_session_id_context(SSL_CTX *ctx, const unsigned char *sid_ctx,
    unsigned int sid_ctx_len)
{
	if (sid_ctx_len > sizeof ctx->sid_ctx) {
		SSLerrorx(SSL_R_SSL_SESSION_ID_CONTEXT_TOO_LONG);
		return (0);
	}
	ctx->sid_ctx_length = sid_ctx_len;
	memcpy(ctx->sid_ctx, sid_ctx, sid_ctx_len);

	return (1);
}

int
SSL_set_session_id_context(SSL *ssl, const unsigned char *sid_ctx,
    unsigned int sid_ctx_len)
{
	if (sid_ctx_len > SSL_MAX_SID_CTX_LENGTH) {
		SSLerror(ssl, SSL_R_SSL_SESSION_ID_CONTEXT_TOO_LONG);
		return (0);
	}
	ssl->sid_ctx_length = sid_ctx_len;
	memcpy(ssl->sid_ctx, sid_ctx, sid_ctx_len);

	return (1);
}

int
SSL_CTX_set_generate_session_id(SSL_CTX *ctx, GEN_SESSION_CB cb)
{
	CRYPTO_w_lock(CRYPTO_LOCK_SSL_CTX);
	ctx->internal->generate_session_id = cb;
	CRYPTO_w_unlock(CRYPTO_LOCK_SSL_CTX);
	return (1);
}

int
SSL_set_generate_session_id(SSL *ssl, GEN_SESSION_CB cb)
{
	CRYPTO_w_lock(CRYPTO_LOCK_SSL);
	ssl->internal->generate_session_id = cb;
	CRYPTO_w_unlock(CRYPTO_LOCK_SSL);
	return (1);
}

int
SSL_has_matching_session_id(const SSL *ssl, const unsigned char *id,
    unsigned int id_len)
{
	/*
	 * A quick examination of SSL_SESSION_hash and SSL_SESSION_cmp
	 * shows how we can "construct" a session to give us the desired
	 * check - ie. to find if there's a session in the hash table
	 * that would conflict with any new session built out of this
	 * id/id_len and the ssl_version in use by this SSL.
	 */
	SSL_SESSION r, *p;

	if (id_len > sizeof r.session_id)
		return (0);

	r.ssl_version = ssl->version;
	r.session_id_length = id_len;
	memcpy(r.session_id, id, id_len);

	CRYPTO_r_lock(CRYPTO_LOCK_SSL_CTX);
	p = lh_SSL_SESSION_retrieve(ssl->ctx->internal->sessions, &r);
	CRYPTO_r_unlock(CRYPTO_LOCK_SSL_CTX);
	return (p != NULL);
}

int
SSL_CTX_set_purpose(SSL_CTX *s, int purpose)
{
	return (X509_VERIFY_PARAM_set_purpose(s->param, purpose));
}

int
SSL_set_purpose(SSL *s, int purpose)
{
	return (X509_VERIFY_PARAM_set_purpose(s->param, purpose));
}

int
SSL_CTX_set_trust(SSL_CTX *s, int trust)
{
	return (X509_VERIFY_PARAM_set_trust(s->param, trust));
}

int
SSL_set_trust(SSL *s, int trust)
{
	return (X509_VERIFY_PARAM_set_trust(s->param, trust));
}

int
SSL_CTX_set1_param(SSL_CTX *ctx, X509_VERIFY_PARAM *vpm)
{
	return (X509_VERIFY_PARAM_set1(ctx->param, vpm));
}

int
SSL_set1_param(SSL *ssl, X509_VERIFY_PARAM *vpm)
{
	return (X509_VERIFY_PARAM_set1(ssl->param, vpm));
}

void
SSL_free(SSL *s)
{
	int	i;

	if (s == NULL)
		return;

	i = CRYPTO_add(&s->references, -1, CRYPTO_LOCK_SSL);
	if (i > 0)
		return;

	X509_VERIFY_PARAM_free(s->param);

	CRYPTO_free_ex_data(CRYPTO_EX_INDEX_SSL, s, &s->internal->ex_data);

	if (s->bbio != NULL) {
		/* If the buffering BIO is in place, pop it off */
		if (s->bbio == s->wbio) {
			s->wbio = BIO_pop(s->wbio);
		}
		BIO_free(s->bbio);
		s->bbio = NULL;
	}

	if (s->rbio != s->wbio)
		BIO_free_all(s->rbio);
	BIO_free_all(s->wbio);

	BUF_MEM_free(s->internal->init_buf);

	/* add extra stuff */
	sk_SSL_CIPHER_free(s->cipher_list);
	sk_SSL_CIPHER_free(s->internal->cipher_list_by_id);

	/* Make the next call work :-) */
	if (s->session != NULL) {
		ssl_clear_bad_session(s);
		SSL_SESSION_free(s->session);
	}

	ssl_clear_cipher_ctx(s);
	ssl_clear_hash_ctx(&s->read_hash);
	ssl_clear_hash_ctx(&s->internal->write_hash);

	ssl_cert_free(s->cert);

	free(s->tlsext_hostname);
	SSL_CTX_free(s->initial_ctx);

	free(s->internal->tlsext_ecpointformatlist);
	free(s->internal->tlsext_supportedgroups);

	sk_X509_EXTENSION_pop_free(s->internal->tlsext_ocsp_exts,
	    X509_EXTENSION_free);
	sk_OCSP_RESPID_pop_free(s->internal->tlsext_ocsp_ids, OCSP_RESPID_free);
	free(s->internal->tlsext_ocsp_resp);

	sk_X509_NAME_pop_free(s->internal->client_CA, X509_NAME_free);

	if (s->method != NULL)
		s->method->internal->ssl_free(s);

	SSL_CTX_free(s->ctx);

	free(s->internal->alpn_client_proto_list);

#ifndef OPENSSL_NO_SRTP
	sk_SRTP_PROTECTION_PROFILE_free(s->internal->srtp_profiles);
#endif

	free(s->internal);
	free(s);
}

void
SSL_set_bio(SSL *s, BIO *rbio, BIO *wbio)
{
	/* If the output buffering BIO is still in place, remove it */
	if (s->bbio != NULL) {
		if (s->wbio == s->bbio) {
			s->wbio = s->wbio->next_bio;
			s->bbio->next_bio = NULL;
		}
	}

	if (s->rbio != rbio && s->rbio != s->wbio)
		BIO_free_all(s->rbio);
	if (s->wbio != wbio)
		BIO_free_all(s->wbio);
	s->rbio = rbio;
	s->wbio = wbio;
}

BIO *
SSL_get_rbio(const SSL *s)
{
	return (s->rbio);
}

BIO *
SSL_get_wbio(const SSL *s)
{
	return (s->wbio);
}

int
SSL_get_fd(const SSL *s)
{
	return (SSL_get_rfd(s));
}

int
SSL_get_rfd(const SSL *s)
{
	int	 ret = -1;
	BIO	*b, *r;

	b = SSL_get_rbio(s);
	r = BIO_find_type(b, BIO_TYPE_DESCRIPTOR);
	if (r != NULL)
		BIO_get_fd(r, &ret);
	return (ret);
}

int
SSL_get_wfd(const SSL *s)
{
	int	 ret = -1;
	BIO	*b, *r;

	b = SSL_get_wbio(s);
	r = BIO_find_type(b, BIO_TYPE_DESCRIPTOR);
	if (r != NULL)
		BIO_get_fd(r, &ret);
	return (ret);
}

int
SSL_set_fd(SSL *s, int fd)
{
	int	 ret = 0;
	BIO	*bio = NULL;

	bio = BIO_new(BIO_s_socket());

	if (bio == NULL) {
		SSLerror(s, ERR_R_BUF_LIB);
		goto err;
	}
	BIO_set_fd(bio, fd, BIO_NOCLOSE);
	SSL_set_bio(s, bio, bio);
	ret = 1;
err:
	return (ret);
}

int
SSL_set_wfd(SSL *s, int fd)
{
	int	 ret = 0;
	BIO	*bio = NULL;

	if ((s->rbio == NULL) || (BIO_method_type(s->rbio) != BIO_TYPE_SOCKET)
	    || ((int)BIO_get_fd(s->rbio, NULL) != fd)) {
		bio = BIO_new(BIO_s_socket());

		if (bio == NULL) {
			SSLerror(s, ERR_R_BUF_LIB);
			goto err;
		}
		BIO_set_fd(bio, fd, BIO_NOCLOSE);
		SSL_set_bio(s, SSL_get_rbio(s), bio);
	} else
		SSL_set_bio(s, SSL_get_rbio(s), SSL_get_rbio(s));
	ret = 1;
err:
	return (ret);
}

int
SSL_set_rfd(SSL *s, int fd)
{
	int	 ret = 0;
	BIO	*bio = NULL;

	if ((s->wbio == NULL) || (BIO_method_type(s->wbio) != BIO_TYPE_SOCKET)
	    || ((int)BIO_get_fd(s->wbio, NULL) != fd)) {
		bio = BIO_new(BIO_s_socket());

		if (bio == NULL) {
			SSLerror(s, ERR_R_BUF_LIB);
			goto err;
		}
		BIO_set_fd(bio, fd, BIO_NOCLOSE);
		SSL_set_bio(s, bio, SSL_get_wbio(s));
	} else
		SSL_set_bio(s, SSL_get_wbio(s), SSL_get_wbio(s));
	ret = 1;
err:
	return (ret);
}


/* return length of latest Finished message we sent, copy to 'buf' */
size_t
SSL_get_finished(const SSL *s, void *buf, size_t count)
{
	size_t	ret = 0;

	if (s->s3 != NULL) {
		ret = S3I(s)->tmp.finish_md_len;
		if (count > ret)
			count = ret;
		memcpy(buf, S3I(s)->tmp.finish_md, count);
	}
	return (ret);
}

/* return length of latest Finished message we expected, copy to 'buf' */
size_t
SSL_get_peer_finished(const SSL *s, void *buf, size_t count)
{
	size_t	ret = 0;

	if (s->s3 != NULL) {
		ret = S3I(s)->tmp.peer_finish_md_len;
		if (count > ret)
			count = ret;
		memcpy(buf, S3I(s)->tmp.peer_finish_md, count);
	}
	return (ret);
}


int
SSL_get_verify_mode(const SSL *s)
{
	return (s->verify_mode);
}

int
SSL_get_verify_depth(const SSL *s)
{
	return (X509_VERIFY_PARAM_get_depth(s->param));
}

int
(*SSL_get_verify_callback(const SSL *s))(int, X509_STORE_CTX *)
{
	return (s->internal->verify_callback);
}

int
SSL_CTX_get_verify_mode(const SSL_CTX *ctx)
{
	return (ctx->verify_mode);
}

int
SSL_CTX_get_verify_depth(const SSL_CTX *ctx)
{
	return (X509_VERIFY_PARAM_get_depth(ctx->param));
}

int (*SSL_CTX_get_verify_callback(const SSL_CTX *ctx))(int, X509_STORE_CTX *)
{
	return (ctx->internal->default_verify_callback);
}

void
SSL_set_verify(SSL *s, int mode,
    int (*callback)(int ok, X509_STORE_CTX *ctx))
{
	s->verify_mode = mode;
	if (callback != NULL)
		s->internal->verify_callback = callback;
}

void
SSL_set_verify_depth(SSL *s, int depth)
{
	X509_VERIFY_PARAM_set_depth(s->param, depth);
}

void
SSL_set_read_ahead(SSL *s, int yes)
{
	s->internal->read_ahead = yes;
}

int
SSL_get_read_ahead(const SSL *s)
{
	return (s->internal->read_ahead);
}

int
SSL_pending(const SSL *s)
{
	/*
	 * SSL_pending cannot work properly if read-ahead is enabled
	 * (SSL_[CTX_]ctrl(..., SSL_CTRL_SET_READ_AHEAD, 1, NULL)),
	 * and it is impossible to fix since SSL_pending cannot report
	 * errors that may be observed while scanning the new data.
	 * (Note that SSL_pending() is often used as a boolean value,
	 * so we'd better not return -1.)
	 */
	return (s->method->internal->ssl_pending(s));
}

X509 *
SSL_get_peer_certificate(const SSL *s)
{
	X509	*r;

	if ((s == NULL) || (s->session == NULL))
		r = NULL;
	else
		r = s->session->peer;

	if (r == NULL)
		return (r);

	CRYPTO_add(&r->references, 1, CRYPTO_LOCK_X509);

	return (r);
}

STACK_OF(X509) *
SSL_get_peer_cert_chain(const SSL *s)
{
	STACK_OF(X509)	*r;

	if ((s == NULL) || (s->session == NULL) ||
	    (SSI(s)->sess_cert == NULL))
		r = NULL;
	else
		r = SSI(s)->sess_cert->cert_chain;

	/*
	 * If we are a client, cert_chain includes the peer's own
	 * certificate;
	 * if we are a server, it does not.
	 */
	return (r);
}

/*
 * Now in theory, since the calling process own 't' it should be safe to
 * modify.  We need to be able to read f without being hassled
 */
void
SSL_copy_session_id(SSL *t, const SSL *f)
{
	CERT	*tmp;

	/* Do we need to to SSL locking? */
	SSL_set_session(t, SSL_get_session(f));

	/*
	 * What if we are setup as SSLv2 but want to talk SSLv3 or
	 * vice-versa.
	 */
	if (t->method != f->method) {
		t->method->internal->ssl_free(t);	/* cleanup current */
		t->method = f->method;	/* change method */
		t->method->internal->ssl_new(t);	/* setup new */
	}

	tmp = t->cert;
	if (f->cert != NULL) {
		CRYPTO_add(&f->cert->references, 1, CRYPTO_LOCK_SSL_CERT);
		t->cert = f->cert;
	} else
		t->cert = NULL;
	ssl_cert_free(tmp);
	SSL_set_session_id_context(t, f->sid_ctx, f->sid_ctx_length);
}

/* Fix this so it checks all the valid key/cert options */
int
SSL_CTX_check_private_key(const SSL_CTX *ctx)
{
	if ((ctx == NULL) || (ctx->internal->cert == NULL) ||
	    (ctx->internal->cert->key->x509 == NULL)) {
		SSLerrorx(SSL_R_NO_CERTIFICATE_ASSIGNED);
		return (0);
	}
	if (ctx->internal->cert->key->privatekey == NULL) {
		SSLerrorx(SSL_R_NO_PRIVATE_KEY_ASSIGNED);
		return (0);
	}
	return (X509_check_private_key(ctx->internal->cert->key->x509,
	    ctx->internal->cert->key->privatekey));
}

/* Fix this function so that it takes an optional type parameter */
int
SSL_check_private_key(const SSL *ssl)
{
	if (ssl == NULL) {
		SSLerrorx(ERR_R_PASSED_NULL_PARAMETER);
		return (0);
	}
	if (ssl->cert == NULL) {
		SSLerror(ssl, SSL_R_NO_CERTIFICATE_ASSIGNED);
		return (0);
	}
	if (ssl->cert->key->x509 == NULL) {
		SSLerror(ssl, SSL_R_NO_CERTIFICATE_ASSIGNED);
		return (0);
	}
	if (ssl->cert->key->privatekey == NULL) {
		SSLerror(ssl, SSL_R_NO_PRIVATE_KEY_ASSIGNED);
		return (0);
	}
	return (X509_check_private_key(ssl->cert->key->x509,
	    ssl->cert->key->privatekey));
}

int
SSL_accept(SSL *s)
{
	if (s->internal->handshake_func == NULL)
		SSL_set_accept_state(s); /* Not properly initialized yet */

	return (s->method->internal->ssl_accept(s));
}

int
SSL_connect(SSL *s)
{
	if (s->internal->handshake_func == NULL)
		SSL_set_connect_state(s); /* Not properly initialized yet */

	return (s->method->internal->ssl_connect(s));
}

long
SSL_get_default_timeout(const SSL *s)
{
	return (s->method->internal->get_timeout());
}

int
SSL_read(SSL *s, void *buf, int num)
{
	if (s->internal->handshake_func == NULL) {
		SSLerror(s, SSL_R_UNINITIALIZED);
		return (-1);
	}

	if (s->internal->shutdown & SSL_RECEIVED_SHUTDOWN) {
		s->internal->rwstate = SSL_NOTHING;
		return (0);
	}
	return (s->method->internal->ssl_read(s, buf, num));
}

int
SSL_peek(SSL *s, void *buf, int num)
{
	if (s->internal->handshake_func == NULL) {
		SSLerror(s, SSL_R_UNINITIALIZED);
		return (-1);
	}

	if (s->internal->shutdown & SSL_RECEIVED_SHUTDOWN) {
		return (0);
	}
	return (s->method->internal->ssl_peek(s, buf, num));
}

int
SSL_write(SSL *s, const void *buf, int num)
{
	if (s->internal->handshake_func == NULL) {
		SSLerror(s, SSL_R_UNINITIALIZED);
		return (-1);
	}

	if (s->internal->shutdown & SSL_SENT_SHUTDOWN) {
		s->internal->rwstate = SSL_NOTHING;
		SSLerror(s, SSL_R_PROTOCOL_IS_SHUTDOWN);
		return (-1);
	}
	return (s->method->internal->ssl_write(s, buf, num));
}

int
SSL_shutdown(SSL *s)
{
	/*
	 * Note that this function behaves differently from what one might
	 * expect.  Return values are 0 for no success (yet),
	 * 1 for success; but calling it once is usually not enough,
	 * even if blocking I/O is used (see ssl3_shutdown).
	 */

	if (s->internal->handshake_func == NULL) {
		SSLerror(s, SSL_R_UNINITIALIZED);
		return (-1);
	}

	if ((s != NULL) && !SSL_in_init(s))
		return (s->method->internal->ssl_shutdown(s));
	else
		return (1);
}

int
SSL_renegotiate(SSL *s)
{
	if (s->internal->renegotiate == 0)
		s->internal->renegotiate = 1;

	s->internal->new_session = 1;

	return (s->method->internal->ssl_renegotiate(s));
}

int
SSL_renegotiate_abbreviated(SSL *s)
{
	if (s->internal->renegotiate == 0)
		s->internal->renegotiate = 1;

	s->internal->new_session = 0;

	return (s->method->internal->ssl_renegotiate(s));
}

int
SSL_renegotiate_pending(SSL *s)
{
	/*
	 * Becomes true when negotiation is requested;
	 * false again once a handshake has finished.
	 */
	return (s->internal->renegotiate != 0);
}

long
SSL_ctrl(SSL *s, int cmd, long larg, void *parg)
{
	long	l;

	switch (cmd) {
	case SSL_CTRL_GET_READ_AHEAD:
		return (s->internal->read_ahead);
	case SSL_CTRL_SET_READ_AHEAD:
		l = s->internal->read_ahead;
		s->internal->read_ahead = larg;
		return (l);

	case SSL_CTRL_SET_MSG_CALLBACK_ARG:
		s->internal->msg_callback_arg = parg;
		return (1);

	case SSL_CTRL_OPTIONS:
		return (s->internal->options|=larg);
	case SSL_CTRL_CLEAR_OPTIONS:
		return (s->internal->options&=~larg);
	case SSL_CTRL_MODE:
		return (s->internal->mode|=larg);
	case SSL_CTRL_CLEAR_MODE:
		return (s->internal->mode &=~larg);
	case SSL_CTRL_GET_MAX_CERT_LIST:
		return (s->internal->max_cert_list);
	case SSL_CTRL_SET_MAX_CERT_LIST:
		l = s->internal->max_cert_list;
		s->internal->max_cert_list = larg;
		return (l);
	case SSL_CTRL_SET_MTU:
#ifndef OPENSSL_NO_DTLS1
		if (larg < (long)dtls1_min_mtu())
			return (0);
#endif
		if (SSL_IS_DTLS(s)) {
			D1I(s)->mtu = larg;
			return (larg);
		}
		return (0);
	case SSL_CTRL_SET_MAX_SEND_FRAGMENT:
		if (larg < 512 || larg > SSL3_RT_MAX_PLAIN_LENGTH)
			return (0);
		s->max_send_fragment = larg;
		return (1);
	case SSL_CTRL_GET_RI_SUPPORT:
		if (s->s3)
			return (S3I(s)->send_connection_binding);
		else return (0);
	default:
		if (SSL_IS_DTLS(s))
			return dtls1_ctrl(s, cmd, larg, parg);
		return ssl3_ctrl(s, cmd, larg, parg);
	}
}

long
SSL_callback_ctrl(SSL *s, int cmd, void (*fp)(void))
{
	switch (cmd) {
	case SSL_CTRL_SET_MSG_CALLBACK:
		s->internal->msg_callback = (void (*)(int write_p, int version,
		    int content_type, const void *buf, size_t len,
		    SSL *ssl, void *arg))(fp);
		return (1);

	default:
		return (ssl3_callback_ctrl(s, cmd, fp));
	}
}

struct lhash_st_SSL_SESSION *
SSL_CTX_sessions(SSL_CTX *ctx)
{
	return (ctx->internal->sessions);
}

long
SSL_CTX_ctrl(SSL_CTX *ctx, int cmd, long larg, void *parg)
{
	long	l;

	switch (cmd) {
	case SSL_CTRL_GET_READ_AHEAD:
		return (ctx->internal->read_ahead);
	case SSL_CTRL_SET_READ_AHEAD:
		l = ctx->internal->read_ahead;
		ctx->internal->read_ahead = larg;
		return (l);

	case SSL_CTRL_SET_MSG_CALLBACK_ARG:
		ctx->internal->msg_callback_arg = parg;
		return (1);

	case SSL_CTRL_GET_MAX_CERT_LIST:
		return (ctx->internal->max_cert_list);
	case SSL_CTRL_SET_MAX_CERT_LIST:
		l = ctx->internal->max_cert_list;
		ctx->internal->max_cert_list = larg;
		return (l);

	case SSL_CTRL_SET_SESS_CACHE_SIZE:
		l = ctx->internal->session_cache_size;
		ctx->internal->session_cache_size = larg;
		return (l);
	case SSL_CTRL_GET_SESS_CACHE_SIZE:
		return (ctx->internal->session_cache_size);
	case SSL_CTRL_SET_SESS_CACHE_MODE:
		l = ctx->internal->session_cache_mode;
		ctx->internal->session_cache_mode = larg;
		return (l);
	case SSL_CTRL_GET_SESS_CACHE_MODE:
		return (ctx->internal->session_cache_mode);

	case SSL_CTRL_SESS_NUMBER:
		return (lh_SSL_SESSION_num_items(ctx->internal->sessions));
	case SSL_CTRL_SESS_CONNECT:
		return (ctx->internal->stats.sess_connect);
	case SSL_CTRL_SESS_CONNECT_GOOD:
		return (ctx->internal->stats.sess_connect_good);
	case SSL_CTRL_SESS_CONNECT_RENEGOTIATE:
		return (ctx->internal->stats.sess_connect_renegotiate);
	case SSL_CTRL_SESS_ACCEPT:
		return (ctx->internal->stats.sess_accept);
	case SSL_CTRL_SESS_ACCEPT_GOOD:
		return (ctx->internal->stats.sess_accept_good);
	case SSL_CTRL_SESS_ACCEPT_RENEGOTIATE:
		return (ctx->internal->stats.sess_accept_renegotiate);
	case SSL_CTRL_SESS_HIT:
		return (ctx->internal->stats.sess_hit);
	case SSL_CTRL_SESS_CB_HIT:
		return (ctx->internal->stats.sess_cb_hit);
	case SSL_CTRL_SESS_MISSES:
		return (ctx->internal->stats.sess_miss);
	case SSL_CTRL_SESS_TIMEOUTS:
		return (ctx->internal->stats.sess_timeout);
	case SSL_CTRL_SESS_CACHE_FULL:
		return (ctx->internal->stats.sess_cache_full);
	case SSL_CTRL_OPTIONS:
		return (ctx->internal->options|=larg);
	case SSL_CTRL_CLEAR_OPTIONS:
		return (ctx->internal->options&=~larg);
	case SSL_CTRL_MODE:
		return (ctx->internal->mode|=larg);
	case SSL_CTRL_CLEAR_MODE:
		return (ctx->internal->mode&=~larg);
	case SSL_CTRL_SET_MAX_SEND_FRAGMENT:
		if (larg < 512 || larg > SSL3_RT_MAX_PLAIN_LENGTH)
			return (0);
		ctx->internal->max_send_fragment = larg;
		return (1);
	default:
		return (ssl3_ctx_ctrl(ctx, cmd, larg, parg));
	}
}

long
SSL_CTX_callback_ctrl(SSL_CTX *ctx, int cmd, void (*fp)(void))
{
	switch (cmd) {
	case SSL_CTRL_SET_MSG_CALLBACK:
		ctx->internal->msg_callback = (void (*)(int write_p, int version,
		    int content_type, const void *buf, size_t len, SSL *ssl,
		    void *arg))(fp);
		return (1);

	default:
		return (ssl3_ctx_callback_ctrl(ctx, cmd, fp));
	}
}

int
ssl_cipher_id_cmp(const SSL_CIPHER *a, const SSL_CIPHER *b)
{
	long	l;

	l = a->id - b->id;
	if (l == 0L)
		return (0);
	else
		return ((l > 0) ? 1:-1);
}

int
ssl_cipher_ptr_id_cmp(const SSL_CIPHER * const *ap,
    const SSL_CIPHER * const *bp)
{
	long	l;

	l = (*ap)->id - (*bp)->id;
	if (l == 0L)
		return (0);
	else
		return ((l > 0) ? 1:-1);
}

/*
 * Return a STACK of the ciphers available for the SSL and in order of
 * preference.
 */
STACK_OF(SSL_CIPHER) *
SSL_get_ciphers(const SSL *s)
{
	if (s != NULL) {
		if (s->cipher_list != NULL) {
			return (s->cipher_list);
		} else if ((s->ctx != NULL) && (s->ctx->cipher_list != NULL)) {
			return (s->ctx->cipher_list);
		}
	}
	return (NULL);
}

/*
 * Return a STACK of the ciphers available for the SSL and in order of
 * algorithm id.
 */
STACK_OF(SSL_CIPHER) *
ssl_get_ciphers_by_id(SSL *s)
{
	if (s != NULL) {
		if (s->internal->cipher_list_by_id != NULL) {
			return (s->internal->cipher_list_by_id);
		} else if ((s->ctx != NULL) &&
		    (s->ctx->internal->cipher_list_by_id != NULL)) {
			return (s->ctx->internal->cipher_list_by_id);
		}
	}
	return (NULL);
}

/* See if we have any ECC cipher suites. */
int
ssl_has_ecc_ciphers(SSL *s)
{
	STACK_OF(SSL_CIPHER) *ciphers;
	unsigned long alg_k, alg_a;
	SSL_CIPHER *cipher;
	int i;

	if (s->version == DTLS1_VERSION)
		return 0;
	if ((ciphers = SSL_get_ciphers(s)) == NULL)
		return 0;

	for (i = 0; i < sk_SSL_CIPHER_num(ciphers); i++) {
		cipher = sk_SSL_CIPHER_value(ciphers, i);

		alg_k = cipher->algorithm_mkey;
		alg_a = cipher->algorithm_auth;

		if ((alg_k & SSL_kECDHE) || (alg_a & SSL_aECDSA))
			return 1;
	}

	return 0;
}

/* The old interface to get the same thing as SSL_get_ciphers(). */
const char *
SSL_get_cipher_list(const SSL *s, int n)
{
	SSL_CIPHER		*c;
	STACK_OF(SSL_CIPHER)	*sk;

	if (s == NULL)
		return (NULL);
	sk = SSL_get_ciphers(s);
	if ((sk == NULL) || (sk_SSL_CIPHER_num(sk) <= n))
		return (NULL);
	c = sk_SSL_CIPHER_value(sk, n);
	if (c == NULL)
		return (NULL);
	return (c->name);
}

/* Specify the ciphers to be used by default by the SSL_CTX. */
int
SSL_CTX_set_cipher_list(SSL_CTX *ctx, const char *str)
{
	STACK_OF(SSL_CIPHER)	*sk;

	sk = ssl_create_cipher_list(ctx->method, &ctx->cipher_list,
	    &ctx->internal->cipher_list_by_id, str);
	/*
	 * ssl_create_cipher_list may return an empty stack if it
	 * was unable to find a cipher matching the given rule string
	 * (for example if the rule string specifies a cipher which
	 * has been disabled). This is not an error as far as
	 * ssl_create_cipher_list is concerned, and hence
	 * ctx->cipher_list and ctx->internal->cipher_list_by_id has been
	 * updated.
	 */
	if (sk == NULL)
		return (0);
	else if (sk_SSL_CIPHER_num(sk) == 0) {
		SSLerrorx(SSL_R_NO_CIPHER_MATCH);
		return (0);
	}
	return (1);
}

/* Specify the ciphers to be used by the SSL. */
int
SSL_set_cipher_list(SSL *s, const char *str)
{
	STACK_OF(SSL_CIPHER)	*sk;

	sk = ssl_create_cipher_list(s->ctx->method, &s->cipher_list,
	&s->internal->cipher_list_by_id, str);
	/* see comment in SSL_CTX_set_cipher_list */
	if (sk == NULL)
		return (0);
	else if (sk_SSL_CIPHER_num(sk) == 0) {
		SSLerror(s, SSL_R_NO_CIPHER_MATCH);
		return (0);
	}
	return (1);
}

/* works well for SSLv2, not so good for SSLv3 */
char *
SSL_get_shared_ciphers(const SSL *s, char *buf, int len)
{
	char			*end;
	STACK_OF(SSL_CIPHER)	*sk;
	SSL_CIPHER		*c;
	size_t			 curlen = 0;
	int			 i;

	if (s->session == NULL || s->session->ciphers == NULL || len < 2)
		return (NULL);

	sk = s->session->ciphers;
	if (sk_SSL_CIPHER_num(sk) == 0)
		return (NULL);

	buf[0] = '\0';
	for (i = 0; i < sk_SSL_CIPHER_num(sk); i++) {
		c = sk_SSL_CIPHER_value(sk, i);
		end = buf + curlen;
		if (strlcat(buf, c->name, len) >= len ||
		    (curlen = strlcat(buf, ":", len)) >= len) {
			/* remove truncated cipher from list */
			*end = '\0';
			break;
		}
	}
	/* remove trailing colon */
	if ((end = strrchr(buf, ':')) != NULL)
		*end = '\0';
	return (buf);
}

int
ssl_cipher_list_to_bytes(SSL *s, STACK_OF(SSL_CIPHER) *sk, unsigned char *p,
    size_t maxlen, size_t *outlen)
{
	SSL_CIPHER *cipher;
	int ciphers = 0;
	CBB cbb;
	int i;

	*outlen = 0;

	if (sk == NULL)
		return (0);

	if (!CBB_init_fixed(&cbb, p, maxlen))
		goto err;

	for (i = 0; i < sk_SSL_CIPHER_num(sk); i++) {
		cipher = sk_SSL_CIPHER_value(sk, i);

		/* Skip TLS v1.2 only ciphersuites if lower than v1.2 */
		if ((cipher->algorithm_ssl & SSL_TLSV1_2) &&
		    (TLS1_get_client_version(s) < TLS1_2_VERSION))
			continue;

		if (!CBB_add_u16(&cbb, ssl3_cipher_get_value(cipher)))
			goto err;

		ciphers++;
	}

	/* Add SCSV if there are other ciphers and we're not renegotiating. */
	if (ciphers > 0 && !s->internal->renegotiate) {
		if (!CBB_add_u16(&cbb, SSL3_CK_SCSV & SSL3_CK_VALUE_MASK))
			goto err;
	}

	if (!CBB_finish(&cbb, NULL, outlen))
		goto err;

	return 1;

 err:
	CBB_cleanup(&cbb);

	return 0;
}

STACK_OF(SSL_CIPHER) *
ssl_bytes_to_cipher_list(SSL *s, const unsigned char *p, int num)
{
	CBS			 cbs;
	const SSL_CIPHER	*c;
	STACK_OF(SSL_CIPHER)	*sk = NULL;
	unsigned long		 cipher_id;
	uint16_t		 cipher_value, max_version;

	if (s->s3)
		S3I(s)->send_connection_binding = 0;

	/*
	 * RFC 5246 section 7.4.1.2 defines the interval as [2,2^16-2].
	 */
	if (num < 2 || num > 0x10000 - 2) {
		SSLerror(s, SSL_R_ERROR_IN_RECEIVED_CIPHER_LIST);
		return (NULL);
	}

	if ((sk = sk_SSL_CIPHER_new_null()) == NULL) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		goto err;
	}

	CBS_init(&cbs, p, num);
	while (CBS_len(&cbs) > 0) {
		if (!CBS_get_u16(&cbs, &cipher_value)) {
			SSLerror(s, SSL_R_ERROR_IN_RECEIVED_CIPHER_LIST);
			goto err;
		}

		cipher_id = SSL3_CK_ID | cipher_value;

		if (s->s3 != NULL && cipher_id == SSL3_CK_SCSV) {
			/*
			 * TLS_EMPTY_RENEGOTIATION_INFO_SCSV is fatal if
			 * renegotiating.
			 */
			if (s->internal->renegotiate) {
				SSLerror(s, SSL_R_SCSV_RECEIVED_WHEN_RENEGOTIATING);
				ssl3_send_alert(s, SSL3_AL_FATAL,
				    SSL_AD_HANDSHAKE_FAILURE);

				goto err;
			}
			S3I(s)->send_connection_binding = 1;
			continue;
		}

		if (cipher_id == SSL3_CK_FALLBACK_SCSV) {
			/*
			 * TLS_FALLBACK_SCSV indicates that the client
			 * previously tried a higher protocol version.
			 * Fail if the current version is an unexpected
			 * downgrade.
			 */
			max_version = ssl_max_server_version(s);
			if (max_version == 0 || s->version < max_version) {
				SSLerror(s, SSL_R_INAPPROPRIATE_FALLBACK);
				if (s->s3 != NULL)
					ssl3_send_alert(s, SSL3_AL_FATAL,
					    SSL_AD_INAPPROPRIATE_FALLBACK);
				goto err;
			}
			continue;
		}

		if ((c = ssl3_get_cipher_by_value(cipher_value)) != NULL) {
			if (!sk_SSL_CIPHER_push(sk, c)) {
				SSLerror(s, ERR_R_MALLOC_FAILURE);
				goto err;
			}
		}
	}

	return (sk);

err:
	sk_SSL_CIPHER_free(sk);

	return (NULL);
}


/*
 * Return a servername extension value if provided in Client Hello, or NULL.
 * So far, only host_name types are defined (RFC 3546).
 */
const char *
SSL_get_servername(const SSL *s, const int type)
{
	if (type != TLSEXT_NAMETYPE_host_name)
		return (NULL);

	return (s->session && !s->tlsext_hostname ?
	    s->session->tlsext_hostname :
	    s->tlsext_hostname);
}

int
SSL_get_servername_type(const SSL *s)
{
	if (s->session &&
	    (!s->tlsext_hostname ?
	    s->session->tlsext_hostname : s->tlsext_hostname))
		return (TLSEXT_NAMETYPE_host_name);
	return (-1);
}

/*
 * SSL_select_next_proto implements standard protocol selection. It is
 * expected that this function is called from the callback set by
 * SSL_CTX_set_alpn_select_cb.
 *
 * The protocol data is assumed to be a vector of 8-bit, length prefixed byte
 * strings. The length byte itself is not included in the length. A byte
 * string of length 0 is invalid. No byte string may be truncated.
 *
 * It returns either:
 * OPENSSL_NPN_NEGOTIATED if a common protocol was found, or
 * OPENSSL_NPN_NO_OVERLAP if the fallback case was reached.
 */
int
SSL_select_next_proto(unsigned char **out, unsigned char *outlen,
    const unsigned char *server, unsigned int server_len,
    const unsigned char *client, unsigned int client_len)
{
	unsigned int		 i, j;
	const unsigned char	*result;
	int			 status = OPENSSL_NPN_UNSUPPORTED;

	/*
	 * For each protocol in server preference order,
	 * see if we support it.
	 */
	for (i = 0; i < server_len; ) {
		for (j = 0; j < client_len; ) {
			if (server[i] == client[j] &&
			    memcmp(&server[i + 1],
			    &client[j + 1], server[i]) == 0) {
				/* We found a match */
				result = &server[i];
				status = OPENSSL_NPN_NEGOTIATED;
				goto found;
			}
			j += client[j];
			j++;
		}
		i += server[i];
		i++;
	}

	/* There's no overlap between our protocols and the server's list. */
	result = client;
	status = OPENSSL_NPN_NO_OVERLAP;

found:
	*out = (unsigned char *) result + 1;
	*outlen = result[0];
	return (status);
}

/* SSL_get0_next_proto_negotiated is deprecated. */
void
SSL_get0_next_proto_negotiated(const SSL *s, const unsigned char **data,
    unsigned *len)
{
	*data = NULL;
	*len = 0;
}

/* SSL_CTX_set_next_protos_advertised_cb is deprecated. */
void
SSL_CTX_set_next_protos_advertised_cb(SSL_CTX *ctx, int (*cb) (SSL *ssl,
    const unsigned char **out, unsigned int *outlen, void *arg), void *arg)
{
}

/* SSL_CTX_set_next_proto_select_cb is deprecated. */
void
SSL_CTX_set_next_proto_select_cb(SSL_CTX *ctx, int (*cb) (SSL *s,
    unsigned char **out, unsigned char *outlen, const unsigned char *in,
    unsigned int inlen, void *arg), void *arg)
{
}

/*
 * SSL_CTX_set_alpn_protos sets the ALPN protocol list to the specified
 * protocols, which must be in wire-format (i.e. a series of non-empty,
 * 8-bit length-prefixed strings). Returns 0 on success.
 */
int
SSL_CTX_set_alpn_protos(SSL_CTX *ctx, const unsigned char *protos,
    unsigned int protos_len)
{
	int failed = 1;

	if (protos == NULL || protos_len == 0)
		goto err;

	free(ctx->internal->alpn_client_proto_list);
	ctx->internal->alpn_client_proto_list = NULL;
	ctx->internal->alpn_client_proto_list_len = 0;

	if ((ctx->internal->alpn_client_proto_list = malloc(protos_len))
	    == NULL)
		goto err;
	ctx->internal->alpn_client_proto_list_len = protos_len;

	memcpy(ctx->internal->alpn_client_proto_list, protos, protos_len);

	failed = 0;

 err:
	/* NOTE: Return values are the reverse of what you expect. */
	return (failed);
}

/*
 * SSL_set_alpn_protos sets the ALPN protocol list to the specified
 * protocols, which must be in wire-format (i.e. a series of non-empty,
 * 8-bit length-prefixed strings). Returns 0 on success.
 */
int
SSL_set_alpn_protos(SSL *ssl, const unsigned char *protos,
    unsigned int protos_len)
{
	int failed = 1;

	if (protos == NULL || protos_len == 0)
		goto err;

	free(ssl->internal->alpn_client_proto_list);
	ssl->internal->alpn_client_proto_list = NULL;
	ssl->internal->alpn_client_proto_list_len = 0;

	if ((ssl->internal->alpn_client_proto_list = malloc(protos_len))
	    == NULL)
		goto err;
	ssl->internal->alpn_client_proto_list_len = protos_len;

	memcpy(ssl->internal->alpn_client_proto_list, protos, protos_len);

	failed = 0;

 err:
	/* NOTE: Return values are the reverse of what you expect. */
	return (failed);
}

/*
 * SSL_CTX_set_alpn_select_cb sets a callback function that is called during
 * ClientHello processing in order to select an ALPN protocol from the
 * client's list of offered protocols.
 */
void
SSL_CTX_set_alpn_select_cb(SSL_CTX* ctx,
    int (*cb) (SSL *ssl, const unsigned char **out, unsigned char *outlen,
    const unsigned char *in, unsigned int inlen, void *arg), void *arg)
{
	ctx->internal->alpn_select_cb = cb;
	ctx->internal->alpn_select_cb_arg = arg;
}

/*
 * SSL_get0_alpn_selected gets the selected ALPN protocol (if any). On return
 * it sets data to point to len bytes of protocol name (not including the
 * leading length-prefix byte). If the server didn't respond with* a negotiated
 * protocol then len will be zero.
 */
void
SSL_get0_alpn_selected(const SSL *ssl, const unsigned char **data,
    unsigned *len)
{
	*data = NULL;
	*len = 0;

	if (ssl->s3 != NULL) {
		*data = ssl->s3->internal->alpn_selected;
		*len = ssl->s3->internal->alpn_selected_len;
	}
}

int
SSL_export_keying_material(SSL *s, unsigned char *out, size_t olen,
    const char *label, size_t llen, const unsigned char *p, size_t plen,
    int use_context)
{
	return (tls1_export_keying_material(s, out, olen,
	    label, llen, p, plen, use_context));
}

static unsigned long
ssl_session_hash(const SSL_SESSION *a)
{
	unsigned long	l;

	l = (unsigned long)
	    ((unsigned int) a->session_id[0]     )|
	    ((unsigned int) a->session_id[1]<< 8L)|
	    ((unsigned long)a->session_id[2]<<16L)|
	    ((unsigned long)a->session_id[3]<<24L);
	return (l);
}

/*
 * NB: If this function (or indeed the hash function which uses a sort of
 * coarser function than this one) is changed, ensure
 * SSL_CTX_has_matching_session_id() is checked accordingly. It relies on being
 * able to construct an SSL_SESSION that will collide with any existing session
 * with a matching session ID.
 */
static int
ssl_session_cmp(const SSL_SESSION *a, const SSL_SESSION *b)
{
	if (a->ssl_version != b->ssl_version)
		return (1);
	if (a->session_id_length != b->session_id_length)
		return (1);
	if (timingsafe_memcmp(a->session_id, b->session_id, a->session_id_length) != 0)
		return (1);
	return (0);
}

/*
 * These wrapper functions should remain rather than redeclaring
 * SSL_SESSION_hash and SSL_SESSION_cmp for void* types and casting each
 * variable. The reason is that the functions aren't static, they're exposed via
 * ssl.h.
 */
static unsigned long
ssl_session_LHASH_HASH(const void *arg)
{
	const SSL_SESSION *a = arg;

	return ssl_session_hash(a);
}

static int
ssl_session_LHASH_COMP(const void *arg1, const void *arg2)
{
	const SSL_SESSION *a = arg1;
	const SSL_SESSION *b = arg2;

	return ssl_session_cmp(a, b);
}

SSL_CTX *
SSL_CTX_new(const SSL_METHOD *meth)
{
	SSL_CTX	*ret;

	if (meth == NULL) {
		SSLerrorx(SSL_R_NULL_SSL_METHOD_PASSED);
		return (NULL);
	}

	if ((ret = calloc(1, sizeof(*ret))) == NULL) {
		SSLerrorx(ERR_R_MALLOC_FAILURE);
		return (NULL);
	}
	if ((ret->internal = calloc(1, sizeof(*ret->internal))) == NULL) {
		free(ret);
		SSLerrorx(ERR_R_MALLOC_FAILURE);
		return (NULL);
	}

	if (SSL_get_ex_data_X509_STORE_CTX_idx() < 0) {
		SSLerrorx(SSL_R_X509_VERIFICATION_SETUP_PROBLEMS);
		goto err;
	}

	ret->method = meth;
	ret->internal->min_version = meth->internal->min_version;
	ret->internal->max_version = meth->internal->max_version;

	ret->cert_store = NULL;
	ret->internal->session_cache_mode = SSL_SESS_CACHE_SERVER;
	ret->internal->session_cache_size = SSL_SESSION_CACHE_MAX_SIZE_DEFAULT;
	ret->internal->session_cache_head = NULL;
	ret->internal->session_cache_tail = NULL;

	/* We take the system default */
	ret->session_timeout = meth->internal->get_timeout();

	ret->internal->new_session_cb = 0;
	ret->internal->remove_session_cb = 0;
	ret->internal->get_session_cb = 0;
	ret->internal->generate_session_id = 0;

	memset((char *)&ret->internal->stats, 0, sizeof(ret->internal->stats));

	ret->references = 1;
	ret->internal->quiet_shutdown = 0;

	ret->internal->info_callback = NULL;

	ret->internal->app_verify_callback = 0;
	ret->internal->app_verify_arg = NULL;

	ret->internal->max_cert_list = SSL_MAX_CERT_LIST_DEFAULT;
	ret->internal->read_ahead = 0;
	ret->internal->msg_callback = 0;
	ret->internal->msg_callback_arg = NULL;
	ret->verify_mode = SSL_VERIFY_NONE;
	ret->sid_ctx_length = 0;
	ret->internal->default_verify_callback = NULL;
	if ((ret->internal->cert = ssl_cert_new()) == NULL)
		goto err;

	ret->default_passwd_callback = 0;
	ret->default_passwd_callback_userdata = NULL;
	ret->internal->client_cert_cb = 0;
	ret->internal->app_gen_cookie_cb = 0;
	ret->internal->app_verify_cookie_cb = 0;

	ret->internal->sessions = lh_SSL_SESSION_new();
	if (ret->internal->sessions == NULL)
		goto err;
	ret->cert_store = X509_STORE_new();
	if (ret->cert_store == NULL)
		goto err;

	ssl_create_cipher_list(ret->method, &ret->cipher_list,
	    &ret->internal->cipher_list_by_id, SSL_DEFAULT_CIPHER_LIST);
	if (ret->cipher_list == NULL ||
	    sk_SSL_CIPHER_num(ret->cipher_list) <= 0) {
		SSLerrorx(SSL_R_LIBRARY_HAS_NO_CIPHERS);
		goto err2;
	}

	ret->param = X509_VERIFY_PARAM_new();
	if (!ret->param)
		goto err;

	if ((ret->internal->client_CA = sk_X509_NAME_new_null()) == NULL)
		goto err;

	CRYPTO_new_ex_data(CRYPTO_EX_INDEX_SSL_CTX, ret, &ret->internal->ex_data);

	ret->extra_certs = NULL;

	ret->internal->max_send_fragment = SSL3_RT_MAX_PLAIN_LENGTH;

	ret->internal->tlsext_servername_callback = 0;
	ret->internal->tlsext_servername_arg = NULL;

	/* Setup RFC4507 ticket keys */
	arc4random_buf(ret->internal->tlsext_tick_key_name, 16);
	arc4random_buf(ret->internal->tlsext_tick_hmac_key, 16);
	arc4random_buf(ret->internal->tlsext_tick_aes_key, 16);

	ret->internal->tlsext_status_cb = 0;
	ret->internal->tlsext_status_arg = NULL;

#ifndef OPENSSL_NO_ENGINE
	ret->internal->client_cert_engine = NULL;
#ifdef OPENSSL_SSL_CLIENT_ENGINE_AUTO
#define eng_strx(x)	#x
#define eng_str(x)	eng_strx(x)
	/* Use specific client engine automatically... ignore errors */
	{
		ENGINE *eng;
		eng = ENGINE_by_id(eng_str(OPENSSL_SSL_CLIENT_ENGINE_AUTO));
		if (!eng) {
			ERR_clear_error();
			ENGINE_load_builtin_engines();
			eng = ENGINE_by_id(eng_str(
			    OPENSSL_SSL_CLIENT_ENGINE_AUTO));
		}
		if (!eng || !SSL_CTX_set_client_cert_engine(ret, eng))
			ERR_clear_error();
	}
#endif
#endif
	/*
	 * Default is to connect to non-RI servers. When RI is more widely
	 * deployed might change this.
	 */
	ret->internal->options |= SSL_OP_LEGACY_SERVER_CONNECT;

	return (ret);
err:
	SSLerrorx(ERR_R_MALLOC_FAILURE);
err2:
	SSL_CTX_free(ret);
	return (NULL);
}

void
SSL_CTX_free(SSL_CTX *ctx)
{
	int	i;

	if (ctx == NULL)
		return;

	i = CRYPTO_add(&ctx->references, -1, CRYPTO_LOCK_SSL_CTX);
	if (i > 0)
		return;

	X509_VERIFY_PARAM_free(ctx->param);

	/*
	 * Free internal session cache. However: the remove_cb() may reference
	 * the ex_data of SSL_CTX, thus the ex_data store can only be removed
	 * after the sessions were flushed.
	 * As the ex_data handling routines might also touch the session cache,
	 * the most secure solution seems to be: empty (flush) the cache, then
	 * free ex_data, then finally free the cache.
	 * (See ticket [openssl.org #212].)
	 */
	if (ctx->internal->sessions != NULL)
		SSL_CTX_flush_sessions(ctx, 0);

	CRYPTO_free_ex_data(CRYPTO_EX_INDEX_SSL_CTX, ctx, &ctx->internal->ex_data);

	lh_SSL_SESSION_free(ctx->internal->sessions);

	X509_STORE_free(ctx->cert_store);
	sk_SSL_CIPHER_free(ctx->cipher_list);
	sk_SSL_CIPHER_free(ctx->internal->cipher_list_by_id);
	ssl_cert_free(ctx->internal->cert);
	sk_X509_NAME_pop_free(ctx->internal->client_CA, X509_NAME_free);
	sk_X509_pop_free(ctx->extra_certs, X509_free);

#ifndef OPENSSL_NO_SRTP
	if (ctx->internal->srtp_profiles)
		sk_SRTP_PROTECTION_PROFILE_free(ctx->internal->srtp_profiles);
#endif

#ifndef OPENSSL_NO_ENGINE
	if (ctx->internal->client_cert_engine)
		ENGINE_finish(ctx->internal->client_cert_engine);
#endif

	free(ctx->internal->tlsext_ecpointformatlist);
	free(ctx->internal->tlsext_supportedgroups);

	free(ctx->internal->alpn_client_proto_list);

	free(ctx->internal);
	free(ctx);
}

void
SSL_CTX_set_default_passwd_cb(SSL_CTX *ctx, pem_password_cb *cb)
{
	ctx->default_passwd_callback = cb;
}

void
SSL_CTX_set_default_passwd_cb_userdata(SSL_CTX *ctx, void *u)
{
	ctx->default_passwd_callback_userdata = u;
}

void
SSL_CTX_set_cert_verify_callback(SSL_CTX *ctx, int (*cb)(X509_STORE_CTX *,
    void *), void *arg)
{
	ctx->internal->app_verify_callback = cb;
	ctx->internal->app_verify_arg = arg;
}

void
SSL_CTX_set_verify(SSL_CTX *ctx, int mode, int (*cb)(int, X509_STORE_CTX *))
{
	ctx->verify_mode = mode;
	ctx->internal->default_verify_callback = cb;
}

void
SSL_CTX_set_verify_depth(SSL_CTX *ctx, int depth)
{
	X509_VERIFY_PARAM_set_depth(ctx->param, depth);
}

void
ssl_set_cert_masks(CERT *c, const SSL_CIPHER *cipher)
{
	int		 rsa_enc, rsa_sign, dh_tmp;
	int		 have_ecc_cert;
	unsigned long	 mask_k, mask_a;
	X509		*x = NULL;
	CERT_PKEY	*cpk;

	if (c == NULL)
		return;

	dh_tmp = (c->dh_tmp != NULL || c->dh_tmp_cb != NULL ||
	    c->dh_tmp_auto != 0);

	cpk = &(c->pkeys[SSL_PKEY_RSA_ENC]);
	rsa_enc = (cpk->x509 != NULL && cpk->privatekey != NULL);
	cpk = &(c->pkeys[SSL_PKEY_RSA_SIGN]);
	rsa_sign = (cpk->x509 != NULL && cpk->privatekey != NULL);
	cpk = &(c->pkeys[SSL_PKEY_ECC]);
	have_ecc_cert = (cpk->x509 != NULL && cpk->privatekey != NULL);

	mask_k = 0;
	mask_a = 0;

	cpk = &(c->pkeys[SSL_PKEY_GOST01]);
	if (cpk->x509 != NULL && cpk->privatekey !=NULL) {
		mask_k |= SSL_kGOST;
		mask_a |= SSL_aGOST01;
	}

	if (rsa_enc)
		mask_k |= SSL_kRSA;

	if (dh_tmp)
		mask_k |= SSL_kDHE;

	if (rsa_enc || rsa_sign)
		mask_a |= SSL_aRSA;

	mask_a |= SSL_aNULL;

	/*
	 * An ECC certificate may be usable for ECDH and/or
	 * ECDSA cipher suites depending on the key usage extension.
	 */
	if (have_ecc_cert) {
		x = (c->pkeys[SSL_PKEY_ECC]).x509;

		/* This call populates extension flags (ex_flags). */
		X509_check_purpose(x, -1, 0);

		/* Key usage, if present, must allow signing. */
		if ((x->ex_flags & EXFLAG_KUSAGE) == 0 ||
		    (x->ex_kusage & X509v3_KU_DIGITAL_SIGNATURE))
			mask_a |= SSL_aECDSA;
	}

	mask_k |= SSL_kECDHE;

	c->mask_k = mask_k;
	c->mask_a = mask_a;
	c->valid = 1;
}

/* See if this handshake is using an ECC cipher suite. */
int
ssl_using_ecc_cipher(SSL *s)
{
	unsigned long alg_a, alg_k;

	alg_a = S3I(s)->hs.new_cipher->algorithm_auth;
	alg_k = S3I(s)->hs.new_cipher->algorithm_mkey;

	return SSI(s)->tlsext_ecpointformatlist != NULL &&
	    SSI(s)->tlsext_ecpointformatlist_length > 0 &&
	    ((alg_k & SSL_kECDHE) || (alg_a & SSL_aECDSA));
}

int
ssl_check_srvr_ecc_cert_and_alg(X509 *x, SSL *s)
{
	const SSL_CIPHER	*cs = S3I(s)->hs.new_cipher;
	unsigned long		 alg_a;

	alg_a = cs->algorithm_auth;

	if (alg_a & SSL_aECDSA) {
		/* This call populates extension flags (ex_flags). */
		X509_check_purpose(x, -1, 0);

		/* Key usage, if present, must allow signing. */
		if ((x->ex_flags & EXFLAG_KUSAGE) &&
		    ((x->ex_kusage & X509v3_KU_DIGITAL_SIGNATURE) == 0)) {
			SSLerror(s, SSL_R_ECC_CERT_NOT_FOR_SIGNING);
			return (0);
		}
	}

	return (1);
}

CERT_PKEY *
ssl_get_server_send_pkey(const SSL *s)
{
	unsigned long	 alg_a;
	CERT		*c;
	int		 i;

	c = s->cert;
	ssl_set_cert_masks(c, S3I(s)->hs.new_cipher);

	alg_a = S3I(s)->hs.new_cipher->algorithm_auth;

	if (alg_a & SSL_aECDSA) {
		i = SSL_PKEY_ECC;
	} else if (alg_a & SSL_aRSA) {
		if (c->pkeys[SSL_PKEY_RSA_ENC].x509 == NULL)
			i = SSL_PKEY_RSA_SIGN;
		else
			i = SSL_PKEY_RSA_ENC;
	} else if (alg_a & SSL_aGOST01) {
		i = SSL_PKEY_GOST01;
	} else { /* if (alg_a & SSL_aNULL) */
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		return (NULL);
	}

	return (c->pkeys + i);
}

X509 *
ssl_get_server_send_cert(const SSL *s)
{
	CERT_PKEY	*cpk;

	cpk = ssl_get_server_send_pkey(s);
	if (!cpk)
		return (NULL);
	return (cpk->x509);
}

EVP_PKEY *
ssl_get_sign_pkey(SSL *s, const SSL_CIPHER *cipher, const EVP_MD **pmd)
{
	unsigned long	 alg_a;
	CERT		*c;
	int		 idx = -1;

	alg_a = cipher->algorithm_auth;
	c = s->cert;

	if (alg_a & SSL_aRSA) {
		if (c->pkeys[SSL_PKEY_RSA_SIGN].privatekey != NULL)
			idx = SSL_PKEY_RSA_SIGN;
		else if (c->pkeys[SSL_PKEY_RSA_ENC].privatekey != NULL)
			idx = SSL_PKEY_RSA_ENC;
	} else if ((alg_a & SSL_aECDSA) &&
	    (c->pkeys[SSL_PKEY_ECC].privatekey != NULL))
		idx = SSL_PKEY_ECC;
	if (idx == -1) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		return (NULL);
	}
	if (pmd)
		*pmd = c->pkeys[idx].digest;
	return (c->pkeys[idx].privatekey);
}

DH *
ssl_get_auto_dh(SSL *s)
{
	CERT_PKEY *cpk;
	int keylen;
	DH *dhp;

	if (s->cert->dh_tmp_auto == 2) {
		keylen = 1024;
	} else if (S3I(s)->hs.new_cipher->algorithm_auth & SSL_aNULL) {
		keylen = 1024;
		if (S3I(s)->hs.new_cipher->strength_bits == 256)
			keylen = 3072;
	} else {
		if ((cpk = ssl_get_server_send_pkey(s)) == NULL)
			return (NULL);
		if (cpk->privatekey == NULL || cpk->privatekey->pkey.dh == NULL)
			return (NULL);
		keylen = EVP_PKEY_bits(cpk->privatekey);
	}

	if ((dhp = DH_new()) == NULL)
		return (NULL);

	dhp->g = BN_new();
	if (dhp->g != NULL)
		BN_set_word(dhp->g, 2);

	if (keylen >= 8192)
		dhp->p = get_rfc3526_prime_8192(NULL);
	else if (keylen >= 4096)
		dhp->p = get_rfc3526_prime_4096(NULL);
	else if (keylen >= 3072)
		dhp->p = get_rfc3526_prime_3072(NULL);
	else if (keylen >= 2048)
		dhp->p = get_rfc3526_prime_2048(NULL);
	else if (keylen >= 1536)
		dhp->p = get_rfc3526_prime_1536(NULL);
	else
		dhp->p = get_rfc2409_prime_1024(NULL);

	if (dhp->p == NULL || dhp->g == NULL) {
		DH_free(dhp);
		return (NULL);
	}
	return (dhp);
}

void
ssl_update_cache(SSL *s, int mode)
{
	int	i;

	/*
	 * If the session_id_length is 0, we are not supposed to cache it,
	 * and it would be rather hard to do anyway :-)
	 */
	if (s->session->session_id_length == 0)
		return;

	i = s->session_ctx->internal->session_cache_mode;
	if ((i & mode) && (!s->internal->hit) && ((i & SSL_SESS_CACHE_NO_INTERNAL_STORE)
	    || SSL_CTX_add_session(s->session_ctx, s->session))
	    && (s->session_ctx->internal->new_session_cb != NULL)) {
		CRYPTO_add(&s->session->references, 1, CRYPTO_LOCK_SSL_SESSION);
		if (!s->session_ctx->internal->new_session_cb(s, s->session))
			SSL_SESSION_free(s->session);
	}

	/* auto flush every 255 connections */
	if ((!(i & SSL_SESS_CACHE_NO_AUTO_CLEAR)) &&
	    ((i & mode) == mode)) {
		if ((((mode & SSL_SESS_CACHE_CLIENT) ?
		    s->session_ctx->internal->stats.sess_connect_good :
		    s->session_ctx->internal->stats.sess_accept_good) & 0xff) == 0xff) {
			SSL_CTX_flush_sessions(s->session_ctx, time(NULL));
		}
	}
}

const SSL_METHOD *
SSL_get_ssl_method(SSL *s)
{
	return (s->method);
}

int
SSL_set_ssl_method(SSL *s, const SSL_METHOD *meth)
{
	int	conn = -1;
	int	ret = 1;

	if (s->method != meth) {
		if (s->internal->handshake_func != NULL)
			conn = (s->internal->handshake_func == s->method->internal->ssl_connect);

		if (s->method->internal->version == meth->internal->version)
			s->method = meth;
		else {
			s->method->internal->ssl_free(s);
			s->method = meth;
			ret = s->method->internal->ssl_new(s);
		}

		if (conn == 1)
			s->internal->handshake_func = meth->internal->ssl_connect;
		else if (conn == 0)
			s->internal->handshake_func = meth->internal->ssl_accept;
	}
	return (ret);
}

int
SSL_get_error(const SSL *s, int i)
{
	int		 reason;
	unsigned long	 l;
	BIO		*bio;

	if (i > 0)
		return (SSL_ERROR_NONE);

	/* Make things return SSL_ERROR_SYSCALL when doing SSL_do_handshake
	 * etc, where we do encode the error */
	if ((l = ERR_peek_error()) != 0) {
		if (ERR_GET_LIB(l) == ERR_LIB_SYS)
			return (SSL_ERROR_SYSCALL);
		else
			return (SSL_ERROR_SSL);
	}

	if ((i < 0) && SSL_want_read(s)) {
		bio = SSL_get_rbio(s);
		if (BIO_should_read(bio)) {
			return (SSL_ERROR_WANT_READ);
		} else if (BIO_should_write(bio)) {
			/*
			 * This one doesn't make too much sense...  We never
			 * try to write to the rbio, and an application
			 * program where rbio and wbio are separate couldn't
			 * even know what it should wait for.  However if we
			 * ever set s->internal->rwstate incorrectly (so that we have
			 * SSL_want_read(s) instead of SSL_want_write(s))
			 * and rbio and wbio *are* the same, this test works
			 * around that bug; so it might be safer to keep it.
			 */
			return (SSL_ERROR_WANT_WRITE);
		} else if (BIO_should_io_special(bio)) {
			reason = BIO_get_retry_reason(bio);
			if (reason == BIO_RR_CONNECT)
				return (SSL_ERROR_WANT_CONNECT);
			else if (reason == BIO_RR_ACCEPT)
				return (SSL_ERROR_WANT_ACCEPT);
			else
				return (SSL_ERROR_SYSCALL); /* unknown */
		}
	}

	if ((i < 0) && SSL_want_write(s)) {
		bio = SSL_get_wbio(s);
		if (BIO_should_write(bio)) {
			return (SSL_ERROR_WANT_WRITE);
		} else if (BIO_should_read(bio)) {
			/*
			 * See above (SSL_want_read(s) with
			 * BIO_should_write(bio))
			 */
			return (SSL_ERROR_WANT_READ);
		} else if (BIO_should_io_special(bio)) {
			reason = BIO_get_retry_reason(bio);
			if (reason == BIO_RR_CONNECT)
				return (SSL_ERROR_WANT_CONNECT);
			else if (reason == BIO_RR_ACCEPT)
				return (SSL_ERROR_WANT_ACCEPT);
			else
				return (SSL_ERROR_SYSCALL);
		}
	}
	if ((i < 0) && SSL_want_x509_lookup(s)) {
		return (SSL_ERROR_WANT_X509_LOOKUP);
	}

	if (i == 0) {
		if ((s->internal->shutdown & SSL_RECEIVED_SHUTDOWN) &&
		    (S3I(s)->warn_alert == SSL_AD_CLOSE_NOTIFY))
		return (SSL_ERROR_ZERO_RETURN);
	}
	return (SSL_ERROR_SYSCALL);
}

int
SSL_do_handshake(SSL *s)
{
	int	ret = 1;

	if (s->internal->handshake_func == NULL) {
		SSLerror(s, SSL_R_CONNECTION_TYPE_NOT_SET);
		return (-1);
	}

	s->method->internal->ssl_renegotiate_check(s);

	if (SSL_in_init(s) || SSL_in_before(s)) {
		ret = s->internal->handshake_func(s);
	}
	return (ret);
}

/*
 * For the next 2 functions, SSL_clear() sets shutdown and so
 * one of these calls will reset it
 */
void
SSL_set_accept_state(SSL *s)
{
	s->server = 1;
	s->internal->shutdown = 0;
	S3I(s)->hs.state = SSL_ST_ACCEPT|SSL_ST_BEFORE;
	s->internal->handshake_func = s->method->internal->ssl_accept;
	/* clear the current cipher */
	ssl_clear_cipher_ctx(s);
	ssl_clear_hash_ctx(&s->read_hash);
	ssl_clear_hash_ctx(&s->internal->write_hash);
}

void
SSL_set_connect_state(SSL *s)
{
	s->server = 0;
	s->internal->shutdown = 0;
	S3I(s)->hs.state = SSL_ST_CONNECT|SSL_ST_BEFORE;
	s->internal->handshake_func = s->method->internal->ssl_connect;
	/* clear the current cipher */
	ssl_clear_cipher_ctx(s);
	ssl_clear_hash_ctx(&s->read_hash);
	ssl_clear_hash_ctx(&s->internal->write_hash);
}

int
ssl_undefined_function(SSL *s)
{
	SSLerror(s, ERR_R_SHOULD_NOT_HAVE_BEEN_CALLED);
	return (0);
}

int
ssl_undefined_void_function(void)
{
	SSLerrorx(ERR_R_SHOULD_NOT_HAVE_BEEN_CALLED);
	return (0);
}

int
ssl_undefined_const_function(const SSL *s)
{
	SSLerror(s, ERR_R_SHOULD_NOT_HAVE_BEEN_CALLED);
	return (0);
}

const char *
ssl_version_string(int ver)
{
	switch (ver) {
	case DTLS1_VERSION:
		return (SSL_TXT_DTLS1);
	case TLS1_VERSION:
		return (SSL_TXT_TLSV1);
	case TLS1_1_VERSION:
		return (SSL_TXT_TLSV1_1);
	case TLS1_2_VERSION:
		return (SSL_TXT_TLSV1_2);
	default:
		return ("unknown");
	}
}

const char *
SSL_get_version(const SSL *s)
{
	return ssl_version_string(s->version);
}

SSL *
SSL_dup(SSL *s)
{
	STACK_OF(X509_NAME) *sk;
	X509_NAME *xn;
	SSL *ret;
	int i;

	if ((ret = SSL_new(SSL_get_SSL_CTX(s))) == NULL)
		return (NULL);

	ret->version = s->version;
	ret->internal->type = s->internal->type;
	ret->method = s->method;

	if (s->session != NULL) {
		/* This copies session-id, SSL_METHOD, sid_ctx, and 'cert' */
		SSL_copy_session_id(ret, s);
	} else {
		/*
		 * No session has been established yet, so we have to expect
		 * that s->cert or ret->cert will be changed later --
		 * they should not both point to the same object,
		 * and thus we can't use SSL_copy_session_id.
		 */

		ret->method->internal->ssl_free(ret);
		ret->method = s->method;
		ret->method->internal->ssl_new(ret);

		if (s->cert != NULL) {
			ssl_cert_free(ret->cert);
			ret->cert = ssl_cert_dup(s->cert);
			if (ret->cert == NULL)
				goto err;
		}

		SSL_set_session_id_context(ret,
		s->sid_ctx, s->sid_ctx_length);
	}

	ret->internal->options = s->internal->options;
	ret->internal->mode = s->internal->mode;
	SSL_set_max_cert_list(ret, SSL_get_max_cert_list(s));
	SSL_set_read_ahead(ret, SSL_get_read_ahead(s));
	ret->internal->msg_callback = s->internal->msg_callback;
	ret->internal->msg_callback_arg = s->internal->msg_callback_arg;
	SSL_set_verify(ret, SSL_get_verify_mode(s),
	SSL_get_verify_callback(s));
	SSL_set_verify_depth(ret, SSL_get_verify_depth(s));
	ret->internal->generate_session_id = s->internal->generate_session_id;

	SSL_set_info_callback(ret, SSL_get_info_callback(s));

	ret->internal->debug = s->internal->debug;

	/* copy app data, a little dangerous perhaps */
	if (!CRYPTO_dup_ex_data(CRYPTO_EX_INDEX_SSL,
	    &ret->internal->ex_data, &s->internal->ex_data))
		goto err;

	/* setup rbio, and wbio */
	if (s->rbio != NULL) {
		if (!BIO_dup_state(s->rbio,(char *)&ret->rbio))
			goto err;
	}
	if (s->wbio != NULL) {
		if (s->wbio != s->rbio) {
			if (!BIO_dup_state(s->wbio,(char *)&ret->wbio))
				goto err;
		} else
			ret->wbio = ret->rbio;
	}
	ret->internal->rwstate = s->internal->rwstate;
	ret->internal->in_handshake = s->internal->in_handshake;
	ret->internal->handshake_func = s->internal->handshake_func;
	ret->server = s->server;
	ret->internal->renegotiate = s->internal->renegotiate;
	ret->internal->new_session = s->internal->new_session;
	ret->internal->quiet_shutdown = s->internal->quiet_shutdown;
	ret->internal->shutdown = s->internal->shutdown;
	/* SSL_dup does not really work at any state, though */
	S3I(ret)->hs.state = S3I(s)->hs.state;
	ret->internal->rstate = s->internal->rstate;

	/*
	 * Would have to copy ret->init_buf, ret->init_msg, ret->init_num,
	 * ret->init_off
	 */
	ret->internal->init_num = 0;

	ret->internal->hit = s->internal->hit;

	X509_VERIFY_PARAM_inherit(ret->param, s->param);

	/* dup the cipher_list and cipher_list_by_id stacks */
	if (s->cipher_list != NULL) {
		if ((ret->cipher_list =
		    sk_SSL_CIPHER_dup(s->cipher_list)) == NULL)
			goto err;
	}
	if (s->internal->cipher_list_by_id != NULL) {
		if ((ret->internal->cipher_list_by_id =
		    sk_SSL_CIPHER_dup(s->internal->cipher_list_by_id)) == NULL)
			goto err;
	}

	/* Dup the client_CA list */
	if (s->internal->client_CA != NULL) {
		if ((sk = sk_X509_NAME_dup(s->internal->client_CA)) == NULL) goto err;
			ret->internal->client_CA = sk;
		for (i = 0; i < sk_X509_NAME_num(sk); i++) {
			xn = sk_X509_NAME_value(sk, i);
			if (sk_X509_NAME_set(sk, i,
			    X509_NAME_dup(xn)) == NULL) {
				X509_NAME_free(xn);
				goto err;
			}
		}
	}

	if (0) {
err:
		if (ret != NULL)
			SSL_free(ret);
		ret = NULL;
	}
	return (ret);
}

void
ssl_clear_cipher_ctx(SSL *s)
{
	EVP_CIPHER_CTX_free(s->enc_read_ctx);
	s->enc_read_ctx = NULL;
	EVP_CIPHER_CTX_free(s->internal->enc_write_ctx);
	s->internal->enc_write_ctx = NULL;

	if (s->internal->aead_read_ctx != NULL) {
		EVP_AEAD_CTX_cleanup(&s->internal->aead_read_ctx->ctx);
		free(s->internal->aead_read_ctx);
		s->internal->aead_read_ctx = NULL;
	}
	if (s->internal->aead_write_ctx != NULL) {
		EVP_AEAD_CTX_cleanup(&s->internal->aead_write_ctx->ctx);
		free(s->internal->aead_write_ctx);
		s->internal->aead_write_ctx = NULL;
	}

}

/* Fix this function so that it takes an optional type parameter */
X509 *
SSL_get_certificate(const SSL *s)
{
	if (s->cert != NULL)
		return (s->cert->key->x509);
	else
		return (NULL);
}

/* Fix this function so that it takes an optional type parameter */
EVP_PKEY *
SSL_get_privatekey(SSL *s)
{
	if (s->cert != NULL)
		return (s->cert->key->privatekey);
	else
		return (NULL);
}

const SSL_CIPHER *
SSL_get_current_cipher(const SSL *s)
{
	if ((s->session != NULL) && (s->session->cipher != NULL))
		return (s->session->cipher);
	return (NULL);
}
const void *
SSL_get_current_compression(SSL *s)
{
	return (NULL);
}

const void *
SSL_get_current_expansion(SSL *s)
{
	return (NULL);
}

int
ssl_init_wbio_buffer(SSL *s, int push)
{
	BIO	*bbio;

	if (s->bbio == NULL) {
		bbio = BIO_new(BIO_f_buffer());
		if (bbio == NULL)
			return (0);
		s->bbio = bbio;
	} else {
		bbio = s->bbio;
		if (s->bbio == s->wbio)
			s->wbio = BIO_pop(s->wbio);
	}
	(void)BIO_reset(bbio);
/*	if (!BIO_set_write_buffer_size(bbio,16*1024)) */
	if (!BIO_set_read_buffer_size(bbio, 1)) {
		SSLerror(s, ERR_R_BUF_LIB);
		return (0);
	}
	if (push) {
		if (s->wbio != bbio)
			s->wbio = BIO_push(bbio, s->wbio);
	} else {
		if (s->wbio == bbio)
			s->wbio = BIO_pop(bbio);
	}
	return (1);
}

void
ssl_free_wbio_buffer(SSL *s)
{
	if (s == NULL)
		return;

	if (s->bbio == NULL)
		return;

	if (s->bbio == s->wbio) {
		/* remove buffering */
		s->wbio = BIO_pop(s->wbio);
	}
	BIO_free(s->bbio);
	s->bbio = NULL;
}

void
SSL_CTX_set_quiet_shutdown(SSL_CTX *ctx, int mode)
{
	ctx->internal->quiet_shutdown = mode;
}

int
SSL_CTX_get_quiet_shutdown(const SSL_CTX *ctx)
{
	return (ctx->internal->quiet_shutdown);
}

void
SSL_set_quiet_shutdown(SSL *s, int mode)
{
	s->internal->quiet_shutdown = mode;
}

int
SSL_get_quiet_shutdown(const SSL *s)
{
	return (s->internal->quiet_shutdown);
}

void
SSL_set_shutdown(SSL *s, int mode)
{
	s->internal->shutdown = mode;
}

int
SSL_get_shutdown(const SSL *s)
{
	return (s->internal->shutdown);
}

int
SSL_version(const SSL *s)
{
	return (s->version);
}

SSL_CTX *
SSL_get_SSL_CTX(const SSL *ssl)
{
	return (ssl->ctx);
}

SSL_CTX *
SSL_set_SSL_CTX(SSL *ssl, SSL_CTX* ctx)
{
	CERT *ocert = ssl->cert;

	if (ssl->ctx == ctx)
		return (ssl->ctx);
	if (ctx == NULL)
		ctx = ssl->initial_ctx;
	ssl->cert = ssl_cert_dup(ctx->internal->cert);
	if (ocert != NULL) {
		int i;
		/* Copy negotiated digests from original certificate. */
		for (i = 0; i < SSL_PKEY_NUM; i++)
			ssl->cert->pkeys[i].digest = ocert->pkeys[i].digest;
		ssl_cert_free(ocert);
	}
	CRYPTO_add(&ctx->references, 1, CRYPTO_LOCK_SSL_CTX);
	SSL_CTX_free(ssl->ctx); /* decrement reference count */
	ssl->ctx = ctx;
	return (ssl->ctx);
}

int
SSL_CTX_set_default_verify_paths(SSL_CTX *ctx)
{
	return (X509_STORE_set_default_paths(ctx->cert_store));
}

int
SSL_CTX_load_verify_locations(SSL_CTX *ctx, const char *CAfile,
    const char *CApath)
{
	return (X509_STORE_load_locations(ctx->cert_store, CAfile, CApath));
}

int
SSL_CTX_load_verify_mem(SSL_CTX *ctx, void *buf, int len)
{
	return (X509_STORE_load_mem(ctx->cert_store, buf, len));
}

void
SSL_set_info_callback(SSL *ssl, void (*cb)(const SSL *ssl, int type, int val))
{
	ssl->internal->info_callback = cb;
}

void (*SSL_get_info_callback(const SSL *ssl))(const SSL *ssl, int type, int val)
{
	return (ssl->internal->info_callback);
}

int
SSL_state(const SSL *ssl)
{
	return (S3I(ssl)->hs.state);
}

void
SSL_set_state(SSL *ssl, int state)
{
	S3I(ssl)->hs.state = state;
}

void
SSL_set_verify_result(SSL *ssl, long arg)
{
	ssl->verify_result = arg;
}

long
SSL_get_verify_result(const SSL *ssl)
{
	return (ssl->verify_result);
}

int
SSL_get_ex_new_index(long argl, void *argp, CRYPTO_EX_new *new_func,
    CRYPTO_EX_dup *dup_func, CRYPTO_EX_free *free_func)
{
	return (CRYPTO_get_ex_new_index(CRYPTO_EX_INDEX_SSL, argl, argp,
	    new_func, dup_func, free_func));
}

int
SSL_set_ex_data(SSL *s, int idx, void *arg)
{
	return (CRYPTO_set_ex_data(&s->internal->ex_data, idx, arg));
}

void *
SSL_get_ex_data(const SSL *s, int idx)
{
	return (CRYPTO_get_ex_data(&s->internal->ex_data, idx));
}

int
SSL_CTX_get_ex_new_index(long argl, void *argp, CRYPTO_EX_new *new_func,
    CRYPTO_EX_dup *dup_func, CRYPTO_EX_free *free_func)
{
	return (CRYPTO_get_ex_new_index(CRYPTO_EX_INDEX_SSL_CTX, argl, argp,
	    new_func, dup_func, free_func));
}

int
SSL_CTX_set_ex_data(SSL_CTX *s, int idx, void *arg)
{
	return (CRYPTO_set_ex_data(&s->internal->ex_data, idx, arg));
}

void *
SSL_CTX_get_ex_data(const SSL_CTX *s, int idx)
{
	return (CRYPTO_get_ex_data(&s->internal->ex_data, idx));
}

int
ssl_ok(SSL *s)
{
	return (1);
}

X509_STORE *
SSL_CTX_get_cert_store(const SSL_CTX *ctx)
{
	return (ctx->cert_store);
}

void
SSL_CTX_set_cert_store(SSL_CTX *ctx, X509_STORE *store)
{
	X509_STORE_free(ctx->cert_store);
	ctx->cert_store = store;
}

int
SSL_want(const SSL *s)
{
	return (s->internal->rwstate);
}

void
SSL_CTX_set_tmp_rsa_callback(SSL_CTX *ctx, RSA *(*cb)(SSL *ssl, int is_export,
    int keylength))
{
	SSL_CTX_callback_ctrl(ctx, SSL_CTRL_SET_TMP_RSA_CB,(void (*)(void))cb);
}

void
SSL_set_tmp_rsa_callback(SSL *ssl, RSA *(*cb)(SSL *ssl, int is_export,
    int keylength))
{
	SSL_callback_ctrl(ssl, SSL_CTRL_SET_TMP_RSA_CB,(void (*)(void))cb);
}

void
SSL_CTX_set_tmp_dh_callback(SSL_CTX *ctx, DH *(*dh)(SSL *ssl, int is_export,
    int keylength))
{
	SSL_CTX_callback_ctrl(ctx, SSL_CTRL_SET_TMP_DH_CB,(void (*)(void))dh);
}

void
SSL_set_tmp_dh_callback(SSL *ssl, DH *(*dh)(SSL *ssl, int is_export,
    int keylength))
{
	SSL_callback_ctrl(ssl, SSL_CTRL_SET_TMP_DH_CB,(void (*)(void))dh);
}

void
SSL_CTX_set_tmp_ecdh_callback(SSL_CTX *ctx, EC_KEY *(*ecdh)(SSL *ssl,
    int is_export, int keylength))
{
	SSL_CTX_callback_ctrl(ctx, SSL_CTRL_SET_TMP_ECDH_CB,
	    (void (*)(void))ecdh);
}

void
SSL_set_tmp_ecdh_callback(SSL *ssl, EC_KEY *(*ecdh)(SSL *ssl, int is_export,
    int keylength))
{
	SSL_callback_ctrl(ssl, SSL_CTRL_SET_TMP_ECDH_CB,(void (*)(void))ecdh);
}


void
SSL_CTX_set_msg_callback(SSL_CTX *ctx, void (*cb)(int write_p, int version,
    int content_type, const void *buf, size_t len, SSL *ssl, void *arg))
{
	SSL_CTX_callback_ctrl(ctx, SSL_CTRL_SET_MSG_CALLBACK,
	    (void (*)(void))cb);
}

void
SSL_set_msg_callback(SSL *ssl, void (*cb)(int write_p, int version,
    int content_type, const void *buf, size_t len, SSL *ssl, void *arg))
{
	SSL_callback_ctrl(ssl, SSL_CTRL_SET_MSG_CALLBACK, (void (*)(void))cb);
}

void
ssl_clear_hash_ctx(EVP_MD_CTX **hash)
{
	if (*hash)
		EVP_MD_CTX_destroy(*hash);
	*hash = NULL;
}

void
SSL_set_debug(SSL *s, int debug)
{
	s->internal->debug = debug;
}

int
SSL_cache_hit(SSL *s)
{
	return (s->internal->hit);
}

int
SSL_CTX_set_min_proto_version(SSL_CTX *ctx, uint16_t version)
{
	return ssl_version_set_min(ctx->method, version,
	    ctx->internal->max_version, &ctx->internal->min_version);
}

int
SSL_CTX_set_max_proto_version(SSL_CTX *ctx, uint16_t version)
{
	return ssl_version_set_max(ctx->method, version,
	    ctx->internal->min_version, &ctx->internal->max_version);
}

int
SSL_set_min_proto_version(SSL *ssl, uint16_t version)
{
	return ssl_version_set_min(ssl->method, version,
	    ssl->internal->max_version, &ssl->internal->min_version);
}

int
SSL_set_max_proto_version(SSL *ssl, uint16_t version)
{
	return ssl_version_set_max(ssl->method, version,
	    ssl->internal->min_version, &ssl->internal->max_version);
}

static int
ssl_cipher_id_cmp_BSEARCH_CMP_FN(const void *a_, const void *b_)
{
	SSL_CIPHER const *a = a_;
	SSL_CIPHER const *b = b_;
	return ssl_cipher_id_cmp(a, b);
}

SSL_CIPHER *
OBJ_bsearch_ssl_cipher_id(SSL_CIPHER *key, SSL_CIPHER const *base, int num)
{
	return (SSL_CIPHER *)OBJ_bsearch_(key, base, num, sizeof(SSL_CIPHER),
	    ssl_cipher_id_cmp_BSEARCH_CMP_FN);
}
