/* $OpenBSD: ssl_locl.h,v 1.193 2017/08/28 16:37:04 jsing Exp $ */
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

#ifndef HEADER_SSL_LOCL_H
#define HEADER_SSL_LOCL_H

#include <sys/types.h>

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <openssl/opensslconf.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/dsa.h>
#include <openssl/err.h>
#include <openssl/rsa.h>
#include <openssl/ssl.h>
#include <openssl/stack.h>

#include "bytestring.h"

__BEGIN_HIDDEN_DECLS

#define l2n(l,c)	(*((c)++)=(unsigned char)(((l)>>24)&0xff), \
			 *((c)++)=(unsigned char)(((l)>>16)&0xff), \
			 *((c)++)=(unsigned char)(((l)>> 8)&0xff), \
			 *((c)++)=(unsigned char)(((l)    )&0xff))

#define l2n8(l,c)	(*((c)++)=(unsigned char)(((l)>>56)&0xff), \
			 *((c)++)=(unsigned char)(((l)>>48)&0xff), \
			 *((c)++)=(unsigned char)(((l)>>40)&0xff), \
			 *((c)++)=(unsigned char)(((l)>>32)&0xff), \
			 *((c)++)=(unsigned char)(((l)>>24)&0xff), \
			 *((c)++)=(unsigned char)(((l)>>16)&0xff), \
			 *((c)++)=(unsigned char)(((l)>> 8)&0xff), \
			 *((c)++)=(unsigned char)(((l)    )&0xff))

#define n2s(c,s)	((s=(((unsigned int)(c[0]))<< 8)| \
			    (((unsigned int)(c[1]))    )),c+=2)
#define s2n(s,c)	((c[0]=(unsigned char)(((s)>> 8)&0xff), \
			  c[1]=(unsigned char)(((s)    )&0xff)),c+=2)

#define l2n3(l,c)	((c[0]=(unsigned char)(((l)>>16)&0xff), \
			  c[1]=(unsigned char)(((l)>> 8)&0xff), \
			  c[2]=(unsigned char)(((l)    )&0xff)),c+=3)

/* LOCAL STUFF */

#define SSL_DECRYPT	0
#define SSL_ENCRYPT	1

/*
 * Define the Bitmasks for SSL_CIPHER.algorithms.
 * This bits are used packed as dense as possible. If new methods/ciphers
 * etc will be added, the bits a likely to change, so this information
 * is for internal library use only, even though SSL_CIPHER.algorithms
 * can be publicly accessed.
 * Use the according functions for cipher management instead.
 *
 * The bit mask handling in the selection and sorting scheme in
 * ssl_create_cipher_list() has only limited capabilities, reflecting
 * that the different entities within are mutually exclusive:
 * ONLY ONE BIT PER MASK CAN BE SET AT A TIME.
 */

/* Bits for algorithm_mkey (key exchange algorithm) */
#define SSL_kRSA		0x00000001L /* RSA key exchange */
#define SSL_kDHE		0x00000008L /* tmp DH key no DH cert */
#define SSL_kECDHE		0x00000080L /* ephemeral ECDH */
#define SSL_kGOST		0x00000200L /* GOST key exchange */

/* Bits for algorithm_auth (server authentication) */
#define SSL_aRSA		0x00000001L /* RSA auth */
#define SSL_aDSS 		0x00000002L /* DSS auth */
#define SSL_aNULL 		0x00000004L /* no auth (i.e. use ADH or AECDH) */
#define SSL_aECDSA              0x00000040L /* ECDSA auth*/
#define SSL_aGOST01 		0x00000200L /* GOST R 34.10-2001 signature auth */

/* Bits for algorithm_enc (symmetric encryption) */
#define SSL_DES			0x00000001L
#define SSL_3DES		0x00000002L
#define SSL_RC4			0x00000004L
#define SSL_IDEA		0x00000008L
#define SSL_eNULL		0x00000010L
#define SSL_AES128		0x00000020L
#define SSL_AES256		0x00000040L
#define SSL_CAMELLIA128		0x00000080L
#define SSL_CAMELLIA256		0x00000100L
#define SSL_eGOST2814789CNT	0x00000200L
#define SSL_AES128GCM		0x00000400L
#define SSL_AES256GCM		0x00000800L
#define SSL_CHACHA20POLY1305	0x00001000L

#define SSL_AES        		(SSL_AES128|SSL_AES256|SSL_AES128GCM|SSL_AES256GCM)
#define SSL_CAMELLIA		(SSL_CAMELLIA128|SSL_CAMELLIA256)


/* Bits for algorithm_mac (symmetric authentication) */

#define SSL_MD5			0x00000001L
#define SSL_SHA1		0x00000002L
#define SSL_GOST94      0x00000004L
#define SSL_GOST89MAC   0x00000008L
#define SSL_SHA256		0x00000010L
#define SSL_SHA384		0x00000020L
/* Not a real MAC, just an indication it is part of cipher */
#define SSL_AEAD		0x00000040L
#define SSL_STREEBOG256		0x00000080L

/* Bits for algorithm_ssl (protocol version) */
#define SSL_SSLV3		0x00000002L
#define SSL_TLSV1		SSL_SSLV3	/* for now */
#define SSL_TLSV1_2		0x00000004L


/* Bits for algorithm2 (handshake digests and other extra flags) */

#define SSL_HANDSHAKE_MAC_MASK		0xff0
#define SSL_HANDSHAKE_MAC_MD5		0x010
#define SSL_HANDSHAKE_MAC_SHA		0x020
#define SSL_HANDSHAKE_MAC_GOST94	0x040
#define SSL_HANDSHAKE_MAC_SHA256	0x080
#define SSL_HANDSHAKE_MAC_SHA384	0x100
#define SSL_HANDSHAKE_MAC_STREEBOG256	0x200
#define SSL_HANDSHAKE_MAC_DEFAULT (SSL_HANDSHAKE_MAC_MD5 | SSL_HANDSHAKE_MAC_SHA)

/* When adding new digest in the ssl_ciph.c and increment SSM_MD_NUM_IDX
 * make sure to update this constant too */
#define SSL_MAX_DIGEST 7

#define SSL3_CK_ID		0x03000000
#define SSL3_CK_VALUE_MASK	0x0000ffff

#define TLS1_PRF_DGST_MASK	(0xff << TLS1_PRF_DGST_SHIFT)

#define TLS1_PRF_DGST_SHIFT 10
#define TLS1_PRF_MD5 (SSL_HANDSHAKE_MAC_MD5 << TLS1_PRF_DGST_SHIFT)
#define TLS1_PRF_SHA1 (SSL_HANDSHAKE_MAC_SHA << TLS1_PRF_DGST_SHIFT)
#define TLS1_PRF_SHA256 (SSL_HANDSHAKE_MAC_SHA256 << TLS1_PRF_DGST_SHIFT)
#define TLS1_PRF_SHA384 (SSL_HANDSHAKE_MAC_SHA384 << TLS1_PRF_DGST_SHIFT)
#define TLS1_PRF_GOST94 (SSL_HANDSHAKE_MAC_GOST94 << TLS1_PRF_DGST_SHIFT)
#define TLS1_PRF_STREEBOG256 (SSL_HANDSHAKE_MAC_STREEBOG256 << TLS1_PRF_DGST_SHIFT)
#define TLS1_PRF (TLS1_PRF_MD5 | TLS1_PRF_SHA1)

/* Stream MAC for GOST ciphersuites from cryptopro draft
 * (currently this also goes into algorithm2) */
#define TLS1_STREAM_MAC 0x04

/*
 * SSL_CIPHER_ALGORITHM2_VARIABLE_NONCE_IN_RECORD is an algorithm2 flag that
 * indicates that the variable part of the nonce is included as a prefix of
 * the record (AES-GCM, for example, does this with an 8-byte variable nonce.)
 */
#define SSL_CIPHER_ALGORITHM2_VARIABLE_NONCE_IN_RECORD (1 << 22)

/*
 * SSL_CIPHER_ALGORITHM2_AEAD is an algorithm2 flag that indicates the cipher
 * is implemented via an EVP_AEAD.
 */
#define SSL_CIPHER_ALGORITHM2_AEAD (1 << 23)

/*
 * SSL_CIPHER_AEAD_FIXED_NONCE_LEN returns the number of bytes of fixed nonce
 * for an SSL_CIPHER with the SSL_CIPHER_ALGORITHM2_AEAD flag.
 */
#define SSL_CIPHER_AEAD_FIXED_NONCE_LEN(ssl_cipher) \
	(((ssl_cipher->algorithm2 >> 24) & 0xf) * 2)

/*
 * Cipher strength information.
 */
#define SSL_STRONG_MASK		0x000001fcL
#define SSL_STRONG_NONE		0x00000004L
#define SSL_LOW			0x00000020L
#define SSL_MEDIUM		0x00000040L
#define SSL_HIGH		0x00000080L

/*
 * The keylength (measured in RSA key bits, I guess)  for temporary keys.
 * Cipher argument is so that this can be variable in the future.
 */
#define SSL_C_PKEYLENGTH(c)	1024

/* Check if an SSL structure is using DTLS. */
#define SSL_IS_DTLS(s) \
	(s->method->internal->version == DTLS1_VERSION)

/* See if we need explicit IV. */
#define SSL_USE_EXPLICIT_IV(s) \
	(s->method->internal->ssl3_enc->enc_flags & SSL_ENC_FLAG_EXPLICIT_IV)

/* See if we use signature algorithms extension. */
#define SSL_USE_SIGALGS(s) \
	(s->method->internal->ssl3_enc->enc_flags & SSL_ENC_FLAG_SIGALGS)

/* Allow TLS 1.2 ciphersuites: applies to DTLS 1.2 as well as TLS 1.2. */
#define SSL_USE_TLS1_2_CIPHERS(s) \
	(s->method->internal->ssl3_enc->enc_flags & SSL_ENC_FLAG_TLS1_2_CIPHERS)

#define SSL_PKEY_RSA_ENC	0
#define SSL_PKEY_RSA_SIGN	1
#define SSL_PKEY_DH_RSA		2
#define SSL_PKEY_ECC            3
#define SSL_PKEY_GOST01		4
#define SSL_PKEY_NUM		5

#define SSL_MAX_EMPTY_RECORDS	32

/* SSL_kRSA <- RSA_ENC | (RSA_TMP & RSA_SIGN) |
 * 	    <- (EXPORT & (RSA_ENC | RSA_TMP) & RSA_SIGN)
 * SSL_kDH  <- DH_ENC & (RSA_ENC | RSA_SIGN | DSA_SIGN)
 * SSL_kDHE <- RSA_ENC | RSA_SIGN | DSA_SIGN
 * SSL_aRSA <- RSA_ENC | RSA_SIGN
 * SSL_aDSS <- DSA_SIGN
 */

/*
#define CERT_INVALID		0
#define CERT_PUBLIC_KEY		1
#define CERT_PRIVATE_KEY	2
*/

/* From ECC-TLS draft, used in encoding the curve type in
 * ECParameters
 */
#define EXPLICIT_PRIME_CURVE_TYPE  1
#define EXPLICIT_CHAR2_CURVE_TYPE  2
#define NAMED_CURVE_TYPE           3

typedef struct ssl_method_internal_st {
	int version;

	uint16_t min_version;
	uint16_t max_version;

	int (*ssl_new)(SSL *s);
	void (*ssl_clear)(SSL *s);
	void (*ssl_free)(SSL *s);

	int (*ssl_accept)(SSL *s);
	int (*ssl_connect)(SSL *s);
	int (*ssl_read)(SSL *s, void *buf, int len);
	int (*ssl_peek)(SSL *s, void *buf, int len);
	int (*ssl_write)(SSL *s, const void *buf, int len);
	int (*ssl_shutdown)(SSL *s);

	int (*ssl_renegotiate)(SSL *s);
	int (*ssl_renegotiate_check)(SSL *s);

	long (*ssl_get_message)(SSL *s, int st1, int stn, int mt,
	    long max, int *ok);
	int (*ssl_read_bytes)(SSL *s, int type, unsigned char *buf,
	    int len, int peek);
	int (*ssl_write_bytes)(SSL *s, int type, const void *buf_, int len);

	int (*ssl_pending)(const SSL *s);
	const struct ssl_method_st *(*get_ssl_method)(int version);

	long (*get_timeout)(void);
	int (*ssl_version)(void);

	struct ssl3_enc_method *ssl3_enc; /* Extra SSLv3/TLS stuff */
} SSL_METHOD_INTERNAL;

typedef struct ssl_session_internal_st {
	CRYPTO_EX_DATA ex_data; /* application specific data */

	/* These are used to make removal of session-ids more
	 * efficient and to implement a maximum cache size. */
	struct ssl_session_st *prev, *next;

	/* Used to indicate that session resumption is not allowed.
	 * Applications can also set this bit for a new session via
	 * not_resumable_session_cb to disable session caching and tickets. */
	int not_resumable;

	/* The cert is the certificate used to establish this connection */
	struct sess_cert_st /* SESS_CERT */ *sess_cert;

	size_t tlsext_ecpointformatlist_length;
	uint8_t *tlsext_ecpointformatlist; /* peer's list */
	size_t tlsext_supportedgroups_length;
	uint16_t *tlsext_supportedgroups; /* peer's list */
} SSL_SESSION_INTERNAL;
#define SSI(s) (s->session->internal)

typedef struct ssl_handshake_st {
	/* state contains one of the SSL3_ST_* values. */
	int state;

	/* used when SSL_ST_FLUSH_DATA is entered */
	int next_state;

	/*  new_cipher is the cipher being negotiated in this handshake. */
	const SSL_CIPHER *new_cipher;

	/* key_block is the record-layer key block for TLS 1.2 and earlier. */
	int key_block_len;
	unsigned char *key_block;
} SSL_HANDSHAKE;

typedef struct ssl_ctx_internal_st {
	uint16_t min_version;
	uint16_t max_version;

	unsigned long options;
	unsigned long mode;

	/* If this callback is not null, it will be called each
	 * time a session id is added to the cache.  If this function
	 * returns 1, it means that the callback will do a
	 * SSL_SESSION_free() when it has finished using it.  Otherwise,
	 * on 0, it means the callback has finished with it.
	 * If remove_session_cb is not null, it will be called when
	 * a session-id is removed from the cache.  After the call,
	 * OpenSSL will SSL_SESSION_free() it. */
	int (*new_session_cb)(struct ssl_st *ssl, SSL_SESSION *sess);
	void (*remove_session_cb)(struct ssl_ctx_st *ctx, SSL_SESSION *sess);
	SSL_SESSION *(*get_session_cb)(struct ssl_st *ssl,
	    unsigned char *data, int len, int *copy);

	/* if defined, these override the X509_verify_cert() calls */
	int (*app_verify_callback)(X509_STORE_CTX *, void *);
	    void *app_verify_arg;

	/* get client cert callback */
	int (*client_cert_cb)(SSL *ssl, X509 **x509, EVP_PKEY **pkey);

	/* cookie generate callback */
	int (*app_gen_cookie_cb)(SSL *ssl, unsigned char *cookie,
	    unsigned int *cookie_len);

	/* verify cookie callback */
	int (*app_verify_cookie_cb)(SSL *ssl, unsigned char *cookie,
	    unsigned int cookie_len);

	void (*info_callback)(const SSL *ssl,int type,int val); /* used if SSL's info_callback is NULL */

	/* callback that allows applications to peek at protocol messages */
	void (*msg_callback)(int write_p, int version, int content_type,
	    const void *buf, size_t len, SSL *ssl, void *arg);
	void *msg_callback_arg;

	int (*default_verify_callback)(int ok,X509_STORE_CTX *ctx); /* called 'verify_callback' in the SSL */

	/* Default generate session ID callback. */
	GEN_SESSION_CB generate_session_id;

	/* TLS extensions servername callback */
	int (*tlsext_servername_callback)(SSL*, int *, void *);
	void *tlsext_servername_arg;

	/* Callback to support customisation of ticket key setting */
	int (*tlsext_ticket_key_cb)(SSL *ssl, unsigned char *name,
	    unsigned char *iv, EVP_CIPHER_CTX *ectx, HMAC_CTX *hctx, int enc);

	/* certificate status request info */
	/* Callback for status request */
	int (*tlsext_status_cb)(SSL *ssl, void *arg);
	void *tlsext_status_arg;

	struct lhash_st_SSL_SESSION *sessions;

	/* Most session-ids that will be cached, default is
	 * SSL_SESSION_CACHE_MAX_SIZE_DEFAULT. 0 is unlimited. */
	unsigned long session_cache_size;
	struct ssl_session_st *session_cache_head;
	struct ssl_session_st *session_cache_tail;

	/* This can have one of 2 values, ored together,
	 * SSL_SESS_CACHE_CLIENT,
	 * SSL_SESS_CACHE_SERVER,
	 * Default is SSL_SESSION_CACHE_SERVER, which means only
	 * SSL_accept which cache SSL_SESSIONS. */
	int session_cache_mode;

	struct {
		int sess_connect;	/* SSL new conn - started */
		int sess_connect_renegotiate;/* SSL reneg - requested */
		int sess_connect_good;	/* SSL new conne/reneg - finished */
		int sess_accept;	/* SSL new accept - started */
		int sess_accept_renegotiate;/* SSL reneg - requested */
		int sess_accept_good;	/* SSL accept/reneg - finished */
		int sess_miss;		/* session lookup misses  */
		int sess_timeout;	/* reuse attempt on timeouted session */
		int sess_cache_full;	/* session removed due to full cache */
		int sess_hit;		/* session reuse actually done */
		int sess_cb_hit;	/* session-id that was not
					 * in the cache was
					 * passed back via the callback.  This
					 * indicates that the application is
					 * supplying session-id's from other
					 * processes - spooky :-) */
	} stats;

	CRYPTO_EX_DATA ex_data;

	/* same cipher_list but sorted for lookup */
	STACK_OF(SSL_CIPHER) *cipher_list_by_id;

	struct cert_st /* CERT */ *cert;

	/* Default values used when no per-SSL value is defined follow */

	/* what we put in client cert requests */
	STACK_OF(X509_NAME) *client_CA;

	long max_cert_list;

	int read_ahead;

	int quiet_shutdown;

	/* Maximum amount of data to send in one fragment.
	 * actual record size can be more than this due to
	 * padding and MAC overheads.
	 */
	unsigned int max_send_fragment;

#ifndef OPENSSL_NO_ENGINE
	/* Engine to pass requests for client certs to
	 */
	ENGINE *client_cert_engine;
#endif

	/* RFC 4507 session ticket keys */
	unsigned char tlsext_tick_key_name[16];
	unsigned char tlsext_tick_hmac_key[16];
	unsigned char tlsext_tick_aes_key[16];

	/* SRTP profiles we are willing to do from RFC 5764 */
	STACK_OF(SRTP_PROTECTION_PROFILE) *srtp_profiles;

	/*
	 * ALPN information.
	 */

	/*
	 * Server callback function that allows the server to select the
	 * protocol for the connection.
	 *   out: on successful return, this must point to the raw protocol
	 *       name (without the length prefix).
	 *   outlen: on successful return, this contains the length of out.
	 *   in: points to the client's list of supported protocols in
	 *       wire-format.
	 *   inlen: the length of in.
	 */
	int (*alpn_select_cb)(SSL *s, const unsigned char **out,
	    unsigned char *outlen, const unsigned char *in, unsigned int inlen,
	    void *arg);
	void *alpn_select_cb_arg;

	/* Client list of supported protocols in wire format. */
	unsigned char *alpn_client_proto_list;
	unsigned int alpn_client_proto_list_len;

	size_t tlsext_ecpointformatlist_length;
	uint8_t *tlsext_ecpointformatlist; /* our list */
	size_t tlsext_supportedgroups_length;
	uint16_t *tlsext_supportedgroups; /* our list */
} SSL_CTX_INTERNAL;

typedef struct ssl_internal_st {
	uint16_t min_version;
	uint16_t max_version;

	unsigned long options; /* protocol behaviour */
	unsigned long mode; /* API behaviour */

	/* Client list of supported protocols in wire format. */
	unsigned char *alpn_client_proto_list;
	unsigned int alpn_client_proto_list_len;

	/* XXX Callbacks */

	/* true when we are actually in SSL_accept() or SSL_connect() */
	int in_handshake;
	int (*handshake_func)(SSL *);
	/* callback that allows applications to peek at protocol messages */
	void (*msg_callback)(int write_p, int version, int content_type,
	    const void *buf, size_t len, SSL *ssl, void *arg);
	void *msg_callback_arg;

	/* Default generate session ID callback. */
	GEN_SESSION_CB generate_session_id;

	int (*verify_callback)(int ok,X509_STORE_CTX *ctx); /* fail if callback returns 0 */

	void (*info_callback)(const SSL *ssl,int type,int val); /* optional informational callback */

	/* TLS extension debug callback */
	void (*tlsext_debug_cb)(SSL *s, int client_server, int type,
	    unsigned char *data, int len, void *arg);
	void *tlsext_debug_arg;

	/* TLS Session Ticket extension callback */
	tls_session_ticket_ext_cb_fn tls_session_ticket_ext_cb;
	void *tls_session_ticket_ext_cb_arg;

	/* TLS pre-shared secret session resumption */
	tls_session_secret_cb_fn tls_session_secret_cb;
	void *tls_session_secret_cb_arg;

	/* XXX non-callback */

	int type; /* SSL_ST_CONNECT or SSL_ST_ACCEPT */

	/* This holds a variable that indicates what we were doing
	 * when a 0 or -1 is returned.  This is needed for
	 * non-blocking IO so we know what request needs re-doing when
	 * in SSL_accept or SSL_connect */
	int rwstate;

	/* Imagine that here's a boolean member "init" that is
	 * switched as soon as SSL_set_{accept/connect}_state
	 * is called for the first time, so that "state" and
	 * "handshake_func" are properly initialized.  But as
	 * handshake_func is == 0 until then, we use this
	 * test instead of an "init" member.
	 */

	int new_session;/* Generate a new session or reuse an old one.
			 * NB: For servers, the 'new' session may actually be a previously
			 * cached session or even the previous session unless
			 * SSL_OP_NO_SESSION_RESUMPTION_ON_RENEGOTIATION is set */
	int quiet_shutdown;/* don't send shutdown packets */
	int shutdown;	/* we have shut things down, 0x01 sent, 0x02
			 * for received */
	BUF_MEM *init_buf;	/* buffer used during init */
	void *init_msg;		/* pointer to handshake message body, set by ssl3_get_message() */
	int init_num;		/* amount read/written */
	int init_off;		/* amount read/written */

	/* used internally to point at a raw packet */
	unsigned char *packet;
	unsigned int packet_length;

	int read_ahead;		/* Read as many input bytes as possible
				 * (for non-blocking reads) */

	int hit;		/* reusing a previous session */

	/* crypto */
	STACK_OF(SSL_CIPHER) *cipher_list_by_id;

	/* These are the ones being used, the ones in SSL_SESSION are
	 * the ones to be 'copied' into these ones */
	int mac_flags;

	SSL_AEAD_CTX *aead_read_ctx;	/* AEAD context. If non-NULL, then
					   enc_read_ctx and read_hash are
					   ignored. */

	SSL_AEAD_CTX *aead_write_ctx;	/* AEAD context. If non-NULL, then
					   enc_write_ctx and write_hash are
					   ignored. */

	EVP_CIPHER_CTX *enc_write_ctx;		/* cryptographic state */
	EVP_MD_CTX *write_hash;			/* used for mac generation */

	/* session info */

	/* extra application data */
	CRYPTO_EX_DATA ex_data;

	/* client cert? */
	/* for server side, keep the list of CA_dn we can use */
	STACK_OF(X509_NAME) *client_CA;

	/* set this flag to 1 and a sleep(1) is put into all SSL_read()
	 * and SSL_write() calls, good for nbio debuging :-) */
	int debug;
	long max_cert_list;
	int first_packet;

	int servername_done;	/* no further mod of servername
				   0 : call the servername extension callback.
				   1 : prepare 2, allow last ack just after in server callback.
				   2 : don't call servername callback, no ack in server hello
				   */

	/* Expect OCSP CertificateStatus message */
	int tlsext_status_expected;
	/* OCSP status request only */
	STACK_OF(OCSP_RESPID) *tlsext_ocsp_ids;
	X509_EXTENSIONS *tlsext_ocsp_exts;
	/* OCSP response received or to be sent */
	unsigned char *tlsext_ocsp_resp;
	int tlsext_ocsp_resplen;

	/* RFC4507 session ticket expected to be received or sent */
	int tlsext_ticket_expected;

	size_t tlsext_ecpointformatlist_length;
	uint8_t *tlsext_ecpointformatlist; /* our list */
	size_t tlsext_supportedgroups_length;
	uint16_t *tlsext_supportedgroups; /* our list */

	/* TLS Session Ticket extension override */
	TLS_SESSION_TICKET_EXT *tlsext_session_ticket;

	STACK_OF(SRTP_PROTECTION_PROFILE) *srtp_profiles;	/* What we'll do */
	SRTP_PROTECTION_PROFILE *srtp_profile;			/* What's been chosen */

	int renegotiate;/* 1 if we are renegotiating.
		 	 * 2 if we are a server and are inside a handshake
	                 * (i.e. not just sending a HelloRequest) */

	int rstate;	/* where we are when reading */

	int mac_packet;

	int empty_record_count;
} SSL_INTERNAL;

typedef struct ssl3_state_internal_st {
	int delay_buf_pop_ret;

	unsigned char read_sequence[SSL3_SEQUENCE_SIZE];
	int read_mac_secret_size;
	unsigned char read_mac_secret[EVP_MAX_MD_SIZE];
	unsigned char write_sequence[SSL3_SEQUENCE_SIZE];
	int write_mac_secret_size;
	unsigned char write_mac_secret[EVP_MAX_MD_SIZE];

	/* flags for countermeasure against known-IV weakness */
	int need_empty_fragments;
	int empty_fragment_done;

	SSL3_RECORD rrec;	/* each decoded record goes in here */
	SSL3_RECORD wrec;	/* goes out from here */

	/* storage for Alert/Handshake protocol data received but not
	 * yet processed by ssl3_read_bytes: */
	unsigned char alert_fragment[2];
	unsigned int alert_fragment_len;
	unsigned char handshake_fragment[4];
	unsigned int handshake_fragment_len;

	/* partial write - check the numbers match */
	unsigned int wnum;	/* number of bytes sent so far */
	int wpend_tot;		/* number bytes written */
	int wpend_type;
	int wpend_ret;		/* number of bytes submitted */
	const unsigned char *wpend_buf;

	/* used during startup, digest all incoming/outgoing packets */
	BIO *handshake_buffer;

	/* Rolling hash of handshake messages. */
	EVP_MD_CTX *handshake_hash;

	/* this is set whenerver we see a change_cipher_spec message
	 * come in when we are not looking for one */
	int change_cipher_spec;

	int warn_alert;
	int fatal_alert;

	/* This flag is set when we should renegotiate ASAP, basically when
	 * there is no more data in the read or write buffers */
	int renegotiate;
	int total_renegotiations;
	int num_renegotiations;

	int in_read_app_data;

	SSL_HANDSHAKE hs;

	struct	{
		/* actually only needs to be 16+20 */
		unsigned char cert_verify_md[EVP_MAX_MD_SIZE*2];

		/* actually only need to be 16+20 for SSLv3 and 12 for TLS */
		unsigned char finish_md[EVP_MAX_MD_SIZE*2];
		int finish_md_len;
		unsigned char peer_finish_md[EVP_MAX_MD_SIZE*2];
		int peer_finish_md_len;

		unsigned long message_size;
		int message_type;

		DH *dh;

		EC_KEY *ecdh; /* holds short lived ECDH key */

		uint8_t *x25519;

		int reuse_message;

		/* used for certificate requests */
		int cert_req;
		int ctype_num;
		char ctype[SSL3_CT_NUMBER];
		STACK_OF(X509_NAME) *ca_names;

		const EVP_CIPHER *new_sym_enc;
		const EVP_AEAD *new_aead;
		const EVP_MD *new_hash;
		int new_mac_pkey_type;
		int cert_request;
	} tmp;

	/* Connection binding to prevent renegotiation attacks */
	unsigned char previous_client_finished[EVP_MAX_MD_SIZE];
	unsigned char previous_client_finished_len;
	unsigned char previous_server_finished[EVP_MAX_MD_SIZE];
	unsigned char previous_server_finished_len;
	int send_connection_binding; /* TODOEKR */

	/* Set if we saw a Renegotiation Indication extension from our peer. */
	int renegotiate_seen;

	/*
	 * ALPN information.
	 *
	 * In a server these point to the selected ALPN protocol after the
	 * ClientHello has been processed. In a client these contain the
	 * protocol that the server selected once the ServerHello has been
	 * processed.
	 */
	unsigned char *alpn_selected;
	size_t alpn_selected_len;
} SSL3_STATE_INTERNAL;
#define S3I(s) (s->s3->internal)

typedef struct dtls1_state_internal_st {
	unsigned int send_cookie;
	unsigned char cookie[DTLS1_COOKIE_LENGTH];
	unsigned char rcvd_cookie[DTLS1_COOKIE_LENGTH];
	unsigned int cookie_len;

	/*
	 * The current data and handshake epoch.  This is initially
	 * undefined, and starts at zero once the initial handshake is
	 * completed
	 */
	unsigned short r_epoch;
	unsigned short w_epoch;

	/* records being received in the current epoch */
	DTLS1_BITMAP bitmap;

	/* renegotiation starts a new set of sequence numbers */
	DTLS1_BITMAP next_bitmap;

	/* handshake message numbers */
	unsigned short handshake_write_seq;
	unsigned short next_handshake_write_seq;

	unsigned short handshake_read_seq;

	/* save last sequence number for retransmissions */
	unsigned char last_write_sequence[8];

	/* Received handshake records (processed and unprocessed) */
	record_pqueue unprocessed_rcds;
	record_pqueue processed_rcds;

	/* Buffered handshake messages */
	struct _pqueue *buffered_messages;

	/* Buffered application records.
	 * Only for records between CCS and Finished
	 * to prevent either protocol violation or
	 * unnecessary message loss.
	 */
	record_pqueue buffered_app_data;

	/* Is set when listening for new connections with dtls1_listen() */
	unsigned int listen;

	unsigned int mtu; /* max DTLS packet size */

	struct hm_header_st w_msg_hdr;
	struct hm_header_st r_msg_hdr;

	struct dtls1_timeout_st timeout;

	/* storage for Alert/Handshake protocol data received but not
	 * yet processed by ssl3_read_bytes: */
	unsigned char alert_fragment[DTLS1_AL_HEADER_LENGTH];
	unsigned int alert_fragment_len;
	unsigned char handshake_fragment[DTLS1_HM_HEADER_LENGTH];
	unsigned int handshake_fragment_len;

	unsigned int retransmitting;
	unsigned int change_cipher_spec_ok;
} DTLS1_STATE_INTERNAL;
#define D1I(s) (s->d1->internal)

typedef struct cert_pkey_st {
	X509 *x509;
	EVP_PKEY *privatekey;
	/* Digest to use when signing */
	const EVP_MD *digest;
} CERT_PKEY;

typedef struct cert_st {
	/* Current active set */
	CERT_PKEY *key; /* ALWAYS points to an element of the pkeys array
			 * Probably it would make more sense to store
			 * an index, not a pointer. */

	/* The following masks are for the key and auth
	 * algorithms that are supported by the certs below */
	int valid;
	unsigned long mask_k;
	unsigned long mask_a;

	DH *dh_tmp;
	DH *(*dh_tmp_cb)(SSL *ssl, int is_export, int keysize);
	int dh_tmp_auto;

	EC_KEY *ecdh_tmp;

	CERT_PKEY pkeys[SSL_PKEY_NUM];

	int references; /* >1 only if SSL_copy_session_id is used */
} CERT;


typedef struct sess_cert_st {
	STACK_OF(X509) *cert_chain; /* as received from peer */

	/* The 'peer_...' members are used only by clients. */
	int peer_cert_type;

	CERT_PKEY *peer_key; /* points to an element of peer_pkeys (never NULL!) */
	CERT_PKEY peer_pkeys[SSL_PKEY_NUM];
	/* Obviously we don't have the private keys of these,
	 * so maybe we shouldn't even use the CERT_PKEY type here. */

	DH *peer_dh_tmp;
	EC_KEY *peer_ecdh_tmp;
	uint8_t *peer_x25519_tmp;

	int references; /* actually always 1 at the moment */
} SESS_CERT;

/*#define SSL_DEBUG	*/
/*#define RSA_DEBUG	*/

typedef struct ssl3_enc_method {
	int (*enc)(SSL *, int);
	unsigned int enc_flags;
} SSL3_ENC_METHOD;

/*
 * Flag values for enc_flags.
 */

/* Uses explicit IV. */
#define SSL_ENC_FLAG_EXPLICIT_IV        (1 << 0)

/* Uses signature algorithms extension. */
#define SSL_ENC_FLAG_SIGALGS            (1 << 1)

/* Uses SHA256 default PRF. */
#define SSL_ENC_FLAG_SHA256_PRF         (1 << 2)

/* Allow TLS 1.2 ciphersuites: applies to DTLS 1.2 as well as TLS 1.2. */
#define SSL_ENC_FLAG_TLS1_2_CIPHERS     (1 << 4)

/*
 * ssl_aead_ctx_st contains information about an AEAD that is being used to
 * encrypt an SSL connection.
 */
struct ssl_aead_ctx_st {
	EVP_AEAD_CTX ctx;
	/*
	 * fixed_nonce contains any bytes of the nonce that are fixed for all
	 * records.
	 */
	unsigned char fixed_nonce[12];
	unsigned char fixed_nonce_len;
	unsigned char variable_nonce_len;
	unsigned char xor_fixed_nonce;
	unsigned char tag_len;
	/*
	 * variable_nonce_in_record is non-zero if the variable nonce
	 * for a record is included as a prefix before the ciphertext.
	 */
	char variable_nonce_in_record;
};

extern SSL_CIPHER ssl3_ciphers[];

const char *ssl_version_string(int ver);
int ssl_enabled_version_range(SSL *s, uint16_t *min_ver, uint16_t *max_ver);
int ssl_supported_version_range(SSL *s, uint16_t *min_ver, uint16_t *max_ver);
int ssl_max_shared_version(SSL *s, uint16_t peer_ver, uint16_t *max_ver);
int ssl_version_set_min(const SSL_METHOD *meth, uint16_t ver, uint16_t max_ver,
    uint16_t *out_ver);
int ssl_version_set_max(const SSL_METHOD *meth, uint16_t ver, uint16_t min_ver,
    uint16_t *out_ver);
uint16_t ssl_max_server_version(SSL *s);

const SSL_METHOD *dtls1_get_client_method(int ver);
const SSL_METHOD *dtls1_get_server_method(int ver);
const SSL_METHOD *tls1_get_client_method(int ver);
const SSL_METHOD *tls1_get_server_method(int ver);

extern SSL3_ENC_METHOD DTLSv1_enc_data;
extern SSL3_ENC_METHOD TLSv1_enc_data;
extern SSL3_ENC_METHOD TLSv1_1_enc_data;
extern SSL3_ENC_METHOD TLSv1_2_enc_data;

void ssl_clear_cipher_ctx(SSL *s);
int ssl_clear_bad_session(SSL *s);
CERT *ssl_cert_new(void);
CERT *ssl_cert_dup(CERT *cert);
int ssl_cert_inst(CERT **o);
void ssl_cert_free(CERT *c);
SESS_CERT *ssl_sess_cert_new(void);
void ssl_sess_cert_free(SESS_CERT *sc);
int ssl_get_new_session(SSL *s, int session);
int ssl_get_prev_session(SSL *s, unsigned char *session, int len,
    const unsigned char *limit);
int ssl_cipher_id_cmp(const SSL_CIPHER *a, const SSL_CIPHER *b);
SSL_CIPHER *OBJ_bsearch_ssl_cipher_id(SSL_CIPHER *key, SSL_CIPHER const *base, int num);
int ssl_cipher_ptr_id_cmp(const SSL_CIPHER * const *ap,
    const SSL_CIPHER * const *bp);
STACK_OF(SSL_CIPHER) *ssl_bytes_to_cipher_list(SSL *s, const unsigned char *p,
    int num);
int ssl_cipher_list_to_bytes(SSL *s, STACK_OF(SSL_CIPHER) *sk,
    unsigned char *p, size_t maxlen, size_t *outlen);
STACK_OF(SSL_CIPHER) *ssl_create_cipher_list(const SSL_METHOD *meth,
    STACK_OF(SSL_CIPHER) **pref, STACK_OF(SSL_CIPHER) **sorted,
    const char *rule_str);
void ssl_update_cache(SSL *s, int mode);
int ssl_cipher_get_evp(const SSL_SESSION *s, const EVP_CIPHER **enc,
    const EVP_MD **md, int *mac_pkey_type, int *mac_secret_size);
int ssl_cipher_get_evp_aead(const SSL_SESSION *s, const EVP_AEAD **aead);
int ssl_get_handshake_evp_md(SSL *s, const EVP_MD **md);

int ssl_verify_cert_chain(SSL *s, STACK_OF(X509) *sk);
int ssl_undefined_function(SSL *s);
int ssl_undefined_void_function(void);
int ssl_undefined_const_function(const SSL *s);
CERT_PKEY *ssl_get_server_send_pkey(const SSL *s);
X509 *ssl_get_server_send_cert(const SSL *);
EVP_PKEY *ssl_get_sign_pkey(SSL *s, const SSL_CIPHER *c, const EVP_MD **pmd);
DH *ssl_get_auto_dh(SSL *s);
int ssl_cert_type(X509 *x, EVP_PKEY *pkey);
void ssl_set_cert_masks(CERT *c, const SSL_CIPHER *cipher);
STACK_OF(SSL_CIPHER) *ssl_get_ciphers_by_id(SSL *s);
int ssl_has_ecc_ciphers(SSL *s);
int ssl_verify_alarm_type(long type);
void ssl_load_ciphers(void);

const SSL_CIPHER *ssl3_get_cipher_by_char(const unsigned char *p);
int ssl3_put_cipher_by_char(const SSL_CIPHER *c, unsigned char *p);
int ssl3_send_server_certificate(SSL *s);
int ssl3_send_newsession_ticket(SSL *s);
int ssl3_send_cert_status(SSL *s);
int ssl3_get_finished(SSL *s, int state_a, int state_b);
int ssl3_send_change_cipher_spec(SSL *s, int state_a, int state_b);
int ssl3_do_write(SSL *s, int type);
int ssl3_send_alert(SSL *s, int level, int desc);
int ssl3_get_req_cert_types(SSL *s, CBB *cbb);
long ssl3_get_message(SSL *s, int st1, int stn, int mt, long max, int *ok);
int ssl3_send_finished(SSL *s, int a, int b, const char *sender, int slen);
int ssl3_num_ciphers(void);
const SSL_CIPHER *ssl3_get_cipher(unsigned int u);
const SSL_CIPHER *ssl3_get_cipher_by_id(unsigned int id);
const SSL_CIPHER *ssl3_get_cipher_by_value(uint16_t value);
uint16_t ssl3_cipher_get_value(const SSL_CIPHER *c);
int ssl3_renegotiate(SSL *ssl);

int ssl3_renegotiate_check(SSL *ssl);

int ssl3_dispatch_alert(SSL *s);
int ssl3_read_bytes(SSL *s, int type, unsigned char *buf, int len, int peek);
int ssl3_write_bytes(SSL *s, int type, const void *buf, int len);
int ssl3_output_cert_chain(SSL *s, CBB *cbb, X509 *x);
SSL_CIPHER *ssl3_choose_cipher(SSL *ssl, STACK_OF(SSL_CIPHER) *clnt,
    STACK_OF(SSL_CIPHER) *srvr);
int	ssl3_setup_buffers(SSL *s);
int	ssl3_setup_init_buffer(SSL *s);
int	ssl3_setup_read_buffer(SSL *s);
int	ssl3_setup_write_buffer(SSL *s);
int	ssl3_release_read_buffer(SSL *s);
int	ssl3_release_write_buffer(SSL *s);
int	ssl3_new(SSL *s);
void	ssl3_free(SSL *s);
int	ssl3_accept(SSL *s);
int	ssl3_connect(SSL *s);
int	ssl3_read(SSL *s, void *buf, int len);
int	ssl3_peek(SSL *s, void *buf, int len);
int	ssl3_write(SSL *s, const void *buf, int len);
int	ssl3_shutdown(SSL *s);
void	ssl3_clear(SSL *s);
long	ssl3_ctrl(SSL *s, int cmd, long larg, void *parg);
long	ssl3_ctx_ctrl(SSL_CTX *s, int cmd, long larg, void *parg);
long	ssl3_callback_ctrl(SSL *s, int cmd, void (*fp)(void));
long	ssl3_ctx_callback_ctrl(SSL_CTX *s, int cmd, void (*fp)(void));
int	ssl3_pending(const SSL *s);

int ssl3_handshake_msg_hdr_len(SSL *s);
unsigned char *ssl3_handshake_msg_start(SSL *s, uint8_t htype);
void ssl3_handshake_msg_finish(SSL *s, unsigned int len);
int ssl3_handshake_msg_start_cbb(SSL *s, CBB *handshake, CBB *body,
    uint8_t msg_type);
int ssl3_handshake_msg_finish_cbb(SSL *s, CBB *handshake);
int ssl3_handshake_write(SSL *s);

void tls1_record_sequence_increment(unsigned char *seq);
int ssl3_do_change_cipher_spec(SSL *ssl);

int ssl23_read(SSL *s, void *buf, int len);
int ssl23_peek(SSL *s, void *buf, int len);
int ssl23_write(SSL *s, const void *buf, int len);
long ssl23_default_timeout(void);

long tls1_default_timeout(void);
int dtls1_do_write(SSL *s, int type);
int ssl3_packet_read(SSL *s, int plen);
int ssl3_packet_extend(SSL *s, int plen);
int ssl_server_legacy_first_packet(SSL *s);
int dtls1_read_bytes(SSL *s, int type, unsigned char *buf, int len, int peek);
int ssl3_write_pending(SSL *s, int type, const unsigned char *buf,
    unsigned int len);
void dtls1_set_message_header(SSL *s, unsigned char mt, unsigned long len,
    unsigned long frag_off, unsigned long frag_len);

int dtls1_write_app_data_bytes(SSL *s, int type, const void *buf, int len);
int dtls1_write_bytes(SSL *s, int type, const void *buf, int len);

int dtls1_send_change_cipher_spec(SSL *s, int a, int b);
unsigned long dtls1_output_cert_chain(SSL *s, X509 *x);
int dtls1_read_failed(SSL *s, int code);
int dtls1_buffer_message(SSL *s, int ccs);
int dtls1_retransmit_message(SSL *s, unsigned short seq,
    unsigned long frag_off, int *found);
int dtls1_get_queue_priority(unsigned short seq, int is_ccs);
int dtls1_retransmit_buffered_messages(SSL *s);
void dtls1_clear_record_buffer(SSL *s);
int dtls1_get_message_header(unsigned char *data,
    struct hm_header_st *msg_hdr);
void dtls1_get_ccs_header(unsigned char *data, struct ccs_header_st *ccs_hdr);
void dtls1_reset_seq_numbers(SSL *s, int rw);
void dtls1_build_sequence_number(unsigned char *dst, unsigned char *seq,
    unsigned short epoch);
long dtls1_default_timeout(void);
struct timeval* dtls1_get_timeout(SSL *s, struct timeval* timeleft);
int dtls1_check_timeout_num(SSL *s);
int dtls1_handle_timeout(SSL *s);
const SSL_CIPHER *dtls1_get_cipher(unsigned int u);
void dtls1_start_timer(SSL *s);
void dtls1_stop_timer(SSL *s);
int dtls1_is_timer_expired(SSL *s);
void dtls1_double_timeout(SSL *s);
unsigned int dtls1_min_mtu(void);

/* some client-only functions */
int ssl3_client_hello(SSL *s);
int ssl3_get_server_hello(SSL *s);
int ssl3_get_certificate_request(SSL *s);
int ssl3_get_new_session_ticket(SSL *s);
int ssl3_get_cert_status(SSL *s);
int ssl3_get_server_done(SSL *s);
int ssl3_send_client_verify(SSL *s);
int ssl3_send_client_certificate(SSL *s);
int ssl_do_client_cert_cb(SSL *s, X509 **px509, EVP_PKEY **ppkey);
int ssl3_send_client_key_exchange(SSL *s);
int ssl3_get_server_key_exchange(SSL *s);
int ssl3_get_server_certificate(SSL *s);
int ssl3_check_cert_and_algorithm(SSL *s);
int ssl3_check_finished(SSL *s);

/* some server-only functions */
int ssl3_get_client_hello(SSL *s);
int ssl3_send_server_hello(SSL *s);
int ssl3_send_hello_request(SSL *s);
int ssl3_send_server_key_exchange(SSL *s);
int ssl3_send_certificate_request(SSL *s);
int ssl3_send_server_done(SSL *s);
int ssl3_get_client_certificate(SSL *s);
int ssl3_get_client_key_exchange(SSL *s);
int ssl3_get_cert_verify(SSL *s);

int ssl23_accept(SSL *s);
int ssl23_connect(SSL *s);
int ssl23_read_bytes(SSL *s, int n);
int ssl23_write_bytes(SSL *s);

int tls1_new(SSL *s);
void tls1_free(SSL *s);
void tls1_clear(SSL *s);

int dtls1_new(SSL *s);
int dtls1_accept(SSL *s);
int dtls1_connect(SSL *s);
void dtls1_free(SSL *s);
void dtls1_clear(SSL *s);
long dtls1_ctrl(SSL *s, int cmd, long larg, void *parg);
int dtls1_shutdown(SSL *s);

long dtls1_get_message(SSL *s, int st1, int stn, int mt, long max, int *ok);
int dtls1_get_record(SSL *s);
int dtls1_dispatch_alert(SSL *s);
int dtls1_enc(SSL *s, int snd);

int ssl_init_wbio_buffer(SSL *s, int push);
void ssl_free_wbio_buffer(SSL *s);

int tls1_handshake_hash_init(SSL *s);
int tls1_handshake_hash_update(SSL *s, const unsigned char *buf, size_t len);
int tls1_handshake_hash_value(SSL *s, const unsigned char *out, size_t len,
    size_t *outlen);
void tls1_handshake_hash_free(SSL *s);

int tls1_init_finished_mac(SSL *s);
int tls1_finish_mac(SSL *s, const unsigned char *buf, int len);
void tls1_free_digest_list(SSL *s);
void tls1_cleanup_key_block(SSL *s);
int tls1_digest_cached_records(SSL *s);
int tls1_change_cipher_state(SSL *s, int which);
int tls1_setup_key_block(SSL *s);
int tls1_enc(SSL *s, int snd);
int tls1_final_finish_mac(SSL *s, const char *str, int slen, unsigned char *p);
int tls1_mac(SSL *ssl, unsigned char *md, int snd);
int tls1_generate_master_secret(SSL *s, unsigned char *out,
    unsigned char *p, int len);
int tls1_export_keying_material(SSL *s, unsigned char *out, size_t olen,
    const char *label, size_t llen, const unsigned char *p, size_t plen,
    int use_context);
int tls1_alert_code(int code);
int ssl_ok(SSL *s);

int ssl_using_ecc_cipher(SSL *s);
int ssl_check_srvr_ecc_cert_and_alg(X509 *x, SSL *s);

int tls1_set_groups(uint16_t **out_group_ids, size_t *out_group_ids_len,
    const int *groups, size_t ngroups);
int tls1_set_groups_list(uint16_t **out_group_ids, size_t *out_group_ids_len,
    const char *groups);

int tls1_ec_curve_id2nid(const uint16_t curve_id);
uint16_t tls1_ec_nid2curve_id(const int nid);
int tls1_check_curve(SSL *s, const uint16_t curve_id);
int tls1_get_shared_curve(SSL *s);

unsigned char *ssl_add_clienthello_tlsext(SSL *s, unsigned char *p,
    unsigned char *limit);

unsigned char *ssl_add_serverhello_tlsext(SSL *s, unsigned char *p,
    unsigned char *limit);

int ssl_parse_clienthello_tlsext(SSL *s, unsigned char **data,
    unsigned char *d, int n, int *al);
int ssl_parse_serverhello_tlsext(SSL *s, unsigned char **data,
    size_t n, int *al);
int ssl_check_clienthello_tlsext_early(SSL *s);
int ssl_check_clienthello_tlsext_late(SSL *s);
int ssl_check_serverhello_tlsext(SSL *s);

#define tlsext_tick_md	EVP_sha256
int tls1_process_ticket(SSL *s, const unsigned char *session_id, int len,
    const unsigned char *limit, SSL_SESSION **ret);
int tls12_get_sigandhash(unsigned char *p, const EVP_PKEY *pk,
    const EVP_MD *md);
int tls12_get_sigid(const EVP_PKEY *pk);
const EVP_MD *tls12_get_hash(unsigned char hash_alg);

void ssl_clear_hash_ctx(EVP_MD_CTX **hash);
long ssl_get_algorithm2(SSL *s);
int tls1_process_sigalgs(SSL *s, CBS *cbs);
void tls12_get_req_sig_algs(SSL *s, unsigned char **sigalgs,
    size_t *sigalgs_len);

int tls1_check_ec_server_key(SSL *s);
int tls1_check_ec_tmp_key(SSL *s);

int ssl_add_clienthello_use_srtp_ext(SSL *s, unsigned char *p,
    int *len, int maxlen);
int ssl_parse_clienthello_use_srtp_ext(SSL *s, const unsigned char *d,
    int len, int *al);
int ssl_add_serverhello_use_srtp_ext(SSL *s, unsigned char *p,
    int *len, int maxlen);
int ssl_parse_serverhello_use_srtp_ext(SSL *s, const unsigned char *d,
    int len, int *al);

/* s3_cbc.c */
void ssl3_cbc_copy_mac(unsigned char *out, const SSL3_RECORD *rec,
    unsigned md_size, unsigned orig_len);
int tls1_cbc_remove_padding(const SSL *s, SSL3_RECORD *rec,
    unsigned block_size, unsigned mac_size);
char ssl3_cbc_record_digest_supported(const EVP_MD_CTX *ctx);
int ssl3_cbc_digest_record(const EVP_MD_CTX *ctx, unsigned char *md_out,
    size_t *md_out_size, const unsigned char header[13],
    const unsigned char *data, size_t data_plus_mac_size,
    size_t data_plus_mac_plus_padding_size, const unsigned char *mac_secret,
    unsigned mac_secret_length);
int SSL_state_func_code(int _state);

#define SSLerror(s, r) SSL_error_internal(s, r, __FILE__, __LINE__)
#define SSLerrorx(r) ERR_PUT_error(ERR_LIB_SSL,(0xfff),(r),__FILE__,__LINE__)
void SSL_error_internal(const SSL *s, int r, char *f, int l);

void tls1_get_formatlist(SSL *s, int client_formats, const uint8_t **pformats,
    size_t *pformatslen);
void tls1_get_curvelist(SSL *s, int client_curves, const uint16_t **pcurves,
    size_t *pcurveslen);

#ifndef OPENSSL_NO_SRTP

int srtp_find_profile_by_name(char *profile_name,
    SRTP_PROTECTION_PROFILE **pptr, unsigned len);
int srtp_find_profile_by_num(unsigned profile_num,
    SRTP_PROTECTION_PROFILE **pptr);

#endif /* OPENSSL_NO_SRTP */

__END_HIDDEN_DECLS

#endif
