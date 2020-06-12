/* $OpenBSD: ssl.h,v 1.134 2017/08/30 16:24:21 jsing Exp $ */
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

#ifndef HEADER_SSL_H
#define HEADER_SSL_H

#include <stdint.h>

#include <openssl/opensslconf.h>
#include <openssl/hmac.h>
#include <openssl/pem.h>
#include <openssl/safestack.h>

#include <openssl/bio.h>

#ifndef OPENSSL_NO_DEPRECATED
#include <openssl/buffer.h>
#include <openssl/crypto.h>
#include <openssl/lhash.h>

#ifndef OPENSSL_NO_X509
#include <openssl/x509.h>
#endif
#endif

#ifdef  __cplusplus
extern "C" {
#endif

/* SSLeay version number for ASN.1 encoding of the session information */
/* Version 0 - initial version
 * Version 1 - added the optional peer certificate
 */
#define SSL_SESSION_ASN1_VERSION 0x0001

/* text strings for the ciphers */
#define SSL_TXT_NULL_WITH_MD5		SSL2_TXT_NULL_WITH_MD5
#define SSL_TXT_RC4_128_WITH_MD5	SSL2_TXT_RC4_128_WITH_MD5
#define SSL_TXT_RC4_128_EXPORT40_WITH_MD5 SSL2_TXT_RC4_128_EXPORT40_WITH_MD5
#define SSL_TXT_RC2_128_CBC_WITH_MD5	SSL2_TXT_RC2_128_CBC_WITH_MD5
#define SSL_TXT_RC2_128_CBC_EXPORT40_WITH_MD5 SSL2_TXT_RC2_128_CBC_EXPORT40_WITH_MD5
#define SSL_TXT_IDEA_128_CBC_WITH_MD5	SSL2_TXT_IDEA_128_CBC_WITH_MD5
#define SSL_TXT_DES_64_CBC_WITH_MD5	SSL2_TXT_DES_64_CBC_WITH_MD5
#define SSL_TXT_DES_64_CBC_WITH_SHA	SSL2_TXT_DES_64_CBC_WITH_SHA
#define SSL_TXT_DES_192_EDE3_CBC_WITH_MD5 SSL2_TXT_DES_192_EDE3_CBC_WITH_MD5
#define SSL_TXT_DES_192_EDE3_CBC_WITH_SHA SSL2_TXT_DES_192_EDE3_CBC_WITH_SHA

/*    VRS Additional Kerberos5 entries
 */
#define SSL_TXT_KRB5_DES_64_CBC_SHA   SSL3_TXT_KRB5_DES_64_CBC_SHA
#define SSL_TXT_KRB5_DES_192_CBC3_SHA SSL3_TXT_KRB5_DES_192_CBC3_SHA
#define SSL_TXT_KRB5_RC4_128_SHA      SSL3_TXT_KRB5_RC4_128_SHA
#define SSL_TXT_KRB5_IDEA_128_CBC_SHA SSL3_TXT_KRB5_IDEA_128_CBC_SHA
#define SSL_TXT_KRB5_DES_64_CBC_MD5   SSL3_TXT_KRB5_DES_64_CBC_MD5
#define SSL_TXT_KRB5_DES_192_CBC3_MD5 SSL3_TXT_KRB5_DES_192_CBC3_MD5
#define SSL_TXT_KRB5_RC4_128_MD5      SSL3_TXT_KRB5_RC4_128_MD5
#define SSL_TXT_KRB5_IDEA_128_CBC_MD5 SSL3_TXT_KRB5_IDEA_128_CBC_MD5

#define SSL_TXT_KRB5_DES_40_CBC_SHA   SSL3_TXT_KRB5_DES_40_CBC_SHA
#define SSL_TXT_KRB5_RC2_40_CBC_SHA   SSL3_TXT_KRB5_RC2_40_CBC_SHA
#define SSL_TXT_KRB5_RC4_40_SHA	      SSL3_TXT_KRB5_RC4_40_SHA
#define SSL_TXT_KRB5_DES_40_CBC_MD5   SSL3_TXT_KRB5_DES_40_CBC_MD5
#define SSL_TXT_KRB5_RC2_40_CBC_MD5   SSL3_TXT_KRB5_RC2_40_CBC_MD5
#define SSL_TXT_KRB5_RC4_40_MD5	      SSL3_TXT_KRB5_RC4_40_MD5

#define SSL_TXT_KRB5_DES_40_CBC_SHA   SSL3_TXT_KRB5_DES_40_CBC_SHA
#define SSL_TXT_KRB5_DES_40_CBC_MD5   SSL3_TXT_KRB5_DES_40_CBC_MD5
#define SSL_TXT_KRB5_DES_64_CBC_SHA   SSL3_TXT_KRB5_DES_64_CBC_SHA
#define SSL_TXT_KRB5_DES_64_CBC_MD5   SSL3_TXT_KRB5_DES_64_CBC_MD5
#define SSL_TXT_KRB5_DES_192_CBC3_SHA SSL3_TXT_KRB5_DES_192_CBC3_SHA
#define SSL_TXT_KRB5_DES_192_CBC3_MD5 SSL3_TXT_KRB5_DES_192_CBC3_MD5
#define SSL_MAX_KRB5_PRINCIPAL_LENGTH  256

#define SSL_MAX_SSL_SESSION_ID_LENGTH		32
#define SSL_MAX_SID_CTX_LENGTH			32

#define SSL_MIN_RSA_MODULUS_LENGTH_IN_BYTES	(512/8)
#define SSL_MAX_KEY_ARG_LENGTH			8
#define SSL_MAX_MASTER_KEY_LENGTH		48


/* These are used to specify which ciphers to use and not to use */

#define SSL_TXT_LOW		"LOW"
#define SSL_TXT_MEDIUM		"MEDIUM"
#define SSL_TXT_HIGH		"HIGH"

#define SSL_TXT_kFZA		"kFZA" /* unused! */
#define	SSL_TXT_aFZA		"aFZA" /* unused! */
#define SSL_TXT_eFZA		"eFZA" /* unused! */
#define SSL_TXT_FZA		"FZA"  /* unused! */

#define	SSL_TXT_aNULL		"aNULL"
#define	SSL_TXT_eNULL		"eNULL"
#define	SSL_TXT_NULL		"NULL"

#define SSL_TXT_kRSA		"kRSA"
#define SSL_TXT_kDHr		"kDHr" /* no such ciphersuites supported! */
#define SSL_TXT_kDHd		"kDHd" /* no such ciphersuites supported! */
#define SSL_TXT_kDH 		"kDH"  /* no such ciphersuites supported! */
#define SSL_TXT_kEDH		"kEDH"
#define SSL_TXT_kKRB5     	"kKRB5"
#define SSL_TXT_kECDHr		"kECDHr"
#define SSL_TXT_kECDHe		"kECDHe"
#define SSL_TXT_kECDH		"kECDH"
#define SSL_TXT_kEECDH		"kEECDH"
#define SSL_TXT_kPSK            "kPSK"
#define SSL_TXT_kGOST		"kGOST"
#define SSL_TXT_kSRP		"kSRP"

#define	SSL_TXT_aRSA		"aRSA"
#define	SSL_TXT_aDSS		"aDSS"
#define	SSL_TXT_aDH		"aDH" /* no such ciphersuites supported! */
#define	SSL_TXT_aECDH		"aECDH"
#define SSL_TXT_aKRB5     	"aKRB5"
#define SSL_TXT_aECDSA		"aECDSA"
#define SSL_TXT_aPSK            "aPSK"
#define SSL_TXT_aGOST94		"aGOST94"
#define SSL_TXT_aGOST01		"aGOST01"
#define SSL_TXT_aGOST		"aGOST"

#define	SSL_TXT_DSS		"DSS"
#define SSL_TXT_DH		"DH"
#define SSL_TXT_DHE		"DHE" /* same as "kDHE:-ADH" */
#define SSL_TXT_EDH		"EDH" /* previous name for DHE */
#define SSL_TXT_ADH		"ADH"
#define SSL_TXT_RSA		"RSA"
#define SSL_TXT_ECDH		"ECDH"
#define SSL_TXT_ECDHE		"ECDHE" /* same as "kECDHE:-AECDH" */
#define SSL_TXT_EECDH		"EECDH" /* previous name for ECDHE */
#define SSL_TXT_AECDH		"AECDH"
#define SSL_TXT_ECDSA		"ECDSA"
#define SSL_TXT_KRB5      	"KRB5"
#define SSL_TXT_PSK             "PSK"
#define SSL_TXT_SRP		"SRP"

#define SSL_TXT_DES		"DES"
#define SSL_TXT_3DES		"3DES"
#define SSL_TXT_RC4		"RC4"
#define SSL_TXT_RC2		"RC2"
#define SSL_TXT_IDEA		"IDEA"
#define SSL_TXT_SEED		"SEED"
#define SSL_TXT_AES128		"AES128"
#define SSL_TXT_AES256		"AES256"
#define SSL_TXT_AES		"AES"
#define SSL_TXT_AES_GCM		"AESGCM"
#define SSL_TXT_CAMELLIA128	"CAMELLIA128"
#define SSL_TXT_CAMELLIA256	"CAMELLIA256"
#define SSL_TXT_CAMELLIA	"CAMELLIA"
#define SSL_TXT_CHACHA20	"CHACHA20"

#define SSL_TXT_AEAD		"AEAD"
#define SSL_TXT_MD5		"MD5"
#define SSL_TXT_SHA1		"SHA1"
#define SSL_TXT_SHA		"SHA" /* same as "SHA1" */
#define SSL_TXT_GOST94		"GOST94"
#define SSL_TXT_GOST89MAC		"GOST89MAC"
#define SSL_TXT_SHA256		"SHA256"
#define SSL_TXT_SHA384		"SHA384"
#define SSL_TXT_STREEBOG256		"STREEBOG256"
#define SSL_TXT_STREEBOG512		"STREEBOG512"

#define SSL_TXT_DTLS1		"DTLSv1"
#define SSL_TXT_SSLV2		"SSLv2"
#define SSL_TXT_SSLV3		"SSLv3"
#define SSL_TXT_TLSV1		"TLSv1"
#define SSL_TXT_TLSV1_1		"TLSv1.1"
#define SSL_TXT_TLSV1_2		"TLSv1.2"

#define SSL_TXT_EXP		"EXP"
#define SSL_TXT_EXPORT		"EXPORT"

#define SSL_TXT_ALL		"ALL"

/*
 * COMPLEMENTOF* definitions. These identifiers are used to (de-select)
 * ciphers normally not being used.
 * Example: "RC4" will activate all ciphers using RC4 including ciphers
 * without authentication, which would normally disabled by DEFAULT (due
 * the "!ADH" being part of default). Therefore "RC4:!COMPLEMENTOFDEFAULT"
 * will make sure that it is also disabled in the specific selection.
 * COMPLEMENTOF* identifiers are portable between version, as adjustments
 * to the default cipher setup will also be included here.
 *
 * COMPLEMENTOFDEFAULT does not experience the same special treatment that
 * DEFAULT gets, as only selection is being done and no sorting as needed
 * for DEFAULT.
 */
#define SSL_TXT_CMPALL		"COMPLEMENTOFALL"
#define SSL_TXT_CMPDEF		"COMPLEMENTOFDEFAULT"

/* The following cipher list is used by default.
 * It also is substituted when an application-defined cipher list string
 * starts with 'DEFAULT'. */
#define SSL_DEFAULT_CIPHER_LIST	"ALL:!aNULL:!eNULL:!SSLv2"
/* As of OpenSSL 1.0.0, ssl_create_cipher_list() in ssl/ssl_ciph.c always
 * starts with a reasonable order, and all we have to do for DEFAULT is
 * throwing out anonymous and unencrypted ciphersuites!
 * (The latter are not actually enabled by ALL, but "ALL:RSA" would enable
 * some of them.)
 */

/* Used in SSL_set_shutdown()/SSL_get_shutdown(); */
#define SSL_SENT_SHUTDOWN	1
#define SSL_RECEIVED_SHUTDOWN	2


#define SSL_FILETYPE_ASN1	X509_FILETYPE_ASN1
#define SSL_FILETYPE_PEM	X509_FILETYPE_PEM

/* This is needed to stop compilers complaining about the
 * 'struct ssl_st *' function parameters used to prototype callbacks
 * in SSL_CTX. */
typedef struct ssl_st *ssl_crock_st;

typedef struct tls_session_ticket_ext_st TLS_SESSION_TICKET_EXT;
typedef struct ssl_method_st SSL_METHOD;
typedef struct ssl_cipher_st SSL_CIPHER;
typedef struct ssl_session_st SSL_SESSION;

DECLARE_STACK_OF(SSL_CIPHER)

/* SRTP protection profiles for use with the use_srtp extension (RFC 5764)*/
typedef struct srtp_protection_profile_st {
	const char *name;
	unsigned long id;
} SRTP_PROTECTION_PROFILE;

DECLARE_STACK_OF(SRTP_PROTECTION_PROFILE)

typedef int (*tls_session_ticket_ext_cb_fn)(SSL *s, const unsigned char *data,
    int len, void *arg);
typedef int (*tls_session_secret_cb_fn)(SSL *s, void *secret, int *secret_len,
    STACK_OF(SSL_CIPHER) *peer_ciphers, SSL_CIPHER **cipher, void *arg);

#ifndef OPENSSL_NO_SSL_INTERN

/* used to hold info on the particular ciphers used */
struct ssl_cipher_st {
	int valid;
	const char *name;		/* text name */
	unsigned long id;		/* id, 4 bytes, first is version */

	unsigned long algorithm_mkey;	/* key exchange algorithm */
	unsigned long algorithm_auth;	/* server authentication */
	unsigned long algorithm_enc;	/* symmetric encryption */
	unsigned long algorithm_mac;	/* symmetric authentication */
	unsigned long algorithm_ssl;	/* (major) protocol version */

	unsigned long algo_strength;	/* strength and export flags */
	unsigned long algorithm2;	/* Extra flags */
	int strength_bits;		/* Number of bits really used */
	int alg_bits;			/* Number of bits for algorithm */
};


/* Used to hold functions for SSLv3/TLSv1 functions */
struct ssl_method_internal_st;

struct ssl_method_st {
	int (*ssl_dispatch_alert)(SSL *s);
	int (*num_ciphers)(void);
	const SSL_CIPHER *(*get_cipher)(unsigned ncipher);
	const SSL_CIPHER *(*get_cipher_by_char)(const unsigned char *ptr);
	int (*put_cipher_by_char)(const SSL_CIPHER *cipher, unsigned char *ptr);

	const struct ssl_method_internal_st *internal;
};

/* Lets make this into an ASN.1 type structure as follows
 * SSL_SESSION_ID ::= SEQUENCE {
 *	version 		INTEGER,	-- structure version number
 *	SSLversion 		INTEGER,	-- SSL version number
 *	Cipher 			OCTET STRING,	-- the 3 byte cipher ID
 *	Session_ID 		OCTET STRING,	-- the Session ID
 *	Master_key 		OCTET STRING,	-- the master key
 *	KRB5_principal		OCTET STRING	-- optional Kerberos principal
 *	Time [ 1 ] EXPLICIT	INTEGER,	-- optional Start Time
 *	Timeout [ 2 ] EXPLICIT	INTEGER,	-- optional Timeout ins seconds
 *	Peer [ 3 ] EXPLICIT	X509,		-- optional Peer Certificate
 *	Session_ID_context [ 4 ] EXPLICIT OCTET STRING,   -- the Session ID context
 *	Verify_result [ 5 ] EXPLICIT INTEGER,   -- X509_V_... code for `Peer'
 *	HostName [ 6 ] EXPLICIT OCTET STRING,   -- optional HostName from servername TLS extension
 *	PSK_identity_hint [ 7 ] EXPLICIT OCTET STRING, -- optional PSK identity hint
 *	PSK_identity [ 8 ] EXPLICIT OCTET STRING,  -- optional PSK identity
 *	Ticket_lifetime_hint [9] EXPLICIT INTEGER, -- server's lifetime hint for session ticket
 *	Ticket [10]             EXPLICIT OCTET STRING, -- session ticket (clients only)
 *	Compression_meth [11]   EXPLICIT OCTET STRING, -- optional compression method
 *	SRP_username [ 12 ] EXPLICIT OCTET STRING -- optional SRP username
 *	}
 * Look in ssl/ssl_asn1.c for more details
 * I'm using EXPLICIT tags so I can read the damn things using asn1parse :-).
 */
struct ssl_session_internal_st;

struct ssl_session_st {
	int ssl_version;	/* what ssl version session info is
				 * being kept in here? */

	int master_key_length;
	unsigned char master_key[SSL_MAX_MASTER_KEY_LENGTH];

	/* session_id - valid? */
	unsigned int session_id_length;
	unsigned char session_id[SSL_MAX_SSL_SESSION_ID_LENGTH];

	/* this is used to determine whether the session is being reused in
	 * the appropriate context. It is up to the application to set this,
	 * via SSL_new */
	unsigned int sid_ctx_length;
	unsigned char sid_ctx[SSL_MAX_SID_CTX_LENGTH];

	/* This is the cert for the other end. */
	X509 *peer;

	/* when app_verify_callback accepts a session where the peer's certificate
	 * is not ok, we must remember the error for session reuse: */
	long verify_result; /* only for servers */

	long timeout;
	time_t time;
	int references;

	const SSL_CIPHER *cipher;
	unsigned long cipher_id;	/* when ASN.1 loaded, this
					 * needs to be used to load
					 * the 'cipher' structure */

	STACK_OF(SSL_CIPHER) *ciphers; /* shared ciphers? */

	char *tlsext_hostname;

	/* RFC4507 info */
	unsigned char *tlsext_tick;	/* Session ticket */
	size_t tlsext_ticklen;		/* Session ticket length */
	long tlsext_tick_lifetime_hint;	/* Session lifetime hint in seconds */

	struct ssl_session_internal_st *internal;
};

#endif

/* Allow initial connection to servers that don't support RI */
#define SSL_OP_LEGACY_SERVER_CONNECT			0x00000004L

/* Disable SSL 3.0/TLS 1.0 CBC vulnerability workaround that was added
 * in OpenSSL 0.9.6d.  Usually (depending on the application protocol)
 * the workaround is not needed.
 * Unfortunately some broken SSL/TLS implementations cannot handle it
 * at all, which is why it was previously included in SSL_OP_ALL.
 * Now it's not.
 */
#define SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS		0x00000800L

/* DTLS options */
#define SSL_OP_NO_QUERY_MTU				0x00001000L
/* Turn on Cookie Exchange (on relevant for servers) */
#define SSL_OP_COOKIE_EXCHANGE				0x00002000L
/* Don't use RFC4507 ticket extension */
#define SSL_OP_NO_TICKET				0x00004000L

/* As server, disallow session resumption on renegotiation */
#define SSL_OP_NO_SESSION_RESUMPTION_ON_RENEGOTIATION	0x00010000L
/* Disallow client initiated renegotiation. */
#define SSL_OP_NO_CLIENT_RENEGOTIATION			0x00020000L
/* If set, always create a new key when using tmp_dh parameters */
#define SSL_OP_SINGLE_DH_USE				0x00100000L
/* Set on servers to choose the cipher according to the server's
 * preferences */
#define SSL_OP_CIPHER_SERVER_PREFERENCE			0x00400000L
/* If set, a server will allow a client to issue a SSLv3.0 version number
 * as latest version supported in the premaster secret, even when TLSv1.0
 * (version 3.1) was announced in the client hello. Normally this is
 * forbidden to prevent version rollback attacks. */
#define SSL_OP_TLS_ROLLBACK_BUG				0x00800000L

#define SSL_OP_NO_TLSv1					0x04000000L
#define SSL_OP_NO_TLSv1_2				0x08000000L
#define SSL_OP_NO_TLSv1_1				0x10000000L

/* SSL_OP_ALL: various bug workarounds that should be rather harmless. */
#define SSL_OP_ALL \
    (SSL_OP_LEGACY_SERVER_CONNECT)

/* Obsolete flags kept for compatibility. No sane code should use them. */
#define SSL_OP_ALLOW_UNSAFE_LEGACY_RENEGOTIATION	0x0
#define SSL_OP_CISCO_ANYCONNECT				0x0
#define SSL_OP_CRYPTOPRO_TLSEXT_BUG			0x0
#define SSL_OP_EPHEMERAL_RSA				0x0
#define SSL_OP_MICROSOFT_BIG_SSLV3_BUFFER		0x0
#define SSL_OP_MICROSOFT_SESS_ID_BUG			0x0
#define SSL_OP_MSIE_SSLV2_RSA_PADDING			0x0
#define SSL_OP_NETSCAPE_CA_DN_BUG			0x0
#define SSL_OP_NETSCAPE_CHALLENGE_BUG			0x0
#define SSL_OP_NETSCAPE_DEMO_CIPHER_CHANGE_BUG		0x0
#define SSL_OP_NETSCAPE_REUSE_CIPHER_CHANGE_BUG		0x0
#define SSL_OP_NO_COMPRESSION				0x0
#define SSL_OP_NO_SSLv2					0x0
#define SSL_OP_NO_SSLv3					0x0
#define SSL_OP_PKCS1_CHECK_1				0x0
#define SSL_OP_PKCS1_CHECK_2				0x0
#define SSL_OP_SAFARI_ECDHE_ECDSA_BUG			0x0
#define SSL_OP_SINGLE_ECDH_USE				0x0
#define SSL_OP_SSLEAY_080_CLIENT_DH_BUG			0x0
#define SSL_OP_SSLREF2_REUSE_CERT_TYPE_BUG		0x0
#define SSL_OP_TLSEXT_PADDING				0x0
#define SSL_OP_TLS_BLOCK_PADDING_BUG			0x0
#define SSL_OP_TLS_D5_BUG				0x0

/* Allow SSL_write(..., n) to return r with 0 < r < n (i.e. report success
 * when just a single record has been written): */
#define SSL_MODE_ENABLE_PARTIAL_WRITE       0x00000001L
/* Make it possible to retry SSL_write() with changed buffer location
 * (buffer contents must stay the same!); this is not the default to avoid
 * the misconception that non-blocking SSL_write() behaves like
 * non-blocking write(): */
#define SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER 0x00000002L
/* Never bother the application with retries if the transport
 * is blocking: */
#define SSL_MODE_AUTO_RETRY 0x00000004L
/* Don't attempt to automatically build certificate chain */
#define SSL_MODE_NO_AUTO_CHAIN 0x00000008L
/* Save RAM by releasing read and write buffers when they're empty. (SSL3 and
 * TLS only.)  "Released" buffers are put onto a free-list in the context
 * or just freed (depending on the context's setting for freelist_max_len). */
#define SSL_MODE_RELEASE_BUFFERS 0x00000010L

/* Note: SSL[_CTX]_set_{options,mode} use |= op on the previous value,
 * they cannot be used to clear bits. */

#define SSL_CTX_set_options(ctx,op) \
	SSL_CTX_ctrl((ctx),SSL_CTRL_OPTIONS,(op),NULL)
#define SSL_CTX_clear_options(ctx,op) \
	SSL_CTX_ctrl((ctx),SSL_CTRL_CLEAR_OPTIONS,(op),NULL)
#define SSL_CTX_get_options(ctx) \
	SSL_CTX_ctrl((ctx),SSL_CTRL_OPTIONS,0,NULL)
#define SSL_set_options(ssl,op) \
	SSL_ctrl((ssl),SSL_CTRL_OPTIONS,(op),NULL)
#define SSL_clear_options(ssl,op) \
	SSL_ctrl((ssl),SSL_CTRL_CLEAR_OPTIONS,(op),NULL)
#define SSL_get_options(ssl) \
        SSL_ctrl((ssl),SSL_CTRL_OPTIONS,0,NULL)

#define SSL_CTX_set_mode(ctx,op) \
	SSL_CTX_ctrl((ctx),SSL_CTRL_MODE,(op),NULL)
#define SSL_CTX_clear_mode(ctx,op) \
	SSL_CTX_ctrl((ctx),SSL_CTRL_CLEAR_MODE,(op),NULL)
#define SSL_CTX_get_mode(ctx) \
	SSL_CTX_ctrl((ctx),SSL_CTRL_MODE,0,NULL)
#define SSL_clear_mode(ssl,op) \
	SSL_ctrl((ssl),SSL_CTRL_CLEAR_MODE,(op),NULL)
#define SSL_set_mode(ssl,op) \
	SSL_ctrl((ssl),SSL_CTRL_MODE,(op),NULL)
#define SSL_get_mode(ssl) \
        SSL_ctrl((ssl),SSL_CTRL_MODE,0,NULL)
#define SSL_set_mtu(ssl, mtu) \
        SSL_ctrl((ssl),SSL_CTRL_SET_MTU,(mtu),NULL)

#define SSL_get_secure_renegotiation_support(ssl) \
	SSL_ctrl((ssl), SSL_CTRL_GET_RI_SUPPORT, 0, NULL)

void SSL_CTX_set_msg_callback(SSL_CTX *ctx, void (*cb)(int write_p,
    int version, int content_type, const void *buf, size_t len, SSL *ssl,
    void *arg));
void SSL_set_msg_callback(SSL *ssl, void (*cb)(int write_p, int version,
    int content_type, const void *buf, size_t len, SSL *ssl, void *arg));
#define SSL_CTX_set_msg_callback_arg(ctx, arg) SSL_CTX_ctrl((ctx), SSL_CTRL_SET_MSG_CALLBACK_ARG, 0, (arg))
#define SSL_set_msg_callback_arg(ssl, arg) SSL_ctrl((ssl), SSL_CTRL_SET_MSG_CALLBACK_ARG, 0, (arg))

struct ssl_aead_ctx_st;
typedef struct ssl_aead_ctx_st SSL_AEAD_CTX;

#define SSL_MAX_CERT_LIST_DEFAULT 1024*100 /* 100k max cert list :-) */

#define SSL_SESSION_CACHE_MAX_SIZE_DEFAULT	(1024*20)

/* This callback type is used inside SSL_CTX, SSL, and in the functions that set
 * them. It is used to override the generation of SSL/TLS session IDs in a
 * server. Return value should be zero on an error, non-zero to proceed. Also,
 * callbacks should themselves check if the id they generate is unique otherwise
 * the SSL handshake will fail with an error - callbacks can do this using the
 * 'ssl' value they're passed by;
 *      SSL_has_matching_session_id(ssl, id, *id_len)
 * The length value passed in is set at the maximum size the session ID can be.
 * In SSLv2 this is 16 bytes, whereas SSLv3/TLSv1 it is 32 bytes. The callback
 * can alter this length to be less if desired, but under SSLv2 session IDs are
 * supposed to be fixed at 16 bytes so the id will be padded after the callback
 * returns in this case. It is also an error for the callback to set the size to
 * zero. */
typedef int (*GEN_SESSION_CB)(const SSL *ssl, unsigned char *id,
    unsigned int *id_len);

typedef struct ssl_comp_st SSL_COMP;

#ifndef OPENSSL_NO_SSL_INTERN

struct ssl_comp_st {
	int id;
	const char *name;
};

DECLARE_STACK_OF(SSL_COMP)
struct lhash_st_SSL_SESSION {
	int dummy;
};

struct ssl_ctx_internal_st;

struct ssl_ctx_st {
	const SSL_METHOD *method;

	STACK_OF(SSL_CIPHER) *cipher_list;

	struct x509_store_st /* X509_STORE */ *cert_store;

	/* If timeout is not 0, it is the default timeout value set
	 * when SSL_new() is called.  This has been put in to make
	 * life easier to set things up */
	long session_timeout;

	int references;

	/* Default values to use in SSL structures follow (these are copied by SSL_new) */

	STACK_OF(X509) *extra_certs;

	int verify_mode;
	unsigned int sid_ctx_length;
	unsigned char sid_ctx[SSL_MAX_SID_CTX_LENGTH];

	X509_VERIFY_PARAM *param;

	/*
	 * XXX
	 * default_passwd_cb used by python and openvpn, need to keep it until we
	 * add an accessor
	 */
	/* Default password callback. */
	pem_password_cb *default_passwd_callback;

	/* Default password callback user data. */
	void *default_passwd_callback_userdata;

	struct ssl_ctx_internal_st *internal;
};

#endif

#define SSL_SESS_CACHE_OFF			0x0000
#define SSL_SESS_CACHE_CLIENT			0x0001
#define SSL_SESS_CACHE_SERVER			0x0002
#define SSL_SESS_CACHE_BOTH	(SSL_SESS_CACHE_CLIENT|SSL_SESS_CACHE_SERVER)
#define SSL_SESS_CACHE_NO_AUTO_CLEAR		0x0080
/* enough comments already ... see SSL_CTX_set_session_cache_mode(3) */
#define SSL_SESS_CACHE_NO_INTERNAL_LOOKUP	0x0100
#define SSL_SESS_CACHE_NO_INTERNAL_STORE	0x0200
#define SSL_SESS_CACHE_NO_INTERNAL \
	(SSL_SESS_CACHE_NO_INTERNAL_LOOKUP|SSL_SESS_CACHE_NO_INTERNAL_STORE)

struct lhash_st_SSL_SESSION *SSL_CTX_sessions(SSL_CTX *ctx);
#define SSL_CTX_sess_number(ctx) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SESS_NUMBER,0,NULL)
#define SSL_CTX_sess_connect(ctx) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SESS_CONNECT,0,NULL)
#define SSL_CTX_sess_connect_good(ctx) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SESS_CONNECT_GOOD,0,NULL)
#define SSL_CTX_sess_connect_renegotiate(ctx) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SESS_CONNECT_RENEGOTIATE,0,NULL)
#define SSL_CTX_sess_accept(ctx) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SESS_ACCEPT,0,NULL)
#define SSL_CTX_sess_accept_renegotiate(ctx) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SESS_ACCEPT_RENEGOTIATE,0,NULL)
#define SSL_CTX_sess_accept_good(ctx) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SESS_ACCEPT_GOOD,0,NULL)
#define SSL_CTX_sess_hits(ctx) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SESS_HIT,0,NULL)
#define SSL_CTX_sess_cb_hits(ctx) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SESS_CB_HIT,0,NULL)
#define SSL_CTX_sess_misses(ctx) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SESS_MISSES,0,NULL)
#define SSL_CTX_sess_timeouts(ctx) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SESS_TIMEOUTS,0,NULL)
#define SSL_CTX_sess_cache_full(ctx) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SESS_CACHE_FULL,0,NULL)

void SSL_CTX_sess_set_new_cb(SSL_CTX *ctx,
    int (*new_session_cb)(struct ssl_st *ssl, SSL_SESSION *sess));
int (*SSL_CTX_sess_get_new_cb(SSL_CTX *ctx))(struct ssl_st *ssl,
    SSL_SESSION *sess);
void SSL_CTX_sess_set_remove_cb(SSL_CTX *ctx,
    void (*remove_session_cb)(struct ssl_ctx_st *ctx, SSL_SESSION *sess));
void (*SSL_CTX_sess_get_remove_cb(SSL_CTX *ctx))(struct ssl_ctx_st *ctx,
    SSL_SESSION *sess);
void SSL_CTX_sess_set_get_cb(SSL_CTX *ctx,
    SSL_SESSION *(*get_session_cb)(struct ssl_st *ssl, unsigned char *data,
    int len, int *copy));
SSL_SESSION *(*SSL_CTX_sess_get_get_cb(SSL_CTX *ctx))(struct ssl_st *ssl,
    unsigned char *Data, int len, int *copy);
void SSL_CTX_set_info_callback(SSL_CTX *ctx, void (*cb)(const SSL *ssl,
    int type, int val));
void (*SSL_CTX_get_info_callback(SSL_CTX *ctx))(const SSL *ssl, int type,
    int val);
void SSL_CTX_set_client_cert_cb(SSL_CTX *ctx,
    int (*client_cert_cb)(SSL *ssl, X509 **x509, EVP_PKEY **pkey));
int (*SSL_CTX_get_client_cert_cb(SSL_CTX *ctx))(SSL *ssl, X509 **x509,
    EVP_PKEY **pkey);
#ifndef OPENSSL_NO_ENGINE
int SSL_CTX_set_client_cert_engine(SSL_CTX *ctx, ENGINE *e);
#endif
void SSL_CTX_set_cookie_generate_cb(SSL_CTX *ctx,
    int (*app_gen_cookie_cb)(SSL *ssl, unsigned char *cookie,
    unsigned int *cookie_len));
void SSL_CTX_set_cookie_verify_cb(SSL_CTX *ctx,
    int (*app_verify_cookie_cb)(SSL *ssl, unsigned char *cookie,
    unsigned int cookie_len));
void SSL_CTX_set_next_protos_advertised_cb(SSL_CTX *s, int (*cb)(SSL *ssl,
    const unsigned char **out, unsigned int *outlen, void *arg), void *arg);
void SSL_CTX_set_next_proto_select_cb(SSL_CTX *s, int (*cb)(SSL *ssl,
    unsigned char **out, unsigned char *outlen, const unsigned char *in,
    unsigned int inlen, void *arg), void *arg);

int SSL_select_next_proto(unsigned char **out, unsigned char *outlen,
    const unsigned char *in, unsigned int inlen, const unsigned char *client,
    unsigned int client_len);
void SSL_get0_next_proto_negotiated(const SSL *s, const unsigned char **data,
    unsigned *len);

#define OPENSSL_NPN_UNSUPPORTED	0
#define OPENSSL_NPN_NEGOTIATED	1
#define OPENSSL_NPN_NO_OVERLAP	2

int SSL_CTX_set_alpn_protos(SSL_CTX *ctx, const unsigned char *protos,
    unsigned int protos_len);
int SSL_set_alpn_protos(SSL *ssl, const unsigned char *protos,
    unsigned int protos_len);
void SSL_CTX_set_alpn_select_cb(SSL_CTX *ctx,
    int (*cb)(SSL *ssl, const unsigned char **out, unsigned char *outlen,
    const unsigned char *in, unsigned int inlen, void *arg), void *arg);
void SSL_get0_alpn_selected(const SSL *ssl, const unsigned char **data,
    unsigned int *len);

#define SSL_NOTHING	1
#define SSL_WRITING	2
#define SSL_READING	3
#define SSL_X509_LOOKUP	4

/* These will only be used when doing non-blocking IO */
#define SSL_want_nothing(s)	(SSL_want(s) == SSL_NOTHING)
#define SSL_want_read(s)	(SSL_want(s) == SSL_READING)
#define SSL_want_write(s)	(SSL_want(s) == SSL_WRITING)
#define SSL_want_x509_lookup(s)	(SSL_want(s) == SSL_X509_LOOKUP)

#define SSL_MAC_FLAG_READ_MAC_STREAM 1
#define SSL_MAC_FLAG_WRITE_MAC_STREAM 2

#ifndef OPENSSL_NO_SSL_INTERN
struct ssl_internal_st;

struct ssl_st {
	/* protocol version
	 * (one of SSL2_VERSION, SSL3_VERSION, TLS1_VERSION, DTLS1_VERSION)
	 */
	int version;

	const SSL_METHOD *method; /* SSLv3 */

	/* There are 2 BIO's even though they are normally both the
	 * same.  This is so data can be read and written to different
	 * handlers */

	BIO *rbio; /* used by SSL_read */
	BIO *wbio; /* used by SSL_write */
	BIO *bbio; /* used during session-id reuse to concatenate
		    * messages */
	int server;	/* are we the server side? - mostly used by SSL_clear*/

	struct ssl3_state_st *s3; /* SSLv3 variables */
	struct dtls1_state_st *d1; /* DTLSv1 variables */

	X509_VERIFY_PARAM *param;

	/* crypto */
	STACK_OF(SSL_CIPHER) *cipher_list;

	/* This is used to hold the server certificate used */
	struct cert_st /* CERT */ *cert;

	/* the session_id_context is used to ensure sessions are only reused
	 * in the appropriate context */
	unsigned int sid_ctx_length;
	unsigned char sid_ctx[SSL_MAX_SID_CTX_LENGTH];

	/* This can also be in the session once a session is established */
	SSL_SESSION *session;

	/* Used in SSL2 and SSL3 */
	int verify_mode;	/* 0 don't care about verify failure.
				 * 1 fail if verify fails */
	int error;		/* error bytes to be written */
	int error_code;		/* actual code */

	SSL_CTX *ctx;

	long verify_result;

	int references;

	int client_version;	/* what was passed, used for
				 * SSLv3/TLS rollback check */

	unsigned int max_send_fragment;

	char *tlsext_hostname;

	/* certificate status request info */
	/* Status type or -1 if no status type */
	int tlsext_status_type;

	SSL_CTX * initial_ctx; /* initial ctx, used to store sessions */
#define session_ctx initial_ctx

	/*
	 * XXX really should be internal, but is
	 * touched unnaturally by wpa-supplicant
	 * and freeradius and other perversions
	 */
	EVP_CIPHER_CTX *enc_read_ctx;		/* cryptographic state */
	EVP_MD_CTX *read_hash;			/* used for mac generation */

	struct ssl_internal_st *internal;
};

#endif

#ifdef __cplusplus
}
#endif

#include <openssl/ssl2.h>
#include <openssl/ssl3.h>
#include <openssl/tls1.h>	/* This is mostly sslv3 with a few tweaks */
#include <openssl/dtls1.h>	/* Datagram TLS */
#include <openssl/ssl23.h>
#include <openssl/srtp.h>	/* Support for the use_srtp extension */

#ifdef  __cplusplus
extern "C" {
#endif

/* compatibility */
#define SSL_set_app_data(s,arg)		(SSL_set_ex_data(s,0,(char *)arg))
#define SSL_get_app_data(s)		(SSL_get_ex_data(s,0))
#define SSL_SESSION_set_app_data(s,a)	(SSL_SESSION_set_ex_data(s,0,(char *)a))
#define SSL_SESSION_get_app_data(s)	(SSL_SESSION_get_ex_data(s,0))
#define SSL_CTX_get_app_data(ctx)	(SSL_CTX_get_ex_data(ctx,0))
#define SSL_CTX_set_app_data(ctx,arg)	(SSL_CTX_set_ex_data(ctx,0,(char *)arg))

/* The following are the possible values for ssl->state are are
 * used to indicate where we are up to in the SSL connection establishment.
 * The macros that follow are about the only things you should need to use
 * and even then, only when using non-blocking IO.
 * It can also be useful to work out where you were when the connection
 * failed */

#define SSL_ST_CONNECT			0x1000
#define SSL_ST_ACCEPT			0x2000
#define SSL_ST_MASK			0x0FFF
#define SSL_ST_INIT			(SSL_ST_CONNECT|SSL_ST_ACCEPT)
#define SSL_ST_BEFORE			0x4000
#define SSL_ST_OK			0x03
#define SSL_ST_RENEGOTIATE		(0x04|SSL_ST_INIT)

#define SSL_CB_LOOP			0x01
#define SSL_CB_EXIT			0x02
#define SSL_CB_READ			0x04
#define SSL_CB_WRITE			0x08
#define SSL_CB_ALERT			0x4000 /* used in callback */
#define SSL_CB_READ_ALERT		(SSL_CB_ALERT|SSL_CB_READ)
#define SSL_CB_WRITE_ALERT		(SSL_CB_ALERT|SSL_CB_WRITE)
#define SSL_CB_ACCEPT_LOOP		(SSL_ST_ACCEPT|SSL_CB_LOOP)
#define SSL_CB_ACCEPT_EXIT		(SSL_ST_ACCEPT|SSL_CB_EXIT)
#define SSL_CB_CONNECT_LOOP		(SSL_ST_CONNECT|SSL_CB_LOOP)
#define SSL_CB_CONNECT_EXIT		(SSL_ST_CONNECT|SSL_CB_EXIT)
#define SSL_CB_HANDSHAKE_START		0x10
#define SSL_CB_HANDSHAKE_DONE		0x20

/* Is the SSL_connection established? */
#define SSL_get_state(a)		(SSL_state((a)))
#define SSL_is_init_finished(a)		(SSL_state((a)) == SSL_ST_OK)
#define SSL_in_init(a)			(SSL_state((a))&SSL_ST_INIT)
#define SSL_in_before(a)		(SSL_state((a))&SSL_ST_BEFORE)
#define SSL_in_connect_init(a)		(SSL_state((a))&SSL_ST_CONNECT)
#define SSL_in_accept_init(a)		(SSL_state((a))&SSL_ST_ACCEPT)

/* The following 2 states are kept in ssl->rstate when reads fail,
 * you should not need these */
#define SSL_ST_READ_HEADER		0xF0
#define SSL_ST_READ_BODY		0xF1
#define SSL_ST_READ_DONE		0xF2

/* Obtain latest Finished message
 *   -- that we sent (SSL_get_finished)
 *   -- that we expected from peer (SSL_get_peer_finished).
 * Returns length (0 == no Finished so far), copies up to 'count' bytes. */
size_t SSL_get_finished(const SSL *s, void *buf, size_t count);
size_t SSL_get_peer_finished(const SSL *s, void *buf, size_t count);

/* use either SSL_VERIFY_NONE or SSL_VERIFY_PEER, the last 2 options
 * are 'ored' with SSL_VERIFY_PEER if they are desired */
#define SSL_VERIFY_NONE			0x00
#define SSL_VERIFY_PEER			0x01
#define SSL_VERIFY_FAIL_IF_NO_PEER_CERT	0x02
#define SSL_VERIFY_CLIENT_ONCE		0x04

#define OpenSSL_add_ssl_algorithms()	SSL_library_init()
#define SSLeay_add_ssl_algorithms()	SSL_library_init()

/* More backward compatibility */
#define SSL_get_cipher(s) \
		SSL_CIPHER_get_name(SSL_get_current_cipher(s))
#define SSL_get_cipher_bits(s,np) \
		SSL_CIPHER_get_bits(SSL_get_current_cipher(s),np)
#define SSL_get_cipher_version(s) \
		SSL_CIPHER_get_version(SSL_get_current_cipher(s))
#define SSL_get_cipher_name(s) \
		SSL_CIPHER_get_name(SSL_get_current_cipher(s))
#define SSL_get_time(a)		SSL_SESSION_get_time(a)
#define SSL_set_time(a,b)	SSL_SESSION_set_time((a),(b))
#define SSL_get_timeout(a)	SSL_SESSION_get_timeout(a)
#define SSL_set_timeout(a,b)	SSL_SESSION_set_timeout((a),(b))

#define d2i_SSL_SESSION_bio(bp,s_id) ASN1_d2i_bio_of(SSL_SESSION,SSL_SESSION_new,d2i_SSL_SESSION,bp,s_id)
#define i2d_SSL_SESSION_bio(bp,s_id) ASN1_i2d_bio_of(SSL_SESSION,i2d_SSL_SESSION,bp,s_id)

SSL_SESSION *PEM_read_bio_SSL_SESSION(BIO *bp, SSL_SESSION **x,
    pem_password_cb *cb, void *u);
SSL_SESSION *PEM_read_SSL_SESSION(FILE *fp, SSL_SESSION **x,
    pem_password_cb *cb, void *u);
int PEM_write_bio_SSL_SESSION(BIO *bp, SSL_SESSION *x);
int PEM_write_SSL_SESSION(FILE *fp, SSL_SESSION *x);

#define SSL_AD_REASON_OFFSET		1000 /* offset to get SSL_R_... value from SSL_AD_... */

/* These alert types are for SSLv3 and TLSv1 */
#define SSL_AD_CLOSE_NOTIFY		SSL3_AD_CLOSE_NOTIFY
#define SSL_AD_UNEXPECTED_MESSAGE	SSL3_AD_UNEXPECTED_MESSAGE /* fatal */
#define SSL_AD_BAD_RECORD_MAC		SSL3_AD_BAD_RECORD_MAC     /* fatal */
#define SSL_AD_DECRYPTION_FAILED	TLS1_AD_DECRYPTION_FAILED
#define SSL_AD_RECORD_OVERFLOW		TLS1_AD_RECORD_OVERFLOW
#define SSL_AD_DECOMPRESSION_FAILURE	SSL3_AD_DECOMPRESSION_FAILURE/* fatal */
#define SSL_AD_HANDSHAKE_FAILURE	SSL3_AD_HANDSHAKE_FAILURE/* fatal */
#define SSL_AD_NO_CERTIFICATE		SSL3_AD_NO_CERTIFICATE /* Not for TLS */
#define SSL_AD_BAD_CERTIFICATE		SSL3_AD_BAD_CERTIFICATE
#define SSL_AD_UNSUPPORTED_CERTIFICATE	SSL3_AD_UNSUPPORTED_CERTIFICATE
#define SSL_AD_CERTIFICATE_REVOKED	SSL3_AD_CERTIFICATE_REVOKED
#define SSL_AD_CERTIFICATE_EXPIRED	SSL3_AD_CERTIFICATE_EXPIRED
#define SSL_AD_CERTIFICATE_UNKNOWN	SSL3_AD_CERTIFICATE_UNKNOWN
#define SSL_AD_ILLEGAL_PARAMETER	SSL3_AD_ILLEGAL_PARAMETER   /* fatal */
#define SSL_AD_UNKNOWN_CA		TLS1_AD_UNKNOWN_CA	/* fatal */
#define SSL_AD_ACCESS_DENIED		TLS1_AD_ACCESS_DENIED	/* fatal */
#define SSL_AD_DECODE_ERROR		TLS1_AD_DECODE_ERROR	/* fatal */
#define SSL_AD_DECRYPT_ERROR		TLS1_AD_DECRYPT_ERROR
#define SSL_AD_EXPORT_RESTRICTION	TLS1_AD_EXPORT_RESTRICTION/* fatal */
#define SSL_AD_PROTOCOL_VERSION		TLS1_AD_PROTOCOL_VERSION /* fatal */
#define SSL_AD_INSUFFICIENT_SECURITY	TLS1_AD_INSUFFICIENT_SECURITY/* fatal */
#define SSL_AD_INTERNAL_ERROR		TLS1_AD_INTERNAL_ERROR	/* fatal */
#define SSL_AD_INAPPROPRIATE_FALLBACK	TLS1_AD_INAPPROPRIATE_FALLBACK /* fatal */
#define SSL_AD_USER_CANCELLED		TLS1_AD_USER_CANCELLED
#define SSL_AD_NO_RENEGOTIATION		TLS1_AD_NO_RENEGOTIATION
#define SSL_AD_UNSUPPORTED_EXTENSION	TLS1_AD_UNSUPPORTED_EXTENSION
#define SSL_AD_CERTIFICATE_UNOBTAINABLE TLS1_AD_CERTIFICATE_UNOBTAINABLE
#define SSL_AD_UNRECOGNIZED_NAME	TLS1_AD_UNRECOGNIZED_NAME
#define SSL_AD_BAD_CERTIFICATE_STATUS_RESPONSE TLS1_AD_BAD_CERTIFICATE_STATUS_RESPONSE
#define SSL_AD_BAD_CERTIFICATE_HASH_VALUE TLS1_AD_BAD_CERTIFICATE_HASH_VALUE
#define SSL_AD_UNKNOWN_PSK_IDENTITY	TLS1_AD_UNKNOWN_PSK_IDENTITY /* fatal */

#define SSL_ERROR_NONE			0
#define SSL_ERROR_SSL			1
#define SSL_ERROR_WANT_READ		2
#define SSL_ERROR_WANT_WRITE		3
#define SSL_ERROR_WANT_X509_LOOKUP	4
#define SSL_ERROR_SYSCALL		5 /* look at error stack/return value/errno */
#define SSL_ERROR_ZERO_RETURN		6
#define SSL_ERROR_WANT_CONNECT		7
#define SSL_ERROR_WANT_ACCEPT		8

#define SSL_CTRL_NEED_TMP_RSA			1
#define SSL_CTRL_SET_TMP_RSA			2
#define SSL_CTRL_SET_TMP_DH			3
#define SSL_CTRL_SET_TMP_ECDH			4
#define SSL_CTRL_SET_TMP_RSA_CB			5
#define SSL_CTRL_SET_TMP_DH_CB			6
#define SSL_CTRL_SET_TMP_ECDH_CB		7

#define SSL_CTRL_GET_SESSION_REUSED		8
#define SSL_CTRL_GET_CLIENT_CERT_REQUEST	9
#define SSL_CTRL_GET_NUM_RENEGOTIATIONS		10
#define SSL_CTRL_CLEAR_NUM_RENEGOTIATIONS	11
#define SSL_CTRL_GET_TOTAL_RENEGOTIATIONS	12
#define SSL_CTRL_GET_FLAGS			13
#define SSL_CTRL_EXTRA_CHAIN_CERT		14

#define SSL_CTRL_SET_MSG_CALLBACK               15
#define SSL_CTRL_SET_MSG_CALLBACK_ARG           16

/* only applies to datagram connections */
#define SSL_CTRL_SET_MTU                17
/* Stats */
#define SSL_CTRL_SESS_NUMBER			20
#define SSL_CTRL_SESS_CONNECT			21
#define SSL_CTRL_SESS_CONNECT_GOOD		22
#define SSL_CTRL_SESS_CONNECT_RENEGOTIATE	23
#define SSL_CTRL_SESS_ACCEPT			24
#define SSL_CTRL_SESS_ACCEPT_GOOD		25
#define SSL_CTRL_SESS_ACCEPT_RENEGOTIATE	26
#define SSL_CTRL_SESS_HIT			27
#define SSL_CTRL_SESS_CB_HIT			28
#define SSL_CTRL_SESS_MISSES			29
#define SSL_CTRL_SESS_TIMEOUTS			30
#define SSL_CTRL_SESS_CACHE_FULL		31
#define SSL_CTRL_OPTIONS			32
#define SSL_CTRL_MODE				33

#define SSL_CTRL_GET_READ_AHEAD			40
#define SSL_CTRL_SET_READ_AHEAD			41
#define SSL_CTRL_SET_SESS_CACHE_SIZE		42
#define SSL_CTRL_GET_SESS_CACHE_SIZE		43
#define SSL_CTRL_SET_SESS_CACHE_MODE		44
#define SSL_CTRL_GET_SESS_CACHE_MODE		45

#define SSL_CTRL_GET_MAX_CERT_LIST		50
#define SSL_CTRL_SET_MAX_CERT_LIST		51

#define SSL_CTRL_SET_MAX_SEND_FRAGMENT		52

/* see tls1.h for macros based on these */
#define SSL_CTRL_SET_TLSEXT_SERVERNAME_CB	53
#define SSL_CTRL_SET_TLSEXT_SERVERNAME_ARG	54
#define SSL_CTRL_SET_TLSEXT_HOSTNAME		55
#define SSL_CTRL_SET_TLSEXT_DEBUG_CB		56
#define SSL_CTRL_SET_TLSEXT_DEBUG_ARG		57
#define SSL_CTRL_GET_TLSEXT_TICKET_KEYS		58
#define SSL_CTRL_SET_TLSEXT_TICKET_KEYS		59
#define SSL_CTRL_SET_TLSEXT_STATUS_REQ_CB	63
#define SSL_CTRL_SET_TLSEXT_STATUS_REQ_CB_ARG	64
#define SSL_CTRL_SET_TLSEXT_STATUS_REQ_TYPE	65
#define SSL_CTRL_GET_TLSEXT_STATUS_REQ_EXTS	66
#define SSL_CTRL_SET_TLSEXT_STATUS_REQ_EXTS	67
#define SSL_CTRL_GET_TLSEXT_STATUS_REQ_IDS	68
#define SSL_CTRL_SET_TLSEXT_STATUS_REQ_IDS	69
#define SSL_CTRL_GET_TLSEXT_STATUS_REQ_OCSP_RESP	70
#define SSL_CTRL_SET_TLSEXT_STATUS_REQ_OCSP_RESP	71

#define SSL_CTRL_SET_TLSEXT_TICKET_KEY_CB	72

#define SSL_CTRL_SET_TLS_EXT_SRP_USERNAME_CB	75
#define SSL_CTRL_SET_SRP_VERIFY_PARAM_CB		76
#define SSL_CTRL_SET_SRP_GIVE_CLIENT_PWD_CB		77

#define SSL_CTRL_SET_SRP_ARG		78
#define SSL_CTRL_SET_TLS_EXT_SRP_USERNAME		79
#define SSL_CTRL_SET_TLS_EXT_SRP_STRENGTH		80
#define SSL_CTRL_SET_TLS_EXT_SRP_PASSWORD		81

#define DTLS_CTRL_GET_TIMEOUT		73
#define DTLS_CTRL_HANDLE_TIMEOUT	74
#define DTLS_CTRL_LISTEN			75

#define SSL_CTRL_GET_RI_SUPPORT			76
#define SSL_CTRL_CLEAR_OPTIONS			77
#define SSL_CTRL_CLEAR_MODE			78

#define SSL_CTRL_GET_EXTRA_CHAIN_CERTS		82
#define SSL_CTRL_CLEAR_EXTRA_CHAIN_CERTS	83

#define SSL_CTRL_SET_GROUPS				91
#define SSL_CTRL_SET_GROUPS_LIST			92

#define SSL_CTRL_SET_ECDH_AUTO			94

#define SSL_CTRL_GET_SERVER_TMP_KEY		109

#define SSL_CTRL_SET_DH_AUTO			118

#define SSL_CTRL_SET_MIN_PROTO_VERSION			123
#define SSL_CTRL_SET_MAX_PROTO_VERSION			124

#define DTLSv1_get_timeout(ssl, arg) \
	SSL_ctrl(ssl,DTLS_CTRL_GET_TIMEOUT,0, (void *)arg)
#define DTLSv1_handle_timeout(ssl) \
	SSL_ctrl(ssl,DTLS_CTRL_HANDLE_TIMEOUT,0, NULL)
#define DTLSv1_listen(ssl, peer) \
	SSL_ctrl(ssl,DTLS_CTRL_LISTEN,0, (void *)peer)

#define SSL_session_reused(ssl) \
	SSL_ctrl((ssl),SSL_CTRL_GET_SESSION_REUSED,0,NULL)
#define SSL_num_renegotiations(ssl) \
	SSL_ctrl((ssl),SSL_CTRL_GET_NUM_RENEGOTIATIONS,0,NULL)
#define SSL_clear_num_renegotiations(ssl) \
	SSL_ctrl((ssl),SSL_CTRL_CLEAR_NUM_RENEGOTIATIONS,0,NULL)
#define SSL_total_renegotiations(ssl) \
	SSL_ctrl((ssl),SSL_CTRL_GET_TOTAL_RENEGOTIATIONS,0,NULL)

#define SSL_CTX_need_tmp_RSA(ctx) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_NEED_TMP_RSA,0,NULL)
#define SSL_CTX_set_tmp_rsa(ctx,rsa) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SET_TMP_RSA,0,(char *)rsa)
#define SSL_CTX_set_tmp_dh(ctx,dh) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SET_TMP_DH,0,(char *)dh)
#define SSL_CTX_set_tmp_ecdh(ctx,ecdh) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SET_TMP_ECDH,0,(char *)ecdh)
#define SSL_CTX_set_dh_auto(ctx, onoff) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SET_DH_AUTO,onoff,NULL)
#define SSL_CTX_set_ecdh_auto(ctx, onoff) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SET_ECDH_AUTO,onoff,NULL)

#define SSL_need_tmp_RSA(ssl) \
	SSL_ctrl(ssl,SSL_CTRL_NEED_TMP_RSA,0,NULL)
#define SSL_set_tmp_rsa(ssl,rsa) \
	SSL_ctrl(ssl,SSL_CTRL_SET_TMP_RSA,0,(char *)rsa)
#define SSL_set_tmp_dh(ssl,dh) \
	SSL_ctrl(ssl,SSL_CTRL_SET_TMP_DH,0,(char *)dh)
#define SSL_set_tmp_ecdh(ssl,ecdh) \
	SSL_ctrl(ssl,SSL_CTRL_SET_TMP_ECDH,0,(char *)ecdh)
#define SSL_set_dh_auto(s, onoff) \
	SSL_ctrl(s,SSL_CTRL_SET_DH_AUTO,onoff,NULL)
#define SSL_set_ecdh_auto(s, onoff) \
	SSL_ctrl(s,SSL_CTRL_SET_ECDH_AUTO,onoff,NULL)

int SSL_CTX_set1_groups(SSL_CTX *ctx, const int *groups, size_t groups_len);
int SSL_CTX_set1_groups_list(SSL_CTX *ctx, const char *groups);

int SSL_set1_groups(SSL *ssl, const int *groups, size_t groups_len);
int SSL_set1_groups_list(SSL *ssl, const char *groups);

int SSL_CTX_set_min_proto_version(SSL_CTX *ctx, uint16_t version);
int SSL_CTX_set_max_proto_version(SSL_CTX *ctx, uint16_t version);

int SSL_set_min_proto_version(SSL *ssl, uint16_t version);
int SSL_set_max_proto_version(SSL *ssl, uint16_t version);

#ifndef LIBRESSL_INTERNAL
#define SSL_CTRL_SET_CURVES			SSL_CTRL_SET_GROUPS
#define SSL_CTRL_SET_CURVES_LIST		SSL_CTRL_SET_GROUPS_LIST

#define SSL_CTX_set1_curves SSL_CTX_set1_groups
#define SSL_CTX_set1_curves_list SSL_CTX_set1_groups_list
#define SSL_set1_curves SSL_set1_groups
#define SSL_set1_curves_list SSL_set1_groups_list
#endif

#define SSL_CTX_add_extra_chain_cert(ctx,x509) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_EXTRA_CHAIN_CERT,0,(char *)x509)
#define SSL_CTX_get_extra_chain_certs(ctx,px509) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_GET_EXTRA_CHAIN_CERTS,0,px509)
#define SSL_CTX_clear_extra_chain_certs(ctx) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_CLEAR_EXTRA_CHAIN_CERTS,0,NULL)

#define SSL_get_server_tmp_key(s, pk) \
	SSL_ctrl(s,SSL_CTRL_GET_SERVER_TMP_KEY,0,pk)

BIO_METHOD *BIO_f_ssl(void);
BIO *BIO_new_ssl(SSL_CTX *ctx, int client);
BIO *BIO_new_ssl_connect(SSL_CTX *ctx);
BIO *BIO_new_buffer_ssl_connect(SSL_CTX *ctx);
int BIO_ssl_copy_session_id(BIO *to, BIO *from);
void BIO_ssl_shutdown(BIO *ssl_bio);

int	SSL_CTX_set_cipher_list(SSL_CTX *, const char *str);
SSL_CTX *SSL_CTX_new(const SSL_METHOD *meth);
void	SSL_CTX_free(SSL_CTX *);
long SSL_CTX_set_timeout(SSL_CTX *ctx, long t);
long SSL_CTX_get_timeout(const SSL_CTX *ctx);
X509_STORE *SSL_CTX_get_cert_store(const SSL_CTX *);
void SSL_CTX_set_cert_store(SSL_CTX *, X509_STORE *);
int SSL_want(const SSL *s);
int	SSL_clear(SSL *s);

void	SSL_CTX_flush_sessions(SSL_CTX *ctx, long tm);

const SSL_CIPHER *SSL_get_current_cipher(const SSL *s);
const SSL_CIPHER *SSL_CIPHER_get_by_id(unsigned int id);
const SSL_CIPHER *SSL_CIPHER_get_by_value(uint16_t value);
int	SSL_CIPHER_get_bits(const SSL_CIPHER *c, int *alg_bits);
char *	SSL_CIPHER_get_version(const SSL_CIPHER *c);
const char *	SSL_CIPHER_get_name(const SSL_CIPHER *c);
unsigned long 	SSL_CIPHER_get_id(const SSL_CIPHER *c);
uint16_t SSL_CIPHER_get_value(const SSL_CIPHER *c);

int	SSL_get_fd(const SSL *s);
int	SSL_get_rfd(const SSL *s);
int	SSL_get_wfd(const SSL *s);
const char  * SSL_get_cipher_list(const SSL *s, int n);
char *	SSL_get_shared_ciphers(const SSL *s, char *buf, int len);
int	SSL_get_read_ahead(const SSL * s);
int	SSL_pending(const SSL *s);
int	SSL_set_fd(SSL *s, int fd);
int	SSL_set_rfd(SSL *s, int fd);
int	SSL_set_wfd(SSL *s, int fd);
void	SSL_set_bio(SSL *s, BIO *rbio, BIO *wbio);
BIO *	SSL_get_rbio(const SSL *s);
BIO *	SSL_get_wbio(const SSL *s);
int	SSL_set_cipher_list(SSL *s, const char *str);
void	SSL_set_read_ahead(SSL *s, int yes);
int	SSL_get_verify_mode(const SSL *s);
int	SSL_get_verify_depth(const SSL *s);
int	(*SSL_get_verify_callback(const SSL *s))(int, X509_STORE_CTX *);
void	SSL_set_verify(SSL *s, int mode,
	    int (*callback)(int ok, X509_STORE_CTX *ctx));
void	SSL_set_verify_depth(SSL *s, int depth);
int	SSL_use_RSAPrivateKey(SSL *ssl, RSA *rsa);
int	SSL_use_RSAPrivateKey_ASN1(SSL *ssl, unsigned char *d, long len);
int	SSL_use_PrivateKey(SSL *ssl, EVP_PKEY *pkey);
int	SSL_use_PrivateKey_ASN1(int pk, SSL *ssl, const unsigned char *d, long len);
int	SSL_use_certificate(SSL *ssl, X509 *x);
int	SSL_use_certificate_ASN1(SSL *ssl, const unsigned char *d, int len);

int	SSL_use_RSAPrivateKey_file(SSL *ssl, const char *file, int type);
int	SSL_use_PrivateKey_file(SSL *ssl, const char *file, int type);
int	SSL_use_certificate_file(SSL *ssl, const char *file, int type);
int	SSL_CTX_use_RSAPrivateKey_file(SSL_CTX *ctx, const char *file, int type);
int	SSL_CTX_use_PrivateKey_file(SSL_CTX *ctx, const char *file, int type);
int	SSL_CTX_use_certificate_file(SSL_CTX *ctx, const char *file, int type);
int	SSL_CTX_use_certificate_chain_file(SSL_CTX *ctx, const char *file); /* PEM type */
int	SSL_CTX_use_certificate_chain_mem(SSL_CTX *ctx, void *buf, int len);
STACK_OF(X509_NAME) *SSL_load_client_CA_file(const char *file);
int	SSL_add_file_cert_subjects_to_stack(STACK_OF(X509_NAME) *stackCAs,
	    const char *file);
int	SSL_add_dir_cert_subjects_to_stack(STACK_OF(X509_NAME) *stackCAs,
	    const char *dir);

void	SSL_load_error_strings(void );
const char *SSL_state_string(const SSL *s);
const char *SSL_rstate_string(const SSL *s);
const char *SSL_state_string_long(const SSL *s);
const char *SSL_rstate_string_long(const SSL *s);
long	SSL_SESSION_get_time(const SSL_SESSION *s);
long	SSL_SESSION_set_time(SSL_SESSION *s, long t);
long	SSL_SESSION_get_timeout(const SSL_SESSION *s);
long	SSL_SESSION_set_timeout(SSL_SESSION *s, long t);
void	SSL_copy_session_id(SSL *to, const SSL *from);
X509	*SSL_SESSION_get0_peer(SSL_SESSION *s);
int	SSL_SESSION_set1_id_context(SSL_SESSION *s,
	    const unsigned char *sid_ctx, unsigned int sid_ctx_len);

SSL_SESSION *SSL_SESSION_new(void);
const unsigned char *SSL_SESSION_get_id(const SSL_SESSION *s,
	    unsigned int *len);
unsigned int SSL_SESSION_get_compress_id(const SSL_SESSION *s);
int	SSL_SESSION_print_fp(FILE *fp, const SSL_SESSION *ses);
int	SSL_SESSION_print(BIO *fp, const SSL_SESSION *ses);
void	SSL_SESSION_free(SSL_SESSION *ses);
int	i2d_SSL_SESSION(SSL_SESSION *in, unsigned char **pp);
int	SSL_set_session(SSL *to, SSL_SESSION *session);
int	SSL_CTX_add_session(SSL_CTX *s, SSL_SESSION *c);
int	SSL_CTX_remove_session(SSL_CTX *, SSL_SESSION *c);
int	SSL_CTX_set_generate_session_id(SSL_CTX *, GEN_SESSION_CB);
int	SSL_set_generate_session_id(SSL *, GEN_SESSION_CB);
int	SSL_has_matching_session_id(const SSL *ssl, const unsigned char *id,
	    unsigned int id_len);
SSL_SESSION *d2i_SSL_SESSION(SSL_SESSION **a, const unsigned char **pp,
	    long length);

#ifdef HEADER_X509_H
X509 *	SSL_get_peer_certificate(const SSL *s);
#endif

STACK_OF(X509) *SSL_get_peer_cert_chain(const SSL *s);

int SSL_CTX_get_verify_mode(const SSL_CTX *ctx);
int SSL_CTX_get_verify_depth(const SSL_CTX *ctx);
int (*SSL_CTX_get_verify_callback(const SSL_CTX *ctx))(int, X509_STORE_CTX *);
void SSL_CTX_set_verify(SSL_CTX *ctx, int mode,
    int (*callback)(int, X509_STORE_CTX *));
void SSL_CTX_set_verify_depth(SSL_CTX *ctx, int depth);
void SSL_CTX_set_cert_verify_callback(SSL_CTX *ctx, int (*cb)(X509_STORE_CTX *, void *), void *arg);
int SSL_CTX_use_RSAPrivateKey(SSL_CTX *ctx, RSA *rsa);
int SSL_CTX_use_RSAPrivateKey_ASN1(SSL_CTX *ctx, const unsigned char *d, long len);
int SSL_CTX_use_PrivateKey(SSL_CTX *ctx, EVP_PKEY *pkey);
int SSL_CTX_use_PrivateKey_ASN1(int pk, SSL_CTX *ctx, const unsigned char *d, long len);
int SSL_CTX_use_certificate(SSL_CTX *ctx, X509 *x);
int SSL_CTX_use_certificate_ASN1(SSL_CTX *ctx, int len, const unsigned char *d);

void SSL_CTX_set_default_passwd_cb(SSL_CTX *ctx, pem_password_cb *cb);
void SSL_CTX_set_default_passwd_cb_userdata(SSL_CTX *ctx, void *u);

int SSL_CTX_check_private_key(const SSL_CTX *ctx);
int SSL_check_private_key(const SSL *ctx);

int SSL_CTX_set_session_id_context(SSL_CTX *ctx, const unsigned char *sid_ctx, unsigned int sid_ctx_len);

SSL *SSL_new(SSL_CTX *ctx);
int SSL_set_session_id_context(SSL *ssl, const unsigned char *sid_ctx, unsigned int sid_ctx_len);

int SSL_CTX_set_purpose(SSL_CTX *s, int purpose);
int SSL_set_purpose(SSL *s, int purpose);
int SSL_CTX_set_trust(SSL_CTX *s, int trust);
int SSL_set_trust(SSL *s, int trust);

int SSL_CTX_set1_param(SSL_CTX *ctx, X509_VERIFY_PARAM *vpm);
int SSL_set1_param(SSL *ssl, X509_VERIFY_PARAM *vpm);


void	SSL_free(SSL *ssl);
int 	SSL_accept(SSL *ssl);
int 	SSL_connect(SSL *ssl);
int 	SSL_read(SSL *ssl, void *buf, int num);
int 	SSL_peek(SSL *ssl, void *buf, int num);
int 	SSL_write(SSL *ssl, const void *buf, int num);
long	SSL_ctrl(SSL *ssl, int cmd, long larg, void *parg);
long	SSL_callback_ctrl(SSL *, int, void (*)(void));
long	SSL_CTX_ctrl(SSL_CTX *ctx, int cmd, long larg, void *parg);
long	SSL_CTX_callback_ctrl(SSL_CTX *, int, void (*)(void));

int	SSL_get_error(const SSL *s, int ret_code);
const char *SSL_get_version(const SSL *s);

/* This sets the 'default' SSL version that SSL_new() will create */
int SSL_CTX_set_ssl_version(SSL_CTX *ctx, const SSL_METHOD *meth);

const SSL_METHOD *SSLv23_method(void);		/* SSLv3 or TLSv1.* */
const SSL_METHOD *SSLv23_server_method(void);	/* SSLv3 or TLSv1.* */
const SSL_METHOD *SSLv23_client_method(void);	/* SSLv3 or TLSv1.* */

const SSL_METHOD *TLSv1_method(void);		/* TLSv1.0 */
const SSL_METHOD *TLSv1_server_method(void);	/* TLSv1.0 */
const SSL_METHOD *TLSv1_client_method(void);	/* TLSv1.0 */

const SSL_METHOD *TLSv1_1_method(void);		/* TLSv1.1 */
const SSL_METHOD *TLSv1_1_server_method(void);	/* TLSv1.1 */
const SSL_METHOD *TLSv1_1_client_method(void);	/* TLSv1.1 */

const SSL_METHOD *TLSv1_2_method(void);		/* TLSv1.2 */
const SSL_METHOD *TLSv1_2_server_method(void);	/* TLSv1.2 */
const SSL_METHOD *TLSv1_2_client_method(void);	/* TLSv1.2 */

const SSL_METHOD *TLS_method(void);		/* TLS v1.0 or later */
const SSL_METHOD *TLS_server_method(void);	/* TLS v1.0 or later */
const SSL_METHOD *TLS_client_method(void);	/* TLS v1.0 or later */

const SSL_METHOD *DTLSv1_method(void);		/* DTLSv1.0 */
const SSL_METHOD *DTLSv1_server_method(void);	/* DTLSv1.0 */
const SSL_METHOD *DTLSv1_client_method(void);	/* DTLSv1.0 */

STACK_OF(SSL_CIPHER) *SSL_get_ciphers(const SSL *s);

int SSL_do_handshake(SSL *s);
int SSL_renegotiate(SSL *s);
int SSL_renegotiate_abbreviated(SSL *s);
int SSL_renegotiate_pending(SSL *s);
int SSL_shutdown(SSL *s);

const SSL_METHOD *SSL_get_ssl_method(SSL *s);
int SSL_set_ssl_method(SSL *s, const SSL_METHOD *method);
const char *SSL_alert_type_string_long(int value);
const char *SSL_alert_type_string(int value);
const char *SSL_alert_desc_string_long(int value);
const char *SSL_alert_desc_string(int value);

void SSL_set_client_CA_list(SSL *s, STACK_OF(X509_NAME) *name_list);
void SSL_CTX_set_client_CA_list(SSL_CTX *ctx, STACK_OF(X509_NAME) *name_list);
STACK_OF(X509_NAME) *SSL_get_client_CA_list(const SSL *s);
STACK_OF(X509_NAME) *SSL_CTX_get_client_CA_list(const SSL_CTX *s);
int SSL_add_client_CA(SSL *ssl, X509 *x);
int SSL_CTX_add_client_CA(SSL_CTX *ctx, X509 *x);

void SSL_set_connect_state(SSL *s);
void SSL_set_accept_state(SSL *s);

long SSL_get_default_timeout(const SSL *s);

int SSL_library_init(void );

char *SSL_CIPHER_description(const SSL_CIPHER *, char *buf, int size);
STACK_OF(X509_NAME) *SSL_dup_CA_list(STACK_OF(X509_NAME) *sk);

SSL *SSL_dup(SSL *ssl);

X509 *SSL_get_certificate(const SSL *ssl);
/* EVP_PKEY */ struct evp_pkey_st *SSL_get_privatekey(SSL *ssl);

void SSL_CTX_set_quiet_shutdown(SSL_CTX *ctx,int mode);
int SSL_CTX_get_quiet_shutdown(const SSL_CTX *ctx);
void SSL_set_quiet_shutdown(SSL *ssl,int mode);
int SSL_get_quiet_shutdown(const SSL *ssl);
void SSL_set_shutdown(SSL *ssl,int mode);
int SSL_get_shutdown(const SSL *ssl);
int SSL_version(const SSL *ssl);
int SSL_CTX_set_default_verify_paths(SSL_CTX *ctx);
int SSL_CTX_load_verify_locations(SSL_CTX *ctx, const char *CAfile,
    const char *CApath);
int SSL_CTX_load_verify_mem(SSL_CTX *ctx, void *buf, int len);
#define SSL_get0_session SSL_get_session /* just peek at pointer */
SSL_SESSION *SSL_get_session(const SSL *ssl);
SSL_SESSION *SSL_get1_session(SSL *ssl); /* obtain a reference count */
SSL_CTX *SSL_get_SSL_CTX(const SSL *ssl);
SSL_CTX *SSL_set_SSL_CTX(SSL *ssl, SSL_CTX* ctx);
void SSL_set_info_callback(SSL *ssl,
    void (*cb)(const SSL *ssl, int type, int val));
void (*SSL_get_info_callback(const SSL *ssl))(const SSL *ssl, int type, int val);
int SSL_state(const SSL *ssl);
void SSL_set_state(SSL *ssl, int state);

void SSL_set_verify_result(SSL *ssl, long v);
long SSL_get_verify_result(const SSL *ssl);

int SSL_set_ex_data(SSL *ssl, int idx, void *data);
void *SSL_get_ex_data(const SSL *ssl, int idx);
int SSL_get_ex_new_index(long argl, void *argp, CRYPTO_EX_new *new_func,
    CRYPTO_EX_dup *dup_func, CRYPTO_EX_free *free_func);

int SSL_SESSION_set_ex_data(SSL_SESSION *ss, int idx, void *data);
void *SSL_SESSION_get_ex_data(const SSL_SESSION *ss, int idx);
int SSL_SESSION_get_ex_new_index(long argl, void *argp,
    CRYPTO_EX_new *new_func, CRYPTO_EX_dup *dup_func,
    CRYPTO_EX_free *free_func);

int SSL_CTX_set_ex_data(SSL_CTX *ssl, int idx, void *data);
void *SSL_CTX_get_ex_data(const SSL_CTX *ssl, int idx);
int SSL_CTX_get_ex_new_index(long argl, void *argp, CRYPTO_EX_new *new_func,
    CRYPTO_EX_dup *dup_func, CRYPTO_EX_free *free_func);

int SSL_get_ex_data_X509_STORE_CTX_idx(void );

#define SSL_CTX_sess_set_cache_size(ctx,t) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SET_SESS_CACHE_SIZE,t,NULL)
#define SSL_CTX_sess_get_cache_size(ctx) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_GET_SESS_CACHE_SIZE,0,NULL)
#define SSL_CTX_set_session_cache_mode(ctx,m) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SET_SESS_CACHE_MODE,m,NULL)
#define SSL_CTX_get_session_cache_mode(ctx) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_GET_SESS_CACHE_MODE,0,NULL)

#define SSL_CTX_get_default_read_ahead(ctx) SSL_CTX_get_read_ahead(ctx)
#define SSL_CTX_set_default_read_ahead(ctx,m) SSL_CTX_set_read_ahead(ctx,m)
#define SSL_CTX_get_read_ahead(ctx) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_GET_READ_AHEAD,0,NULL)
#define SSL_CTX_set_read_ahead(ctx,m) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SET_READ_AHEAD,m,NULL)
#define SSL_CTX_get_max_cert_list(ctx) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_GET_MAX_CERT_LIST,0,NULL)
#define SSL_CTX_set_max_cert_list(ctx,m) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SET_MAX_CERT_LIST,m,NULL)
#define SSL_get_max_cert_list(ssl) \
	SSL_ctrl(ssl,SSL_CTRL_GET_MAX_CERT_LIST,0,NULL)
#define SSL_set_max_cert_list(ssl,m) \
	SSL_ctrl(ssl,SSL_CTRL_SET_MAX_CERT_LIST,m,NULL)

#define SSL_CTX_set_max_send_fragment(ctx,m) \
	SSL_CTX_ctrl(ctx,SSL_CTRL_SET_MAX_SEND_FRAGMENT,m,NULL)
#define SSL_set_max_send_fragment(ssl,m) \
	SSL_ctrl(ssl,SSL_CTRL_SET_MAX_SEND_FRAGMENT,m,NULL)

/* NB: the keylength is only applicable when is_export is true */
void SSL_CTX_set_tmp_rsa_callback(SSL_CTX *ctx,
    RSA *(*cb)(SSL *ssl, int is_export, int keylength));

void SSL_set_tmp_rsa_callback(SSL *ssl,
    RSA *(*cb)(SSL *ssl, int is_export, int keylength));
void SSL_CTX_set_tmp_dh_callback(SSL_CTX *ctx,
    DH *(*dh)(SSL *ssl, int is_export, int keylength));
void SSL_set_tmp_dh_callback(SSL *ssl,
    DH *(*dh)(SSL *ssl, int is_export, int keylength));
void SSL_CTX_set_tmp_ecdh_callback(SSL_CTX *ctx,
    EC_KEY *(*ecdh)(SSL *ssl, int is_export, int keylength));
void SSL_set_tmp_ecdh_callback(SSL *ssl,
    EC_KEY *(*ecdh)(SSL *ssl, int is_export, int keylength));

const void *SSL_get_current_compression(SSL *s);
const void *SSL_get_current_expansion(SSL *s);

const char *SSL_COMP_get_name(const void *comp);
void *SSL_COMP_get_compression_methods(void);
int SSL_COMP_add_compression_method(int id, void *cm);

/* TLS extensions functions */
int SSL_set_session_ticket_ext(SSL *s, void *ext_data, int ext_len);

int SSL_set_session_ticket_ext_cb(SSL *s,
    tls_session_ticket_ext_cb_fn cb, void *arg);

/* Pre-shared secret session resumption functions */
int SSL_set_session_secret_cb(SSL *s,
    tls_session_secret_cb_fn tls_session_secret_cb, void *arg);

void SSL_set_debug(SSL *s, int debug);
int SSL_cache_hit(SSL *s);

/* BEGIN ERROR CODES */
/* The following lines are auto generated by the script mkerr.pl. Any changes
 * made after this point may be overwritten when the script is next run.
 */
void ERR_load_SSL_strings(void);

/* Error codes for the SSL functions. */

/* Function codes. */
#define SSL_F_CLIENT_CERTIFICATE			 100
#define SSL_F_CLIENT_FINISHED				 167
#define SSL_F_CLIENT_HELLO				 101
#define SSL_F_CLIENT_MASTER_KEY				 102
#define SSL_F_D2I_SSL_SESSION				 103
#define SSL_F_DO_DTLS1_WRITE				 245
#define SSL_F_DO_SSL3_WRITE				 104
#define SSL_F_DTLS1_ACCEPT				 246
#define SSL_F_DTLS1_ADD_CERT_TO_BUF			 295
#define SSL_F_DTLS1_BUFFER_RECORD			 247
#define SSL_F_DTLS1_CHECK_TIMEOUT_NUM			 316
#define SSL_F_DTLS1_CLIENT_HELLO			 248
#define SSL_F_DTLS1_CONNECT				 249
#define SSL_F_DTLS1_ENC					 250
#define SSL_F_DTLS1_GET_HELLO_VERIFY			 251
#define SSL_F_DTLS1_GET_MESSAGE				 252
#define SSL_F_DTLS1_GET_MESSAGE_FRAGMENT		 253
#define SSL_F_DTLS1_GET_RECORD				 254
#define SSL_F_DTLS1_HANDLE_TIMEOUT			 297
#define SSL_F_DTLS1_HEARTBEAT				 305
#define SSL_F_DTLS1_OUTPUT_CERT_CHAIN			 255
#define SSL_F_DTLS1_PREPROCESS_FRAGMENT			 288
#define SSL_F_DTLS1_PROCESS_OUT_OF_SEQ_MESSAGE		 256
#define SSL_F_DTLS1_PROCESS_RECORD			 257
#define SSL_F_DTLS1_READ_BYTES				 258
#define SSL_F_DTLS1_READ_FAILED				 259
#define SSL_F_DTLS1_SEND_CERTIFICATE_REQUEST		 260
#define SSL_F_DTLS1_SEND_CLIENT_CERTIFICATE		 261
#define SSL_F_DTLS1_SEND_CLIENT_KEY_EXCHANGE		 262
#define SSL_F_DTLS1_SEND_CLIENT_VERIFY			 263
#define SSL_F_DTLS1_SEND_HELLO_VERIFY_REQUEST		 264
#define SSL_F_DTLS1_SEND_SERVER_CERTIFICATE		 265
#define SSL_F_DTLS1_SEND_SERVER_HELLO			 266
#define SSL_F_DTLS1_SEND_SERVER_KEY_EXCHANGE		 267
#define SSL_F_DTLS1_WRITE_APP_DATA_BYTES		 268
#define SSL_F_GET_CLIENT_FINISHED			 105
#define SSL_F_GET_CLIENT_HELLO				 106
#define SSL_F_GET_CLIENT_MASTER_KEY			 107
#define SSL_F_GET_SERVER_FINISHED			 108
#define SSL_F_GET_SERVER_HELLO				 109
#define SSL_F_GET_SERVER_VERIFY				 110
#define SSL_F_I2D_SSL_SESSION				 111
#define SSL_F_READ_N					 112
#define SSL_F_REQUEST_CERTIFICATE			 113
#define SSL_F_SERVER_FINISH				 239
#define SSL_F_SERVER_HELLO				 114
#define SSL_F_SERVER_VERIFY				 240
#define SSL_F_SSL23_ACCEPT				 115
#define SSL_F_SSL23_CLIENT_HELLO			 116
#define SSL_F_SSL23_CONNECT				 117
#define SSL_F_SSL23_GET_CLIENT_HELLO			 118
#define SSL_F_SSL23_GET_SERVER_HELLO			 119
#define SSL_F_SSL23_PEEK				 237
#define SSL_F_SSL23_READ				 120
#define SSL_F_SSL23_WRITE				 121
#define SSL_F_SSL2_ACCEPT				 122
#define SSL_F_SSL2_CONNECT				 123
#define SSL_F_SSL2_ENC_INIT				 124
#define SSL_F_SSL2_GENERATE_KEY_MATERIAL		 241
#define SSL_F_SSL2_PEEK					 234
#define SSL_F_SSL2_READ					 125
#define SSL_F_SSL2_READ_INTERNAL			 236
#define SSL_F_SSL2_SET_CERTIFICATE			 126
#define SSL_F_SSL2_WRITE				 127
#define SSL_F_SSL3_ACCEPT				 128
#define SSL_F_SSL3_ADD_CERT_TO_BUF			 296
#define SSL_F_SSL3_CALLBACK_CTRL			 233
#define SSL_F_SSL3_CHANGE_CIPHER_STATE			 129
#define SSL_F_SSL3_CHECK_CERT_AND_ALGORITHM		 130
#define SSL_F_SSL3_CHECK_CLIENT_HELLO			 304
#define SSL_F_SSL3_CLIENT_HELLO				 131
#define SSL_F_SSL3_CONNECT				 132
#define SSL_F_SSL3_CTRL					 213
#define SSL_F_SSL3_CTX_CTRL				 133
#define SSL_F_SSL3_DIGEST_CACHED_RECORDS		 293
#define SSL_F_SSL3_DO_CHANGE_CIPHER_SPEC		 292
#define SSL_F_SSL3_ENC					 134
#define SSL_F_SSL3_GENERATE_KEY_BLOCK			 238
#define SSL_F_SSL3_GET_CERTIFICATE_REQUEST		 135
#define SSL_F_SSL3_GET_CERT_STATUS			 289
#define SSL_F_SSL3_GET_CERT_VERIFY			 136
#define SSL_F_SSL3_GET_CLIENT_CERTIFICATE		 137
#define SSL_F_SSL3_GET_CLIENT_HELLO			 138
#define SSL_F_SSL3_GET_CLIENT_KEY_EXCHANGE		 139
#define SSL_F_SSL3_GET_FINISHED				 140
#define SSL_F_SSL3_GET_KEY_EXCHANGE			 141
#define SSL_F_SSL3_GET_MESSAGE				 142
#define SSL_F_SSL3_GET_NEW_SESSION_TICKET		 283
#define SSL_F_SSL3_GET_NEXT_PROTO			 306
#define SSL_F_SSL3_GET_RECORD				 143
#define SSL_F_SSL3_GET_SERVER_CERTIFICATE		 144
#define SSL_F_SSL3_GET_SERVER_DONE			 145
#define SSL_F_SSL3_GET_SERVER_HELLO			 146
#define SSL_F_SSL3_HANDSHAKE_MAC			 285
#define SSL_F_SSL3_NEW_SESSION_TICKET			 287
#define SSL_F_SSL3_OUTPUT_CERT_CHAIN			 147
#define SSL_F_SSL3_PEEK					 235
#define SSL_F_SSL3_READ_BYTES				 148
#define SSL_F_SSL3_READ_N				 149
#define SSL_F_SSL3_SEND_CERTIFICATE_REQUEST		 150
#define SSL_F_SSL3_SEND_CLIENT_CERTIFICATE		 151
#define SSL_F_SSL3_SEND_CLIENT_KEY_EXCHANGE		 152
#define SSL_F_SSL3_SEND_CLIENT_VERIFY			 153
#define SSL_F_SSL3_SEND_SERVER_CERTIFICATE		 154
#define SSL_F_SSL3_SEND_SERVER_HELLO			 242
#define SSL_F_SSL3_SEND_SERVER_KEY_EXCHANGE		 155
#define SSL_F_SSL3_SETUP_KEY_BLOCK			 157
#define SSL_F_SSL3_SETUP_READ_BUFFER			 156
#define SSL_F_SSL3_SETUP_WRITE_BUFFER			 291
#define SSL_F_SSL3_WRITE_BYTES				 158
#define SSL_F_SSL3_WRITE_PENDING			 159
#define SSL_F_SSL_ADD_CLIENTHELLO_RENEGOTIATE_EXT	 298
#define SSL_F_SSL_ADD_CLIENTHELLO_TLSEXT		 277
#define SSL_F_SSL_ADD_CLIENTHELLO_USE_SRTP_EXT		 307
#define SSL_F_SSL_ADD_DIR_CERT_SUBJECTS_TO_STACK	 215
#define SSL_F_SSL_ADD_FILE_CERT_SUBJECTS_TO_STACK	 216
#define SSL_F_SSL_ADD_SERVERHELLO_RENEGOTIATE_EXT	 299
#define SSL_F_SSL_ADD_SERVERHELLO_TLSEXT		 278
#define SSL_F_SSL_ADD_SERVERHELLO_USE_SRTP_EXT		 308
#define SSL_F_SSL_BAD_METHOD				 160
#define SSL_F_SSL_BYTES_TO_CIPHER_LIST			 161
#define SSL_F_SSL_CERT_DUP				 221
#define SSL_F_SSL_CERT_INST				 222
#define SSL_F_SSL_CERT_INSTANTIATE			 214
#define SSL_F_SSL_CERT_NEW				 162
#define SSL_F_SSL_CHECK_PRIVATE_KEY			 163
#define SSL_F_SSL_CHECK_SERVERHELLO_TLSEXT		 280
#define SSL_F_SSL_CHECK_SRVR_ECC_CERT_AND_ALG		 279
#define SSL_F_SSL_CIPHER_PROCESS_RULESTR		 230
#define SSL_F_SSL_CIPHER_STRENGTH_SORT			 231
#define SSL_F_SSL_CLEAR					 164
#define SSL_F_SSL_COMP_ADD_COMPRESSION_METHOD		 165
#define SSL_F_SSL_CREATE_CIPHER_LIST			 166
#define SSL_F_SSL_CTRL					 232
#define SSL_F_SSL_CTX_CHECK_PRIVATE_KEY			 168
#define SSL_F_SSL_CTX_MAKE_PROFILES			 309
#define SSL_F_SSL_CTX_NEW				 169
#define SSL_F_SSL_CTX_SET_CIPHER_LIST			 269
#define SSL_F_SSL_CTX_SET_CLIENT_CERT_ENGINE		 290
#define SSL_F_SSL_CTX_SET_PURPOSE			 226
#define SSL_F_SSL_CTX_SET_SESSION_ID_CONTEXT		 219
#define SSL_F_SSL_CTX_SET_SSL_VERSION			 170
#define SSL_F_SSL_CTX_SET_TRUST				 229
#define SSL_F_SSL_CTX_USE_CERTIFICATE			 171
#define SSL_F_SSL_CTX_USE_CERTIFICATE_ASN1		 172
#define SSL_F_SSL_CTX_USE_CERTIFICATE_CHAIN_FILE	 220
#define SSL_F_SSL_CTX_USE_CERTIFICATE_FILE		 173
#define SSL_F_SSL_CTX_USE_PRIVATEKEY			 174
#define SSL_F_SSL_CTX_USE_PRIVATEKEY_ASN1		 175
#define SSL_F_SSL_CTX_USE_PRIVATEKEY_FILE		 176
#define SSL_F_SSL_CTX_USE_PSK_IDENTITY_HINT		 272
#define SSL_F_SSL_CTX_USE_RSAPRIVATEKEY			 177
#define SSL_F_SSL_CTX_USE_RSAPRIVATEKEY_ASN1		 178
#define SSL_F_SSL_CTX_USE_RSAPRIVATEKEY_FILE		 179
#define SSL_F_SSL_DO_HANDSHAKE				 180
#define SSL_F_SSL_GET_NEW_SESSION			 181
#define SSL_F_SSL_GET_PREV_SESSION			 217
#define SSL_F_SSL_GET_SERVER_SEND_CERT			 182
#define SSL_F_SSL_GET_SERVER_SEND_PKEY			 317
#define SSL_F_SSL_GET_SIGN_PKEY				 183
#define SSL_F_SSL_INIT_WBIO_BUFFER			 184
#define SSL_F_SSL_LOAD_CLIENT_CA_FILE			 185
#define SSL_F_SSL_NEW					 186
#define SSL_F_SSL_PARSE_CLIENTHELLO_RENEGOTIATE_EXT	 300
#define SSL_F_SSL_PARSE_CLIENTHELLO_TLSEXT		 302
#define SSL_F_SSL_PARSE_CLIENTHELLO_USE_SRTP_EXT	 310
#define SSL_F_SSL_PARSE_SERVERHELLO_RENEGOTIATE_EXT	 301
#define SSL_F_SSL_PARSE_SERVERHELLO_TLSEXT		 303
#define SSL_F_SSL_PARSE_SERVERHELLO_USE_SRTP_EXT	 311
#define SSL_F_SSL_PEEK					 270
#define SSL_F_SSL_PREPARE_CLIENTHELLO_TLSEXT		 281
#define SSL_F_SSL_PREPARE_SERVERHELLO_TLSEXT		 282
#define SSL_F_SSL_READ					 223
#define SSL_F_SSL_RSA_PRIVATE_DECRYPT			 187
#define SSL_F_SSL_RSA_PUBLIC_ENCRYPT			 188
#define SSL_F_SSL_SESSION_NEW				 189
#define SSL_F_SSL_SESSION_PRINT_FP			 190
#define SSL_F_SSL_SESSION_SET1_ID_CONTEXT		 312
#define SSL_F_SSL_SESS_CERT_NEW				 225
#define SSL_F_SSL_SET_CERT				 191
#define SSL_F_SSL_SET_CIPHER_LIST			 271
#define SSL_F_SSL_SET_FD				 192
#define SSL_F_SSL_SET_PKEY				 193
#define SSL_F_SSL_SET_PURPOSE				 227
#define SSL_F_SSL_SET_RFD				 194
#define SSL_F_SSL_SET_SESSION				 195
#define SSL_F_SSL_SET_SESSION_ID_CONTEXT		 218
#define SSL_F_SSL_SET_SESSION_TICKET_EXT		 294
#define SSL_F_SSL_SET_TRUST				 228
#define SSL_F_SSL_SET_WFD				 196
#define SSL_F_SSL_SHUTDOWN				 224
#define SSL_F_SSL_SRP_CTX_INIT				 313
#define SSL_F_SSL_UNDEFINED_CONST_FUNCTION		 243
#define SSL_F_SSL_UNDEFINED_FUNCTION			 197
#define SSL_F_SSL_UNDEFINED_VOID_FUNCTION		 244
#define SSL_F_SSL_USE_CERTIFICATE			 198
#define SSL_F_SSL_USE_CERTIFICATE_ASN1			 199
#define SSL_F_SSL_USE_CERTIFICATE_FILE			 200
#define SSL_F_SSL_USE_PRIVATEKEY			 201
#define SSL_F_SSL_USE_PRIVATEKEY_ASN1			 202
#define SSL_F_SSL_USE_PRIVATEKEY_FILE			 203
#define SSL_F_SSL_USE_PSK_IDENTITY_HINT			 273
#define SSL_F_SSL_USE_RSAPRIVATEKEY			 204
#define SSL_F_SSL_USE_RSAPRIVATEKEY_ASN1		 205
#define SSL_F_SSL_USE_RSAPRIVATEKEY_FILE		 206
#define SSL_F_SSL_VERIFY_CERT_CHAIN			 207
#define SSL_F_SSL_WRITE					 208
#define SSL_F_TLS1_AEAD_CTX_INIT			 339
#define SSL_F_TLS1_CERT_VERIFY_MAC			 286
#define SSL_F_TLS1_CHANGE_CIPHER_STATE			 209
#define SSL_F_TLS1_CHANGE_CIPHER_STATE_AEAD		 340
#define SSL_F_TLS1_CHANGE_CIPHER_STATE_CIPHER		 338
#define SSL_F_TLS1_CHECK_SERVERHELLO_TLSEXT		 274
#define SSL_F_TLS1_ENC					 210
#define SSL_F_TLS1_EXPORT_KEYING_MATERIAL		 314
#define SSL_F_TLS1_HEARTBEAT				 315
#define SSL_F_TLS1_PREPARE_CLIENTHELLO_TLSEXT		 275
#define SSL_F_TLS1_PREPARE_SERVERHELLO_TLSEXT		 276
#define SSL_F_TLS1_PRF					 284
#define SSL_F_TLS1_SETUP_KEY_BLOCK			 211
#define SSL_F_WRITE_PENDING				 212

/* Reason codes. */
#define SSL_R_APP_DATA_IN_HANDSHAKE			 100
#define SSL_R_ATTEMPT_TO_REUSE_SESSION_IN_DIFFERENT_CONTEXT 272
#define SSL_R_BAD_ALERT_RECORD				 101
#define SSL_R_BAD_AUTHENTICATION_TYPE			 102
#define SSL_R_BAD_CHANGE_CIPHER_SPEC			 103
#define SSL_R_BAD_CHECKSUM				 104
#define SSL_R_BAD_DATA_RETURNED_BY_CALLBACK		 106
#define SSL_R_BAD_DECOMPRESSION				 107
#define SSL_R_BAD_DH_G_LENGTH				 108
#define SSL_R_BAD_DH_PUB_KEY_LENGTH			 109
#define SSL_R_BAD_DH_P_LENGTH				 110
#define SSL_R_BAD_DIGEST_LENGTH				 111
#define SSL_R_BAD_DSA_SIGNATURE				 112
#define SSL_R_BAD_ECC_CERT				 304
#define SSL_R_BAD_ECDSA_SIGNATURE			 305
#define SSL_R_BAD_ECPOINT				 306
#define SSL_R_BAD_HANDSHAKE_LENGTH			 332
#define SSL_R_BAD_HELLO_REQUEST				 105
#define SSL_R_BAD_LENGTH				 271
#define SSL_R_BAD_MAC_DECODE				 113
#define SSL_R_BAD_MAC_LENGTH				 333
#define SSL_R_BAD_MESSAGE_TYPE				 114
#define SSL_R_BAD_PACKET_LENGTH				 115
#define SSL_R_BAD_PROTOCOL_VERSION_NUMBER		 116
#define SSL_R_BAD_PSK_IDENTITY_HINT_LENGTH		 316
#define SSL_R_BAD_RESPONSE_ARGUMENT			 117
#define SSL_R_BAD_RSA_DECRYPT				 118
#define SSL_R_BAD_RSA_ENCRYPT				 119
#define SSL_R_BAD_RSA_E_LENGTH				 120
#define SSL_R_BAD_RSA_MODULUS_LENGTH			 121
#define SSL_R_BAD_RSA_SIGNATURE				 122
#define SSL_R_BAD_SIGNATURE				 123
#define SSL_R_BAD_SRP_A_LENGTH				 347
#define SSL_R_BAD_SRP_B_LENGTH				 348
#define SSL_R_BAD_SRP_G_LENGTH				 349
#define SSL_R_BAD_SRP_N_LENGTH				 350
#define SSL_R_BAD_SRP_S_LENGTH				 351
#define SSL_R_BAD_SRTP_MKI_VALUE			 352
#define SSL_R_BAD_SRTP_PROTECTION_PROFILE_LIST		 353
#define SSL_R_BAD_SSL_FILETYPE				 124
#define SSL_R_BAD_SSL_SESSION_ID_LENGTH			 125
#define SSL_R_BAD_STATE					 126
#define SSL_R_BAD_WRITE_RETRY				 127
#define SSL_R_BIO_NOT_SET				 128
#define SSL_R_BLOCK_CIPHER_PAD_IS_WRONG			 129
#define SSL_R_BN_LIB					 130
#define SSL_R_CA_DN_LENGTH_MISMATCH			 131
#define SSL_R_CA_DN_TOO_LONG				 132
#define SSL_R_CCS_RECEIVED_EARLY			 133
#define SSL_R_CERTIFICATE_VERIFY_FAILED			 134
#define SSL_R_CERT_LENGTH_MISMATCH			 135
#define SSL_R_CHALLENGE_IS_DIFFERENT			 136
#define SSL_R_CIPHER_CODE_WRONG_LENGTH			 137
#define SSL_R_CIPHER_COMPRESSION_UNAVAILABLE		 371
#define SSL_R_CIPHER_OR_HASH_UNAVAILABLE		 138
#define SSL_R_CIPHER_TABLE_SRC_ERROR			 139
#define SSL_R_CLIENTHELLO_TLSEXT			 226
#define SSL_R_COMPRESSED_LENGTH_TOO_LONG		 140
#define SSL_R_COMPRESSION_DISABLED			 343
#define SSL_R_COMPRESSION_FAILURE			 141
#define SSL_R_COMPRESSION_ID_NOT_WITHIN_PRIVATE_RANGE	 307
#define SSL_R_COMPRESSION_LIBRARY_ERROR			 142
#define SSL_R_CONNECTION_ID_IS_DIFFERENT		 143
#define SSL_R_CONNECTION_TYPE_NOT_SET			 144
#define SSL_R_COOKIE_MISMATCH				 308
#define SSL_R_DATA_BETWEEN_CCS_AND_FINISHED		 145
#define SSL_R_DATA_LENGTH_TOO_LONG			 146
#define SSL_R_DECRYPTION_FAILED				 147
#define SSL_R_DECRYPTION_FAILED_OR_BAD_RECORD_MAC	 281
#define SSL_R_DH_PUBLIC_VALUE_LENGTH_IS_WRONG		 148
#define SSL_R_DIGEST_CHECK_FAILED			 149
#define SSL_R_DTLS_MESSAGE_TOO_BIG			 334
#define SSL_R_DUPLICATE_COMPRESSION_ID			 309
#define SSL_R_ECC_CERT_NOT_FOR_KEY_AGREEMENT		 317
#define SSL_R_ECC_CERT_NOT_FOR_SIGNING			 318
#define SSL_R_ECC_CERT_SHOULD_HAVE_RSA_SIGNATURE	 322
#define SSL_R_ECC_CERT_SHOULD_HAVE_SHA1_SIGNATURE	 323
#define SSL_R_ECGROUP_TOO_LARGE_FOR_CIPHER		 310
#define SSL_R_EMPTY_SRTP_PROTECTION_PROFILE_LIST	 354
#define SSL_R_ENCRYPTED_LENGTH_TOO_LONG			 150
#define SSL_R_ERROR_GENERATING_TMP_RSA_KEY		 282
#define SSL_R_ERROR_IN_RECEIVED_CIPHER_LIST		 151
#define SSL_R_EXCESSIVE_MESSAGE_SIZE			 152
#define SSL_R_EXTRA_DATA_IN_MESSAGE			 153
#define SSL_R_GOT_A_FIN_BEFORE_A_CCS			 154
#define SSL_R_GOT_NEXT_PROTO_BEFORE_A_CCS		 355
#define SSL_R_GOT_NEXT_PROTO_WITHOUT_EXTENSION		 356
#define SSL_R_HTTPS_PROXY_REQUEST			 155
#define SSL_R_HTTP_REQUEST				 156
#define SSL_R_ILLEGAL_PADDING				 283
#define SSL_R_INAPPROPRIATE_FALLBACK			 373
#define SSL_R_INCONSISTENT_COMPRESSION			 340
#define SSL_R_INVALID_CHALLENGE_LENGTH			 158
#define SSL_R_INVALID_COMMAND				 280
#define SSL_R_INVALID_COMPRESSION_ALGORITHM		 341
#define SSL_R_INVALID_PURPOSE				 278
#define SSL_R_INVALID_SRP_USERNAME			 357
#define SSL_R_INVALID_STATUS_RESPONSE			 328
#define SSL_R_INVALID_TICKET_KEYS_LENGTH		 325
#define SSL_R_INVALID_TRUST				 279
#define SSL_R_KEY_ARG_TOO_LONG				 284
#define SSL_R_KRB5					 285
#define SSL_R_KRB5_C_CC_PRINC				 286
#define SSL_R_KRB5_C_GET_CRED				 287
#define SSL_R_KRB5_C_INIT				 288
#define SSL_R_KRB5_C_MK_REQ				 289
#define SSL_R_KRB5_S_BAD_TICKET				 290
#define SSL_R_KRB5_S_INIT				 291
#define SSL_R_KRB5_S_RD_REQ				 292
#define SSL_R_KRB5_S_TKT_EXPIRED			 293
#define SSL_R_KRB5_S_TKT_NYV				 294
#define SSL_R_KRB5_S_TKT_SKEW				 295
#define SSL_R_LENGTH_MISMATCH				 159
#define SSL_R_LENGTH_TOO_SHORT				 160
#define SSL_R_LIBRARY_BUG				 274
#define SSL_R_LIBRARY_HAS_NO_CIPHERS			 161
#define SSL_R_MESSAGE_TOO_LONG				 296
#define SSL_R_MISSING_DH_DSA_CERT			 162
#define SSL_R_MISSING_DH_KEY				 163
#define SSL_R_MISSING_DH_RSA_CERT			 164
#define SSL_R_MISSING_DSA_SIGNING_CERT			 165
#define SSL_R_MISSING_EXPORT_TMP_DH_KEY			 166
#define SSL_R_MISSING_EXPORT_TMP_RSA_KEY		 167
#define SSL_R_MISSING_RSA_CERTIFICATE			 168
#define SSL_R_MISSING_RSA_ENCRYPTING_CERT		 169
#define SSL_R_MISSING_RSA_SIGNING_CERT			 170
#define SSL_R_MISSING_SRP_PARAM				 358
#define SSL_R_MISSING_TMP_DH_KEY			 171
#define SSL_R_MISSING_TMP_ECDH_KEY			 311
#define SSL_R_MISSING_TMP_RSA_KEY			 172
#define SSL_R_MISSING_TMP_RSA_PKEY			 173
#define SSL_R_MISSING_VERIFY_MESSAGE			 174
#define SSL_R_MULTIPLE_SGC_RESTARTS			 346
#define SSL_R_NON_SSLV2_INITIAL_PACKET			 175
#define SSL_R_NO_CERTIFICATES_RETURNED			 176
#define SSL_R_NO_CERTIFICATE_ASSIGNED			 177
#define SSL_R_NO_CERTIFICATE_RETURNED			 178
#define SSL_R_NO_CERTIFICATE_SET			 179
#define SSL_R_NO_CERTIFICATE_SPECIFIED			 180
#define SSL_R_NO_CIPHERS_AVAILABLE			 181
#define SSL_R_NO_CIPHERS_PASSED				 182
#define SSL_R_NO_CIPHERS_SPECIFIED			 183
#define SSL_R_NO_CIPHER_LIST				 184
#define SSL_R_NO_CIPHER_MATCH				 185
#define SSL_R_NO_CLIENT_CERT_METHOD			 331
#define SSL_R_NO_CLIENT_CERT_RECEIVED			 186
#define SSL_R_NO_COMPRESSION_SPECIFIED			 187
#define SSL_R_NO_GOST_CERTIFICATE_SENT_BY_PEER		 330
#define SSL_R_NO_METHOD_SPECIFIED			 188
#define SSL_R_NO_PRIVATEKEY				 189
#define SSL_R_NO_PRIVATE_KEY_ASSIGNED			 190
#define SSL_R_NO_PROTOCOLS_AVAILABLE			 191
#define SSL_R_NO_PUBLICKEY				 192
#define SSL_R_NO_RENEGOTIATION				 339
#define SSL_R_NO_REQUIRED_DIGEST			 324
#define SSL_R_NO_SHARED_CIPHER				 193
#define SSL_R_NO_SRTP_PROFILES				 359
#define SSL_R_NO_VERIFY_CALLBACK			 194
#define SSL_R_NULL_SSL_CTX				 195
#define SSL_R_NULL_SSL_METHOD_PASSED			 196
#define SSL_R_OLD_SESSION_CIPHER_NOT_RETURNED		 197
#define SSL_R_OLD_SESSION_COMPRESSION_ALGORITHM_NOT_RETURNED 344
#define SSL_R_ONLY_TLS_ALLOWED_IN_FIPS_MODE		 297
#define SSL_R_PACKET_LENGTH_TOO_LONG			 198
#define SSL_R_PARSE_TLSEXT				 227
#define SSL_R_PATH_TOO_LONG				 270
#define SSL_R_PEER_DID_NOT_RETURN_A_CERTIFICATE		 199
#define SSL_R_PEER_ERROR				 200
#define SSL_R_PEER_ERROR_CERTIFICATE			 201
#define SSL_R_PEER_ERROR_NO_CERTIFICATE			 202
#define SSL_R_PEER_ERROR_NO_CIPHER			 203
#define SSL_R_PEER_ERROR_UNSUPPORTED_CERTIFICATE_TYPE	 204
#define SSL_R_PRE_MAC_LENGTH_TOO_LONG			 205
#define SSL_R_PROBLEMS_MAPPING_CIPHER_FUNCTIONS		 206
#define SSL_R_PROTOCOL_IS_SHUTDOWN			 207
#define SSL_R_PSK_IDENTITY_NOT_FOUND			 223
#define SSL_R_PSK_NO_CLIENT_CB				 224
#define SSL_R_PSK_NO_SERVER_CB				 225
#define SSL_R_PUBLIC_KEY_ENCRYPT_ERROR			 208
#define SSL_R_PUBLIC_KEY_IS_NOT_RSA			 209
#define SSL_R_PUBLIC_KEY_NOT_RSA			 210
#define SSL_R_READ_BIO_NOT_SET				 211
#define SSL_R_READ_TIMEOUT_EXPIRED			 312
#define SSL_R_READ_WRONG_PACKET_TYPE			 212
#define SSL_R_RECORD_LENGTH_MISMATCH			 213
#define SSL_R_RECORD_TOO_LARGE				 214
#define SSL_R_RECORD_TOO_SMALL				 298
#define SSL_R_RENEGOTIATE_EXT_TOO_LONG			 335
#define SSL_R_RENEGOTIATION_ENCODING_ERR		 336
#define SSL_R_RENEGOTIATION_MISMATCH			 337
#define SSL_R_REQUIRED_CIPHER_MISSING			 215
#define SSL_R_REQUIRED_COMPRESSSION_ALGORITHM_MISSING	 342
#define SSL_R_REUSE_CERT_LENGTH_NOT_ZERO		 216
#define SSL_R_REUSE_CERT_TYPE_NOT_ZERO			 217
#define SSL_R_REUSE_CIPHER_LIST_NOT_ZERO		 218
#define SSL_R_SCSV_RECEIVED_WHEN_RENEGOTIATING		 345
#define SSL_R_SERVERHELLO_TLSEXT			 275
#define SSL_R_SESSION_ID_CONTEXT_UNINITIALIZED		 277
#define SSL_R_SHORT_READ				 219
#define SSL_R_SIGNATURE_ALGORITHMS_ERROR		 360
#define SSL_R_SIGNATURE_FOR_NON_SIGNING_CERTIFICATE	 220
#define SSL_R_SRP_A_CALC				 361
#define SSL_R_SRTP_COULD_NOT_ALLOCATE_PROFILES		 362
#define SSL_R_SRTP_PROTECTION_PROFILE_LIST_TOO_LONG	 363
#define SSL_R_SRTP_UNKNOWN_PROTECTION_PROFILE		 364
#define SSL_R_SSL23_DOING_SESSION_ID_REUSE		 221
#define SSL_R_SSL2_CONNECTION_ID_TOO_LONG		 299
#define SSL_R_SSL3_EXT_INVALID_ECPOINTFORMAT		 321
#define SSL_R_SSL3_EXT_INVALID_SERVERNAME		 319
#define SSL_R_SSL3_EXT_INVALID_SERVERNAME_TYPE		 320
#define SSL_R_SSL3_SESSION_ID_TOO_LONG			 300
#define SSL_R_SSL3_SESSION_ID_TOO_SHORT			 222
#define SSL_R_SSLV3_ALERT_BAD_CERTIFICATE		 1042
#define SSL_R_SSLV3_ALERT_BAD_RECORD_MAC		 1020
#define SSL_R_SSLV3_ALERT_CERTIFICATE_EXPIRED		 1045
#define SSL_R_SSLV3_ALERT_CERTIFICATE_REVOKED		 1044
#define SSL_R_SSLV3_ALERT_CERTIFICATE_UNKNOWN		 1046
#define SSL_R_SSLV3_ALERT_DECOMPRESSION_FAILURE		 1030
#define SSL_R_SSLV3_ALERT_HANDSHAKE_FAILURE		 1040
#define SSL_R_SSLV3_ALERT_ILLEGAL_PARAMETER		 1047
#define SSL_R_SSLV3_ALERT_NO_CERTIFICATE		 1041
#define SSL_R_SSLV3_ALERT_UNEXPECTED_MESSAGE		 1010
#define SSL_R_SSLV3_ALERT_UNSUPPORTED_CERTIFICATE	 1043
#define SSL_R_SSL_CTX_HAS_NO_DEFAULT_SSL_VERSION	 228
#define SSL_R_SSL_HANDSHAKE_FAILURE			 229
#define SSL_R_SSL_LIBRARY_HAS_NO_CIPHERS		 230
#define SSL_R_SSL_SESSION_ID_CALLBACK_FAILED		 301
#define SSL_R_SSL_SESSION_ID_CONFLICT			 302
#define SSL_R_SSL_SESSION_ID_CONTEXT_TOO_LONG		 273
#define SSL_R_SSL_SESSION_ID_HAS_BAD_LENGTH		 303
#define SSL_R_SSL_SESSION_ID_IS_DIFFERENT		 231
#define SSL_R_TLSV1_ALERT_ACCESS_DENIED			 1049
#define SSL_R_TLSV1_ALERT_DECODE_ERROR			 1050
#define SSL_R_TLSV1_ALERT_DECRYPTION_FAILED		 1021
#define SSL_R_TLSV1_ALERT_DECRYPT_ERROR			 1051
#define SSL_R_TLSV1_ALERT_EXPORT_RESTRICTION		 1060
#define SSL_R_TLSV1_ALERT_INAPPROPRIATE_FALLBACK	 1086
#define SSL_R_TLSV1_ALERT_INSUFFICIENT_SECURITY		 1071
#define SSL_R_TLSV1_ALERT_INTERNAL_ERROR		 1080
#define SSL_R_TLSV1_ALERT_NO_RENEGOTIATION		 1100
#define SSL_R_TLSV1_ALERT_PROTOCOL_VERSION		 1070
#define SSL_R_TLSV1_ALERT_RECORD_OVERFLOW		 1022
#define SSL_R_TLSV1_ALERT_UNKNOWN_CA			 1048
#define SSL_R_TLSV1_ALERT_USER_CANCELLED		 1090
#define SSL_R_TLSV1_BAD_CERTIFICATE_HASH_VALUE		 1114
#define SSL_R_TLSV1_BAD_CERTIFICATE_STATUS_RESPONSE	 1113
#define SSL_R_TLSV1_CERTIFICATE_UNOBTAINABLE		 1111
#define SSL_R_TLSV1_UNRECOGNIZED_NAME			 1112
#define SSL_R_TLSV1_UNSUPPORTED_EXTENSION		 1110
#define SSL_R_TLS_CLIENT_CERT_REQ_WITH_ANON_CIPHER	 232
#define SSL_R_TLS_HEARTBEAT_PEER_DOESNT_ACCEPT		 365
#define SSL_R_TLS_HEARTBEAT_PENDING			 366
#define SSL_R_TLS_ILLEGAL_EXPORTER_LABEL		 367
#define SSL_R_TLS_INVALID_ECPOINTFORMAT_LIST		 157
#define SSL_R_TLS_PEER_DID_NOT_RESPOND_WITH_CERTIFICATE_LIST 233
#define SSL_R_TLS_RSA_ENCRYPTED_VALUE_LENGTH_IS_WRONG	 234
#define SSL_R_TRIED_TO_USE_UNSUPPORTED_CIPHER		 235
#define SSL_R_UNABLE_TO_DECODE_DH_CERTS			 236
#define SSL_R_UNABLE_TO_DECODE_ECDH_CERTS		 313
#define SSL_R_UNABLE_TO_EXTRACT_PUBLIC_KEY		 237
#define SSL_R_UNABLE_TO_FIND_DH_PARAMETERS		 238
#define SSL_R_UNABLE_TO_FIND_ECDH_PARAMETERS		 314
#define SSL_R_UNABLE_TO_FIND_PUBLIC_KEY_PARAMETERS	 239
#define SSL_R_UNABLE_TO_FIND_SSL_METHOD			 240
#define SSL_R_UNABLE_TO_LOAD_SSL2_MD5_ROUTINES		 241
#define SSL_R_UNABLE_TO_LOAD_SSL3_MD5_ROUTINES		 242
#define SSL_R_UNABLE_TO_LOAD_SSL3_SHA1_ROUTINES		 243
#define SSL_R_UNEXPECTED_MESSAGE			 244
#define SSL_R_UNEXPECTED_RECORD				 245
#define SSL_R_UNINITIALIZED				 276
#define SSL_R_UNKNOWN_ALERT_TYPE			 246
#define SSL_R_UNKNOWN_CERTIFICATE_TYPE			 247
#define SSL_R_UNKNOWN_CIPHER_RETURNED			 248
#define SSL_R_UNKNOWN_CIPHER_TYPE			 249
#define SSL_R_UNKNOWN_DIGEST				 368
#define SSL_R_UNKNOWN_KEY_EXCHANGE_TYPE			 250
#define SSL_R_UNKNOWN_PKEY_TYPE				 251
#define SSL_R_UNKNOWN_PROTOCOL				 252
#define SSL_R_UNKNOWN_REMOTE_ERROR_TYPE			 253
#define SSL_R_UNKNOWN_SSL_VERSION			 254
#define SSL_R_UNKNOWN_STATE				 255
#define SSL_R_UNSAFE_LEGACY_RENEGOTIATION_DISABLED	 338
#define SSL_R_UNSUPPORTED_CIPHER			 256
#define SSL_R_UNSUPPORTED_COMPRESSION_ALGORITHM		 257
#define SSL_R_UNSUPPORTED_DIGEST_TYPE			 326
#define SSL_R_UNSUPPORTED_ELLIPTIC_CURVE		 315
#define SSL_R_UNSUPPORTED_PROTOCOL			 258
#define SSL_R_UNSUPPORTED_SSL_VERSION			 259
#define SSL_R_UNSUPPORTED_STATUS_TYPE			 329
#define SSL_R_USE_SRTP_NOT_NEGOTIATED			 369
#define SSL_R_WRITE_BIO_NOT_SET				 260
#define SSL_R_WRONG_CIPHER_RETURNED			 261
#define SSL_R_WRONG_CURVE				 378
#define SSL_R_WRONG_MESSAGE_TYPE			 262
#define SSL_R_WRONG_NUMBER_OF_KEY_BITS			 263
#define SSL_R_WRONG_SIGNATURE_LENGTH			 264
#define SSL_R_WRONG_SIGNATURE_SIZE			 265
#define SSL_R_WRONG_SIGNATURE_TYPE			 370
#define SSL_R_WRONG_SSL_VERSION				 266
#define SSL_R_WRONG_VERSION_NUMBER			 267
#define SSL_R_X509_LIB					 268
#define SSL_R_X509_VERIFICATION_SETUP_PROBLEMS		 269
#define SSL_R_PEER_BEHAVING_BADLY			 666

#ifdef  __cplusplus
}
#endif
#endif
