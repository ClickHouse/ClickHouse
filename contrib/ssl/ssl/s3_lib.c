/* $OpenBSD: s3_lib.c,v 1.161 2017/09/25 18:04:08 jsing Exp $ */
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
#include <stdio.h>

#include <openssl/bn.h>
#include <openssl/curve25519.h>
#include <openssl/dh.h>
#include <openssl/md5.h>
#include <openssl/objects.h>

#include "ssl_locl.h"
#include "bytestring.h"

#define SSL3_NUM_CIPHERS	(sizeof(ssl3_ciphers) / sizeof(SSL_CIPHER))

/*
 * FIXED_NONCE_LEN is a macro that provides in the correct value to set the
 * fixed nonce length in algorithms2. It is the inverse of the
 * SSL_CIPHER_AEAD_FIXED_NONCE_LEN macro.
 */
#define FIXED_NONCE_LEN(x) (((x / 2) & 0xf) << 24)

/* list of available SSLv3 ciphers (sorted by id) */
SSL_CIPHER ssl3_ciphers[] = {

	/* The RSA ciphers */
	/* Cipher 01 */
	{
		.valid = 1,
		.name = SSL3_TXT_RSA_NULL_MD5,
		.id = SSL3_CK_RSA_NULL_MD5,
		.algorithm_mkey = SSL_kRSA,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_eNULL,
		.algorithm_mac = SSL_MD5,
		.algorithm_ssl = SSL_SSLV3,
		.algo_strength = SSL_STRONG_NONE,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 0,
		.alg_bits = 0,
	},

	/* Cipher 02 */
	{
		.valid = 1,
		.name = SSL3_TXT_RSA_NULL_SHA,
		.id = SSL3_CK_RSA_NULL_SHA,
		.algorithm_mkey = SSL_kRSA,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_eNULL,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_SSLV3,
		.algo_strength = SSL_STRONG_NONE,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 0,
		.alg_bits = 0,
	},

	/* Cipher 04 */
	{
		.valid = 1,
		.name = SSL3_TXT_RSA_RC4_128_MD5,
		.id = SSL3_CK_RSA_RC4_128_MD5,
		.algorithm_mkey = SSL_kRSA,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_RC4,
		.algorithm_mac = SSL_MD5,
		.algorithm_ssl = SSL_SSLV3,
		.algo_strength = SSL_LOW,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher 05 */
	{
		.valid = 1,
		.name = SSL3_TXT_RSA_RC4_128_SHA,
		.id = SSL3_CK_RSA_RC4_128_SHA,
		.algorithm_mkey = SSL_kRSA,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_RC4,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_SSLV3,
		.algo_strength = SSL_LOW,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher 09 */
	{
		.valid = 1,
		.name = SSL3_TXT_RSA_DES_64_CBC_SHA,
		.id = SSL3_CK_RSA_DES_64_CBC_SHA,
		.algorithm_mkey = SSL_kRSA,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_DES,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_SSLV3,
		.algo_strength = SSL_LOW,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 56,
		.alg_bits = 56,
	},

	/* Cipher 0A */
	{
		.valid = 1,
		.name = SSL3_TXT_RSA_DES_192_CBC3_SHA,
		.id = SSL3_CK_RSA_DES_192_CBC3_SHA,
		.algorithm_mkey = SSL_kRSA,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_3DES,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_SSLV3,
		.algo_strength = SSL_MEDIUM,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 112,
		.alg_bits = 168,
	},

	/*
	 * Ephemeral DH (DHE) ciphers.
	 */

	/* Cipher 15 */
	{
		.valid = 1,
		.name = SSL3_TXT_EDH_RSA_DES_64_CBC_SHA,
		.id = SSL3_CK_EDH_RSA_DES_64_CBC_SHA,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_DES,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_SSLV3,
		.algo_strength = SSL_LOW,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 56,
		.alg_bits = 56,
	},

	/* Cipher 16 */
	{
		.valid = 1,
		.name = SSL3_TXT_EDH_RSA_DES_192_CBC3_SHA,
		.id = SSL3_CK_EDH_RSA_DES_192_CBC3_SHA,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_3DES,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_SSLV3,
		.algo_strength = SSL_MEDIUM,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 112,
		.alg_bits = 168,
	},

	/* Cipher 18 */
	{
		.valid = 1,
		.name = SSL3_TXT_ADH_RC4_128_MD5,
		.id = SSL3_CK_ADH_RC4_128_MD5,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aNULL,
		.algorithm_enc = SSL_RC4,
		.algorithm_mac = SSL_MD5,
		.algorithm_ssl = SSL_SSLV3,
		.algo_strength = SSL_LOW,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher 1A */
	{
		.valid = 1,
		.name = SSL3_TXT_ADH_DES_64_CBC_SHA,
		.id = SSL3_CK_ADH_DES_64_CBC_SHA,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aNULL,
		.algorithm_enc = SSL_DES,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_SSLV3,
		.algo_strength = SSL_LOW,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 56,
		.alg_bits = 56,
	},

	/* Cipher 1B */
	{
		.valid = 1,
		.name = SSL3_TXT_ADH_DES_192_CBC_SHA,
		.id = SSL3_CK_ADH_DES_192_CBC_SHA,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aNULL,
		.algorithm_enc = SSL_3DES,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_SSLV3,
		.algo_strength = SSL_MEDIUM,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 112,
		.alg_bits = 168,
	},

	/*
	 * AES ciphersuites.
	 */

	/* Cipher 2F */
	{
		.valid = 1,
		.name = TLS1_TXT_RSA_WITH_AES_128_SHA,
		.id = TLS1_CK_RSA_WITH_AES_128_SHA,
		.algorithm_mkey = SSL_kRSA,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_AES128,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher 33 */
	{
		.valid = 1,
		.name = TLS1_TXT_DHE_RSA_WITH_AES_128_SHA,
		.id = TLS1_CK_DHE_RSA_WITH_AES_128_SHA,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_AES128,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher 34 */
	{
		.valid = 1,
		.name = TLS1_TXT_ADH_WITH_AES_128_SHA,
		.id = TLS1_CK_ADH_WITH_AES_128_SHA,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aNULL,
		.algorithm_enc = SSL_AES128,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher 35 */
	{
		.valid = 1,
		.name = TLS1_TXT_RSA_WITH_AES_256_SHA,
		.id = TLS1_CK_RSA_WITH_AES_256_SHA,
		.algorithm_mkey = SSL_kRSA,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_AES256,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* Cipher 39 */
	{
		.valid = 1,
		.name = TLS1_TXT_DHE_RSA_WITH_AES_256_SHA,
		.id = TLS1_CK_DHE_RSA_WITH_AES_256_SHA,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_AES256,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* Cipher 3A */
	{
		.valid = 1,
		.name = TLS1_TXT_ADH_WITH_AES_256_SHA,
		.id = TLS1_CK_ADH_WITH_AES_256_SHA,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aNULL,
		.algorithm_enc = SSL_AES256,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* TLS v1.2 ciphersuites */
	/* Cipher 3B */
	{
		.valid = 1,
		.name = TLS1_TXT_RSA_WITH_NULL_SHA256,
		.id = TLS1_CK_RSA_WITH_NULL_SHA256,
		.algorithm_mkey = SSL_kRSA,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_eNULL,
		.algorithm_mac = SSL_SHA256,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_STRONG_NONE,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 0,
		.alg_bits = 0,
	},

	/* Cipher 3C */
	{
		.valid = 1,
		.name = TLS1_TXT_RSA_WITH_AES_128_SHA256,
		.id = TLS1_CK_RSA_WITH_AES_128_SHA256,
		.algorithm_mkey = SSL_kRSA,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_AES128,
		.algorithm_mac = SSL_SHA256,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher 3D */
	{
		.valid = 1,
		.name = TLS1_TXT_RSA_WITH_AES_256_SHA256,
		.id = TLS1_CK_RSA_WITH_AES_256_SHA256,
		.algorithm_mkey = SSL_kRSA,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_AES256,
		.algorithm_mac = SSL_SHA256,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 256,
		.alg_bits = 256,
	},

#ifndef OPENSSL_NO_CAMELLIA
	/* Camellia ciphersuites from RFC4132 (128-bit portion) */

	/* Cipher 41 */
	{
		.valid = 1,
		.name = TLS1_TXT_RSA_WITH_CAMELLIA_128_CBC_SHA,
		.id = TLS1_CK_RSA_WITH_CAMELLIA_128_CBC_SHA,
		.algorithm_mkey = SSL_kRSA,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_CAMELLIA128,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher 45 */
	{
		.valid = 1,
		.name = TLS1_TXT_DHE_RSA_WITH_CAMELLIA_128_CBC_SHA,
		.id = TLS1_CK_DHE_RSA_WITH_CAMELLIA_128_CBC_SHA,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_CAMELLIA128,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher 46 */
	{
		.valid = 1,
		.name = TLS1_TXT_ADH_WITH_CAMELLIA_128_CBC_SHA,
		.id = TLS1_CK_ADH_WITH_CAMELLIA_128_CBC_SHA,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aNULL,
		.algorithm_enc = SSL_CAMELLIA128,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 128,
		.alg_bits = 128,
	},
#endif /* OPENSSL_NO_CAMELLIA */

	/* TLS v1.2 ciphersuites */
	/* Cipher 67 */
	{
		.valid = 1,
		.name = TLS1_TXT_DHE_RSA_WITH_AES_128_SHA256,
		.id = TLS1_CK_DHE_RSA_WITH_AES_128_SHA256,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_AES128,
		.algorithm_mac = SSL_SHA256,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher 6B */
	{
		.valid = 1,
		.name = TLS1_TXT_DHE_RSA_WITH_AES_256_SHA256,
		.id = TLS1_CK_DHE_RSA_WITH_AES_256_SHA256,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_AES256,
		.algorithm_mac = SSL_SHA256,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* Cipher 6C */
	{
		.valid = 1,
		.name = TLS1_TXT_ADH_WITH_AES_128_SHA256,
		.id = TLS1_CK_ADH_WITH_AES_128_SHA256,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aNULL,
		.algorithm_enc = SSL_AES128,
		.algorithm_mac = SSL_SHA256,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher 6D */
	{
		.valid = 1,
		.name = TLS1_TXT_ADH_WITH_AES_256_SHA256,
		.id = TLS1_CK_ADH_WITH_AES_256_SHA256,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aNULL,
		.algorithm_enc = SSL_AES256,
		.algorithm_mac = SSL_SHA256,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* GOST Ciphersuites */

	/* Cipher 81 */
	{
		.valid = 1,
		.name = "GOST2001-GOST89-GOST89",
		.id = 0x3000081,
		.algorithm_mkey = SSL_kGOST,
		.algorithm_auth = SSL_aGOST01,
		.algorithm_enc = SSL_eGOST2814789CNT,
		.algorithm_mac = SSL_GOST89MAC,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_GOST94|TLS1_PRF_GOST94|
		    TLS1_STREAM_MAC,
		.strength_bits = 256,
		.alg_bits = 256
	},

	/* Cipher 83 */
	{
		.valid = 1,
		.name = "GOST2001-NULL-GOST94",
		.id = 0x3000083,
		.algorithm_mkey = SSL_kGOST,
		.algorithm_auth = SSL_aGOST01,
		.algorithm_enc = SSL_eNULL,
		.algorithm_mac = SSL_GOST94,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_STRONG_NONE,
		.algorithm2 = SSL_HANDSHAKE_MAC_GOST94|TLS1_PRF_GOST94,
		.strength_bits = 0,
		.alg_bits = 0
	},

#ifndef OPENSSL_NO_CAMELLIA
	/* Camellia ciphersuites from RFC4132 (256-bit portion) */

	/* Cipher 84 */
	{
		.valid = 1,
		.name = TLS1_TXT_RSA_WITH_CAMELLIA_256_CBC_SHA,
		.id = TLS1_CK_RSA_WITH_CAMELLIA_256_CBC_SHA,
		.algorithm_mkey = SSL_kRSA,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_CAMELLIA256,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* Cipher 88 */
	{
		.valid = 1,
		.name = TLS1_TXT_DHE_RSA_WITH_CAMELLIA_256_CBC_SHA,
		.id = TLS1_CK_DHE_RSA_WITH_CAMELLIA_256_CBC_SHA,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_CAMELLIA256,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* Cipher 89 */
	{
		.valid = 1,
		.name = TLS1_TXT_ADH_WITH_CAMELLIA_256_CBC_SHA,
		.id = TLS1_CK_ADH_WITH_CAMELLIA_256_CBC_SHA,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aNULL,
		.algorithm_enc = SSL_CAMELLIA256,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 256,
		.alg_bits = 256,
	},
#endif /* OPENSSL_NO_CAMELLIA */

	/*
	 * GCM ciphersuites from RFC5288.
	 */

	/* Cipher 9C */
	{
		.valid = 1,
		.name = TLS1_TXT_RSA_WITH_AES_128_GCM_SHA256,
		.id = TLS1_CK_RSA_WITH_AES_128_GCM_SHA256,
		.algorithm_mkey = SSL_kRSA,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_AES128GCM,
		.algorithm_mac = SSL_AEAD,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA256|TLS1_PRF_SHA256|
		    SSL_CIPHER_ALGORITHM2_AEAD|FIXED_NONCE_LEN(4)|
		    SSL_CIPHER_ALGORITHM2_VARIABLE_NONCE_IN_RECORD,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher 9D */
	{
		.valid = 1,
		.name = TLS1_TXT_RSA_WITH_AES_256_GCM_SHA384,
		.id = TLS1_CK_RSA_WITH_AES_256_GCM_SHA384,
		.algorithm_mkey = SSL_kRSA,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_AES256GCM,
		.algorithm_mac = SSL_AEAD,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA384|TLS1_PRF_SHA384|
		    SSL_CIPHER_ALGORITHM2_AEAD|FIXED_NONCE_LEN(4)|
		    SSL_CIPHER_ALGORITHM2_VARIABLE_NONCE_IN_RECORD,
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* Cipher 9E */
	{
		.valid = 1,
		.name = TLS1_TXT_DHE_RSA_WITH_AES_128_GCM_SHA256,
		.id = TLS1_CK_DHE_RSA_WITH_AES_128_GCM_SHA256,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_AES128GCM,
		.algorithm_mac = SSL_AEAD,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA256|TLS1_PRF_SHA256|
		    SSL_CIPHER_ALGORITHM2_AEAD|FIXED_NONCE_LEN(4)|
		    SSL_CIPHER_ALGORITHM2_VARIABLE_NONCE_IN_RECORD,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher 9F */
	{
		.valid = 1,
		.name = TLS1_TXT_DHE_RSA_WITH_AES_256_GCM_SHA384,
		.id = TLS1_CK_DHE_RSA_WITH_AES_256_GCM_SHA384,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_AES256GCM,
		.algorithm_mac = SSL_AEAD,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA384|TLS1_PRF_SHA384|
		    SSL_CIPHER_ALGORITHM2_AEAD|FIXED_NONCE_LEN(4)|
		    SSL_CIPHER_ALGORITHM2_VARIABLE_NONCE_IN_RECORD,
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* Cipher A6 */
	{
		.valid = 1,
		.name = TLS1_TXT_ADH_WITH_AES_128_GCM_SHA256,
		.id = TLS1_CK_ADH_WITH_AES_128_GCM_SHA256,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aNULL,
		.algorithm_enc = SSL_AES128GCM,
		.algorithm_mac = SSL_AEAD,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA256|TLS1_PRF_SHA256|
		    SSL_CIPHER_ALGORITHM2_AEAD|FIXED_NONCE_LEN(4)|
		    SSL_CIPHER_ALGORITHM2_VARIABLE_NONCE_IN_RECORD,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher A7 */
	{
		.valid = 1,
		.name = TLS1_TXT_ADH_WITH_AES_256_GCM_SHA384,
		.id = TLS1_CK_ADH_WITH_AES_256_GCM_SHA384,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aNULL,
		.algorithm_enc = SSL_AES256GCM,
		.algorithm_mac = SSL_AEAD,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA384|TLS1_PRF_SHA384|
		    SSL_CIPHER_ALGORITHM2_AEAD|FIXED_NONCE_LEN(4)|
		    SSL_CIPHER_ALGORITHM2_VARIABLE_NONCE_IN_RECORD,
		.strength_bits = 256,
		.alg_bits = 256,
	},

#ifndef OPENSSL_NO_CAMELLIA
	/* TLS 1.2 Camellia SHA-256 ciphersuites from RFC5932 */

	/* Cipher BA */
	{
		.valid = 1,
		.name = TLS1_TXT_RSA_WITH_CAMELLIA_128_CBC_SHA256,
		.id = TLS1_CK_RSA_WITH_CAMELLIA_128_CBC_SHA256,
		.algorithm_mkey = SSL_kRSA,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_CAMELLIA128,
		.algorithm_mac = SSL_SHA256,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA256|TLS1_PRF_SHA256,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher BE */
	{
		.valid = 1,
		.name = TLS1_TXT_DHE_RSA_WITH_CAMELLIA_128_CBC_SHA256,
		.id = TLS1_CK_DHE_RSA_WITH_CAMELLIA_128_CBC_SHA256,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_CAMELLIA128,
		.algorithm_mac = SSL_SHA256,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA256|TLS1_PRF_SHA256,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher BF */
	{
		.valid = 1,
		.name = TLS1_TXT_ADH_WITH_CAMELLIA_128_CBC_SHA256,
		.id = TLS1_CK_ADH_WITH_CAMELLIA_128_CBC_SHA256,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aNULL,
		.algorithm_enc = SSL_CAMELLIA128,
		.algorithm_mac = SSL_SHA256,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA256|TLS1_PRF_SHA256,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher C0 */
	{
		.valid = 1,
		.name = TLS1_TXT_RSA_WITH_CAMELLIA_256_CBC_SHA256,
		.id = TLS1_CK_RSA_WITH_CAMELLIA_256_CBC_SHA256,
		.algorithm_mkey = SSL_kRSA,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_CAMELLIA256,
		.algorithm_mac = SSL_SHA256,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA256|TLS1_PRF_SHA256,
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* Cipher C4 */
	{
		.valid = 1,
		.name = TLS1_TXT_DHE_RSA_WITH_CAMELLIA_256_CBC_SHA256,
		.id = TLS1_CK_DHE_RSA_WITH_CAMELLIA_256_CBC_SHA256,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_CAMELLIA256,
		.algorithm_mac = SSL_SHA256,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA256|TLS1_PRF_SHA256,
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* Cipher C5 */
	{
		.valid = 1,
		.name = TLS1_TXT_ADH_WITH_CAMELLIA_256_CBC_SHA256,
		.id = TLS1_CK_ADH_WITH_CAMELLIA_256_CBC_SHA256,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aNULL,
		.algorithm_enc = SSL_CAMELLIA256,
		.algorithm_mac = SSL_SHA256,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA256|TLS1_PRF_SHA256,
		.strength_bits = 256,
		.alg_bits = 256,
	},
#endif /* OPENSSL_NO_CAMELLIA */

	/* Cipher C006 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_ECDSA_WITH_NULL_SHA,
		.id = TLS1_CK_ECDHE_ECDSA_WITH_NULL_SHA,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aECDSA,
		.algorithm_enc = SSL_eNULL,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_STRONG_NONE,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 0,
		.alg_bits = 0,
	},

	/* Cipher C007 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_ECDSA_WITH_RC4_128_SHA,
		.id = TLS1_CK_ECDHE_ECDSA_WITH_RC4_128_SHA,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aECDSA,
		.algorithm_enc = SSL_RC4,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_LOW,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher C008 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_ECDSA_WITH_DES_192_CBC3_SHA,
		.id = TLS1_CK_ECDHE_ECDSA_WITH_DES_192_CBC3_SHA,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aECDSA,
		.algorithm_enc = SSL_3DES,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_MEDIUM,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 112,
		.alg_bits = 168,
	},

	/* Cipher C009 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
		.id = TLS1_CK_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aECDSA,
		.algorithm_enc = SSL_AES128,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher C00A */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
		.id = TLS1_CK_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aECDSA,
		.algorithm_enc = SSL_AES256,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* Cipher C010 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_RSA_WITH_NULL_SHA,
		.id = TLS1_CK_ECDHE_RSA_WITH_NULL_SHA,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_eNULL,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_STRONG_NONE,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 0,
		.alg_bits = 0,
	},

	/* Cipher C011 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_RSA_WITH_RC4_128_SHA,
		.id = TLS1_CK_ECDHE_RSA_WITH_RC4_128_SHA,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_RC4,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_LOW,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher C012 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_RSA_WITH_DES_192_CBC3_SHA,
		.id = TLS1_CK_ECDHE_RSA_WITH_DES_192_CBC3_SHA,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_3DES,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_MEDIUM,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 112,
		.alg_bits = 168,
	},

	/* Cipher C013 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_RSA_WITH_AES_128_CBC_SHA,
		.id = TLS1_CK_ECDHE_RSA_WITH_AES_128_CBC_SHA,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_AES128,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher C014 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_RSA_WITH_AES_256_CBC_SHA,
		.id = TLS1_CK_ECDHE_RSA_WITH_AES_256_CBC_SHA,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_AES256,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* Cipher C015 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDH_anon_WITH_NULL_SHA,
		.id = TLS1_CK_ECDH_anon_WITH_NULL_SHA,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aNULL,
		.algorithm_enc = SSL_eNULL,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_STRONG_NONE,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 0,
		.alg_bits = 0,
	},

	/* Cipher C016 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDH_anon_WITH_RC4_128_SHA,
		.id = TLS1_CK_ECDH_anon_WITH_RC4_128_SHA,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aNULL,
		.algorithm_enc = SSL_RC4,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_LOW,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher C017 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDH_anon_WITH_DES_192_CBC3_SHA,
		.id = TLS1_CK_ECDH_anon_WITH_DES_192_CBC3_SHA,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aNULL,
		.algorithm_enc = SSL_3DES,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_MEDIUM,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 112,
		.alg_bits = 168,
	},

	/* Cipher C018 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDH_anon_WITH_AES_128_CBC_SHA,
		.id = TLS1_CK_ECDH_anon_WITH_AES_128_CBC_SHA,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aNULL,
		.algorithm_enc = SSL_AES128,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher C019 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDH_anon_WITH_AES_256_CBC_SHA,
		.id = TLS1_CK_ECDH_anon_WITH_AES_256_CBC_SHA,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aNULL,
		.algorithm_enc = SSL_AES256,
		.algorithm_mac = SSL_SHA1,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF,
		.strength_bits = 256,
		.alg_bits = 256,
	},


	/* HMAC based TLS v1.2 ciphersuites from RFC5289 */

	/* Cipher C023 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_ECDSA_WITH_AES_128_SHA256,
		.id = TLS1_CK_ECDHE_ECDSA_WITH_AES_128_SHA256,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aECDSA,
		.algorithm_enc = SSL_AES128,
		.algorithm_mac = SSL_SHA256,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA256|TLS1_PRF_SHA256,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher C024 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_ECDSA_WITH_AES_256_SHA384,
		.id = TLS1_CK_ECDHE_ECDSA_WITH_AES_256_SHA384,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aECDSA,
		.algorithm_enc = SSL_AES256,
		.algorithm_mac = SSL_SHA384,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA384|TLS1_PRF_SHA384,
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* Cipher C027 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_RSA_WITH_AES_128_SHA256,
		.id = TLS1_CK_ECDHE_RSA_WITH_AES_128_SHA256,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_AES128,
		.algorithm_mac = SSL_SHA256,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA256|TLS1_PRF_SHA256,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher C028 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_RSA_WITH_AES_256_SHA384,
		.id = TLS1_CK_ECDHE_RSA_WITH_AES_256_SHA384,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_AES256,
		.algorithm_mac = SSL_SHA384,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA384|TLS1_PRF_SHA384,
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* GCM based TLS v1.2 ciphersuites from RFC5289 */

	/* Cipher C02B */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		.id = TLS1_CK_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aECDSA,
		.algorithm_enc = SSL_AES128GCM,
		.algorithm_mac = SSL_AEAD,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA256|TLS1_PRF_SHA256|
		    SSL_CIPHER_ALGORITHM2_AEAD|FIXED_NONCE_LEN(4)|
		    SSL_CIPHER_ALGORITHM2_VARIABLE_NONCE_IN_RECORD,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher C02C */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		.id = TLS1_CK_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aECDSA,
		.algorithm_enc = SSL_AES256GCM,
		.algorithm_mac = SSL_AEAD,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA384|TLS1_PRF_SHA384|
		    SSL_CIPHER_ALGORITHM2_AEAD|FIXED_NONCE_LEN(4)|
		    SSL_CIPHER_ALGORITHM2_VARIABLE_NONCE_IN_RECORD,
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* Cipher C02F */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		.id = TLS1_CK_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_AES128GCM,
		.algorithm_mac = SSL_AEAD,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA256|TLS1_PRF_SHA256|
		    SSL_CIPHER_ALGORITHM2_AEAD|FIXED_NONCE_LEN(4)|
		    SSL_CIPHER_ALGORITHM2_VARIABLE_NONCE_IN_RECORD,
		.strength_bits = 128,
		.alg_bits = 128,
	},

	/* Cipher C030 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		.id = TLS1_CK_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_AES256GCM,
		.algorithm_mac = SSL_AEAD,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA384|TLS1_PRF_SHA384|
		    SSL_CIPHER_ALGORITHM2_AEAD|FIXED_NONCE_LEN(4)|
		    SSL_CIPHER_ALGORITHM2_VARIABLE_NONCE_IN_RECORD,
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* Cipher CCA8 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		.id = TLS1_CK_ECDHE_RSA_CHACHA20_POLY1305,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_CHACHA20POLY1305,
		.algorithm_mac = SSL_AEAD,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA256|TLS1_PRF_SHA256|
		    SSL_CIPHER_ALGORITHM2_AEAD|FIXED_NONCE_LEN(12),
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* Cipher CCA9 */
	{
		.valid = 1,
		.name = TLS1_TXT_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
		.id = TLS1_CK_ECDHE_ECDSA_CHACHA20_POLY1305,
		.algorithm_mkey = SSL_kECDHE,
		.algorithm_auth = SSL_aECDSA,
		.algorithm_enc = SSL_CHACHA20POLY1305,
		.algorithm_mac = SSL_AEAD,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA256|TLS1_PRF_SHA256|
		    SSL_CIPHER_ALGORITHM2_AEAD|FIXED_NONCE_LEN(12),
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* Cipher CCAA */
	{
		.valid = 1,
		.name = TLS1_TXT_DHE_RSA_WITH_CHACHA20_POLY1305,
		.id = TLS1_CK_DHE_RSA_CHACHA20_POLY1305,
		.algorithm_mkey = SSL_kDHE,
		.algorithm_auth = SSL_aRSA,
		.algorithm_enc = SSL_CHACHA20POLY1305,
		.algorithm_mac = SSL_AEAD,
		.algorithm_ssl = SSL_TLSV1_2,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_SHA256|TLS1_PRF_SHA256|
		    SSL_CIPHER_ALGORITHM2_AEAD|FIXED_NONCE_LEN(12),
		.strength_bits = 256,
		.alg_bits = 256,
	},

	/* Cipher FF85 FIXME IANA */
	{
		.valid = 1,
		.name = "GOST2012256-GOST89-GOST89",
		.id = 0x300ff85, /* FIXME IANA */
		.algorithm_mkey = SSL_kGOST,
		.algorithm_auth = SSL_aGOST01,
		.algorithm_enc = SSL_eGOST2814789CNT,
		.algorithm_mac = SSL_GOST89MAC,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_HIGH,
		.algorithm2 = SSL_HANDSHAKE_MAC_STREEBOG256|TLS1_PRF_STREEBOG256|
		    TLS1_STREAM_MAC,
		.strength_bits = 256,
		.alg_bits = 256
	},

	/* Cipher FF87 FIXME IANA */
	{
		.valid = 1,
		.name = "GOST2012256-NULL-STREEBOG256",
		.id = 0x300ff87, /* FIXME IANA */
		.algorithm_mkey = SSL_kGOST,
		.algorithm_auth = SSL_aGOST01,
		.algorithm_enc = SSL_eNULL,
		.algorithm_mac = SSL_STREEBOG256,
		.algorithm_ssl = SSL_TLSV1,
		.algo_strength = SSL_STRONG_NONE,
		.algorithm2 = SSL_HANDSHAKE_MAC_STREEBOG256|TLS1_PRF_STREEBOG256,
		.strength_bits = 0,
		.alg_bits = 0
	},


	/* end of list */
};

int
ssl3_num_ciphers(void)
{
	return (SSL3_NUM_CIPHERS);
}

const SSL_CIPHER *
ssl3_get_cipher(unsigned int u)
{
	if (u < SSL3_NUM_CIPHERS)
		return (&(ssl3_ciphers[SSL3_NUM_CIPHERS - 1 - u]));
	else
		return (NULL);
}

const SSL_CIPHER *
ssl3_get_cipher_by_id(unsigned int id)
{
	const SSL_CIPHER *cp;
	SSL_CIPHER c;

	c.id = id;
	cp = OBJ_bsearch_ssl_cipher_id(&c, ssl3_ciphers, SSL3_NUM_CIPHERS);
	if (cp != NULL && cp->valid == 1)
		return (cp);

	return (NULL);
}

const SSL_CIPHER *
ssl3_get_cipher_by_value(uint16_t value)
{
	return ssl3_get_cipher_by_id(SSL3_CK_ID | value);
}

uint16_t
ssl3_cipher_get_value(const SSL_CIPHER *c)
{
	return (c->id & SSL3_CK_VALUE_MASK);
}

int
ssl3_pending(const SSL *s)
{
	if (s->internal->rstate == SSL_ST_READ_BODY)
		return 0;

	return (S3I(s)->rrec.type == SSL3_RT_APPLICATION_DATA) ?
	    S3I(s)->rrec.length : 0;
}

int
ssl3_handshake_msg_hdr_len(SSL *s)
{
	return (SSL_IS_DTLS(s) ? DTLS1_HM_HEADER_LENGTH :
            SSL3_HM_HEADER_LENGTH);
}

unsigned char *
ssl3_handshake_msg_start(SSL *s, uint8_t msg_type)
{
	unsigned char *d, *p;

	d = p = (unsigned char *)s->internal->init_buf->data;

	/* Handshake message type and length. */
	*(p++) = msg_type;
	l2n3(0, p);

	return (d + ssl3_handshake_msg_hdr_len(s));
}

void
ssl3_handshake_msg_finish(SSL *s, unsigned int len)
{
	unsigned char *p;
	uint8_t msg_type;

	p = (unsigned char *)s->internal->init_buf->data;

	/* Handshake message length. */
	msg_type = *(p++);
	l2n3(len, p);

	s->internal->init_num = ssl3_handshake_msg_hdr_len(s) + (int)len;
	s->internal->init_off = 0;

	if (SSL_IS_DTLS(s)) {
		dtls1_set_message_header(s, msg_type, len, 0, len);
		dtls1_buffer_message(s, 0);
	}
}

int
ssl3_handshake_msg_start_cbb(SSL *s, CBB *handshake, CBB *body,
    uint8_t msg_type)
{
	int ret = 0;

	if (!CBB_init(handshake, SSL3_RT_MAX_PLAIN_LENGTH))
		goto err;
	if (!CBB_add_u8(handshake, msg_type))
		goto err;
	if (SSL_IS_DTLS(s)) {
		unsigned char *data;

		if (!CBB_add_space(handshake, &data, DTLS1_HM_HEADER_LENGTH -
		    SSL3_HM_HEADER_LENGTH))
			goto err;
	}
	if (!CBB_add_u24_length_prefixed(handshake, body))
		goto err;

	ret = 1;

 err:
	return (ret);
}

int
ssl3_handshake_msg_finish_cbb(SSL *s, CBB *handshake)
{
	unsigned char *data = NULL;
	size_t outlen;
	int ret = 0;

	if (!CBB_finish(handshake, &data, &outlen))
		goto err;

	if (outlen > INT_MAX)
		goto err;

	if (!BUF_MEM_grow_clean(s->internal->init_buf, outlen))
		goto err;

	memcpy(s->internal->init_buf->data, data, outlen);

	s->internal->init_num = (int)outlen;
	s->internal->init_off = 0;

	if (SSL_IS_DTLS(s)) {
		unsigned long len;
		uint8_t msg_type;
		CBS cbs;

		CBS_init(&cbs, data, outlen);
		if (!CBS_get_u8(&cbs, &msg_type))
			goto err;

		len = outlen - ssl3_handshake_msg_hdr_len(s);

		dtls1_set_message_header(s, msg_type, len, 0, len);
		dtls1_buffer_message(s, 0);
	}

	ret = 1;

 err:
	free(data);

	return (ret);
}

int
ssl3_handshake_write(SSL *s)
{
	if (SSL_IS_DTLS(s))
		return dtls1_do_write(s, SSL3_RT_HANDSHAKE);

	return ssl3_do_write(s, SSL3_RT_HANDSHAKE);
}

int
ssl3_new(SSL *s)
{
	if ((s->s3 = calloc(1, sizeof(*s->s3))) == NULL)
		return (0);
	if ((S3I(s) = calloc(1, sizeof(*S3I(s)))) == NULL) {
		free(s->s3);
		return (0);
	}

	s->method->internal->ssl_clear(s);

	return (1);
}

void
ssl3_free(SSL *s)
{
	if (s == NULL)
		return;

	tls1_cleanup_key_block(s);
	ssl3_release_read_buffer(s);
	ssl3_release_write_buffer(s);

	DH_free(S3I(s)->tmp.dh);
	EC_KEY_free(S3I(s)->tmp.ecdh);

	freezero(S3I(s)->tmp.x25519, X25519_KEY_LENGTH);

	sk_X509_NAME_pop_free(S3I(s)->tmp.ca_names, X509_NAME_free);

	BIO_free(S3I(s)->handshake_buffer);

	tls1_handshake_hash_free(s);

	free(S3I(s)->alpn_selected);

	freezero(S3I(s), sizeof(*S3I(s)));
	freezero(s->s3, sizeof(*s->s3));

	s->s3 = NULL;
}

void
ssl3_clear(SSL *s)
{
	struct ssl3_state_internal_st *internal;
	unsigned char	*rp, *wp;
	size_t		 rlen, wlen;

	tls1_cleanup_key_block(s);
	sk_X509_NAME_pop_free(S3I(s)->tmp.ca_names, X509_NAME_free);

	DH_free(S3I(s)->tmp.dh);
	S3I(s)->tmp.dh = NULL;
	EC_KEY_free(S3I(s)->tmp.ecdh);
	S3I(s)->tmp.ecdh = NULL;

	freezero(S3I(s)->tmp.x25519, X25519_KEY_LENGTH);
	S3I(s)->tmp.x25519 = NULL;

	rp = s->s3->rbuf.buf;
	wp = s->s3->wbuf.buf;
	rlen = s->s3->rbuf.len;
	wlen = s->s3->wbuf.len;

	BIO_free(S3I(s)->handshake_buffer);
	S3I(s)->handshake_buffer = NULL;

	tls1_handshake_hash_free(s);

	free(S3I(s)->alpn_selected);
	S3I(s)->alpn_selected = NULL;

	memset(S3I(s), 0, sizeof(*S3I(s)));
	internal = S3I(s);
	memset(s->s3, 0, sizeof(*s->s3));
	S3I(s) = internal;

	s->s3->rbuf.buf = rp;
	s->s3->wbuf.buf = wp;
	s->s3->rbuf.len = rlen;
	s->s3->wbuf.len = wlen;

	ssl_free_wbio_buffer(s);

	/* Not needed... */
	S3I(s)->renegotiate = 0;
	S3I(s)->total_renegotiations = 0;
	S3I(s)->num_renegotiations = 0;
	S3I(s)->in_read_app_data = 0;

	s->internal->packet_length = 0;
	s->version = TLS1_VERSION;
}

static long
ssl_ctrl_get_server_tmp_key(SSL *s, EVP_PKEY **pkey_tmp)
{
	EVP_PKEY *pkey = NULL;
	EC_GROUP *group = NULL;
	EC_POINT *point = NULL;
	EC_KEY *ec_key = NULL;
	BIGNUM *order = NULL;
	SESS_CERT *sc;
	int ret = 0;

	*pkey_tmp = NULL;

	if (s->server != 0)
		return 0;
	if (s->session == NULL || SSI(s)->sess_cert == NULL)
		return 0;

	sc = SSI(s)->sess_cert;

	if ((pkey = EVP_PKEY_new()) == NULL)
		return 0;

	if (sc->peer_dh_tmp != NULL) {
		ret = EVP_PKEY_set1_DH(pkey, sc->peer_dh_tmp);
	} else if (sc->peer_ecdh_tmp) {
		ret = EVP_PKEY_set1_EC_KEY(pkey, sc->peer_ecdh_tmp);
	} else if (sc->peer_x25519_tmp != NULL) {
		/* Fudge up an EC_KEY that looks like X25519... */
		if ((group = EC_GROUP_new(EC_GFp_mont_method())) == NULL)
			goto err;
		if ((point = EC_POINT_new(group)) == NULL)
			goto err;
		if ((order = BN_new()) == NULL)
			goto err;
		if (!BN_set_bit(order, 252))
			goto err;
		if (!EC_GROUP_set_generator(group, point, order, NULL))
			goto err;
		EC_GROUP_set_curve_name(group, NID_X25519);
		if ((ec_key = EC_KEY_new()) == NULL)
			goto err;
		if (!EC_KEY_set_group(ec_key, group))
			goto err;
		ret = EVP_PKEY_set1_EC_KEY(pkey, ec_key);
	}

	if (ret == 1) {
		*pkey_tmp = pkey;
		pkey = NULL;
	}

  err:
	EVP_PKEY_free(pkey);
	EC_GROUP_free(group);
	EC_POINT_free(point);
	EC_KEY_free(ec_key);
	BN_free(order);

	return (ret);
}

static int
_SSL_session_reused(SSL *s)
{
	return s->internal->hit;
}

static int
_SSL_num_renegotiations(SSL *s)
{
	return S3I(s)->num_renegotiations;
}

static int
_SSL_clear_num_renegotiations(SSL *s)
{
	int renegs;

	renegs = S3I(s)->num_renegotiations;
	S3I(s)->num_renegotiations = 0;

	return renegs;
}

static int
_SSL_total_renegotiations(SSL *s)
{
	return S3I(s)->total_renegotiations;
}

static int
_SSL_set_tmp_dh(SSL *s, DH *dh)
{
	DH *dh_tmp;

	if (!ssl_cert_inst(&s->cert)) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		return 0;
	}

	if (dh == NULL) {
		SSLerror(s, ERR_R_PASSED_NULL_PARAMETER);
		return 0;
	}

	if ((dh_tmp = DHparams_dup(dh)) == NULL) {
		SSLerror(s, ERR_R_DH_LIB);
		return 0;
	}

	DH_free(s->cert->dh_tmp);
	s->cert->dh_tmp = dh_tmp;

	return 1;
}

static int
_SSL_set_dh_auto(SSL *s, int state)
{
	s->cert->dh_tmp_auto = state;
	return 1;
}

static int
_SSL_set_tmp_ecdh(SSL *s, EC_KEY *ecdh)
{
	const EC_GROUP *group;
	int nid;

	if (!ssl_cert_inst(&s->cert)) {
		SSLerror(s, ERR_R_MALLOC_FAILURE);
		return 0;
	}

	if (ecdh == NULL)
		return 0;
	if ((group = EC_KEY_get0_group(ecdh)) == NULL)
		return 0;

	nid = EC_GROUP_get_curve_name(group);
	return SSL_set1_groups(s, &nid, 1);
}

static int
_SSL_set_ecdh_auto(SSL *s, int state)
{
	return 1;
}

static int
_SSL_set_tlsext_host_name(SSL *s, const char *name)
{
	free(s->tlsext_hostname);
	s->tlsext_hostname = NULL;

	if (name == NULL)
		return 1;

	if (strlen(name) > TLSEXT_MAXLEN_host_name) {
		SSLerror(s, SSL_R_SSL3_EXT_INVALID_SERVERNAME);
		return 0;
	}

	if ((s->tlsext_hostname = strdup(name)) == NULL) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		return 0;
	}

	return 1;
}

static int
_SSL_set_tlsext_debug_arg(SSL *s, void *arg)
{
	s->internal->tlsext_debug_arg = arg;
	return 1;
}

static int
_SSL_set_tlsext_status_type(SSL *s, int type)
{
	s->tlsext_status_type = type;
	return 1;
}

static int
_SSL_get_tlsext_status_exts(SSL *s, STACK_OF(X509_EXTENSION) **exts)
{
	*exts = s->internal->tlsext_ocsp_exts;
	return 1;
}

static int
_SSL_set_tlsext_status_exts(SSL *s, STACK_OF(X509_EXTENSION) *exts)
{
	/* XXX - leak... */
	s->internal->tlsext_ocsp_exts = exts;
	return 1;
}

static int
_SSL_get_tlsext_status_ids(SSL *s, STACK_OF(OCSP_RESPID) **ids)
{
	*ids = s->internal->tlsext_ocsp_ids;
	return 1;
}

static int
_SSL_set_tlsext_status_ids(SSL *s, STACK_OF(OCSP_RESPID) *ids)
{
	/* XXX - leak... */
	s->internal->tlsext_ocsp_ids = ids;
	return 1;
}

static int
_SSL_get_tlsext_status_ocsp_resp(SSL *s, unsigned char **resp)
{
	*resp = s->internal->tlsext_ocsp_resp;
	return s->internal->tlsext_ocsp_resplen;
}

static int
_SSL_set_tlsext_status_ocsp_resp(SSL *s, unsigned char *resp, int resp_len)
{
	free(s->internal->tlsext_ocsp_resp);
	s->internal->tlsext_ocsp_resp = resp;
	s->internal->tlsext_ocsp_resplen = resp_len;
	return 1;
}

int
SSL_set1_groups(SSL *s, const int *groups, size_t groups_len)
{
	return tls1_set_groups(&s->internal->tlsext_supportedgroups,
	    &s->internal->tlsext_supportedgroups_length, groups, groups_len);
}

int
SSL_set1_groups_list(SSL *s, const char *groups)
{
	return tls1_set_groups_list(&s->internal->tlsext_supportedgroups,
	    &s->internal->tlsext_supportedgroups_length, groups);
}

long
ssl3_ctrl(SSL *s, int cmd, long larg, void *parg)
{
	switch (cmd) {
	case SSL_CTRL_GET_SESSION_REUSED:
		return _SSL_session_reused(s);

	case SSL_CTRL_GET_NUM_RENEGOTIATIONS:
		return _SSL_num_renegotiations(s);

	case SSL_CTRL_CLEAR_NUM_RENEGOTIATIONS:
		return _SSL_clear_num_renegotiations(s);

	case SSL_CTRL_GET_TOTAL_RENEGOTIATIONS:
		return _SSL_total_renegotiations(s);

	case SSL_CTRL_SET_TMP_DH:
		return _SSL_set_tmp_dh(s, parg);

	case SSL_CTRL_SET_TMP_DH_CB:
		SSLerror(s, ERR_R_SHOULD_NOT_HAVE_BEEN_CALLED);
		return 0;

	case SSL_CTRL_SET_DH_AUTO:
		return _SSL_set_dh_auto(s, larg);

	case SSL_CTRL_SET_TMP_ECDH:
		return _SSL_set_tmp_ecdh(s, parg);

	case SSL_CTRL_SET_TMP_ECDH_CB:
		SSLerror(s, ERR_R_SHOULD_NOT_HAVE_BEEN_CALLED);
		return 0;

	case SSL_CTRL_SET_ECDH_AUTO:
		return _SSL_set_ecdh_auto(s, larg);

	case SSL_CTRL_SET_TLSEXT_HOSTNAME:
		if (larg != TLSEXT_NAMETYPE_host_name) {
			SSLerror(s, SSL_R_SSL3_EXT_INVALID_SERVERNAME_TYPE);
			return 0;
		}
		return _SSL_set_tlsext_host_name(s, parg);

	case SSL_CTRL_SET_TLSEXT_DEBUG_ARG:
		return _SSL_set_tlsext_debug_arg(s, parg);

	case SSL_CTRL_SET_TLSEXT_STATUS_REQ_TYPE:
		return _SSL_set_tlsext_status_type(s, larg);

	case SSL_CTRL_GET_TLSEXT_STATUS_REQ_EXTS:
		return _SSL_get_tlsext_status_exts(s, parg);

	case SSL_CTRL_SET_TLSEXT_STATUS_REQ_EXTS:
		return _SSL_set_tlsext_status_exts(s, parg);

	case SSL_CTRL_GET_TLSEXT_STATUS_REQ_IDS:
		return _SSL_get_tlsext_status_ids(s, parg);

	case SSL_CTRL_SET_TLSEXT_STATUS_REQ_IDS:
		return _SSL_set_tlsext_status_ids(s, parg);

	case SSL_CTRL_GET_TLSEXT_STATUS_REQ_OCSP_RESP:
		return _SSL_get_tlsext_status_ocsp_resp(s, parg);

	case SSL_CTRL_SET_TLSEXT_STATUS_REQ_OCSP_RESP:
		return _SSL_set_tlsext_status_ocsp_resp(s, parg, larg);

	case SSL_CTRL_SET_GROUPS:
		return SSL_set1_groups(s, parg, larg);

	case SSL_CTRL_SET_GROUPS_LIST:
		return SSL_set1_groups_list(s, parg);

	case SSL_CTRL_GET_SERVER_TMP_KEY:
		return ssl_ctrl_get_server_tmp_key(s, parg);

	case SSL_CTRL_SET_MIN_PROTO_VERSION:
		if (larg < 0 || larg > UINT16_MAX)
			return 0;
		return SSL_set_min_proto_version(s, larg);

	case SSL_CTRL_SET_MAX_PROTO_VERSION:
		if (larg < 0 || larg > UINT16_MAX)
			return 0;
		return SSL_set_max_proto_version(s, larg);

	/*
	 * Legacy controls that should eventually be removed.
	 */
	case SSL_CTRL_GET_CLIENT_CERT_REQUEST:
		return 0;

	case SSL_CTRL_GET_FLAGS:
		return (int)(s->s3->flags);

	case SSL_CTRL_NEED_TMP_RSA:
		return 0;

	case SSL_CTRL_SET_TMP_RSA:
	case SSL_CTRL_SET_TMP_RSA_CB:
		SSLerror(s, ERR_R_SHOULD_NOT_HAVE_BEEN_CALLED);
		return 0;
	}

	return 0;
}

long
ssl3_callback_ctrl(SSL *s, int cmd, void (*fp)(void))
{
	if (cmd == SSL_CTRL_SET_TMP_DH_CB || cmd == SSL_CTRL_SET_TMP_ECDH_CB) {
		if (!ssl_cert_inst(&s->cert)) {
			SSLerror(s, ERR_R_MALLOC_FAILURE);
			return 0;
		}
	}

	switch (cmd) {
	case SSL_CTRL_SET_TMP_RSA_CB:
		SSLerror(s, ERR_R_SHOULD_NOT_HAVE_BEEN_CALLED);
		return 0;

	case SSL_CTRL_SET_TMP_DH_CB:
		s->cert->dh_tmp_cb = (DH *(*)(SSL *, int, int))fp;
		return 1;

	case SSL_CTRL_SET_TMP_ECDH_CB:
		return 1;

	case SSL_CTRL_SET_TLSEXT_DEBUG_CB:
		s->internal->tlsext_debug_cb = (void (*)(SSL *, int , int,
		    unsigned char *, int, void *))fp;
		return 1;
	}

	return 0;
}

static int
_SSL_CTX_set_tmp_dh(SSL_CTX *ctx, DH *dh)
{
	DH *dh_tmp;

	if ((dh_tmp = DHparams_dup(dh)) == NULL) {
		SSLerrorx(ERR_R_DH_LIB);
		return 0;
	}

	DH_free(ctx->internal->cert->dh_tmp);
	ctx->internal->cert->dh_tmp = dh_tmp;

	return 1;
}

static int
_SSL_CTX_set_dh_auto(SSL_CTX *ctx, int state)
{
	ctx->internal->cert->dh_tmp_auto = state;
	return 1;
}

static int
_SSL_CTX_set_tmp_ecdh(SSL_CTX *ctx, EC_KEY *ecdh)
{
	const EC_GROUP *group;
	int nid;

	if (ecdh == NULL)
		return 0;
	if ((group = EC_KEY_get0_group(ecdh)) == NULL)
		return 0;

	nid = EC_GROUP_get_curve_name(group);
	return SSL_CTX_set1_groups(ctx, &nid, 1);
}

static int
_SSL_CTX_set_ecdh_auto(SSL_CTX *ctx, int state)
{
	return 1;
}

static int
_SSL_CTX_set_tlsext_servername_arg(SSL_CTX *ctx, void *arg)
{
	ctx->internal->tlsext_servername_arg = arg;
	return 1;
}

static int
_SSL_CTX_get_tlsext_ticket_keys(SSL_CTX *ctx, unsigned char *keys, int keys_len)
{
	if (keys == NULL)
		return 48;

	if (keys_len != 48) {
		SSLerrorx(SSL_R_INVALID_TICKET_KEYS_LENGTH);
		return 0;
	}

	memcpy(keys, ctx->internal->tlsext_tick_key_name, 16);
	memcpy(keys + 16, ctx->internal->tlsext_tick_hmac_key, 16);
	memcpy(keys + 32, ctx->internal->tlsext_tick_aes_key, 16);

	return 1;
}

static int
_SSL_CTX_set_tlsext_ticket_keys(SSL_CTX *ctx, unsigned char *keys, int keys_len)
{
	if (keys == NULL)
		return 48;

	if (keys_len != 48) {
		SSLerrorx(SSL_R_INVALID_TICKET_KEYS_LENGTH);
		return 0;
	}

	memcpy(ctx->internal->tlsext_tick_key_name, keys, 16);
	memcpy(ctx->internal->tlsext_tick_hmac_key, keys + 16, 16);
	memcpy(ctx->internal->tlsext_tick_aes_key, keys + 32, 16);

	return 1;
}

static int
_SSL_CTX_set_tlsext_status_arg(SSL_CTX *ctx, void *arg)
{
	ctx->internal->tlsext_status_arg = arg;
	return 1;
}

static int
_SSL_CTX_add_extra_chain_cert(SSL_CTX *ctx, X509 *cert)
{
	if (ctx->extra_certs == NULL) {
		if ((ctx->extra_certs = sk_X509_new_null()) == NULL)
			return 0;
	}
	if (sk_X509_push(ctx->extra_certs, cert) == 0)
		return 0;

	return 1;
}

int
_SSL_CTX_get_extra_chain_certs(SSL_CTX *ctx, STACK_OF(X509) **certs)
{
	*certs = ctx->extra_certs;
	return 1;
}

int
_SSL_CTX_clear_extra_chain_certs(SSL_CTX *ctx)
{
	sk_X509_pop_free(ctx->extra_certs, X509_free);
	ctx->extra_certs = NULL;
	return 1;
}

int
SSL_CTX_set1_groups(SSL_CTX *ctx, const int *groups, size_t groups_len)
{
	return tls1_set_groups(&ctx->internal->tlsext_supportedgroups,
	    &ctx->internal->tlsext_supportedgroups_length, groups, groups_len);
}

int
SSL_CTX_set1_groups_list(SSL_CTX *ctx, const char *groups)
{
	return tls1_set_groups_list(&ctx->internal->tlsext_supportedgroups,
	    &ctx->internal->tlsext_supportedgroups_length, groups);
}

long
ssl3_ctx_ctrl(SSL_CTX *ctx, int cmd, long larg, void *parg)
{
	switch (cmd) {
	case SSL_CTRL_SET_TMP_DH:
		return _SSL_CTX_set_tmp_dh(ctx, parg);

	case SSL_CTRL_SET_TMP_DH_CB:
		SSLerrorx(ERR_R_SHOULD_NOT_HAVE_BEEN_CALLED);
		return 0;

	case SSL_CTRL_SET_DH_AUTO:
		return _SSL_CTX_set_dh_auto(ctx, larg);

	case SSL_CTRL_SET_TMP_ECDH:
		return _SSL_CTX_set_tmp_ecdh(ctx, parg);

	case SSL_CTRL_SET_TMP_ECDH_CB:
		SSLerrorx(ERR_R_SHOULD_NOT_HAVE_BEEN_CALLED);
		return 0;

	case SSL_CTRL_SET_ECDH_AUTO:
		return _SSL_CTX_set_ecdh_auto(ctx, larg);

	case SSL_CTRL_SET_TLSEXT_SERVERNAME_ARG:
		return _SSL_CTX_set_tlsext_servername_arg(ctx, parg);

	case SSL_CTRL_GET_TLSEXT_TICKET_KEYS:
		return _SSL_CTX_get_tlsext_ticket_keys(ctx, parg, larg);

	case SSL_CTRL_SET_TLSEXT_TICKET_KEYS:
		return _SSL_CTX_set_tlsext_ticket_keys(ctx, parg, larg);

	case SSL_CTRL_SET_TLSEXT_STATUS_REQ_CB_ARG:
		return _SSL_CTX_set_tlsext_status_arg(ctx, parg);

	case SSL_CTRL_EXTRA_CHAIN_CERT:
		return _SSL_CTX_add_extra_chain_cert(ctx, parg);

	case SSL_CTRL_GET_EXTRA_CHAIN_CERTS:
		return _SSL_CTX_get_extra_chain_certs(ctx, parg);

	case SSL_CTRL_CLEAR_EXTRA_CHAIN_CERTS:
		return _SSL_CTX_clear_extra_chain_certs(ctx);

	case SSL_CTRL_SET_GROUPS:
		return SSL_CTX_set1_groups(ctx, parg, larg);

	case SSL_CTRL_SET_GROUPS_LIST:
		return SSL_CTX_set1_groups_list(ctx, parg);

	case SSL_CTRL_SET_MIN_PROTO_VERSION:
		if (larg < 0 || larg > UINT16_MAX)
			return 0;
		return SSL_CTX_set_min_proto_version(ctx, larg);

	case SSL_CTRL_SET_MAX_PROTO_VERSION:
		if (larg < 0 || larg > UINT16_MAX)
			return 0;
		return SSL_CTX_set_max_proto_version(ctx, larg);

	/*
	 * Legacy controls that should eventually be removed.
	 */
	case SSL_CTRL_NEED_TMP_RSA:
		return 0;

	case SSL_CTRL_SET_TMP_RSA:
	case SSL_CTRL_SET_TMP_RSA_CB:
		SSLerrorx(ERR_R_SHOULD_NOT_HAVE_BEEN_CALLED);
		return 0;
	}

	return 0;
}

long
ssl3_ctx_callback_ctrl(SSL_CTX *ctx, int cmd, void (*fp)(void))
{
	switch (cmd) {
	case SSL_CTRL_SET_TMP_RSA_CB:
		SSLerrorx(ERR_R_SHOULD_NOT_HAVE_BEEN_CALLED);
		return 0;

	case SSL_CTRL_SET_TMP_DH_CB:
		ctx->internal->cert->dh_tmp_cb =
		    (DH *(*)(SSL *, int, int))fp;
		return 1;

	case SSL_CTRL_SET_TMP_ECDH_CB:
		return 1;

	case SSL_CTRL_SET_TLSEXT_SERVERNAME_CB:
		ctx->internal->tlsext_servername_callback =
		    (int (*)(SSL *, int *, void *))fp;
		return 1;

	case SSL_CTRL_SET_TLSEXT_STATUS_REQ_CB:
		ctx->internal->tlsext_status_cb = (int (*)(SSL *, void *))fp;
		return 1;

	case SSL_CTRL_SET_TLSEXT_TICKET_KEY_CB:
		ctx->internal->tlsext_ticket_key_cb = (int (*)(SSL *, unsigned char  *,
		    unsigned char *, EVP_CIPHER_CTX *, HMAC_CTX *, int))fp;
		return 1;
	}

	return 0;
}

/*
 * This function needs to check if the ciphers required are actually available.
 */
const SSL_CIPHER *
ssl3_get_cipher_by_char(const unsigned char *p)
{
	CBS cipher;
	uint16_t cipher_value;

	/* We have to assume it is at least 2 bytes due to existing API. */
	CBS_init(&cipher, p, 2);
	if (!CBS_get_u16(&cipher, &cipher_value))
		return NULL;

	return ssl3_get_cipher_by_value(cipher_value);
}

int
ssl3_put_cipher_by_char(const SSL_CIPHER *c, unsigned char *p)
{
	if (p != NULL) {
		if ((c->id & ~SSL3_CK_VALUE_MASK) != SSL3_CK_ID)
			return (0);
		s2n(ssl3_cipher_get_value(c), p); 
	}
	return (2);
}

SSL_CIPHER *
ssl3_choose_cipher(SSL *s, STACK_OF(SSL_CIPHER) *clnt,
    STACK_OF(SSL_CIPHER) *srvr)
{
	unsigned long alg_k, alg_a, mask_k, mask_a;
	STACK_OF(SSL_CIPHER) *prio, *allow;
	SSL_CIPHER *c, *ret = NULL;
	int i, ii, ok;
	CERT *cert;

	/* Let's see which ciphers we can support */
	cert = s->cert;

	/*
	 * Do not set the compare functions, because this may lead to a
	 * reordering by "id". We want to keep the original ordering.
	 * We may pay a price in performance during sk_SSL_CIPHER_find(),
	 * but would have to pay with the price of sk_SSL_CIPHER_dup().
	 */

	if (s->internal->options & SSL_OP_CIPHER_SERVER_PREFERENCE) {
		prio = srvr;
		allow = clnt;
	} else {
		prio = clnt;
		allow = srvr;
	}

	for (i = 0; i < sk_SSL_CIPHER_num(prio); i++) {
		c = sk_SSL_CIPHER_value(prio, i);

		/* Skip TLS v1.2 only ciphersuites if not supported. */
		if ((c->algorithm_ssl & SSL_TLSV1_2) &&
		    !SSL_USE_TLS1_2_CIPHERS(s))
			continue;

		ssl_set_cert_masks(cert, c);
		mask_k = cert->mask_k;
		mask_a = cert->mask_a;

		alg_k = c->algorithm_mkey;
		alg_a = c->algorithm_auth;


		ok = (alg_k & mask_k) && (alg_a & mask_a);

		/*
		 * If we are considering an ECC cipher suite that uses our
		 * certificate check it.
		 */
		if (alg_a & SSL_aECDSA)
			ok = ok && tls1_check_ec_server_key(s);
		/*
		 * If we are considering an ECC cipher suite that uses
		 * an ephemeral EC key check it.
		 */
		if (alg_k & SSL_kECDHE)
			ok = ok && tls1_check_ec_tmp_key(s);

		if (!ok)
			continue;
		ii = sk_SSL_CIPHER_find(allow, c);
		if (ii >= 0) {
			ret = sk_SSL_CIPHER_value(allow, ii);
			break;
		}
	}
	return (ret);
}

int
ssl3_get_req_cert_types(SSL *s, CBB *cbb)
{
	unsigned long alg_k;

	alg_k = S3I(s)->hs.new_cipher->algorithm_mkey;

#ifndef OPENSSL_NO_GOST
	if ((alg_k & SSL_kGOST) != 0) {
		if (!CBB_add_u8(cbb, TLS_CT_GOST94_SIGN))
			return 0;
		if (!CBB_add_u8(cbb, TLS_CT_GOST01_SIGN))
			return 0;
		if (!CBB_add_u8(cbb, TLS_CT_GOST12_256_SIGN))
			return 0;
		if (!CBB_add_u8(cbb, TLS_CT_GOST12_512_SIGN))
			return 0;
	}
#endif

	if ((alg_k & SSL_kDHE) != 0) {
		if (!CBB_add_u8(cbb, SSL3_CT_RSA_FIXED_DH))
			return 0;
	}

	if (!CBB_add_u8(cbb, SSL3_CT_RSA_SIGN))
		return 0;

	/*
	 * ECDSA certs can be used with RSA cipher suites as well
	 * so we don't need to check for SSL_kECDH or SSL_kECDHE.
	 */
	if (!CBB_add_u8(cbb, TLS_CT_ECDSA_SIGN))
		return 0;

	return 1;
}

int
ssl3_shutdown(SSL *s)
{
	int	ret;

	/*
	 * Don't do anything much if we have not done the handshake or
	 * we don't want to send messages :-)
	 */
	if ((s->internal->quiet_shutdown) || (S3I(s)->hs.state == SSL_ST_BEFORE)) {
		s->internal->shutdown = (SSL_SENT_SHUTDOWN|SSL_RECEIVED_SHUTDOWN);
		return (1);
	}

	if (!(s->internal->shutdown & SSL_SENT_SHUTDOWN)) {
		s->internal->shutdown|=SSL_SENT_SHUTDOWN;
		ssl3_send_alert(s, SSL3_AL_WARNING, SSL_AD_CLOSE_NOTIFY);
		/*
		 * Our shutdown alert has been sent now, and if it still needs
	 	 * to be written, s->s3->alert_dispatch will be true
		 */
		if (s->s3->alert_dispatch)
			return(-1);	/* return WANT_WRITE */
	} else if (s->s3->alert_dispatch) {
		/* resend it if not sent */
		ret = s->method->ssl_dispatch_alert(s);
		if (ret == -1) {
			/*
			 * We only get to return -1 here the 2nd/Nth
			 * invocation, we must  have already signalled
			 * return 0 upon a previous invoation,
			 * return WANT_WRITE
			 */
			return (ret);
		}
	} else if (!(s->internal->shutdown & SSL_RECEIVED_SHUTDOWN)) {
		/* If we are waiting for a close from our peer, we are closed */
		s->method->internal->ssl_read_bytes(s, 0, NULL, 0, 0);
		if (!(s->internal->shutdown & SSL_RECEIVED_SHUTDOWN)) {
			return(-1);	/* return WANT_READ */
		}
	}

	if ((s->internal->shutdown == (SSL_SENT_SHUTDOWN|SSL_RECEIVED_SHUTDOWN)) &&
	    !s->s3->alert_dispatch)
		return (1);
	else
		return (0);
}

int
ssl3_write(SSL *s, const void *buf, int len)
{
	int	ret, n;

#if 0
	if (s->internal->shutdown & SSL_SEND_SHUTDOWN) {
		s->internal->rwstate = SSL_NOTHING;
		return (0);
	}
#endif
	errno = 0;
	if (S3I(s)->renegotiate)
		ssl3_renegotiate_check(s);

	/*
	 * This is an experimental flag that sends the
	 * last handshake message in the same packet as the first
	 * use data - used to see if it helps the TCP protocol during
	 * session-id reuse
	 */
	/* The second test is because the buffer may have been removed */
	if ((s->s3->flags & SSL3_FLAGS_POP_BUFFER) && (s->wbio == s->bbio)) {
		/* First time through, we write into the buffer */
		if (S3I(s)->delay_buf_pop_ret == 0) {
			ret = ssl3_write_bytes(s, SSL3_RT_APPLICATION_DATA,
			    buf, len);
			if (ret <= 0)
				return (ret);

			S3I(s)->delay_buf_pop_ret = ret;
		}

		s->internal->rwstate = SSL_WRITING;
		n = BIO_flush(s->wbio);
		if (n <= 0)
			return (n);
		s->internal->rwstate = SSL_NOTHING;

		/* We have flushed the buffer, so remove it */
		ssl_free_wbio_buffer(s);
		s->s3->flags&= ~SSL3_FLAGS_POP_BUFFER;

		ret = S3I(s)->delay_buf_pop_ret;
		S3I(s)->delay_buf_pop_ret = 0;
	} else {
		ret = s->method->internal->ssl_write_bytes(s,
		    SSL3_RT_APPLICATION_DATA, buf, len);
		if (ret <= 0)
			return (ret);
	}

	return (ret);
}

static int
ssl3_read_internal(SSL *s, void *buf, int len, int peek)
{
	int	ret;

	errno = 0;
	if (S3I(s)->renegotiate)
		ssl3_renegotiate_check(s);
	S3I(s)->in_read_app_data = 1;
	ret = s->method->internal->ssl_read_bytes(s,
	    SSL3_RT_APPLICATION_DATA, buf, len, peek);
	if ((ret == -1) && (S3I(s)->in_read_app_data == 2)) {
		/*
		 * ssl3_read_bytes decided to call s->internal->handshake_func, which
		 * called ssl3_read_bytes to read handshake data.
		 * However, ssl3_read_bytes actually found application data
		 * and thinks that application data makes sense here; so disable
		 * handshake processing and try to read application data again.
		 */
		s->internal->in_handshake++;
		ret = s->method->internal->ssl_read_bytes(s,
		    SSL3_RT_APPLICATION_DATA, buf, len, peek);
		s->internal->in_handshake--;
	} else
		S3I(s)->in_read_app_data = 0;

	return (ret);
}

int
ssl3_read(SSL *s, void *buf, int len)
{
	return ssl3_read_internal(s, buf, len, 0);
}

int
ssl3_peek(SSL *s, void *buf, int len)
{
	return ssl3_read_internal(s, buf, len, 1);
}

int
ssl3_renegotiate(SSL *s)
{
	if (s->internal->handshake_func == NULL)
		return (1);

	if (s->s3->flags & SSL3_FLAGS_NO_RENEGOTIATE_CIPHERS)
		return (0);

	S3I(s)->renegotiate = 1;
	return (1);
}

int
ssl3_renegotiate_check(SSL *s)
{
	int	ret = 0;

	if (S3I(s)->renegotiate) {
		if ((s->s3->rbuf.left == 0) && (s->s3->wbuf.left == 0) &&
		    !SSL_in_init(s)) {
			/*
			 * If we are the server, and we have sent
			 * a 'RENEGOTIATE' message, we need to go
			 * to SSL_ST_ACCEPT.
			 */
			/* SSL_ST_ACCEPT */
			S3I(s)->hs.state = SSL_ST_RENEGOTIATE;
			S3I(s)->renegotiate = 0;
			S3I(s)->num_renegotiations++;
			S3I(s)->total_renegotiations++;
			ret = 1;
		}
	}
	return (ret);
}
/*
 * If we are using default SHA1+MD5 algorithms switch to new SHA256 PRF
 * and handshake macs if required.
 */
long
ssl_get_algorithm2(SSL *s)
{
	long	alg2 = S3I(s)->hs.new_cipher->algorithm2;

	if (s->method->internal->ssl3_enc->enc_flags & SSL_ENC_FLAG_SHA256_PRF &&
	    alg2 == (SSL_HANDSHAKE_MAC_DEFAULT|TLS1_PRF))
		return SSL_HANDSHAKE_MAC_SHA256 | TLS1_PRF_SHA256;
	return alg2;
}
