/* $OpenBSD: gost.h,v 1.3 2016/09/04 17:02:31 jsing Exp $ */
/*
 * Copyright (c) 2014 Dmitry Eremin-Solenikov <dbaryshkov@gmail.com>
 * Copyright (c) 2005-2006 Cryptocom LTD
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
 */

#ifndef HEADER_GOST_H
#define HEADER_GOST_H

#include <openssl/opensslconf.h>

#ifdef OPENSSL_NO_GOST
#error GOST is disabled.
#endif

#include <openssl/asn1t.h>
#include <openssl/ec.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct gost2814789_key_st {
	unsigned int key[8];
	unsigned int k87[256],k65[256],k43[256],k21[256];
	unsigned int count;
	unsigned key_meshing : 1;
} GOST2814789_KEY;

int Gost2814789_set_sbox(GOST2814789_KEY *key, int nid);
int Gost2814789_set_key(GOST2814789_KEY *key,
		const unsigned char *userKey, const int bits);
void Gost2814789_ecb_encrypt(const unsigned char *in, unsigned char *out,
	GOST2814789_KEY *key, const int enc);
void Gost2814789_cfb64_encrypt(const unsigned char *in, unsigned char *out,
	size_t length, GOST2814789_KEY *key,
	unsigned char *ivec, int *num, const int enc);
void Gost2814789_cnt_encrypt(const unsigned char *in, unsigned char *out,
	size_t length, GOST2814789_KEY *key,
	unsigned char *ivec, unsigned char *cnt_buf, int *num);

typedef struct {
	ASN1_OCTET_STRING *iv;
	ASN1_OBJECT *enc_param_set;
} GOST_CIPHER_PARAMS;

GOST_CIPHER_PARAMS *GOST_CIPHER_PARAMS_new(void);
void GOST_CIPHER_PARAMS_free(GOST_CIPHER_PARAMS *a);
GOST_CIPHER_PARAMS *d2i_GOST_CIPHER_PARAMS(GOST_CIPHER_PARAMS **a, const unsigned char **in, long len);
int i2d_GOST_CIPHER_PARAMS(GOST_CIPHER_PARAMS *a, unsigned char **out);
extern const ASN1_ITEM GOST_CIPHER_PARAMS_it;

#define GOST2814789IMIT_LENGTH 4
#define GOST2814789IMIT_CBLOCK 8
#define GOST2814789IMIT_LONG unsigned int

typedef struct GOST2814789IMITstate_st {
	GOST2814789IMIT_LONG	Nl, Nh;
	unsigned char		data[GOST2814789IMIT_CBLOCK];
	unsigned int		num;

	GOST2814789_KEY		cipher;
	unsigned char		mac[GOST2814789IMIT_CBLOCK];
} GOST2814789IMIT_CTX;

/* Note, also removed second parameter and removed dctx->cipher setting */
int GOST2814789IMIT_Init(GOST2814789IMIT_CTX *c, int nid);
int GOST2814789IMIT_Update(GOST2814789IMIT_CTX *c, const void *data, size_t len);
int GOST2814789IMIT_Final(unsigned char *md, GOST2814789IMIT_CTX *c);
void GOST2814789IMIT_Transform(GOST2814789IMIT_CTX *c, const unsigned char *data);
unsigned char *GOST2814789IMIT(const unsigned char *d, size_t n,
		unsigned char *md, int nid,
		const unsigned char *key, const unsigned char *iv);

#define GOSTR341194_LONG unsigned int

#define GOSTR341194_LENGTH	32
#define GOSTR341194_CBLOCK	32
#define GOSTR341194_LBLOCK	(GOSTR341194_CBLOCK/4)

typedef struct GOSTR341194state_st {
	GOSTR341194_LONG	Nl, Nh;
	GOSTR341194_LONG	data[GOSTR341194_LBLOCK];
	unsigned int		num;

	GOST2814789_KEY		cipher;
	unsigned char		H[GOSTR341194_CBLOCK];
	unsigned char		S[GOSTR341194_CBLOCK];
} GOSTR341194_CTX;

/* Note, also removed second parameter and removed dctx->cipher setting */
int GOSTR341194_Init(GOSTR341194_CTX *c, int nid);
int GOSTR341194_Update(GOSTR341194_CTX *c, const void *data, size_t len);
int GOSTR341194_Final(unsigned char *md, GOSTR341194_CTX *c);
void GOSTR341194_Transform(GOSTR341194_CTX *c, const unsigned char *data);
unsigned char *GOSTR341194(const unsigned char *d, size_t n,unsigned char *md, int nid);

#if defined(_LP64)
#define STREEBOG_LONG64 unsigned long
#define U64(C)     C##UL
#else
#define STREEBOG_LONG64 unsigned long long
#define U64(C)     C##ULL
#endif

#define STREEBOG_LBLOCK 8
#define STREEBOG_CBLOCK 64
#define STREEBOG256_LENGTH 32
#define STREEBOG512_LENGTH 64

typedef struct STREEBOGstate_st {
	STREEBOG_LONG64	data[STREEBOG_LBLOCK];
	unsigned int	num;
	unsigned int	md_len;
	STREEBOG_LONG64	h[STREEBOG_LBLOCK];
	STREEBOG_LONG64 N[STREEBOG_LBLOCK];
	STREEBOG_LONG64 Sigma[STREEBOG_LBLOCK];
} STREEBOG_CTX;

int STREEBOG256_Init(STREEBOG_CTX *c);
int STREEBOG256_Update(STREEBOG_CTX *c, const void *data, size_t len);
int STREEBOG256_Final(unsigned char *md, STREEBOG_CTX *c);
void STREEBOG256_Transform(STREEBOG_CTX *c, const unsigned char *data);
unsigned char *STREEBOG256(const unsigned char *d, size_t n,unsigned char *md);

int STREEBOG512_Init(STREEBOG_CTX *c);
int STREEBOG512_Update(STREEBOG_CTX *c, const void *data, size_t len);
int STREEBOG512_Final(unsigned char *md, STREEBOG_CTX *c);
void STREEBOG512_Transform(STREEBOG_CTX *c, const unsigned char *data);
unsigned char *STREEBOG512(const unsigned char *d, size_t n,unsigned char *md);

typedef struct gost_key_st GOST_KEY;
GOST_KEY *GOST_KEY_new(void);
void GOST_KEY_free(GOST_KEY * r);
int GOST_KEY_check_key(const GOST_KEY * eckey);
int GOST_KEY_set_public_key_affine_coordinates(GOST_KEY * key, BIGNUM * x, BIGNUM * y);
const EC_GROUP * GOST_KEY_get0_group(const GOST_KEY * key);
int GOST_KEY_set_group(GOST_KEY * key, const EC_GROUP * group);
int GOST_KEY_get_digest(const GOST_KEY * key);
int GOST_KEY_set_digest(GOST_KEY * key, int digest_nid);
const BIGNUM * GOST_KEY_get0_private_key(const GOST_KEY * key);
int GOST_KEY_set_private_key(GOST_KEY * key, const BIGNUM * priv_key);
const EC_POINT * GOST_KEY_get0_public_key(const GOST_KEY * key);
int GOST_KEY_set_public_key(GOST_KEY * key, const EC_POINT * pub_key);
size_t GOST_KEY_get_size(const GOST_KEY * r);

/* Gost-specific pmeth control-function parameters */
/* For GOST R34.10 parameters */
#define EVP_PKEY_CTRL_GOST_PARAMSET	(EVP_PKEY_ALG_CTRL+1)
#define EVP_PKEY_CTRL_GOST_SIG_FORMAT	(EVP_PKEY_ALG_CTRL+2)
#define EVP_PKEY_CTRL_GOST_SET_DIGEST	(EVP_PKEY_ALG_CTRL+3)
#define EVP_PKEY_CTRL_GOST_GET_DIGEST	(EVP_PKEY_ALG_CTRL+4)

#define GOST_SIG_FORMAT_SR_BE	0
#define GOST_SIG_FORMAT_RS_LE	1

/* BEGIN ERROR CODES */
/* The following lines are auto generated by the script mkerr.pl. Any changes
 * made after this point may be overwritten when the script is next run.
 */
void ERR_load_GOST_strings(void);

/* Error codes for the GOST functions. */

/* Function codes. */
#define GOST_F_DECODE_GOST01_ALGOR_PARAMS		 104
#define GOST_F_ENCODE_GOST01_ALGOR_PARAMS		 105
#define GOST_F_GOST2001_COMPUTE_PUBLIC			 106
#define GOST_F_GOST2001_DO_SIGN				 107
#define GOST_F_GOST2001_DO_VERIFY			 108
#define GOST_F_GOST2001_KEYGEN				 109
#define GOST_F_GOST89_GET_ASN1_PARAMETERS		 102
#define GOST_F_GOST89_SET_ASN1_PARAMETERS		 103
#define GOST_F_GOST_KEY_CHECK_KEY			 124
#define GOST_F_GOST_KEY_NEW				 125
#define GOST_F_GOST_KEY_SET_PUBLIC_KEY_AFFINE_COORDINATES 126
#define GOST_F_PARAM_COPY_GOST01			 110
#define GOST_F_PARAM_DECODE_GOST01			 111
#define GOST_F_PKEY_GOST01_CTRL				 116
#define GOST_F_PKEY_GOST01_DECRYPT			 112
#define GOST_F_PKEY_GOST01_DERIVE			 113
#define GOST_F_PKEY_GOST01_ENCRYPT			 114
#define GOST_F_PKEY_GOST01_PARAMGEN			 115
#define GOST_F_PKEY_GOST01_SIGN				 123
#define GOST_F_PKEY_GOST_MAC_CTRL			 100
#define GOST_F_PKEY_GOST_MAC_KEYGEN			 101
#define GOST_F_PRIV_DECODE_GOST01			 117
#define GOST_F_PUB_DECODE_GOST01			 118
#define GOST_F_PUB_ENCODE_GOST01			 119
#define GOST_F_PUB_PRINT_GOST01				 120
#define GOST_F_UNPACK_SIGNATURE_CP			 121
#define GOST_F_UNPACK_SIGNATURE_LE			 122

/* Reason codes. */
#define GOST_R_BAD_KEY_PARAMETERS_FORMAT		 104
#define GOST_R_BAD_PKEY_PARAMETERS_FORMAT		 105
#define GOST_R_CANNOT_PACK_EPHEMERAL_KEY		 106
#define GOST_R_CTRL_CALL_FAILED				 107
#define GOST_R_ERROR_COMPUTING_SHARED_KEY		 108
#define GOST_R_ERROR_PARSING_KEY_TRANSPORT_INFO		 109
#define GOST_R_INCOMPATIBLE_ALGORITHMS			 110
#define GOST_R_INCOMPATIBLE_PEER_KEY			 111
#define GOST_R_INVALID_DIGEST_TYPE			 100
#define GOST_R_INVALID_IV_LENGTH			 103
#define GOST_R_INVALID_MAC_KEY_LENGTH			 101
#define GOST_R_KEY_IS_NOT_INITIALIZED			 112
#define GOST_R_KEY_PARAMETERS_MISSING			 113
#define GOST_R_MAC_KEY_NOT_SET				 102
#define GOST_R_NO_PARAMETERS_SET			 115
#define GOST_R_NO_PEER_KEY				 116
#define GOST_R_NO_PRIVATE_PART_OF_NON_EPHEMERAL_KEYPAIR	 117
#define GOST_R_PUBLIC_KEY_UNDEFINED			 118
#define GOST_R_RANDOM_NUMBER_GENERATOR_FAILED		 120
#define GOST_R_SIGNATURE_MISMATCH			 121
#define GOST_R_SIGNATURE_PARTS_GREATER_THAN_Q		 122
#define GOST_R_UKM_NOT_SET				 123

#ifdef  __cplusplus
}
#endif
#endif
