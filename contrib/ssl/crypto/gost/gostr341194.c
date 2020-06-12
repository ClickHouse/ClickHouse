/* $OpenBSD: gostr341194.c,v 1.5 2015/09/10 15:56:25 jsing Exp $ */
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

#include <string.h>

#include <openssl/opensslconf.h>

#ifndef OPENSSL_NO_GOST
#include <openssl/crypto.h>
#include <openssl/objects.h>
#include <openssl/gost.h>

#include "gost_locl.h"

/* Following functions are various bit meshing routines used in
 * GOST R 34.11-94 algorithms */
static void
swap_bytes(unsigned char *w, unsigned char *k)
{
	int i, j;

	for (i = 0; i < 4; i++)
		for (j = 0; j < 8; j++)
			k[i + 4 * j] = w[8 * i + j];
}

/* was A_A */
static void
circle_xor8(const unsigned char *w, unsigned char *k)
{
	unsigned char buf[8];
	int i;

	memcpy(buf, w, 8);
	memmove(k, w + 8, 24);
	for (i = 0; i < 8; i++)
		k[i + 24] = buf[i] ^ k[i];
}

/* was R_R */
static void
transform_3(unsigned char *data)
{
	unsigned short int acc;

	acc = (data[0] ^ data[2] ^ data[4] ^ data[6] ^ data[24] ^ data[30]) |
	     ((data[1] ^ data[3] ^ data[5] ^ data[7] ^ data[25] ^ data[31]) << 8);
	memmove(data, data + 2, 30);
	data[30] = acc & 0xff;
	data[31] = acc >> 8;
}

/* Adds blocks of N bytes modulo 2**(8*n). Returns carry*/
static int
add_blocks(int n, unsigned char *left, const unsigned char *right)
{
	int i;
	int carry = 0;
	int sum;

	for (i = 0; i < n; i++) {
		sum = (int)left[i] + (int)right[i] + carry;
		left[i] = sum & 0xff;
		carry = sum >> 8;
	}
	return carry;
}

/* Xor two sequences of bytes */
static void
xor_blocks(unsigned char *result, const unsigned char *a,
    const unsigned char *b, size_t len)
{
	size_t i;

	for (i = 0; i < len; i++)
		result[i] = a[i] ^ b[i];
}

/* 
 * 	Calculate H(i+1) = Hash(Hi,Mi) 
 * 	Where H and M are 32 bytes long
 */
static int
hash_step(GOSTR341194_CTX *c, unsigned char *H, const unsigned char *M)
{
	unsigned char U[32], W[32], V[32], S[32], Key[32];
	int i;

	/* Compute first key */
	xor_blocks(W, H, M, 32);
	swap_bytes(W, Key);
	/* Encrypt first 8 bytes of H with first key */
	Gost2814789_set_key(&c->cipher, Key, 256);
	Gost2814789_encrypt(H, S, &c->cipher);

	/* Compute second key */
	circle_xor8(H, U);
	circle_xor8(M, V);
	circle_xor8(V, V);
	xor_blocks(W, U, V, 32);
	swap_bytes(W, Key);
	/* encrypt second 8 bytes of H with second key */
	Gost2814789_set_key(&c->cipher, Key, 256);
	Gost2814789_encrypt(H+8, S+8, &c->cipher);

	/* compute third key */
	circle_xor8(U, U);
	U[31] = ~U[31];
	U[29] = ~U[29];
	U[28] = ~U[28];
	U[24] = ~U[24];
	U[23] = ~U[23];
	U[20] = ~U[20];
	U[18] = ~U[18];
	U[17] = ~U[17];
	U[14] = ~U[14];
	U[12] = ~U[12];
	U[10] = ~U[10];
	U[8] = ~U[8];
	U[7] = ~U[7];
	U[5] = ~U[5];
	U[3] = ~U[3];
	U[1] = ~U[1];
	circle_xor8(V, V);
	circle_xor8(V, V);
	xor_blocks(W, U, V, 32);
	swap_bytes(W, Key);
	/* encrypt third 8 bytes of H with third key */
	Gost2814789_set_key(&c->cipher, Key, 256);
	Gost2814789_encrypt(H+16, S+16, &c->cipher);

	/* Compute fourth key */
	circle_xor8(U, U);
	circle_xor8(V, V);
	circle_xor8(V, V);
	xor_blocks(W, U, V, 32);
	swap_bytes(W, Key);
	/* Encrypt last 8 bytes with fourth key */
	Gost2814789_set_key(&c->cipher, Key, 256);
	Gost2814789_encrypt(H+24, S+24, &c->cipher);

	for (i = 0; i < 12; i++)
		transform_3(S);
	xor_blocks(S, S, M, 32);
	transform_3(S);
	xor_blocks(S, S, H, 32);
	for (i = 0; i < 61; i++)
		transform_3(S);
	memcpy(H, S, 32);
	return 1;
}

int
GOSTR341194_Init(GOSTR341194_CTX *c, int nid)
{
	memset(c, 0, sizeof(*c));
	return Gost2814789_set_sbox(&c->cipher, nid);
}

static void
GOSTR341194_block_data_order(GOSTR341194_CTX *ctx, const unsigned char *p,
    size_t num)
{
	int i;

	for (i = 0; i < num; i++) {
		hash_step(ctx, ctx->H, p);
		add_blocks(32, ctx->S, p);
		p += 32;
	}
}

#define DATA_ORDER_IS_LITTLE_ENDIAN

#define HASH_CBLOCK	GOSTR341194_CBLOCK
#define HASH_LONG	GOSTR341194_LONG
#define HASH_CTX	GOSTR341194_CTX
#define HASH_UPDATE	GOSTR341194_Update
#define HASH_TRANSFORM	GOSTR341194_Transform
#define HASH_NO_FINAL	1
#define HASH_BLOCK_DATA_ORDER	GOSTR341194_block_data_order

#include "md32_common.h"

int
GOSTR341194_Final(unsigned char *md, GOSTR341194_CTX * c)
{
	unsigned char *p = (unsigned char *)c->data;
	unsigned char T[32];

	if (c->num > 0) {
		memset(p + c->num, 0, 32 - c->num);
		hash_step(c, c->H, p);
		add_blocks(32, c->S, p);
	}

	p = T;
	HOST_l2c(c->Nl, p);
	HOST_l2c(c->Nh, p);
	memset(p, 0, 32 - 8);
	hash_step(c, c->H, T);
	hash_step(c, c->H, c->S);

	memcpy(md, c->H, 32);

	return 1;
}

unsigned char *
GOSTR341194(const unsigned char *d, size_t n, unsigned char *md, int nid)
{
	GOSTR341194_CTX c;
	static unsigned char m[GOSTR341194_LENGTH];

	if (md == NULL)
		md = m;
	if (!GOSTR341194_Init(&c, nid))
		return 0;
	GOSTR341194_Update(&c, d, n);
	GOSTR341194_Final(md, &c);
	explicit_bzero(&c, sizeof(c));
	return (md);
}
#endif
