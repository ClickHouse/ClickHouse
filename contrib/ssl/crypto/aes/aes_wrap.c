/* $OpenBSD: aes_wrap.c,v 1.10 2015/09/10 15:56:24 jsing Exp $ */
/* Written by Dr Stephen N Henson (steve@openssl.org) for the OpenSSL
 * project.
 */
/* ====================================================================
 * Copyright (c) 2008 The OpenSSL Project.  All rights reserved.
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
 *    licensing@OpenSSL.org.
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
 */

#include <string.h>

#include <openssl/aes.h>
#include <openssl/bio.h>

static const unsigned char default_iv[] = {
	0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6,
};

int
AES_wrap_key(AES_KEY *key, const unsigned char *iv, unsigned char *out,
    const unsigned char *in, unsigned int inlen)
{
	unsigned char *A, B[16], *R;
	unsigned int i, j, t;
	if ((inlen & 0x7) || (inlen < 8))
		return -1;
	A = B;
	t = 1;
	memcpy(out + 8, in, inlen);
	if (!iv)
		iv = default_iv;

	memcpy(A, iv, 8);

	for (j = 0; j < 6; j++) {
		R = out + 8;
		for (i = 0; i < inlen; i += 8, t++, R += 8) {
			memcpy(B + 8, R, 8);
			AES_encrypt(B, B, key);
			A[7] ^= (unsigned char)(t & 0xff);
			if (t > 0xff) {
				A[6] ^= (unsigned char)((t >> 8) & 0xff);
				A[5] ^= (unsigned char)((t >> 16) & 0xff);
				A[4] ^= (unsigned char)((t >> 24) & 0xff);
			}
			memcpy(R, B + 8, 8);
		}
	}
	memcpy(out, A, 8);
	return inlen + 8;
}

int
AES_unwrap_key(AES_KEY *key, const unsigned char *iv, unsigned char *out,
    const unsigned char *in, unsigned int inlen)
{
	unsigned char *A, B[16], *R;
	unsigned int i, j, t;
	inlen -= 8;
	if (inlen & 0x7)
		return -1;
	if (inlen < 8)
		return -1;
	A = B;
	t = 6 * (inlen >> 3);
	memcpy(A, in, 8);
	memcpy(out, in + 8, inlen);
	for (j = 0; j < 6; j++) {
		R = out + inlen - 8;
		for (i = 0; i < inlen; i += 8, t--, R -= 8) {
			A[7] ^= (unsigned char)(t & 0xff);
			if (t > 0xff) {
				A[6] ^= (unsigned char)((t >> 8) & 0xff);
				A[5] ^= (unsigned char)((t >> 16) & 0xff);
				A[4] ^= (unsigned char)((t >> 24) & 0xff);
			}
			memcpy(B + 8, R, 8);
			AES_decrypt(B, B, key);
			memcpy(R, B + 8, 8);
		}
	}
	if (!iv)
		iv = default_iv;
	if (memcmp(A, iv, 8)) {
		explicit_bzero(out, inlen);
		return 0;
	}
	return inlen;
}
