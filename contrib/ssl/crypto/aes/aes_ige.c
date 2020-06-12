/* $OpenBSD: aes_ige.c,v 1.7 2015/02/10 09:46:30 miod Exp $ */
/* ====================================================================
 * Copyright (c) 2006 The OpenSSL Project.  All rights reserved.
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
 */

#include <openssl/aes.h>
#include <openssl/crypto.h>

#include "aes_locl.h"

#define N_WORDS (AES_BLOCK_SIZE / sizeof(unsigned long))
typedef struct {
	unsigned long data[N_WORDS];
} aes_block_t;

/* XXX: probably some better way to do this */
#if defined(__i386__) || defined(__x86_64__)
#define UNALIGNED_MEMOPS_ARE_FAST 1
#else
#define UNALIGNED_MEMOPS_ARE_FAST 0
#endif

#if UNALIGNED_MEMOPS_ARE_FAST
#define load_block(d, s)        (d) = *(const aes_block_t *)(s)
#define store_block(d, s)       *(aes_block_t *)(d) = (s)
#else
#define load_block(d, s)        memcpy((d).data, (s), AES_BLOCK_SIZE)
#define store_block(d, s)       memcpy((d), (s).data, AES_BLOCK_SIZE)
#endif

/* N.B. The IV for this mode is _twice_ the block size */

void
AES_ige_encrypt(const unsigned char *in, unsigned char *out, size_t length,
    const AES_KEY *key, unsigned char *ivec, const int enc)
{
	size_t n;
	size_t len;

	OPENSSL_assert((length % AES_BLOCK_SIZE) == 0);

	len = length / AES_BLOCK_SIZE;

	if (AES_ENCRYPT == enc) {
		if (in != out && (UNALIGNED_MEMOPS_ARE_FAST ||
		    ((size_t)in|(size_t)out|(size_t)ivec) %
		    sizeof(long) == 0)) {
			aes_block_t *ivp = (aes_block_t *)ivec;
			aes_block_t *iv2p = (aes_block_t *)(ivec + AES_BLOCK_SIZE);

			while (len) {
				aes_block_t *inp = (aes_block_t *)in;
				aes_block_t *outp = (aes_block_t *)out;

				for (n = 0; n < N_WORDS; ++n)
					outp->data[n] = inp->data[n] ^ ivp->data[n];
				AES_encrypt((unsigned char *)outp->data, (unsigned char *)outp->data, key);
				for (n = 0; n < N_WORDS; ++n)
					outp->data[n] ^= iv2p->data[n];
				ivp = outp;
				iv2p = inp;
				--len;
				in += AES_BLOCK_SIZE;
				out += AES_BLOCK_SIZE;
			}
			memcpy(ivec, ivp->data, AES_BLOCK_SIZE);
			memcpy(ivec + AES_BLOCK_SIZE, iv2p->data, AES_BLOCK_SIZE);
		} else {
			aes_block_t tmp, tmp2;
			aes_block_t iv;
			aes_block_t iv2;

			load_block(iv, ivec);
			load_block(iv2, ivec + AES_BLOCK_SIZE);

			while (len) {
				load_block(tmp, in);
				for (n = 0; n < N_WORDS; ++n)
					tmp2.data[n] = tmp.data[n] ^ iv.data[n];
				AES_encrypt((unsigned char *)tmp2.data,
				    (unsigned char *)tmp2.data, key);
				for (n = 0; n < N_WORDS; ++n)
					tmp2.data[n] ^= iv2.data[n];
				store_block(out, tmp2);
				iv = tmp2;
				iv2 = tmp;
				--len;
				in += AES_BLOCK_SIZE;
				out += AES_BLOCK_SIZE;
			}
			memcpy(ivec, iv.data, AES_BLOCK_SIZE);
			memcpy(ivec + AES_BLOCK_SIZE, iv2.data, AES_BLOCK_SIZE);
		}
	} else {
		if (in != out && (UNALIGNED_MEMOPS_ARE_FAST ||
		    ((size_t)in|(size_t)out|(size_t)ivec) %
		    sizeof(long) == 0)) {
			aes_block_t *ivp = (aes_block_t *)ivec;
			aes_block_t *iv2p = (aes_block_t *)(ivec + AES_BLOCK_SIZE);

			while (len) {
				aes_block_t tmp;
				aes_block_t *inp = (aes_block_t *)in;
				aes_block_t *outp = (aes_block_t *)out;

				for (n = 0; n < N_WORDS; ++n)
					tmp.data[n] = inp->data[n] ^ iv2p->data[n];
				AES_decrypt((unsigned char *)tmp.data,
				    (unsigned char *)outp->data, key);
				for (n = 0; n < N_WORDS; ++n)
					outp->data[n] ^= ivp->data[n];
				ivp = inp;
				iv2p = outp;
				--len;
				in += AES_BLOCK_SIZE;
				out += AES_BLOCK_SIZE;
			}
			memcpy(ivec, ivp->data, AES_BLOCK_SIZE);
			memcpy(ivec + AES_BLOCK_SIZE, iv2p->data, AES_BLOCK_SIZE);
		} else {
			aes_block_t tmp, tmp2;
			aes_block_t iv;
			aes_block_t iv2;

			load_block(iv, ivec);
			load_block(iv2, ivec + AES_BLOCK_SIZE);

			while (len) {
				load_block(tmp, in);
				tmp2 = tmp;
				for (n = 0; n < N_WORDS; ++n)
					tmp.data[n] ^= iv2.data[n];
				AES_decrypt((unsigned char *)tmp.data,
				    (unsigned char *)tmp.data, key);
				for (n = 0; n < N_WORDS; ++n)
					tmp.data[n] ^= iv.data[n];
				store_block(out, tmp);
				iv = tmp2;
				iv2 = tmp;
				--len;
				in += AES_BLOCK_SIZE;
				out += AES_BLOCK_SIZE;
			}
			memcpy(ivec, iv.data, AES_BLOCK_SIZE);
			memcpy(ivec + AES_BLOCK_SIZE, iv2.data, AES_BLOCK_SIZE);
		}
	}
}
