/* $OpenBSD: dh_check.c,v 1.16 2016/07/05 02:54:35 bcook Exp $ */
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

#include <stdio.h>

#include <openssl/bn.h>
#include <openssl/dh.h>

/*
 * Check that p is a safe prime and
 * if g is 2, 3 or 5, check that it is a suitable generator
 * where
 * for 2, p mod 24 == 11
 * for 3, p mod 12 == 5
 * for 5, p mod 10 == 3 or 7
 * should hold.
 */

int
DH_check(const DH *dh, int *ret)
{
	int ok = 0;
	BN_CTX *ctx = NULL;
	BN_ULONG l;
	BIGNUM *q = NULL;

	*ret = 0;
	ctx = BN_CTX_new();
	if (ctx == NULL)
		goto err;
	q = BN_new();
	if (q == NULL)
		goto err;

	if (BN_is_word(dh->g, DH_GENERATOR_2)) {
		l = BN_mod_word(dh->p, 24);
		if (l == (BN_ULONG)-1)
			goto err;
		if (l != 11)
			*ret |= DH_NOT_SUITABLE_GENERATOR;
	} else if (BN_is_word(dh->g, DH_GENERATOR_5)) {
		l = BN_mod_word(dh->p, 10);
		if (l == (BN_ULONG)-1)
			goto err;
		if (l != 3 && l != 7)
			*ret |= DH_NOT_SUITABLE_GENERATOR;
	} else
		*ret |= DH_UNABLE_TO_CHECK_GENERATOR;

	if (!BN_is_prime_ex(dh->p, BN_prime_checks, ctx, NULL))
		*ret |= DH_CHECK_P_NOT_PRIME;
	else {
		if (!BN_rshift1(q, dh->p))
			goto err;
		if (!BN_is_prime_ex(q, BN_prime_checks, ctx, NULL))
			*ret |= DH_CHECK_P_NOT_SAFE_PRIME;
	}
	ok = 1;
err:
	BN_CTX_free(ctx);
	BN_free(q);
	return ok;
}

int
DH_check_pub_key(const DH *dh, const BIGNUM *pub_key, int *ret)
{
	BIGNUM *q = NULL;

	*ret = 0;
	q = BN_new();
	if (q == NULL)
		return 0;
	BN_set_word(q, 1);
	if (BN_cmp(pub_key, q) <= 0)
		*ret |= DH_CHECK_PUBKEY_TOO_SMALL;
	BN_copy(q, dh->p);
	BN_sub_word(q, 1);
	if (BN_cmp(pub_key, q) >= 0)
		*ret |= DH_CHECK_PUBKEY_TOO_LARGE;

	BN_free(q);
	return 1;
}
