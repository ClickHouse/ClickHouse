/* $OpenBSD: bn_asm.c,v 1.15 2017/05/02 03:59:44 deraadt Exp $ */
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

#ifndef BN_DEBUG
# undef NDEBUG /* avoid conflicting definitions */
# define NDEBUG
#endif

#include <assert.h>
#include <stdio.h>

#include <openssl/opensslconf.h>

#include "bn_lcl.h"

#if defined(BN_LLONG) || defined(BN_UMULT_HIGH)

BN_ULONG
bn_mul_add_words(BN_ULONG *rp, const BN_ULONG *ap, int num, BN_ULONG w)
{
	BN_ULONG c1 = 0;

	assert(num >= 0);
	if (num <= 0)
		return (c1);

#ifndef OPENSSL_SMALL_FOOTPRINT
	while (num & ~3) {
		mul_add(rp[0], ap[0], w, c1);
		mul_add(rp[1], ap[1], w, c1);
		mul_add(rp[2], ap[2], w, c1);
		mul_add(rp[3], ap[3], w, c1);
		ap += 4;
		rp += 4;
		num -= 4;
	}
#endif
	while (num) {
		mul_add(rp[0], ap[0], w, c1);
		ap++;
		rp++;
		num--;
	}

	return (c1);
}

BN_ULONG
bn_mul_words(BN_ULONG *rp, const BN_ULONG *ap, int num, BN_ULONG w)
{
	BN_ULONG c1 = 0;

	assert(num >= 0);
	if (num <= 0)
		return (c1);

#ifndef OPENSSL_SMALL_FOOTPRINT
	while (num & ~3) {
		mul(rp[0], ap[0], w, c1);
		mul(rp[1], ap[1], w, c1);
		mul(rp[2], ap[2], w, c1);
		mul(rp[3], ap[3], w, c1);
		ap += 4;
		rp += 4;
		num -= 4;
	}
#endif
	while (num) {
		mul(rp[0], ap[0], w, c1);
		ap++;
		rp++;
		num--;
	}
	return (c1);
}

void
bn_sqr_words(BN_ULONG *r, const BN_ULONG *a, int n)
{
	assert(n >= 0);
	if (n <= 0)
		return;

#ifndef OPENSSL_SMALL_FOOTPRINT
	while (n & ~3) {
		sqr(r[0], r[1], a[0]);
		sqr(r[2], r[3], a[1]);
		sqr(r[4], r[5], a[2]);
		sqr(r[6], r[7], a[3]);
		a += 4;
		r += 8;
		n -= 4;
	}
#endif
	while (n) {
		sqr(r[0], r[1], a[0]);
		a++;
		r += 2;
		n--;
	}
}

#else /* !(defined(BN_LLONG) || defined(BN_UMULT_HIGH)) */

BN_ULONG
bn_mul_add_words(BN_ULONG *rp, const BN_ULONG *ap, int num, BN_ULONG w)
{
	BN_ULONG c = 0;
	BN_ULONG bl, bh;

	assert(num >= 0);
	if (num <= 0)
		return ((BN_ULONG)0);

	bl = LBITS(w);
	bh = HBITS(w);

#ifndef OPENSSL_SMALL_FOOTPRINT
	while (num & ~3) {
		mul_add(rp[0], ap[0], bl, bh, c);
		mul_add(rp[1], ap[1], bl, bh, c);
		mul_add(rp[2], ap[2], bl, bh, c);
		mul_add(rp[3], ap[3], bl, bh, c);
		ap += 4;
		rp += 4;
		num -= 4;
	}
#endif
	while (num) {
		mul_add(rp[0], ap[0], bl, bh, c);
		ap++;
		rp++;
		num--;
	}
	return (c);
}

BN_ULONG
bn_mul_words(BN_ULONG *rp, const BN_ULONG *ap, int num, BN_ULONG w)
{
	BN_ULONG carry = 0;
	BN_ULONG bl, bh;

	assert(num >= 0);
	if (num <= 0)
		return ((BN_ULONG)0);

	bl = LBITS(w);
	bh = HBITS(w);

#ifndef OPENSSL_SMALL_FOOTPRINT
	while (num & ~3) {
		mul(rp[0], ap[0], bl, bh, carry);
		mul(rp[1], ap[1], bl, bh, carry);
		mul(rp[2], ap[2], bl, bh, carry);
		mul(rp[3], ap[3], bl, bh, carry);
		ap += 4;
		rp += 4;
		num -= 4;
	}
#endif
	while (num) {
		mul(rp[0], ap[0], bl, bh, carry);
		ap++;
		rp++;
		num--;
	}
	return (carry);
}

void
bn_sqr_words(BN_ULONG *r, const BN_ULONG *a, int n)
{
	assert(n >= 0);
	if (n <= 0)
		return;

#ifndef OPENSSL_SMALL_FOOTPRINT
	while (n & ~3) {
		sqr64(r[0], r[1], a[0]);
		sqr64(r[2], r[3], a[1]);
		sqr64(r[4], r[5], a[2]);
		sqr64(r[6], r[7], a[3]);
		a += 4;
		r += 8;
		n -= 4;
	}
#endif
	while (n) {
		sqr64(r[0], r[1], a[0]);
		a++;
		r += 2;
		n--;
	}
}

#endif /* !(defined(BN_LLONG) || defined(BN_UMULT_HIGH)) */

#if defined(BN_LLONG) && defined(BN_DIV2W)

BN_ULONG
bn_div_words(BN_ULONG h, BN_ULONG l, BN_ULONG d)
{
	return ((BN_ULONG)(((((BN_ULLONG)h) << BN_BITS2)|l)/(BN_ULLONG)d));
}

#else

/* Divide h,l by d and return the result. */
/* I need to test this some more :-( */
BN_ULONG
bn_div_words(BN_ULONG h, BN_ULONG l, BN_ULONG d)
{
	BN_ULONG dh, dl, q,ret = 0, th, tl, t;
	int i, count = 2;

	if (d == 0)
		return (BN_MASK2);

	i = BN_num_bits_word(d);
	assert((i == BN_BITS2) || (h <= (BN_ULONG)1 << i));

	i = BN_BITS2 - i;
	if (h >= d)
		h -= d;

	if (i) {
		d <<= i;
		h = (h << i) | (l >> (BN_BITS2 - i));
		l <<= i;
	}
	dh = (d & BN_MASK2h) >> BN_BITS4;
	dl = (d & BN_MASK2l);
	for (;;) {
		if ((h >> BN_BITS4) == dh)
			q = BN_MASK2l;
		else
			q = h / dh;

		th = q * dh;
		tl = dl * q;
		for (;;) {
			t = h - th;
			if ((t & BN_MASK2h) ||
			    ((tl) <= (
			    (t << BN_BITS4) |
			    ((l & BN_MASK2h) >> BN_BITS4))))
				break;
			q--;
			th -= dh;
			tl -= dl;
		}
		t = (tl >> BN_BITS4);
		tl = (tl << BN_BITS4) & BN_MASK2h;
		th += t;

		if (l < tl)
			th++;
		l -= tl;
		if (h < th) {
			h += d;
			q--;
		}
		h -= th;

		if (--count == 0)
			break;

		ret = q << BN_BITS4;
		h = ((h << BN_BITS4) | (l >> BN_BITS4)) & BN_MASK2;
		l = (l & BN_MASK2l) << BN_BITS4;
	}
	ret |= q;
	return (ret);
}
#endif /* !defined(BN_LLONG) && defined(BN_DIV2W) */

#ifdef BN_LLONG
BN_ULONG
bn_add_words(BN_ULONG *r, const BN_ULONG *a, const BN_ULONG *b, int n)
{
	BN_ULLONG ll = 0;

	assert(n >= 0);
	if (n <= 0)
		return ((BN_ULONG)0);

#ifndef OPENSSL_SMALL_FOOTPRINT
	while (n & ~3) {
		ll += (BN_ULLONG)a[0] + b[0];
		r[0] = (BN_ULONG)ll & BN_MASK2;
		ll >>= BN_BITS2;
		ll += (BN_ULLONG)a[1] + b[1];
		r[1] = (BN_ULONG)ll & BN_MASK2;
		ll >>= BN_BITS2;
		ll += (BN_ULLONG)a[2] + b[2];
		r[2] = (BN_ULONG)ll & BN_MASK2;
		ll >>= BN_BITS2;
		ll += (BN_ULLONG)a[3] + b[3];
		r[3] = (BN_ULONG)ll & BN_MASK2;
		ll >>= BN_BITS2;
		a += 4;
		b += 4;
		r += 4;
		n -= 4;
	}
#endif
	while (n) {
		ll += (BN_ULLONG)a[0] + b[0];
		r[0] = (BN_ULONG)ll & BN_MASK2;
		ll >>= BN_BITS2;
		a++;
		b++;
		r++;
		n--;
	}
	return ((BN_ULONG)ll);
}
#else /* !BN_LLONG */
BN_ULONG
bn_add_words(BN_ULONG *r, const BN_ULONG *a, const BN_ULONG *b, int n)
{
	BN_ULONG c, l, t;

	assert(n >= 0);
	if (n <= 0)
		return ((BN_ULONG)0);

	c = 0;
#ifndef OPENSSL_SMALL_FOOTPRINT
	while (n & ~3) {
		t = a[0];
		t = (t + c) & BN_MASK2;
		c = (t < c);
		l = (t + b[0]) & BN_MASK2;
		c += (l < t);
		r[0] = l;
		t = a[1];
		t = (t + c) & BN_MASK2;
		c = (t < c);
		l = (t + b[1]) & BN_MASK2;
		c += (l < t);
		r[1] = l;
		t = a[2];
		t = (t + c) & BN_MASK2;
		c = (t < c);
		l = (t + b[2]) & BN_MASK2;
		c += (l < t);
		r[2] = l;
		t = a[3];
		t = (t + c) & BN_MASK2;
		c = (t < c);
		l = (t + b[3]) & BN_MASK2;
		c += (l < t);
		r[3] = l;
		a += 4;
		b += 4;
		r += 4;
		n -= 4;
	}
#endif
	while (n) {
		t = a[0];
		t = (t + c) & BN_MASK2;
		c = (t < c);
		l = (t + b[0]) & BN_MASK2;
		c += (l < t);
		r[0] = l;
		a++;
		b++;
		r++;
		n--;
	}
	return ((BN_ULONG)c);
}
#endif /* !BN_LLONG */

BN_ULONG
bn_sub_words(BN_ULONG *r, const BN_ULONG *a, const BN_ULONG *b, int n)
{
	BN_ULONG t1, t2;
	int c = 0;

	assert(n >= 0);
	if (n <= 0)
		return ((BN_ULONG)0);

#ifndef OPENSSL_SMALL_FOOTPRINT
	while (n&~3) {
		t1 = a[0];
		t2 = b[0];
		r[0] = (t1 - t2 - c) & BN_MASK2;
		if (t1 != t2)
			c = (t1 < t2);
		t1 = a[1];
		t2 = b[1];
		r[1] = (t1 - t2 - c) & BN_MASK2;
		if (t1 != t2)
			c = (t1 < t2);
		t1 = a[2];
		t2 = b[2];
		r[2] = (t1 - t2 - c) & BN_MASK2;
		if (t1 != t2)
			c = (t1 < t2);
		t1 = a[3];
		t2 = b[3];
		r[3] = (t1 - t2 - c) & BN_MASK2;
		if (t1 != t2)
			c = (t1 < t2);
		a += 4;
		b += 4;
		r += 4;
		n -= 4;
	}
#endif
	while (n) {
		t1 = a[0];
		t2 = b[0];
		r[0] = (t1 - t2 - c) & BN_MASK2;
		if (t1 != t2)
			c = (t1 < t2);
		a++;
		b++;
		r++;
		n--;
	}
	return (c);
}

#if defined(BN_MUL_COMBA) && !defined(OPENSSL_SMALL_FOOTPRINT)

#undef bn_mul_comba8
#undef bn_mul_comba4
#undef bn_sqr_comba8
#undef bn_sqr_comba4

/* mul_add_c(a,b,c0,c1,c2)  -- c+=a*b for three word number c=(c2,c1,c0) */
/* mul_add_c2(a,b,c0,c1,c2) -- c+=2*a*b for three word number c=(c2,c1,c0) */
/* sqr_add_c(a,i,c0,c1,c2)  -- c+=a[i]^2 for three word number c=(c2,c1,c0) */
/* sqr_add_c2(a,i,c0,c1,c2) -- c+=2*a[i]*a[j] for three word number c=(c2,c1,c0) */

#ifdef BN_LLONG
/*
 * Keep in mind that additions to multiplication result can not
 * overflow, because its high half cannot be all-ones.
 */
#define mul_add_c(a,b,c0,c1,c2)		do {	\
	BN_ULONG hi;				\
	BN_ULLONG t = (BN_ULLONG)(a)*(b);	\
	t += c0;		/* no carry */	\
	c0 = (BN_ULONG)Lw(t);			\
	hi = (BN_ULONG)Hw(t);			\
	c1 = (c1+hi)&BN_MASK2; if (c1<hi) c2++;	\
	} while(0)

#define mul_add_c2(a,b,c0,c1,c2)	do {	\
	BN_ULONG hi;				\
	BN_ULLONG t = (BN_ULLONG)(a)*(b);	\
	BN_ULLONG tt = t+c0;	/* no carry */	\
	c0 = (BN_ULONG)Lw(tt);			\
	hi = (BN_ULONG)Hw(tt);			\
	c1 = (c1+hi)&BN_MASK2; if (c1<hi) c2++;	\
	t += c0;		/* no carry */	\
	c0 = (BN_ULONG)Lw(t);			\
	hi = (BN_ULONG)Hw(t);			\
	c1 = (c1+hi)&BN_MASK2; if (c1<hi) c2++;	\
	} while(0)

#define sqr_add_c(a,i,c0,c1,c2)		do {	\
	BN_ULONG hi;				\
	BN_ULLONG t = (BN_ULLONG)a[i]*a[i];	\
	t += c0;		/* no carry */	\
	c0 = (BN_ULONG)Lw(t);			\
	hi = (BN_ULONG)Hw(t);			\
	c1 = (c1+hi)&BN_MASK2; if (c1<hi) c2++;	\
	} while(0)

#define sqr_add_c2(a,i,j,c0,c1,c2) \
	mul_add_c2((a)[i],(a)[j],c0,c1,c2)

#elif defined(BN_UMULT_LOHI)
/*
 * Keep in mind that additions to hi can not overflow, because
 * the high word of a multiplication result cannot be all-ones.
 */
#define mul_add_c(a,b,c0,c1,c2)		do {	\
	BN_ULONG ta = (a), tb = (b);		\
	BN_ULONG lo, hi;			\
	BN_UMULT_LOHI(lo,hi,ta,tb);		\
	c0 += lo; hi += (c0<lo)?1:0;		\
	c1 += hi; c2 += (c1<hi)?1:0;		\
	} while(0)

#define mul_add_c2(a,b,c0,c1,c2)	do {	\
	BN_ULONG ta = (a), tb = (b);		\
	BN_ULONG lo, hi, tt;			\
	BN_UMULT_LOHI(lo,hi,ta,tb);		\
	c0 += lo; tt = hi+((c0<lo)?1:0);	\
	c1 += tt; c2 += (c1<tt)?1:0;		\
	c0 += lo; hi += (c0<lo)?1:0;		\
	c1 += hi; c2 += (c1<hi)?1:0;		\
	} while(0)

#define sqr_add_c(a,i,c0,c1,c2)		do {	\
	BN_ULONG ta = (a)[i];			\
	BN_ULONG lo, hi;			\
	BN_UMULT_LOHI(lo,hi,ta,ta);		\
	c0 += lo; hi += (c0<lo)?1:0;		\
	c1 += hi; c2 += (c1<hi)?1:0;		\
	} while(0)

#define sqr_add_c2(a,i,j,c0,c1,c2)	\
	mul_add_c2((a)[i],(a)[j],c0,c1,c2)

#elif defined(BN_UMULT_HIGH)
/*
 * Keep in mind that additions to hi can not overflow, because
 * the high word of a multiplication result cannot be all-ones.
 */
#define mul_add_c(a,b,c0,c1,c2)		do {	\
	BN_ULONG ta = (a), tb = (b);		\
	BN_ULONG lo = ta * tb;			\
	BN_ULONG hi = BN_UMULT_HIGH(ta,tb);	\
	c0 += lo; hi += (c0<lo)?1:0;		\
	c1 += hi; c2 += (c1<hi)?1:0;		\
	} while(0)

#define mul_add_c2(a,b,c0,c1,c2)	do {	\
	BN_ULONG ta = (a), tb = (b), tt;	\
	BN_ULONG lo = ta * tb;			\
	BN_ULONG hi = BN_UMULT_HIGH(ta,tb);	\
	c0 += lo; tt = hi + ((c0<lo)?1:0);	\
	c1 += tt; c2 += (c1<tt)?1:0;		\
	c0 += lo; hi += (c0<lo)?1:0;		\
	c1 += hi; c2 += (c1<hi)?1:0;		\
	} while(0)

#define sqr_add_c(a,i,c0,c1,c2)		do {	\
	BN_ULONG ta = (a)[i];			\
	BN_ULONG lo = ta * ta;			\
	BN_ULONG hi = BN_UMULT_HIGH(ta,ta);	\
	c0 += lo; hi += (c0<lo)?1:0;		\
	c1 += hi; c2 += (c1<hi)?1:0;		\
	} while(0)

#define sqr_add_c2(a,i,j,c0,c1,c2)	\
	mul_add_c2((a)[i],(a)[j],c0,c1,c2)

#else /* !BN_LLONG */
/*
 * Keep in mind that additions to hi can not overflow, because
 * the high word of a multiplication result cannot be all-ones.
 */
#define mul_add_c(a,b,c0,c1,c2)		do {	\
	BN_ULONG lo = LBITS(a), hi = HBITS(a);	\
	BN_ULONG bl = LBITS(b), bh = HBITS(b);	\
	mul64(lo,hi,bl,bh);			\
	c0 = (c0+lo)&BN_MASK2; if (c0<lo) hi++;	\
	c1 = (c1+hi)&BN_MASK2; if (c1<hi) c2++;	\
	} while(0)

#define mul_add_c2(a,b,c0,c1,c2)	do {	\
	BN_ULONG tt;				\
	BN_ULONG lo = LBITS(a), hi = HBITS(a);	\
	BN_ULONG bl = LBITS(b), bh = HBITS(b);	\
	mul64(lo,hi,bl,bh);			\
	tt = hi;				\
	c0 = (c0+lo)&BN_MASK2; if (c0<lo) tt++;	\
	c1 = (c1+tt)&BN_MASK2; if (c1<tt) c2++;	\
	c0 = (c0+lo)&BN_MASK2; if (c0<lo) hi++;	\
	c1 = (c1+hi)&BN_MASK2; if (c1<hi) c2++;	\
	} while(0)

#define sqr_add_c(a,i,c0,c1,c2)		do {	\
	BN_ULONG lo, hi;			\
	sqr64(lo,hi,(a)[i]);			\
	c0 = (c0+lo)&BN_MASK2; if (c0<lo) hi++;	\
	c1 = (c1+hi)&BN_MASK2; if (c1<hi) c2++;	\
	} while(0)

#define sqr_add_c2(a,i,j,c0,c1,c2) \
	mul_add_c2((a)[i],(a)[j],c0,c1,c2)
#endif /* !BN_LLONG */

void
bn_mul_comba8(BN_ULONG *r, BN_ULONG *a, BN_ULONG *b)
{
	BN_ULONG c1, c2, c3;

	c1 = 0;
	c2 = 0;
	c3 = 0;
	mul_add_c(a[0], b[0], c1, c2, c3);
	r[0] = c1;
	c1 = 0;
	mul_add_c(a[0], b[1], c2, c3, c1);
	mul_add_c(a[1], b[0], c2, c3, c1);
	r[1] = c2;
	c2 = 0;
	mul_add_c(a[2], b[0], c3, c1, c2);
	mul_add_c(a[1], b[1], c3, c1, c2);
	mul_add_c(a[0], b[2], c3, c1, c2);
	r[2] = c3;
	c3 = 0;
	mul_add_c(a[0], b[3], c1, c2, c3);
	mul_add_c(a[1], b[2], c1, c2, c3);
	mul_add_c(a[2], b[1], c1, c2, c3);
	mul_add_c(a[3], b[0], c1, c2, c3);
	r[3] = c1;
	c1 = 0;
	mul_add_c(a[4], b[0], c2, c3, c1);
	mul_add_c(a[3], b[1], c2, c3, c1);
	mul_add_c(a[2], b[2], c2, c3, c1);
	mul_add_c(a[1], b[3], c2, c3, c1);
	mul_add_c(a[0], b[4], c2, c3, c1);
	r[4] = c2;
	c2 = 0;
	mul_add_c(a[0], b[5], c3, c1, c2);
	mul_add_c(a[1], b[4], c3, c1, c2);
	mul_add_c(a[2], b[3], c3, c1, c2);
	mul_add_c(a[3], b[2], c3, c1, c2);
	mul_add_c(a[4], b[1], c3, c1, c2);
	mul_add_c(a[5], b[0], c3, c1, c2);
	r[5] = c3;
	c3 = 0;
	mul_add_c(a[6], b[0], c1, c2, c3);
	mul_add_c(a[5], b[1], c1, c2, c3);
	mul_add_c(a[4], b[2], c1, c2, c3);
	mul_add_c(a[3], b[3], c1, c2, c3);
	mul_add_c(a[2], b[4], c1, c2, c3);
	mul_add_c(a[1], b[5], c1, c2, c3);
	mul_add_c(a[0], b[6], c1, c2, c3);
	r[6] = c1;
	c1 = 0;
	mul_add_c(a[0], b[7], c2, c3, c1);
	mul_add_c(a[1], b[6], c2, c3, c1);
	mul_add_c(a[2], b[5], c2, c3, c1);
	mul_add_c(a[3], b[4], c2, c3, c1);
	mul_add_c(a[4], b[3], c2, c3, c1);
	mul_add_c(a[5], b[2], c2, c3, c1);
	mul_add_c(a[6], b[1], c2, c3, c1);
	mul_add_c(a[7], b[0], c2, c3, c1);
	r[7] = c2;
	c2 = 0;
	mul_add_c(a[7], b[1], c3, c1, c2);
	mul_add_c(a[6], b[2], c3, c1, c2);
	mul_add_c(a[5], b[3], c3, c1, c2);
	mul_add_c(a[4], b[4], c3, c1, c2);
	mul_add_c(a[3], b[5], c3, c1, c2);
	mul_add_c(a[2], b[6], c3, c1, c2);
	mul_add_c(a[1], b[7], c3, c1, c2);
	r[8] = c3;
	c3 = 0;
	mul_add_c(a[2], b[7], c1, c2, c3);
	mul_add_c(a[3], b[6], c1, c2, c3);
	mul_add_c(a[4], b[5], c1, c2, c3);
	mul_add_c(a[5], b[4], c1, c2, c3);
	mul_add_c(a[6], b[3], c1, c2, c3);
	mul_add_c(a[7], b[2], c1, c2, c3);
	r[9] = c1;
	c1 = 0;
	mul_add_c(a[7], b[3], c2, c3, c1);
	mul_add_c(a[6], b[4], c2, c3, c1);
	mul_add_c(a[5], b[5], c2, c3, c1);
	mul_add_c(a[4], b[6], c2, c3, c1);
	mul_add_c(a[3], b[7], c2, c3, c1);
	r[10] = c2;
	c2 = 0;
	mul_add_c(a[4], b[7], c3, c1, c2);
	mul_add_c(a[5], b[6], c3, c1, c2);
	mul_add_c(a[6], b[5], c3, c1, c2);
	mul_add_c(a[7], b[4], c3, c1, c2);
	r[11] = c3;
	c3 = 0;
	mul_add_c(a[7], b[5], c1, c2, c3);
	mul_add_c(a[6], b[6], c1, c2, c3);
	mul_add_c(a[5], b[7], c1, c2, c3);
	r[12] = c1;
	c1 = 0;
	mul_add_c(a[6], b[7], c2, c3, c1);
	mul_add_c(a[7], b[6], c2, c3, c1);
	r[13] = c2;
	c2 = 0;
	mul_add_c(a[7], b[7], c3, c1, c2);
	r[14] = c3;
	r[15] = c1;
}

void
bn_mul_comba4(BN_ULONG *r, BN_ULONG *a, BN_ULONG *b)
{
	BN_ULONG c1, c2, c3;

	c1 = 0;
	c2 = 0;
	c3 = 0;
	mul_add_c(a[0], b[0], c1, c2, c3);
	r[0] = c1;
	c1 = 0;
	mul_add_c(a[0], b[1], c2, c3, c1);
	mul_add_c(a[1], b[0], c2, c3, c1);
	r[1] = c2;
	c2 = 0;
	mul_add_c(a[2], b[0], c3, c1, c2);
	mul_add_c(a[1], b[1], c3, c1, c2);
	mul_add_c(a[0], b[2], c3, c1, c2);
	r[2] = c3;
	c3 = 0;
	mul_add_c(a[0], b[3], c1, c2, c3);
	mul_add_c(a[1], b[2], c1, c2, c3);
	mul_add_c(a[2], b[1], c1, c2, c3);
	mul_add_c(a[3], b[0], c1, c2, c3);
	r[3] = c1;
	c1 = 0;
	mul_add_c(a[3], b[1], c2, c3, c1);
	mul_add_c(a[2], b[2], c2, c3, c1);
	mul_add_c(a[1], b[3], c2, c3, c1);
	r[4] = c2;
	c2 = 0;
	mul_add_c(a[2], b[3], c3, c1, c2);
	mul_add_c(a[3], b[2], c3, c1, c2);
	r[5] = c3;
	c3 = 0;
	mul_add_c(a[3], b[3], c1, c2, c3);
	r[6] = c1;
	r[7] = c2;
}

void
bn_sqr_comba8(BN_ULONG *r, const BN_ULONG *a)
{
	BN_ULONG c1, c2, c3;

	c1 = 0;
	c2 = 0;
	c3 = 0;
	sqr_add_c(a, 0, c1, c2, c3);
	r[0] = c1;
	c1 = 0;
	sqr_add_c2(a, 1, 0, c2, c3, c1);
	r[1] = c2;
	c2 = 0;
	sqr_add_c(a, 1, c3, c1, c2);
	sqr_add_c2(a, 2, 0, c3, c1, c2);
	r[2] = c3;
	c3 = 0;
	sqr_add_c2(a, 3, 0, c1, c2, c3);
	sqr_add_c2(a, 2, 1, c1, c2, c3);
	r[3] = c1;
	c1 = 0;
	sqr_add_c(a, 2, c2, c3, c1);
	sqr_add_c2(a, 3, 1, c2, c3, c1);
	sqr_add_c2(a, 4, 0, c2, c3, c1);
	r[4] = c2;
	c2 = 0;
	sqr_add_c2(a, 5, 0, c3, c1, c2);
	sqr_add_c2(a, 4, 1, c3, c1, c2);
	sqr_add_c2(a, 3, 2, c3, c1, c2);
	r[5] = c3;
	c3 = 0;
	sqr_add_c(a, 3, c1, c2, c3);
	sqr_add_c2(a, 4, 2, c1, c2, c3);
	sqr_add_c2(a, 5, 1, c1, c2, c3);
	sqr_add_c2(a, 6, 0, c1, c2, c3);
	r[6] = c1;
	c1 = 0;
	sqr_add_c2(a, 7, 0, c2, c3, c1);
	sqr_add_c2(a, 6, 1, c2, c3, c1);
	sqr_add_c2(a, 5, 2, c2, c3, c1);
	sqr_add_c2(a, 4, 3, c2, c3, c1);
	r[7] = c2;
	c2 = 0;
	sqr_add_c(a, 4, c3, c1, c2);
	sqr_add_c2(a, 5, 3, c3, c1, c2);
	sqr_add_c2(a, 6, 2, c3, c1, c2);
	sqr_add_c2(a, 7, 1, c3, c1, c2);
	r[8] = c3;
	c3 = 0;
	sqr_add_c2(a, 7, 2, c1, c2, c3);
	sqr_add_c2(a, 6, 3, c1, c2, c3);
	sqr_add_c2(a, 5, 4, c1, c2, c3);
	r[9] = c1;
	c1 = 0;
	sqr_add_c(a, 5, c2, c3, c1);
	sqr_add_c2(a, 6, 4, c2, c3, c1);
	sqr_add_c2(a, 7, 3, c2, c3, c1);
	r[10] = c2;
	c2 = 0;
	sqr_add_c2(a, 7, 4, c3, c1, c2);
	sqr_add_c2(a, 6, 5, c3, c1, c2);
	r[11] = c3;
	c3 = 0;
	sqr_add_c(a, 6, c1, c2, c3);
	sqr_add_c2(a, 7, 5, c1, c2, c3);
	r[12] = c1;
	c1 = 0;
	sqr_add_c2(a, 7, 6, c2, c3, c1);
	r[13] = c2;
	c2 = 0;
	sqr_add_c(a, 7, c3, c1, c2);
	r[14] = c3;
	r[15] = c1;
}

void
bn_sqr_comba4(BN_ULONG *r, const BN_ULONG *a)
{
	BN_ULONG c1, c2, c3;

	c1 = 0;
	c2 = 0;
	c3 = 0;
	sqr_add_c(a, 0, c1, c2, c3);
	r[0] = c1;
	c1 = 0;
	sqr_add_c2(a, 1, 0, c2, c3, c1);
	r[1] = c2;
	c2 = 0;
	sqr_add_c(a, 1, c3, c1, c2);
	sqr_add_c2(a, 2, 0, c3, c1, c2);
	r[2] = c3;
	c3 = 0;
	sqr_add_c2(a, 3, 0, c1, c2, c3);
	sqr_add_c2(a, 2, 1, c1, c2, c3);
	r[3] = c1;
	c1 = 0;
	sqr_add_c(a, 2, c2, c3, c1);
	sqr_add_c2(a, 3, 1, c2, c3, c1);
	r[4] = c2;
	c2 = 0;
	sqr_add_c2(a, 3, 2, c3, c1, c2);
	r[5] = c3;
	c3 = 0;
	sqr_add_c(a, 3, c1, c2, c3);
	r[6] = c1;
	r[7] = c2;
}

#ifdef OPENSSL_NO_ASM
#ifdef OPENSSL_BN_ASM_MONT
/*
 * This is essentially reference implementation, which may or may not
 * result in performance improvement. E.g. on IA-32 this routine was
 * observed to give 40% faster rsa1024 private key operations and 10%
 * faster rsa4096 ones, while on AMD64 it improves rsa1024 sign only
 * by 10% and *worsens* rsa4096 sign by 15%. Once again, it's a
 * reference implementation, one to be used as starting point for
 * platform-specific assembler. Mentioned numbers apply to compiler
 * generated code compiled with and without -DOPENSSL_BN_ASM_MONT and
 * can vary not only from platform to platform, but even for compiler
 * versions. Assembler vs. assembler improvement coefficients can
 * [and are known to] differ and are to be documented elsewhere.
 */
int
bn_mul_mont(BN_ULONG *rp, const BN_ULONG *ap, const BN_ULONG *bp, const BN_ULONG *np, const BN_ULONG *n0p, int num)
{
	BN_ULONG c0, c1, ml, *tp, n0;
#ifdef mul64
	BN_ULONG mh;
#endif
	int i = 0, j;

#if 0	/* template for platform-specific implementation */
	if (ap == bp)
		return bn_sqr_mont(rp, ap, np, n0p, num);
#endif
	tp = reallocarray(NULL, num + 2, sizeof(BN_ULONG));
	if (tp == NULL)
		return 0;

	n0 = *n0p;

	c0 = 0;
	ml = bp[0];
#ifdef mul64
	mh = HBITS(ml);
	ml = LBITS(ml);
	for (j = 0; j < num; ++j)
		mul(tp[j], ap[j], ml, mh, c0);
#else
	for (j = 0; j < num; ++j)
		mul(tp[j], ap[j], ml, c0);
#endif

	tp[num] = c0;
	tp[num + 1] = 0;
	goto enter;

	for (i = 0; i < num; i++) {
		c0 = 0;
		ml = bp[i];
#ifdef mul64
		mh = HBITS(ml);
		ml = LBITS(ml);
		for (j = 0; j < num; ++j)
			mul_add(tp[j], ap[j], ml, mh, c0);
#else
		for (j = 0; j < num; ++j)
			mul_add(tp[j], ap[j], ml, c0);
#endif
		c1 = (tp[num] + c0) & BN_MASK2;
		tp[num] = c1;
		tp[num + 1] = (c1 < c0 ? 1 : 0);
enter:
		c1 = tp[0];
		ml = (c1 * n0) & BN_MASK2;
		c0 = 0;
#ifdef mul64
		mh = HBITS(ml);
		ml = LBITS(ml);
		mul_add(c1, np[0], ml, mh, c0);
#else
		mul_add(c1, ml, np[0], c0);
#endif
		for (j = 1; j < num; j++) {
			c1 = tp[j];
#ifdef mul64
			mul_add(c1, np[j], ml, mh, c0);
#else
			mul_add(c1, ml, np[j], c0);
#endif
			tp[j - 1] = c1 & BN_MASK2;
		}
		c1 = (tp[num] + c0) & BN_MASK2;
		tp[num - 1] = c1;
		tp[num] = tp[num + 1] + (c1 < c0 ? 1 : 0);
	}

	if (tp[num] != 0 || tp[num - 1] >= np[num - 1]) {
		c0 = bn_sub_words(rp, tp, np, num);
		if (tp[num] != 0 || c0 == 0) {
			goto out;
		}
	}
	memcpy(rp, tp, num * sizeof(BN_ULONG));
out:
	freezero(tp, (num + 2) * sizeof(BN_ULONG));
	return 1;
}
#else
/*
 * Return value of 0 indicates that multiplication/convolution was not
 * performed to signal the caller to fall down to alternative/original
 * code-path.
 */
int bn_mul_mont(BN_ULONG *rp, const BN_ULONG *ap, const BN_ULONG *bp, const BN_ULONG *np, const BN_ULONG *n0, int num)
	{	return 0;
}
#endif /* OPENSSL_BN_ASM_MONT */
#endif

#else /* !BN_MUL_COMBA */

/* hmm... is it faster just to do a multiply? */
#undef bn_sqr_comba4
void
bn_sqr_comba4(BN_ULONG *r, const BN_ULONG *a)
{
	BN_ULONG t[8];
	bn_sqr_normal(r, a, 4, t);
}

#undef bn_sqr_comba8
void
bn_sqr_comba8(BN_ULONG *r, const BN_ULONG *a)
{
	BN_ULONG t[16];
	bn_sqr_normal(r, a, 8, t);
}

void
bn_mul_comba4(BN_ULONG *r, BN_ULONG *a, BN_ULONG *b)
{
	r[4] = bn_mul_words(&(r[0]), a, 4, b[0]);
	r[5] = bn_mul_add_words(&(r[1]), a, 4, b[1]);
	r[6] = bn_mul_add_words(&(r[2]), a, 4, b[2]);
	r[7] = bn_mul_add_words(&(r[3]), a, 4, b[3]);
}

void
bn_mul_comba8(BN_ULONG *r, BN_ULONG *a, BN_ULONG *b)
{
	r[8] = bn_mul_words(&(r[0]), a, 8, b[0]);
	r[9] = bn_mul_add_words(&(r[1]), a, 8, b[1]);
	r[10] = bn_mul_add_words(&(r[2]), a, 8, b[2]);
	r[11] = bn_mul_add_words(&(r[3]), a, 8, b[3]);
	r[12] = bn_mul_add_words(&(r[4]), a, 8, b[4]);
	r[13] = bn_mul_add_words(&(r[5]), a, 8, b[5]);
	r[14] = bn_mul_add_words(&(r[6]), a, 8, b[6]);
	r[15] = bn_mul_add_words(&(r[7]), a, 8, b[7]);
}

#ifdef OPENSSL_NO_ASM
#ifdef OPENSSL_BN_ASM_MONT
int
bn_mul_mont(BN_ULONG *rp, const BN_ULONG *ap, const BN_ULONG *bp,
    const BN_ULONG *np, const BN_ULONG *n0p, int num)
{
	BN_ULONG c0, c1, *tp, n0 = *n0p;
	int i = 0, j;

	tp = calloc(NULL, num + 2, sizeof(BN_ULONG));
	if (tp == NULL)
		return 0;

	for (i = 0; i < num; i++) {
		c0 = bn_mul_add_words(tp, ap, num, bp[i]);
		c1 = (tp[num] + c0) & BN_MASK2;
		tp[num] = c1;
		tp[num + 1] = (c1 < c0 ? 1 : 0);

		c0 = bn_mul_add_words(tp, np, num, tp[0] * n0);
		c1 = (tp[num] + c0) & BN_MASK2;
		tp[num] = c1;
		tp[num + 1] += (c1 < c0 ? 1 : 0);
		for (j = 0; j <= num; j++)
			tp[j] = tp[j + 1];
	}

	if (tp[num] != 0 || tp[num - 1] >= np[num - 1]) {
		c0 = bn_sub_words(rp, tp, np, num);
		if (tp[num] != 0 || c0 == 0) {
			goto out;
		}
	}
	memcpy(rp, tp, num * sizeof(BN_ULONG));
out:
	freezero(tp, (num + 2) * sizeof(BN_ULONG));
	return 1;
}
#else
int
bn_mul_mont(BN_ULONG *rp, const BN_ULONG *ap, const BN_ULONG *bp,
    const BN_ULONG *np, const BN_ULONG *n0, int num)
{
	return 0;
}
#endif /* OPENSSL_BN_ASM_MONT */
#endif

#endif /* !BN_MUL_COMBA */
