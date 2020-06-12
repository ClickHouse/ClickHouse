/* $OpenBSD: c_enc.c,v 1.7 2014/10/28 07:35:58 jsg Exp $ */
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

#include <openssl/cast.h>
#include "cast_lcl.h"

#ifndef OPENBSD_CAST_ASM
void CAST_encrypt(CAST_LONG *data, const CAST_KEY *key)
	{
	CAST_LONG l,r,t;
	const CAST_LONG *k;

	k= &(key->data[0]);
	l=data[0];
	r=data[1];

	E_CAST( 0,k,l,r,+,^,-);
	E_CAST( 1,k,r,l,^,-,+);
	E_CAST( 2,k,l,r,-,+,^);
	E_CAST( 3,k,r,l,+,^,-);
	E_CAST( 4,k,l,r,^,-,+);
	E_CAST( 5,k,r,l,-,+,^);
	E_CAST( 6,k,l,r,+,^,-);
	E_CAST( 7,k,r,l,^,-,+);
	E_CAST( 8,k,l,r,-,+,^);
	E_CAST( 9,k,r,l,+,^,-);
	E_CAST(10,k,l,r,^,-,+);
	E_CAST(11,k,r,l,-,+,^);
	if(!key->short_key)
	    {
	    E_CAST(12,k,l,r,+,^,-);
	    E_CAST(13,k,r,l,^,-,+);
	    E_CAST(14,k,l,r,-,+,^);
	    E_CAST(15,k,r,l,+,^,-);
	    }

	data[1]=l&0xffffffffL;
	data[0]=r&0xffffffffL;
	}

void CAST_decrypt(CAST_LONG *data, const CAST_KEY *key)
	{
	CAST_LONG l,r,t;
	const CAST_LONG *k;

	k= &(key->data[0]);
	l=data[0];
	r=data[1];

	if(!key->short_key)
	    {
	    E_CAST(15,k,l,r,+,^,-);
	    E_CAST(14,k,r,l,-,+,^);
	    E_CAST(13,k,l,r,^,-,+);
	    E_CAST(12,k,r,l,+,^,-);
	    }
	E_CAST(11,k,l,r,-,+,^);
	E_CAST(10,k,r,l,^,-,+);
	E_CAST( 9,k,l,r,+,^,-);
	E_CAST( 8,k,r,l,-,+,^);
	E_CAST( 7,k,l,r,^,-,+);
	E_CAST( 6,k,r,l,+,^,-);
	E_CAST( 5,k,l,r,-,+,^);
	E_CAST( 4,k,r,l,^,-,+);
	E_CAST( 3,k,l,r,+,^,-);
	E_CAST( 2,k,r,l,-,+,^);
	E_CAST( 1,k,l,r,^,-,+);
	E_CAST( 0,k,r,l,+,^,-);

	data[1]=l&0xffffffffL;
	data[0]=r&0xffffffffL;
	}
#endif

void CAST_cbc_encrypt(const unsigned char *in, unsigned char *out, long length,
	     const CAST_KEY *ks, unsigned char *iv, int enc)
	{
	CAST_LONG tin0,tin1;
	CAST_LONG tout0,tout1,xor0,xor1;
	long l=length;
	CAST_LONG tin[2];

	if (enc)
		{
		n2l(iv,tout0);
		n2l(iv,tout1);
		iv-=8;
		for (l-=8; l>=0; l-=8)
			{
			n2l(in,tin0);
			n2l(in,tin1);
			tin0^=tout0;
			tin1^=tout1;
			tin[0]=tin0;
			tin[1]=tin1;
			CAST_encrypt(tin,ks);
			tout0=tin[0];
			tout1=tin[1];
			l2n(tout0,out);
			l2n(tout1,out);
			}
		if (l != -8)
			{
			n2ln(in,tin0,tin1,l+8);
			tin0^=tout0;
			tin1^=tout1;
			tin[0]=tin0;
			tin[1]=tin1;
			CAST_encrypt(tin,ks);
			tout0=tin[0];
			tout1=tin[1];
			l2n(tout0,out);
			l2n(tout1,out);
			}
		l2n(tout0,iv);
		l2n(tout1,iv);
		}
	else
		{
		n2l(iv,xor0);
		n2l(iv,xor1);
		iv-=8;
		for (l-=8; l>=0; l-=8)
			{
			n2l(in,tin0);
			n2l(in,tin1);
			tin[0]=tin0;
			tin[1]=tin1;
			CAST_decrypt(tin,ks);
			tout0=tin[0]^xor0;
			tout1=tin[1]^xor1;
			l2n(tout0,out);
			l2n(tout1,out);
			xor0=tin0;
			xor1=tin1;
			}
		if (l != -8)
			{
			n2l(in,tin0);
			n2l(in,tin1);
			tin[0]=tin0;
			tin[1]=tin1;
			CAST_decrypt(tin,ks);
			tout0=tin[0]^xor0;
			tout1=tin[1]^xor1;
			l2nn(tout0,tout1,out,l+8);
			xor0=tin0;
			xor1=tin1;
			}
		l2n(xor0,iv);
		l2n(xor1,iv);
		}
	tin0=tin1=tout0=tout1=xor0=xor1=0;
	tin[0]=tin[1]=0;
	}
