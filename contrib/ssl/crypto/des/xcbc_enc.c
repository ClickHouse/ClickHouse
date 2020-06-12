/* $OpenBSD: xcbc_enc.c,v 1.9 2015/02/07 13:19:15 doug Exp $ */
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

#include "des_locl.h"

/* RSA's DESX */

void DES_xcbc_encrypt(const unsigned char *in, unsigned char *out,
		      long length, DES_key_schedule *schedule,
		      DES_cblock *ivec, const_DES_cblock *inw,
		      const_DES_cblock *outw, int enc)
	{
	DES_LONG tin0,tin1;
	DES_LONG tout0,tout1,xor0,xor1;
	DES_LONG inW0,inW1,outW0,outW1;
	const unsigned char *in2;
	long l=length;
	DES_LONG tin[2];
	unsigned char *iv;

	in2 = &(*inw)[0];
	c2l(in2,inW0);
	c2l(in2,inW1);
	in2 = &(*outw)[0];
	c2l(in2,outW0);
	c2l(in2,outW1);

	iv = &(*ivec)[0];

	if (enc)
		{
		c2l(iv,tout0);
		c2l(iv,tout1);
		for (l-=8; l>=0; l-=8)
			{
			c2l(in,tin0);
			c2l(in,tin1);
			tin0^=tout0^inW0; tin[0]=tin0;
			tin1^=tout1^inW1; tin[1]=tin1;
			DES_encrypt1(tin,schedule,DES_ENCRYPT);
			tout0=tin[0]^outW0; l2c(tout0,out);
			tout1=tin[1]^outW1; l2c(tout1,out);
			}
		if (l != -8)
			{
			c2ln(in,tin0,tin1,l+8);
			tin0^=tout0^inW0; tin[0]=tin0;
			tin1^=tout1^inW1; tin[1]=tin1;
			DES_encrypt1(tin,schedule,DES_ENCRYPT);
			tout0=tin[0]^outW0; l2c(tout0,out);
			tout1=tin[1]^outW1; l2c(tout1,out);
			}
		iv = &(*ivec)[0];
		l2c(tout0,iv);
		l2c(tout1,iv);
		}
	else
		{
		c2l(iv,xor0);
		c2l(iv,xor1);
		for (l-=8; l>0; l-=8)
			{
			c2l(in,tin0); tin[0]=tin0^outW0;
			c2l(in,tin1); tin[1]=tin1^outW1;
			DES_encrypt1(tin,schedule,DES_DECRYPT);
			tout0=tin[0]^xor0^inW0;
			tout1=tin[1]^xor1^inW1;
			l2c(tout0,out);
			l2c(tout1,out);
			xor0=tin0;
			xor1=tin1;
			}
		if (l != -8)
			{
			c2l(in,tin0); tin[0]=tin0^outW0;
			c2l(in,tin1); tin[1]=tin1^outW1;
			DES_encrypt1(tin,schedule,DES_DECRYPT);
			tout0=tin[0]^xor0^inW0;
			tout1=tin[1]^xor1^inW1;
			l2cn(tout0,tout1,out,l+8);
			xor0=tin0;
			xor1=tin1;
			}

		iv = &(*ivec)[0];
		l2c(xor0,iv);
		l2c(xor1,iv);
		}
	tin0=tin1=tout0=tout1=xor0=xor1=0;
	inW0=inW1=outW0=outW1=0;
	tin[0]=tin[1]=0;
	}

