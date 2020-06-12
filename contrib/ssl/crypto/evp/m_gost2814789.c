/* $OpenBSD: m_gost2814789.c,v 1.2 2014/11/09 23:06:50 miod Exp $ */
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
#include <openssl/opensslconf.h>

#ifndef OPENSSL_NO_GOST

#include <openssl/evp.h>
#include <openssl/gost.h>
#include <openssl/objects.h>

static int
gost2814789_init(EVP_MD_CTX *ctx)
{
	return GOST2814789IMIT_Init(ctx->md_data,
	    NID_id_Gost28147_89_CryptoPro_A_ParamSet);
}

static int
gost2814789_update(EVP_MD_CTX *ctx, const void *data, size_t count)
{
	return GOST2814789IMIT_Update(ctx->md_data, data, count);
}

static int
gost2814789_final(EVP_MD_CTX *ctx, unsigned char *md)
{
	return GOST2814789IMIT_Final(md, ctx->md_data);
}

static int
gost2814789_md_ctrl(EVP_MD_CTX *ctx, int cmd, int p1, void *p2)
{
	GOST2814789IMIT_CTX *gctx = ctx->md_data;

	switch (cmd) {
	case EVP_MD_CTRL_SET_KEY:
		return Gost2814789_set_key(&gctx->cipher, p2, p1);
	case EVP_MD_CTRL_GOST_SET_SBOX:
		return Gost2814789_set_sbox(&gctx->cipher, p1);
	}
	return -2;
}

static const EVP_MD gost2814789imit_md = {
	.type = NID_id_Gost28147_89_MAC,
	.pkey_type = NID_undef,
	.md_size = GOST2814789IMIT_LENGTH,
	.flags = 0,
	.init = gost2814789_init,
	.update = gost2814789_update,
	.final = gost2814789_final,
	.block_size = GOST2814789IMIT_CBLOCK,
	.ctx_size = sizeof(EVP_MD *) + sizeof(GOST2814789IMIT_CTX),
	.md_ctrl = gost2814789_md_ctrl,
};

const EVP_MD *
EVP_gost2814789imit(void)
{
	return (&gost2814789imit_md);
}
#endif
