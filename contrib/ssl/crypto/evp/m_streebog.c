/* $OpenBSD: m_streebog.c,v 1.2 2014/11/09 23:06:50 miod Exp $ */
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
streebog_init256(EVP_MD_CTX *ctx)
{
	return STREEBOG256_Init(ctx->md_data);
}

static int
streebog_update256(EVP_MD_CTX *ctx, const void *data, size_t count)
{
	return STREEBOG256_Update(ctx->md_data, data, count);
}

static int
streebog_final256(EVP_MD_CTX *ctx, unsigned char *md)
{
	return STREEBOG256_Final(md, ctx->md_data);
}

static int
streebog_init512(EVP_MD_CTX *ctx)
{
	return STREEBOG512_Init(ctx->md_data);
}

static int
streebog_update512(EVP_MD_CTX *ctx, const void *data, size_t count)
{
	return STREEBOG512_Update(ctx->md_data, data, count);
}

static int
streebog_final512(EVP_MD_CTX *ctx, unsigned char *md)
{
	return STREEBOG512_Final(md, ctx->md_data);
}

static const EVP_MD streebog256_md = {
	.type = NID_id_tc26_gost3411_2012_256,
	.pkey_type = NID_undef,
	.md_size = STREEBOG256_LENGTH,
	.flags = EVP_MD_FLAG_PKEY_METHOD_SIGNATURE,
	.init = streebog_init256,
	.update = streebog_update256,
	.final = streebog_final256,
	.block_size = STREEBOG_CBLOCK,
	.ctx_size = sizeof(EVP_MD *) + sizeof(STREEBOG_CTX),
};

static const EVP_MD streebog512_md = {
	.type = NID_id_tc26_gost3411_2012_512,
	.pkey_type = NID_undef,
	.md_size = STREEBOG512_LENGTH,
	.flags = EVP_MD_FLAG_PKEY_METHOD_SIGNATURE,
	.init = streebog_init512,
	.update = streebog_update512,
	.final = streebog_final512,
	.block_size = STREEBOG_CBLOCK,
	.ctx_size = sizeof(EVP_MD *) + sizeof(STREEBOG_CTX),
};

const EVP_MD *
EVP_streebog256(void)
{
	return (&streebog256_md);
}

const EVP_MD *
EVP_streebog512(void)
{
	return (&streebog512_md);
}
#endif
