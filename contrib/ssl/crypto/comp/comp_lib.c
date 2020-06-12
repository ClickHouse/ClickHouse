/* $OpenBSD: comp_lib.c,v 1.8 2014/11/03 16:58:28 tedu Exp $ */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <openssl/objects.h>
#include <openssl/comp.h>

COMP_CTX *
COMP_CTX_new(COMP_METHOD *meth)
{
	COMP_CTX *ret;

	if ((ret = calloc(1, sizeof(COMP_CTX))) == NULL) {
		return (NULL);
	}
	ret->meth = meth;
	if ((ret->meth->init != NULL) && !ret->meth->init(ret)) {
		free(ret);
		ret = NULL;
	}
	return (ret);
}

void
COMP_CTX_free(COMP_CTX *ctx)
{
	if (ctx == NULL)
		return;

	if (ctx->meth->finish != NULL)
		ctx->meth->finish(ctx);

	free(ctx);
}

int
COMP_compress_block(COMP_CTX *ctx, unsigned char *out, int olen,
    unsigned char *in, int ilen)
{
	int ret;

	if (ctx->meth->compress == NULL) {
		return (-1);
	}
	ret = ctx->meth->compress(ctx, out, olen, in, ilen);
	if (ret > 0) {
		ctx->compress_in += ilen;
		ctx->compress_out += ret;
	}
	return (ret);
}

int
COMP_expand_block(COMP_CTX *ctx, unsigned char *out, int olen,
    unsigned char *in, int ilen)
{
	int ret;

	if (ctx->meth->expand == NULL) {
		return (-1);
	}
	ret = ctx->meth->expand(ctx, out, olen, in, ilen);
	if (ret > 0) {
		ctx->expand_in += ilen;
		ctx->expand_out += ret;
	}
	return (ret);
}
