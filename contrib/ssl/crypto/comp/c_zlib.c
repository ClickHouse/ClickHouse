/* $OpenBSD: c_zlib.c,v 1.19 2017/01/29 17:49:22 beck Exp $ */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <openssl/objects.h>
#include <openssl/comp.h>
#include <openssl/err.h>

COMP_METHOD *COMP_zlib(void );

static COMP_METHOD zlib_method_nozlib = {
	.type = NID_undef,
	.name = "(undef)"
};

#ifdef ZLIB

#include <zlib.h>

static int zlib_stateful_init(COMP_CTX *ctx);
static void zlib_stateful_finish(COMP_CTX *ctx);
static int zlib_stateful_compress_block(COMP_CTX *ctx, unsigned char *out,
    unsigned int olen, unsigned char *in, unsigned int ilen);
static int zlib_stateful_expand_block(COMP_CTX *ctx, unsigned char *out,
    unsigned int olen, unsigned char *in, unsigned int ilen);


/* memory allocations functions for zlib intialization */
static void*
zlib_zalloc(void* opaque, unsigned int no, unsigned int size)
{
	return calloc(no, size);
}

static void
zlib_zfree(void* opaque, void* address)
{
	free(address);
}

static COMP_METHOD zlib_stateful_method = {
	.type = NID_zlib_compression,
	.name = LN_zlib_compression,
	.init = zlib_stateful_init,
	.finish = zlib_stateful_finish,
	.compress = zlib_stateful_compress_block,
	.expand = zlib_stateful_expand_block
};

struct zlib_state {
	z_stream istream;
	z_stream ostream;
};

static int zlib_stateful_ex_idx = -1;

static int
zlib_stateful_init(COMP_CTX *ctx)
{
	int err;
	struct zlib_state *state = malloc(sizeof(struct zlib_state));

	if (state == NULL)
		goto err;

	state->istream.zalloc = zlib_zalloc;
	state->istream.zfree = zlib_zfree;
	state->istream.opaque = Z_NULL;
	state->istream.next_in = Z_NULL;
	state->istream.next_out = Z_NULL;
	state->istream.avail_in = 0;
	state->istream.avail_out = 0;
	err = inflateInit_(&state->istream, ZLIB_VERSION, sizeof(z_stream));
	if (err != Z_OK)
		goto err;

	state->ostream.zalloc = zlib_zalloc;
	state->ostream.zfree = zlib_zfree;
	state->ostream.opaque = Z_NULL;
	state->ostream.next_in = Z_NULL;
	state->ostream.next_out = Z_NULL;
	state->ostream.avail_in = 0;
	state->ostream.avail_out = 0;
	err = deflateInit_(&state->ostream, Z_DEFAULT_COMPRESSION,
	    ZLIB_VERSION, sizeof(z_stream));
	if (err != Z_OK)
		goto err;

	CRYPTO_new_ex_data(CRYPTO_EX_INDEX_COMP, ctx, &ctx->ex_data);
	CRYPTO_set_ex_data(&ctx->ex_data, zlib_stateful_ex_idx, state);
	return 1;

err:
	free(state);
	return 0;
}

static void
zlib_stateful_finish(COMP_CTX *ctx)
{
	struct zlib_state *state =
	    (struct zlib_state *)CRYPTO_get_ex_data(&ctx->ex_data,
	    zlib_stateful_ex_idx);

	inflateEnd(&state->istream);
	deflateEnd(&state->ostream);
	free(state);
	CRYPTO_free_ex_data(CRYPTO_EX_INDEX_COMP, ctx, &ctx->ex_data);
}

static int
zlib_stateful_compress_block(COMP_CTX *ctx, unsigned char *out,
    unsigned int olen, unsigned char *in, unsigned int ilen)
{
	int err = Z_OK;
	struct zlib_state *state =
	    (struct zlib_state *)CRYPTO_get_ex_data(&ctx->ex_data,
	    zlib_stateful_ex_idx);

	if (state == NULL)
		return -1;

	state->ostream.next_in = in;
	state->ostream.avail_in = ilen;
	state->ostream.next_out = out;
	state->ostream.avail_out = olen;
	if (ilen > 0)
		err = deflate(&state->ostream, Z_SYNC_FLUSH);
	if (err != Z_OK)
		return -1;

#ifdef DEBUG_ZLIB
	fprintf(stderr, "compress(%4d)->%4d %s\n",
	    ilen, olen - state->ostream.avail_out,
	    (ilen != olen - state->ostream.avail_out)?"zlib":"clear");
#endif

	return olen - state->ostream.avail_out;
}

static int
zlib_stateful_expand_block(COMP_CTX *ctx, unsigned char *out,
    unsigned int olen, unsigned char *in, unsigned int ilen)
{
	int err = Z_OK;

	struct zlib_state *state =
	    (struct zlib_state *)CRYPTO_get_ex_data(&ctx->ex_data,
	    zlib_stateful_ex_idx);

	if (state == NULL)
		return 0;

	state->istream.next_in = in;
	state->istream.avail_in = ilen;
	state->istream.next_out = out;
	state->istream.avail_out = olen;
	if (ilen > 0)
		err = inflate(&state->istream, Z_SYNC_FLUSH);
	if (err != Z_OK)
		return -1;

#ifdef DEBUG_ZLIB
	fprintf(stderr, "expand(%4d)->%4d %s\n",
	    ilen, olen - state->istream.avail_out,
	    (ilen != olen - state->istream.avail_out)?"zlib":"clear");
#endif

	return olen - state->istream.avail_out;
}

#endif

COMP_METHOD *
COMP_zlib(void)
{
	COMP_METHOD *meth = &zlib_method_nozlib;

#ifdef ZLIB
	{
		/* init zlib_stateful_ex_idx here so that in a multi-process
		 * application it's enough to intialize openssl before forking
		 * (idx will be inherited in all the children) */
		if (zlib_stateful_ex_idx == -1) {
			CRYPTO_w_lock(CRYPTO_LOCK_COMP);
			if (zlib_stateful_ex_idx == -1)
				zlib_stateful_ex_idx =
			    CRYPTO_get_ex_new_index(CRYPTO_EX_INDEX_COMP,
			    0, NULL, NULL, NULL, NULL);
			CRYPTO_w_unlock(CRYPTO_LOCK_COMP);
			if (zlib_stateful_ex_idx == -1)
				goto err;
		}

		meth = &zlib_stateful_method;
	}

err:
#endif

	return (meth);
}

void
COMP_zlib_cleanup(void)
{
}

#ifdef ZLIB

/* Zlib based compression/decompression filter BIO */

typedef struct {
	unsigned char *ibuf;	/* Input buffer */
	int ibufsize;		/* Buffer size */
	z_stream zin;		/* Input decompress context */
	unsigned char *obuf;	/* Output buffer */
	int obufsize;		/* Output buffer size */
	unsigned char *optr;	/* Position in output buffer */
	int ocount;		/* Amount of data in output buffer */
	int odone;		/* deflate EOF */
	int comp_level;		/* Compression level to use */
	z_stream zout;		/* Output compression context */
} BIO_ZLIB_CTX;

#define ZLIB_DEFAULT_BUFSIZE 1024

static int bio_zlib_new(BIO *bi);
static int bio_zlib_free(BIO *bi);
static int bio_zlib_read(BIO *b, char *out, int outl);
static int bio_zlib_write(BIO *b, const char *in, int inl);
static long bio_zlib_ctrl(BIO *b, int cmd, long num, void *ptr);
static long bio_zlib_callback_ctrl(BIO *b, int cmd, bio_info_cb *fp);

static BIO_METHOD bio_meth_zlib = {
	.type = BIO_TYPE_COMP,
	.name = "zlib",
	.bwrite = bio_zlib_write,
	.bread = bio_zlib_read,
	.ctrl = bio_zlib_ctrl,
	.create = bio_zlib_new,
	.destroy = bio_zlib_free,
	.callback_ctrl = bio_zlib_callback_ctrl
};

BIO_METHOD *
BIO_f_zlib(void)
{
	return &bio_meth_zlib;
}


static int
bio_zlib_new(BIO *bi)
{
	BIO_ZLIB_CTX *ctx;

	ctx = malloc(sizeof(BIO_ZLIB_CTX));
	if (!ctx) {
		COMPerror(ERR_R_MALLOC_FAILURE);
		return 0;
	}
	ctx->ibuf = NULL;
	ctx->obuf = NULL;
	ctx->ibufsize = ZLIB_DEFAULT_BUFSIZE;
	ctx->obufsize = ZLIB_DEFAULT_BUFSIZE;
	ctx->zin.zalloc = Z_NULL;
	ctx->zin.zfree = Z_NULL;
	ctx->zin.next_in = NULL;
	ctx->zin.avail_in = 0;
	ctx->zin.next_out = NULL;
	ctx->zin.avail_out = 0;
	ctx->zout.zalloc = Z_NULL;
	ctx->zout.zfree = Z_NULL;
	ctx->zout.next_in = NULL;
	ctx->zout.avail_in = 0;
	ctx->zout.next_out = NULL;
	ctx->zout.avail_out = 0;
	ctx->odone = 0;
	ctx->comp_level = Z_DEFAULT_COMPRESSION;
	bi->init = 1;
	bi->ptr = (char *)ctx;
	bi->flags = 0;
	return 1;
}

static int
bio_zlib_free(BIO *bi)
{
	BIO_ZLIB_CTX *ctx;

	if (!bi)
		return 0;
	ctx = (BIO_ZLIB_CTX *)bi->ptr;
	if (ctx->ibuf) {
		/* Destroy decompress context */
		inflateEnd(&ctx->zin);
		free(ctx->ibuf);
	}
	if (ctx->obuf) {
		/* Destroy compress context */
		deflateEnd(&ctx->zout);
		free(ctx->obuf);
	}
	free(ctx);
	bi->ptr = NULL;
	bi->init = 0;
	bi->flags = 0;
	return 1;
}

static int
bio_zlib_read(BIO *b, char *out, int outl)
{
	BIO_ZLIB_CTX *ctx;
	int ret;
	z_stream *zin;

	if (!out || !outl)
		return 0;
	ctx = (BIO_ZLIB_CTX *)b->ptr;
	zin = &ctx->zin;
	BIO_clear_retry_flags(b);
	if (!ctx->ibuf) {
		ctx->ibuf = malloc(ctx->ibufsize);
		if (!ctx->ibuf) {
			COMPerror(ERR_R_MALLOC_FAILURE);
			return 0;
		}
		inflateInit(zin);
		zin->next_in = ctx->ibuf;
		zin->avail_in = 0;
	}

	/* Copy output data directly to supplied buffer */
	zin->next_out = (unsigned char *)out;
	zin->avail_out = (unsigned int)outl;
	for (;;) {
		/* Decompress while data available */
		while (zin->avail_in) {
			ret = inflate(zin, 0);
			if ((ret != Z_OK) && (ret != Z_STREAM_END)) {
				COMPerror(COMP_R_ZLIB_INFLATE_ERROR);
				ERR_asprintf_error_data("zlib error:%s",
				    zError(ret));
				return 0;
			}
			/* If EOF or we've read everything then return */
			if ((ret == Z_STREAM_END) || !zin->avail_out)
				return outl - zin->avail_out;
		}

		/* No data in input buffer try to read some in,
		 * if an error then return the total data read.
		 */
		ret = BIO_read(b->next_bio, ctx->ibuf, ctx->ibufsize);
		if (ret <= 0) {
			/* Total data read */
			int tot = outl - zin->avail_out;
			BIO_copy_next_retry(b);
			if (ret < 0)
				return (tot > 0) ? tot : ret;
			return tot;
		}
		zin->avail_in = ret;
		zin->next_in = ctx->ibuf;
	}
}

static int
bio_zlib_write(BIO *b, const char *in, int inl)
{
	BIO_ZLIB_CTX *ctx;
	int ret;
	z_stream *zout;

	if (!in || !inl)
		return 0;
	ctx = (BIO_ZLIB_CTX *)b->ptr;
	if (ctx->odone)
		return 0;
	zout = &ctx->zout;
	BIO_clear_retry_flags(b);
	if (!ctx->obuf) {
		ctx->obuf = malloc(ctx->obufsize);
		/* Need error here */
		if (!ctx->obuf) {
			COMPerror(ERR_R_MALLOC_FAILURE);
			return 0;
		}
		ctx->optr = ctx->obuf;
		ctx->ocount = 0;
		deflateInit(zout, ctx->comp_level);
		zout->next_out = ctx->obuf;
		zout->avail_out = ctx->obufsize;
	}
	/* Obtain input data directly from supplied buffer */
	zout->next_in = (void *)in;
	zout->avail_in = inl;
	for (;;) {
		/* If data in output buffer write it first */
		while (ctx->ocount) {
			ret = BIO_write(b->next_bio, ctx->optr, ctx->ocount);
			if (ret <= 0) {
				/* Total data written */
				int tot = inl - zout->avail_in;
				BIO_copy_next_retry(b);
				if (ret < 0)
					return (tot > 0) ? tot : ret;
				return tot;
			}
			ctx->optr += ret;
			ctx->ocount -= ret;
		}

		/* Have we consumed all supplied data? */
		if (!zout->avail_in)
			return inl;

		/* Compress some more */

		/* Reset buffer */
		ctx->optr = ctx->obuf;
		zout->next_out = ctx->obuf;
		zout->avail_out = ctx->obufsize;
		/* Compress some more */
		ret = deflate(zout, 0);
		if (ret != Z_OK) {
			COMPerror(COMP_R_ZLIB_DEFLATE_ERROR);
			ERR_asprintf_error_data("zlib error:%s", zError(ret));
			return 0;
		}
		ctx->ocount = ctx->obufsize - zout->avail_out;
	}
}

static int
bio_zlib_flush(BIO *b)
{
	BIO_ZLIB_CTX *ctx;
	int ret;
	z_stream *zout;

	ctx = (BIO_ZLIB_CTX *)b->ptr;
	/* If no data written or already flush show success */
	if (!ctx->obuf || (ctx->odone && !ctx->ocount))
		return 1;
	zout = &ctx->zout;
	BIO_clear_retry_flags(b);
	/* No more input data */
	zout->next_in = NULL;
	zout->avail_in = 0;
	for (;;) {
		/* If data in output buffer write it first */
		while (ctx->ocount) {
			ret = BIO_write(b->next_bio, ctx->optr, ctx->ocount);
			if (ret <= 0) {
				BIO_copy_next_retry(b);
				return ret;
			}
			ctx->optr += ret;
			ctx->ocount -= ret;
		}
		if (ctx->odone)
			return 1;

		/* Compress some more */

		/* Reset buffer */
		ctx->optr = ctx->obuf;
		zout->next_out = ctx->obuf;
		zout->avail_out = ctx->obufsize;
		/* Compress some more */
		ret = deflate(zout, Z_FINISH);
		if (ret == Z_STREAM_END)
			ctx->odone = 1;
		else if (ret != Z_OK) {
			COMPerror(COMP_R_ZLIB_DEFLATE_ERROR);
			ERR_asprintf_error_data("zlib error:%s", zError(ret));
			return 0;
		}
		ctx->ocount = ctx->obufsize - zout->avail_out;
	}
}

static long
bio_zlib_ctrl(BIO *b, int cmd, long num, void *ptr)
{
	BIO_ZLIB_CTX *ctx;
	int ret, *ip;
	int ibs, obs;
	if (!b->next_bio)
		return 0;
	ctx = (BIO_ZLIB_CTX *)b->ptr;
	switch (cmd) {

	case BIO_CTRL_RESET:
		ctx->ocount = 0;
		ctx->odone = 0;
		ret = 1;
		break;

	case BIO_CTRL_FLUSH:
		ret = bio_zlib_flush(b);
		if (ret > 0)
			ret = BIO_flush(b->next_bio);
		break;

	case BIO_C_SET_BUFF_SIZE:
		ibs = -1;
		obs = -1;
		if (ptr != NULL) {
			ip = ptr;
			if (*ip == 0)
				ibs = (int) num;
			else
				obs = (int) num;
		} else {
			ibs = (int)num;
			obs = ibs;
		}

		if (ibs != -1) {
			free(ctx->ibuf);
			ctx->ibuf = NULL;
			ctx->ibufsize = ibs;
		}

		if (obs != -1) {
			free(ctx->obuf);
			ctx->obuf = NULL;
			ctx->obufsize = obs;
		}
		ret = 1;
		break;

	case BIO_C_DO_STATE_MACHINE:
		BIO_clear_retry_flags(b);
		ret = BIO_ctrl(b->next_bio, cmd, num, ptr);
		BIO_copy_next_retry(b);
		break;

	default:
		ret = BIO_ctrl(b->next_bio, cmd, num, ptr);
		break;

	}

	return ret;
}


static long
bio_zlib_callback_ctrl(BIO *b, int cmd, bio_info_cb *fp)
{
	if (!b->next_bio)
		return 0;
	return BIO_callback_ctrl(b->next_bio, cmd, fp);
}

#endif
