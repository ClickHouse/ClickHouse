/*	$OpenBSD: bio_meth.c,v 1.5 2018/02/20 18:51:35 tb Exp $	*/
/*
 * Copyright (c) 2018 Theo Buehler <tb@openbsd.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <stdlib.h>

#include <openssl/bio.h>

BIO_METHOD *
BIO_meth_new(int type, const char *name)
{
	BIO_METHOD *biom;

	if ((biom = calloc(1, sizeof(*biom))) == NULL)
		return NULL;

	biom->type = type;
	biom->name = name;

	return biom;
}

void
BIO_meth_free(BIO_METHOD *biom)
{
	free(biom);
}

int
(*BIO_meth_get_write(BIO_METHOD *biom))(BIO *, const char *, int)
{
	return biom->bwrite;
}

int
BIO_meth_set_write(BIO_METHOD *biom, int (*write)(BIO *, const char *, int))
{
	biom->bwrite = write;
	return 1;
}

int
(*BIO_meth_get_read(BIO_METHOD *biom))(BIO *, char *, int)
{
	return biom->bread;
}

int
BIO_meth_set_read(BIO_METHOD *biom, int (*read)(BIO *, char *, int))
{
	biom->bread = read;
	return 1;
}

int
(*BIO_meth_get_puts(BIO_METHOD *biom))(BIO *, const char *)
{
	return biom->bputs;
}

int
BIO_meth_set_puts(BIO_METHOD *biom, int (*puts)(BIO *, const char *))
{
	biom->bputs = puts;
	return 1;
}

int
(*BIO_meth_get_gets(BIO_METHOD *biom))(BIO *, char *, int)
{
	return biom->bgets;
}

int
BIO_meth_set_gets(BIO_METHOD *biom, int (*gets)(BIO *, char *, int))
{
	biom->bgets = gets;
	return 1;
}

long
(*BIO_meth_get_ctrl(BIO_METHOD *biom))(BIO *, int, long, void *)
{
	return biom->ctrl;
}

int
BIO_meth_set_ctrl(BIO_METHOD *biom, long (*ctrl)(BIO *, int, long, void *))
{
	biom->ctrl = ctrl;
	return 1;
}

int
(*BIO_meth_get_create(BIO_METHOD *biom))(BIO *)
{
	return biom->create;
}

int
BIO_meth_set_create(BIO_METHOD *biom, int (*create)(BIO *))
{
	biom->create = create;
	return 1;
}

int
(*BIO_meth_get_destroy(BIO_METHOD *biom))(BIO *)
{
	return biom->destroy;
}

int
BIO_meth_set_destroy(BIO_METHOD *biom, int (*destroy)(BIO *))
{
	biom->destroy = destroy;
	return 1;
}

long
(*BIO_meth_get_callback_ctrl(BIO_METHOD *biom))(BIO *, int, BIO_info_cb *)
{
	return
	    (long (*)(BIO *, int, BIO_info_cb *))biom->callback_ctrl; /* XXX */
}

int
BIO_meth_set_callback_ctrl(BIO_METHOD *biom,
    long (*callback_ctrl)(BIO *, int, BIO_info_cb *))
{
	biom->callback_ctrl =
	    (long (*)(BIO *, int, bio_info_cb *))callback_ctrl;	/* XXX */
	return 1;
}
