/* $OpenBSD: a_object.c,v 1.30 2017/05/02 03:59:44 deraadt Exp $ */
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

#include <limits.h>
#include <stdio.h>
#include <string.h>

#include <openssl/asn1.h>
#include <openssl/bn.h>
#include <openssl/err.h>
#include <openssl/buffer.h>
#include <openssl/objects.h>

int
i2d_ASN1_OBJECT(ASN1_OBJECT *a, unsigned char **pp)
{
	unsigned char *p;
	int objsize;

	if ((a == NULL) || (a->data == NULL))
		return (0);

	objsize = ASN1_object_size(0, a->length, V_ASN1_OBJECT);
	if (pp == NULL)
		return objsize;

	p = *pp;
	ASN1_put_object(&p, 0, a->length, V_ASN1_OBJECT, V_ASN1_UNIVERSAL);
	memcpy(p, a->data, a->length);
	p += a->length;

	*pp = p;
	return (objsize);
}

int
a2d_ASN1_OBJECT(unsigned char *out, int olen, const char *buf, int num)
{
	int i, first, len = 0, c, use_bn;
	char ftmp[24], *tmp = ftmp;
	int tmpsize = sizeof ftmp;
	const char *p;
	unsigned long l;
	BIGNUM *bl = NULL;

	if (num == 0)
		return (0);
	else if (num == -1)
		num = strlen(buf);

	p = buf;
	c = *(p++);
	num--;
	if ((c >= '0') && (c <= '2')) {
		first= c-'0';
	} else {
		ASN1error(ASN1_R_FIRST_NUM_TOO_LARGE);
		goto err;
	}

	if (num <= 0) {
		ASN1error(ASN1_R_MISSING_SECOND_NUMBER);
		goto err;
	}
	c = *(p++);
	num--;
	for (;;) {
		if (num <= 0)
			break;
		if ((c != '.') && (c != ' ')) {
			ASN1error(ASN1_R_INVALID_SEPARATOR);
			goto err;
		}
		l = 0;
		use_bn = 0;
		for (;;) {
			if (num <= 0)
				break;
			num--;
			c = *(p++);
			if ((c == ' ') || (c == '.'))
				break;
			if ((c < '0') || (c > '9')) {
				ASN1error(ASN1_R_INVALID_DIGIT);
				goto err;
			}
			if (!use_bn && l >= ((ULONG_MAX - 80) / 10L)) {
				use_bn = 1;
				if (!bl)
					bl = BN_new();
				if (!bl || !BN_set_word(bl, l))
					goto err;
			}
			if (use_bn) {
				if (!BN_mul_word(bl, 10L) ||
				    !BN_add_word(bl, c-'0'))
					goto err;
			} else
				l = l * 10L + (long)(c - '0');
		}
		if (len == 0) {
			if ((first < 2) && (l >= 40)) {
				ASN1error(ASN1_R_SECOND_NUMBER_TOO_LARGE);
				goto err;
			}
			if (use_bn) {
				if (!BN_add_word(bl, first * 40))
					goto err;
			} else
				l += (long)first * 40;
		}
		i = 0;
		if (use_bn) {
			int blsize;
			blsize = BN_num_bits(bl);
			blsize = (blsize + 6) / 7;
			if (blsize > tmpsize) {
				if (tmp != ftmp)
					free(tmp);
				tmpsize = blsize + 32;
				tmp = malloc(tmpsize);
				if (!tmp)
					goto err;
			}
			while (blsize--)
				tmp[i++] = (unsigned char)BN_div_word(bl, 0x80L);
		} else {

			for (;;) {
				tmp[i++] = (unsigned char)l & 0x7f;
				l >>= 7L;
				if (l == 0L)
					break;
			}

		}
		if (out != NULL) {
			if (len + i > olen) {
				ASN1error(ASN1_R_BUFFER_TOO_SMALL);
				goto err;
			}
			while (--i > 0)
				out[len++] = tmp[i]|0x80;
			out[len++] = tmp[0];
		} else
			len += i;
	}
	if (tmp != ftmp)
		free(tmp);
	BN_free(bl);
	return (len);

err:
	if (tmp != ftmp)
		free(tmp);
	BN_free(bl);
	return (0);
}

int
i2t_ASN1_OBJECT(char *buf, int buf_len, ASN1_OBJECT *a)
{
	return OBJ_obj2txt(buf, buf_len, a, 0);
}

int
i2a_ASN1_OBJECT(BIO *bp, ASN1_OBJECT *a)
{
	char *tmp = NULL;
	size_t tlen = 256;
	int i = -1;

	if ((a == NULL) || (a->data == NULL))
		return(BIO_write(bp, "NULL", 4));
	if ((tmp = malloc(tlen)) == NULL)
		return -1;
	i = i2t_ASN1_OBJECT(tmp, tlen, a);
	if (i > (int)(tlen - 1)) {
		freezero(tmp, tlen);
		if ((tmp = malloc(i + 1)) == NULL)
			return -1;
		tlen = i + 1;
		i = i2t_ASN1_OBJECT(tmp, tlen, a);
	}
	if (i <= 0)
		i = BIO_write(bp, "<INVALID>", 9);
	else
		i = BIO_write(bp, tmp, i);
	freezero(tmp, tlen);
	return (i);
}

ASN1_OBJECT *
d2i_ASN1_OBJECT(ASN1_OBJECT **a, const unsigned char **pp, long length)
{
	const unsigned char *p;
	long len;
	int tag, xclass;
	int inf, i;
	ASN1_OBJECT *ret = NULL;

	p = *pp;
	inf = ASN1_get_object(&p, &len, &tag, &xclass, length);
	if (inf & 0x80) {
		i = ASN1_R_BAD_OBJECT_HEADER;
		goto err;
	}

	if (tag != V_ASN1_OBJECT) {
		i = ASN1_R_EXPECTING_AN_OBJECT;
		goto err;
	}
	ret = c2i_ASN1_OBJECT(a, &p, len);
	if (ret)
		*pp = p;
	return ret;

err:
	ASN1error(i);
	return (NULL);
}

ASN1_OBJECT *
c2i_ASN1_OBJECT(ASN1_OBJECT **a, const unsigned char **pp, long len)
{
	ASN1_OBJECT *ret;
	const unsigned char *p;
	unsigned char *data;
	int i, length;

	/*
	 * Sanity check OID encoding:
	 * - need at least one content octet
	 * - MSB must be clear in the last octet
	 * - can't have leading 0x80 in subidentifiers, see: X.690 8.19.2
	 */
	if (len <= 0 || len > INT_MAX || pp == NULL || (p = *pp) == NULL ||
	    p[len - 1] & 0x80) {
		ASN1error(ASN1_R_INVALID_OBJECT_ENCODING);
		return (NULL);
	}

	/* Now 0 < len <= INT_MAX, so the cast is safe. */
	length = (int)len;
	for (i = 0; i < length; i++, p++) {
		if (*p == 0x80 && (!i || !(p[-1] & 0x80))) {
			ASN1error(ASN1_R_INVALID_OBJECT_ENCODING);
			return (NULL);
		}
	}

	/* only the ASN1_OBJECTs from the 'table' will have values
	 * for ->sn or ->ln */
	if ((a == NULL) || ((*a) == NULL) ||
	    !((*a)->flags & ASN1_OBJECT_FLAG_DYNAMIC)) {
		if ((ret = ASN1_OBJECT_new()) == NULL)
			return (NULL);
	} else
		ret = *a;

	p = *pp;

	/* detach data from object */
	data = (unsigned char *)ret->data;
	freezero(data, ret->length);

	data = malloc(length);
	if (data == NULL) {
		ASN1error(ERR_R_MALLOC_FAILURE);
		goto err;
	}

	memcpy(data, p, length);

	/* reattach data to object, after which it remains const */
	ret->data = data;
	ret->length = length;
	ret->sn = NULL;
	ret->ln = NULL;
	ret->flags |= ASN1_OBJECT_FLAG_DYNAMIC_DATA;
	p += length;

	if (a != NULL)
		*a = ret;
	*pp = p;
	return (ret);

err:
	if (a == NULL || ret != *a)
		ASN1_OBJECT_free(ret);
	return (NULL);
}

ASN1_OBJECT *
ASN1_OBJECT_new(void)
{
	ASN1_OBJECT *ret;

	ret = malloc(sizeof(ASN1_OBJECT));
	if (ret == NULL) {
		ASN1error(ERR_R_MALLOC_FAILURE);
		return (NULL);
	}
	ret->length = 0;
	ret->data = NULL;
	ret->nid = 0;
	ret->sn = NULL;
	ret->ln = NULL;
	ret->flags = ASN1_OBJECT_FLAG_DYNAMIC;
	return (ret);
}

void
ASN1_OBJECT_free(ASN1_OBJECT *a)
{
	if (a == NULL)
		return;
	if (a->flags & ASN1_OBJECT_FLAG_DYNAMIC_STRINGS) {
		free((void *)a->sn);
		free((void *)a->ln);
		a->sn = a->ln = NULL;
	}
	if (a->flags & ASN1_OBJECT_FLAG_DYNAMIC_DATA) {
		freezero((void *)a->data, a->length);
		a->data = NULL;
		a->length = 0;
	}
	if (a->flags & ASN1_OBJECT_FLAG_DYNAMIC)
		free(a);
}

ASN1_OBJECT *
ASN1_OBJECT_create(int nid, unsigned char *data, int len,
    const char *sn, const char *ln)
{
	ASN1_OBJECT o;

	o.sn = sn;
	o.ln = ln;
	o.data = data;
	o.nid = nid;
	o.length = len;
	o.flags = ASN1_OBJECT_FLAG_DYNAMIC | ASN1_OBJECT_FLAG_DYNAMIC_STRINGS |
	    ASN1_OBJECT_FLAG_DYNAMIC_DATA;
	return (OBJ_dup(&o));
}
