/* $OpenBSD: obj_dat.c,v 1.39 2017/01/29 17:49:23 beck Exp $ */
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

#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>

#include <openssl/opensslconf.h>

#include <openssl/asn1.h>
#include <openssl/bn.h>
#include <openssl/err.h>
#include <openssl/lhash.h>
#include <openssl/objects.h>

/* obj_dat.h is generated from objects.h by obj_dat.pl */
#include "obj_dat.h"

static int sn_cmp_BSEARCH_CMP_FN(const void *, const void *);
static int sn_cmp(const ASN1_OBJECT * const *, unsigned int const *);
static unsigned int *OBJ_bsearch_sn(const ASN1_OBJECT * *key, unsigned int const *base, int num);
static int ln_cmp_BSEARCH_CMP_FN(const void *, const void *);
static int ln_cmp(const ASN1_OBJECT * const *, unsigned int const *);
static unsigned int *OBJ_bsearch_ln(const ASN1_OBJECT * *key, unsigned int const *base, int num);
static int obj_cmp_BSEARCH_CMP_FN(const void *, const void *);
static int obj_cmp(const ASN1_OBJECT * const *, unsigned int const *);
static unsigned int *OBJ_bsearch_obj(const ASN1_OBJECT * *key, unsigned int const *base, int num);

#define ADDED_DATA	0
#define ADDED_SNAME	1
#define ADDED_LNAME	2
#define ADDED_NID	3

typedef struct added_obj_st {
	int type;
	ASN1_OBJECT *obj;
} ADDED_OBJ;
DECLARE_LHASH_OF(ADDED_OBJ);

static int new_nid = NUM_NID;
static LHASH_OF(ADDED_OBJ) *added = NULL;

static int sn_cmp(const ASN1_OBJECT * const *a, const unsigned int *b)
{
	return (strcmp((*a)->sn, nid_objs[*b].sn));
}


static int
sn_cmp_BSEARCH_CMP_FN(const void *a_, const void *b_)
{
	const ASN1_OBJECT * const *a = a_;
	unsigned int const *b = b_;
	return sn_cmp(a, b);
}

static unsigned int *
OBJ_bsearch_sn(const ASN1_OBJECT * *key, unsigned int const *base, int num)
{
	return (unsigned int *)OBJ_bsearch_(key, base, num, sizeof(unsigned int),
	    sn_cmp_BSEARCH_CMP_FN);
}

static int ln_cmp(const ASN1_OBJECT * const *a, const unsigned int *b)
{
	return (strcmp((*a)->ln, nid_objs[*b].ln));
}


static int
ln_cmp_BSEARCH_CMP_FN(const void *a_, const void *b_)
{
	const ASN1_OBJECT * const *a = a_;
	unsigned int const *b = b_;
	return ln_cmp(a, b);
}

static unsigned int *
OBJ_bsearch_ln(const ASN1_OBJECT * *key, unsigned int const *base, int num)
{
	return (unsigned int *)OBJ_bsearch_(key, base, num, sizeof(unsigned int),
	    ln_cmp_BSEARCH_CMP_FN);
}

static unsigned long
added_obj_hash(const ADDED_OBJ *ca)
{
	const ASN1_OBJECT *a;
	int i;
	unsigned long ret = 0;
	unsigned char *p;

	a = ca->obj;
	switch (ca->type) {
	case ADDED_DATA:
		ret = a->length << 20L;
		p = (unsigned char *)a->data;
		for (i = 0; i < a->length; i++)
			ret ^= p[i] << ((i * 3) % 24);
		break;
	case ADDED_SNAME:
		ret = lh_strhash(a->sn);
		break;
	case ADDED_LNAME:
		ret = lh_strhash(a->ln);
		break;
	case ADDED_NID:
		ret = a->nid;
		break;
	default:
		/* abort(); */
		return 0;
	}
	ret &= 0x3fffffffL;
	ret |= ca->type << 30L;
	return (ret);
}
static IMPLEMENT_LHASH_HASH_FN(added_obj, ADDED_OBJ)

static int
added_obj_cmp(const ADDED_OBJ *ca, const ADDED_OBJ *cb)
{
	ASN1_OBJECT *a, *b;
	int i;

	i = ca->type - cb->type;
	if (i)
		return (i);
	a = ca->obj;
	b = cb->obj;
	switch (ca->type) {
	case ADDED_DATA:
		i = (a->length - b->length);
		if (i)
			return (i);
		return (memcmp(a->data, b->data, (size_t)a->length));
	case ADDED_SNAME:
		if (a->sn == NULL)
			return (-1);
		else if (b->sn == NULL)
			return (1);
		else
			return (strcmp(a->sn, b->sn));
	case ADDED_LNAME:
		if (a->ln == NULL)
			return (-1);
		else if (b->ln == NULL)
			return (1);
		else
			return (strcmp(a->ln, b->ln));
	case ADDED_NID:
		return (a->nid - b->nid);
	default:
		/* abort(); */
		return 0;
	}
}
static IMPLEMENT_LHASH_COMP_FN(added_obj, ADDED_OBJ)

static int
init_added(void)
{
	if (added != NULL)
		return (1);
	added = lh_ADDED_OBJ_new();
	return (added != NULL);
}

static void
cleanup1_doall(ADDED_OBJ *a)
{
	a->obj->nid = 0;
	a->obj->flags |= ASN1_OBJECT_FLAG_DYNAMIC |
	    ASN1_OBJECT_FLAG_DYNAMIC_STRINGS |
	    ASN1_OBJECT_FLAG_DYNAMIC_DATA;
}

static void cleanup2_doall(ADDED_OBJ *a)
{
	a->obj->nid++;
}

static void
cleanup3_doall(ADDED_OBJ *a)
{
	if (--a->obj->nid == 0)
		ASN1_OBJECT_free(a->obj);
	free(a);
}

static IMPLEMENT_LHASH_DOALL_FN(cleanup1, ADDED_OBJ)
static IMPLEMENT_LHASH_DOALL_FN(cleanup2, ADDED_OBJ)
static IMPLEMENT_LHASH_DOALL_FN(cleanup3, ADDED_OBJ)

/* The purpose of obj_cleanup_defer is to avoid EVP_cleanup() attempting
 * to use freed up OIDs. If neccessary the actual freeing up of OIDs is
 * delayed.
 */

int obj_cleanup_defer = 0;

void
check_defer(int nid)
{
	if (!obj_cleanup_defer && nid >= NUM_NID)
		obj_cleanup_defer = 1;
}

void
OBJ_cleanup(void)
{
	if (obj_cleanup_defer) {
		obj_cleanup_defer = 2;
		return;
	}
	if (added == NULL)
		return;
	lh_ADDED_OBJ_down_load(added) = 0;
	lh_ADDED_OBJ_doall(added, LHASH_DOALL_FN(cleanup1)); /* zero counters */
	lh_ADDED_OBJ_doall(added, LHASH_DOALL_FN(cleanup2)); /* set counters */
	lh_ADDED_OBJ_doall(added, LHASH_DOALL_FN(cleanup3)); /* free objects */
	lh_ADDED_OBJ_free(added);
	added = NULL;
}

int
OBJ_new_nid(int num)
{
	int i;

	i = new_nid;
	new_nid += num;
	return (i);
}

int
OBJ_add_object(const ASN1_OBJECT *obj)
{
	ASN1_OBJECT *o;
	ADDED_OBJ *ao[4] = {NULL, NULL, NULL, NULL}, *aop;
	int i;

	if (added == NULL)
		if (!init_added())
			return (0);
	if ((o = OBJ_dup(obj)) == NULL)
		goto err;
	if (!(ao[ADDED_NID] = malloc(sizeof(ADDED_OBJ))))
		goto err2;
	if ((o->length != 0) && (obj->data != NULL))
		if (!(ao[ADDED_DATA] = malloc(sizeof(ADDED_OBJ))))
			goto err2;
	if (o->sn != NULL)
		if (!(ao[ADDED_SNAME] = malloc(sizeof(ADDED_OBJ))))
			goto err2;
	if (o->ln != NULL)
		if (!(ao[ADDED_LNAME] = malloc(sizeof(ADDED_OBJ))))
			goto err2;

	for (i = ADDED_DATA; i <= ADDED_NID; i++) {
		if (ao[i] != NULL) {
			ao[i]->type = i;
			ao[i]->obj = o;
			aop = lh_ADDED_OBJ_insert(added, ao[i]);
			/* memory leak, buit should not normally matter */
			free(aop);
		}
	}
	o->flags &= ~(ASN1_OBJECT_FLAG_DYNAMIC |
	    ASN1_OBJECT_FLAG_DYNAMIC_STRINGS |
	    ASN1_OBJECT_FLAG_DYNAMIC_DATA);

	return (o->nid);

err2:
	OBJerror(ERR_R_MALLOC_FAILURE);
err:
	for (i = ADDED_DATA; i <= ADDED_NID; i++)
		free(ao[i]);
	free(o);
	return (NID_undef);
}

ASN1_OBJECT *
OBJ_nid2obj(int n)
{
	ADDED_OBJ ad, *adp;
	ASN1_OBJECT ob;

	if ((n >= 0) && (n < NUM_NID)) {
		if ((n != NID_undef) && (nid_objs[n].nid == NID_undef)) {
			OBJerror(OBJ_R_UNKNOWN_NID);
			return (NULL);
		}
		return ((ASN1_OBJECT *)&(nid_objs[n]));
	} else if (added == NULL)
		return (NULL);
	else {
		ad.type = ADDED_NID;
		ad.obj = &ob;
		ob.nid = n;
		adp = lh_ADDED_OBJ_retrieve(added, &ad);
		if (adp != NULL)
			return (adp->obj);
		else {
			OBJerror(OBJ_R_UNKNOWN_NID);
			return (NULL);
		}
	}
}

const char *
OBJ_nid2sn(int n)
{
	ADDED_OBJ ad, *adp;
	ASN1_OBJECT ob;

	if ((n >= 0) && (n < NUM_NID)) {
		if ((n != NID_undef) && (nid_objs[n].nid == NID_undef)) {
			OBJerror(OBJ_R_UNKNOWN_NID);
			return (NULL);
		}
		return (nid_objs[n].sn);
	} else if (added == NULL)
		return (NULL);
	else {
		ad.type = ADDED_NID;
		ad.obj = &ob;
		ob.nid = n;
		adp = lh_ADDED_OBJ_retrieve(added, &ad);
		if (adp != NULL)
			return (adp->obj->sn);
		else {
			OBJerror(OBJ_R_UNKNOWN_NID);
			return (NULL);
		}
	}
}

const char *
OBJ_nid2ln(int n)
{
	ADDED_OBJ ad, *adp;
	ASN1_OBJECT ob;

	if ((n >= 0) && (n < NUM_NID)) {
		if ((n != NID_undef) && (nid_objs[n].nid == NID_undef)) {
			OBJerror(OBJ_R_UNKNOWN_NID);
			return (NULL);
		}
		return (nid_objs[n].ln);
	} else if (added == NULL)
		return (NULL);
	else {
		ad.type = ADDED_NID;
		ad.obj = &ob;
		ob.nid = n;
		adp = lh_ADDED_OBJ_retrieve(added, &ad);
		if (adp != NULL)
			return (adp->obj->ln);
		else {
			OBJerror(OBJ_R_UNKNOWN_NID);
			return (NULL);
		}
	}
}

static int
obj_cmp(const ASN1_OBJECT * const *ap, const unsigned int *bp)
{
	int j;
	const ASN1_OBJECT *a= *ap;
	const ASN1_OBJECT *b = &nid_objs[*bp];

	j = (a->length - b->length);
	if (j)
		return (j);
	return (memcmp(a->data, b->data, a->length));
}


static int
obj_cmp_BSEARCH_CMP_FN(const void *a_, const void *b_)
{
	const ASN1_OBJECT * const *a = a_;
	unsigned int const *b = b_;
	return obj_cmp(a, b);
}

static unsigned int *
OBJ_bsearch_obj(const ASN1_OBJECT * *key, unsigned int const *base, int num)
{
	return (unsigned int *)OBJ_bsearch_(key, base, num, sizeof(unsigned int),
	    obj_cmp_BSEARCH_CMP_FN);
}

int
OBJ_obj2nid(const ASN1_OBJECT *a)
{
	const unsigned int *op;
	ADDED_OBJ ad, *adp;

	if (a == NULL)
		return (NID_undef);
	if (a->nid != 0)
		return (a->nid);

	if (added != NULL) {
		ad.type = ADDED_DATA;
		ad.obj=(ASN1_OBJECT *)a; /* XXX: ugly but harmless */
		adp = lh_ADDED_OBJ_retrieve(added, &ad);
		if (adp != NULL)
			return (adp->obj->nid);
	}
	op = OBJ_bsearch_obj(&a, obj_objs, NUM_OBJ);
	if (op == NULL)
		return (NID_undef);
	return (nid_objs[*op].nid);
}

/* Convert an object name into an ASN1_OBJECT
 * if "noname" is not set then search for short and long names first.
 * This will convert the "dotted" form into an object: unlike OBJ_txt2nid
 * it can be used with any objects, not just registered ones.
 */

ASN1_OBJECT *
OBJ_txt2obj(const char *s, int no_name)
{
	int nid = NID_undef;
	ASN1_OBJECT *op = NULL;
	unsigned char *buf;
	unsigned char *p;
	const unsigned char *cp;
	int i, j;

	if (!no_name) {
		if (((nid = OBJ_sn2nid(s)) != NID_undef) ||
		    ((nid = OBJ_ln2nid(s)) != NID_undef) )
			return OBJ_nid2obj(nid);
	}

	/* Work out size of content octets */
	i = a2d_ASN1_OBJECT(NULL, 0, s, -1);
	if (i <= 0) {
		/* Don't clear the error */
		/*ERR_clear_error();*/
		return NULL;
	}
	/* Work out total size */
	j = ASN1_object_size(0, i, V_ASN1_OBJECT);

	if ((buf = malloc(j)) == NULL)
		return NULL;

	p = buf;
	/* Write out tag+length */
	ASN1_put_object(&p, 0, i, V_ASN1_OBJECT, V_ASN1_UNIVERSAL);
	/* Write out contents */
	a2d_ASN1_OBJECT(p, i, s, -1);

	cp = buf;
	op = d2i_ASN1_OBJECT(NULL, &cp, j);
	free(buf);
	return op;
}

int
OBJ_obj2txt(char *buf, int buf_len, const ASN1_OBJECT *a, int no_name)
{
	int i, ret = 0, len, nid, first = 1, use_bn;
	BIGNUM *bl = NULL;
	unsigned long l;
	const unsigned char *p;

	/* Ensure that, at every state, |buf| is NUL-terminated. */
	if (buf_len > 0)
		buf[0] = '\0';

	if ((a == NULL) || (a->data == NULL))
		goto err;

	if (!no_name && (nid = OBJ_obj2nid(a)) != NID_undef) {
		const char *s;
		s = OBJ_nid2ln(nid);
		if (s == NULL)
			s = OBJ_nid2sn(nid);
		if (s) {
			ret = strlcpy(buf, s, buf_len);
			goto out;
		}
	}

	len = a->length;
	p = a->data;

	while (len > 0) {
		l = 0;
		use_bn = 0;
		for (;;) {
			unsigned char c = *p++;
			len--;
			if ((len == 0) && (c & 0x80))
				goto err;
			if (use_bn) {
				if (!BN_add_word(bl, c & 0x7f))
					goto err;
			} else
				l |= c & 0x7f;
			if (!(c & 0x80))
				break;
			if (!use_bn && (l > (ULONG_MAX >> 7L))) {
				if (!bl && !(bl = BN_new()))
					goto err;
				if (!BN_set_word(bl, l))
					goto err;
				use_bn = 1;
			}
			if (use_bn) {
				if (!BN_lshift(bl, bl, 7))
					goto err;
			} else
				l <<= 7L;
		}

		if (first) {
			first = 0;
			if (l >= 80) {
				i = 2;
				if (use_bn) {
					if (!BN_sub_word(bl, 80))
						goto err;
				} else
					l -= 80;
			} else {
				i = (int)(l / 40);
				l -= (long)(i * 40);
			}
			if (buf_len > 1) {
				*buf++ = i + '0';
				*buf = '\0';
				buf_len--;
			}
			ret++;
		}

		if (use_bn) {
			char *bndec;

			bndec = BN_bn2dec(bl);
			if (!bndec)
				goto err;
			i = snprintf(buf, buf_len, ".%s", bndec);
			free(bndec);
			if (i == -1)
				goto err;
			if (i >= buf_len) {
				buf_len = 0;
			} else {
				buf += i;
				buf_len -= i;
			}
			ret += i;
		} else {
			i = snprintf(buf, buf_len, ".%lu", l);
			if (i == -1)
				goto err;
			if (i >= buf_len) {
				buf_len = 0;
			} else {
				buf += i;
				buf_len -= i;
			}
			ret += i;
			l = 0;
		}
	}

out:
	BN_free(bl);
	return ret;

err:
	ret = 0;
	goto out;
}

int
OBJ_txt2nid(const char *s)
{
	ASN1_OBJECT *obj;
	int nid;

	obj = OBJ_txt2obj(s, 0);
	nid = OBJ_obj2nid(obj);
	ASN1_OBJECT_free(obj);
	return nid;
}

int
OBJ_ln2nid(const char *s)
{
	ASN1_OBJECT o;
	const ASN1_OBJECT *oo = &o;
	ADDED_OBJ ad, *adp;
	const unsigned int *op;

	o.ln = s;
	if (added != NULL) {
		ad.type = ADDED_LNAME;
		ad.obj = &o;
		adp = lh_ADDED_OBJ_retrieve(added, &ad);
		if (adp != NULL)
			return (adp->obj->nid);
	}
	op = OBJ_bsearch_ln(&oo, ln_objs, NUM_LN);
	if (op == NULL)
		return (NID_undef);
	return (nid_objs[*op].nid);
}

int
OBJ_sn2nid(const char *s)
{
	ASN1_OBJECT o;
	const ASN1_OBJECT *oo = &o;
	ADDED_OBJ ad, *adp;
	const unsigned int *op;

	o.sn = s;
	if (added != NULL) {
		ad.type = ADDED_SNAME;
		ad.obj = &o;
		adp = lh_ADDED_OBJ_retrieve(added, &ad);
		if (adp != NULL)
			return (adp->obj->nid);
	}
	op = OBJ_bsearch_sn(&oo, sn_objs, NUM_SN);
	if (op == NULL)
		return (NID_undef);
	return (nid_objs[*op].nid);
}

const void *
OBJ_bsearch_(const void *key, const void *base, int num, int size,
    int (*cmp)(const void *, const void *))
{
	return OBJ_bsearch_ex_(key, base, num, size, cmp, 0);
}

const void *
OBJ_bsearch_ex_(const void *key, const void *base_, int num, int size,
    int (*cmp)(const void *, const void *), int flags)
{
	const char *base = base_;
	int l, h, i = 0, c = 0;
	const char *p = NULL;

	if (num == 0)
		return (NULL);
	l = 0;
	h = num;
	while (l < h) {
		i = (l + h) / 2;
		p = &(base[i * size]);
		c = (*cmp)(key, p);
		if (c < 0)
			h = i;
		else if (c > 0)
			l = i + 1;
		else
			break;
	}
	if (c != 0 && !(flags & OBJ_BSEARCH_VALUE_ON_NOMATCH))
		p = NULL;
	else if (c == 0 && (flags & OBJ_BSEARCH_FIRST_VALUE_ON_MATCH)) {
		while (i > 0 && (*cmp)(key, &(base[(i - 1) * size])) == 0)
			i--;
		p = &(base[i * size]);
	}
	return (p);
}

int
OBJ_create_objects(BIO *in)
{
	char buf[512];
	int i, num = 0;
	char *o, *s, *l = NULL;

	for (;;) {
		s = o = NULL;
		i = BIO_gets(in, buf, 512);
		if (i <= 0)
			return (num);
		buf[i - 1] = '\0';
		if (!isalnum((unsigned char)buf[0]))
			return (num);
		o = s=buf;
		while (isdigit((unsigned char)*s) || (*s == '.'))
			s++;
		if (*s != '\0') {
			*(s++) = '\0';
			while (isspace((unsigned char)*s))
				s++;
			if (*s == '\0')
				s = NULL;
			else {
				l = s;
				while ((*l != '\0') &&
				    !isspace((unsigned char)*l))
					l++;
				if (*l != '\0') {
					*(l++) = '\0';
					while (isspace((unsigned char)*l))
						l++;
					if (*l == '\0')
						l = NULL;
				} else
					l = NULL;
			}
		} else
			s = NULL;
		if ((o == NULL) || (*o == '\0'))
			return (num);
		if (!OBJ_create(o, s, l))
			return (num);
		num++;
	}
	/* return(num); */
}

int
OBJ_create(const char *oid, const char *sn, const char *ln)
{
	int ok = 0;
	ASN1_OBJECT *op = NULL;
	unsigned char *buf;
	int i;

	i = a2d_ASN1_OBJECT(NULL, 0, oid, -1);
	if (i <= 0)
		return (0);

	if ((buf = malloc(i)) == NULL) {
		OBJerror(ERR_R_MALLOC_FAILURE);
		return (0);
	}
	i = a2d_ASN1_OBJECT(buf, i, oid, -1);
	if (i == 0)
		goto err;
	op = (ASN1_OBJECT *)ASN1_OBJECT_create(OBJ_new_nid(1), buf, i, sn, ln);
	if (op == NULL)
		goto err;
	ok = OBJ_add_object(op);

err:
	ASN1_OBJECT_free(op);
	free(buf);
	return (ok);
}
