/* $OpenBSD: x509_vpm.c,v 1.15 2016/12/21 15:15:45 jsing Exp $ */
/* Written by Dr Stephen N Henson (steve@openssl.org) for the OpenSSL
 * project 2004.
 */
/* ====================================================================
 * Copyright (c) 2004 The OpenSSL Project.  All rights reserved.
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
 *    for use in the OpenSSL Toolkit. (http://www.OpenSSL.org/)"
 *
 * 4. The names "OpenSSL Toolkit" and "OpenSSL Project" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For written permission, please contact
 *    licensing@OpenSSL.org.
 *
 * 5. Products derived from this software may not be called "OpenSSL"
 *    nor may "OpenSSL" appear in their names without prior written
 *    permission of the OpenSSL Project.
 *
 * 6. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by the OpenSSL Project
 *    for use in the OpenSSL Toolkit (http://www.OpenSSL.org/)"
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
 *
 * This product includes cryptographic software written by Eric Young
 * (eay@cryptsoft.com).  This product includes software written by Tim
 * Hudson (tjh@cryptsoft.com).
 *
 */

#include <stdio.h>
#include <string.h>

#include <openssl/buffer.h>
#include <openssl/crypto.h>
#include <openssl/lhash.h>
#include <openssl/stack.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include "vpm_int.h"

/* X509_VERIFY_PARAM functions */

int X509_VERIFY_PARAM_set1_email(X509_VERIFY_PARAM *param, const char *email,
    size_t emaillen);
int X509_VERIFY_PARAM_set1_ip(X509_VERIFY_PARAM *param, const unsigned char *ip,
    size_t iplen);

#define SET_HOST 0
#define ADD_HOST 1

static void
str_free(char *s)
{
    free(s);
}

#define string_stack_free(sk) sk_OPENSSL_STRING_pop_free(sk, str_free)


/*
 * Post 1.0.1 sk function "deep_copy".  For the moment we simply make
 * these take void * and use them directly without a glorious blob of
 * obfuscating macros of dubious value in front of them. All this in
 * preparation for a rototilling of safestack.h (likely inspired by
 * this).
 */
static void *
sk_deep_copy(void *sk_void, void *copy_func_void, void *free_func_void)
{
	_STACK *sk = sk_void;
	void *(*copy_func)(void *) = copy_func_void;
	void (*free_func)(void *) = free_func_void;
	_STACK *ret = sk_dup(sk);

	if (ret == NULL)
		return NULL;

	size_t i;
	for (i = 0; i < ret->num; i++) {
		if (ret->data[i] == NULL)
			continue;
		ret->data[i] = copy_func(ret->data[i]);
		if (ret->data[i] == NULL) {
			size_t j;
			for (j = 0; j < i; j++) {
				if (ret->data[j] != NULL)
					free_func(ret->data[j]);
			}
			sk_free(ret);
			return NULL;
		}
	}

	return ret;
}

static int
int_x509_param_set_hosts(X509_VERIFY_PARAM_ID *id, int mode,
    const char *name, size_t namelen)
{
	char *copy;

	/*
	 * Refuse names with embedded NUL bytes.
	 * XXX: Do we need to push an error onto the error stack?
	 */
	if (name && memchr(name, '\0', namelen))
		return 0;

	if (mode == SET_HOST && id->hosts) {
		string_stack_free(id->hosts);
		id->hosts = NULL;
	}
	if (name == NULL || namelen == 0)
		return 1;
	copy = strndup(name, namelen);
	if (copy == NULL)
		return 0;

	if (id->hosts == NULL &&
	    (id->hosts = sk_OPENSSL_STRING_new_null()) == NULL) {
		free(copy);
		return 0;
	}

	if (!sk_OPENSSL_STRING_push(id->hosts, copy)) {
		free(copy);
		if (sk_OPENSSL_STRING_num(id->hosts) == 0) {
			sk_OPENSSL_STRING_free(id->hosts);
			id->hosts = NULL;
		}
		return 0;
	}

	return 1;
}

static void
x509_verify_param_zero(X509_VERIFY_PARAM *param)
{
	X509_VERIFY_PARAM_ID *paramid;
	if (!param)
		return;
	param->name = NULL;
	param->purpose = 0;
	param->trust = 0;
	/*param->inh_flags = X509_VP_FLAG_DEFAULT;*/
	param->inh_flags = 0;
	param->flags = 0;
	param->depth = -1;
	if (param->policies) {
		sk_ASN1_OBJECT_pop_free(param->policies, ASN1_OBJECT_free);
		param->policies = NULL;
	}
	paramid = param->id;
	if (paramid->hosts) {
		string_stack_free(paramid->hosts);
		paramid->hosts = NULL;
	}
	free(paramid->peername);
	paramid->peername = NULL;
	free(paramid->email);
	paramid->email = NULL;
	paramid->emaillen = 0;
	free(paramid->ip);
	paramid->ip = NULL;
	paramid->iplen = 0;
}

X509_VERIFY_PARAM *
X509_VERIFY_PARAM_new(void)
{
	X509_VERIFY_PARAM *param;
	X509_VERIFY_PARAM_ID *paramid;
	param = calloc(1, sizeof(X509_VERIFY_PARAM));
	if (param == NULL)
		return NULL;
	paramid = calloc (1, sizeof(X509_VERIFY_PARAM_ID));
	if (paramid == NULL) {
		free(param);
		return NULL;
	}
	param->id = paramid;
	x509_verify_param_zero(param);
	return param;
}

void
X509_VERIFY_PARAM_free(X509_VERIFY_PARAM *param)
{
	if (param == NULL)
		return;
	x509_verify_param_zero(param);
	free(param->id);
	free(param);
}

/* This function determines how parameters are "inherited" from one structure
 * to another. There are several different ways this can happen.
 *
 * 1. If a child structure needs to have its values initialized from a parent
 *    they are simply copied across. For example SSL_CTX copied to SSL.
 * 2. If the structure should take on values only if they are currently unset.
 *    For example the values in an SSL structure will take appropriate value
 *    for SSL servers or clients but only if the application has not set new
 *    ones.
 *
 * The "inh_flags" field determines how this function behaves.
 *
 * Normally any values which are set in the default are not copied from the
 * destination and verify flags are ORed together.
 *
 * If X509_VP_FLAG_DEFAULT is set then anything set in the source is copied
 * to the destination. Effectively the values in "to" become default values
 * which will be used only if nothing new is set in "from".
 *
 * If X509_VP_FLAG_OVERWRITE is set then all value are copied across whether
 * they are set or not. Flags is still Ored though.
 *
 * If X509_VP_FLAG_RESET_FLAGS is set then the flags value is copied instead
 * of ORed.
 *
 * If X509_VP_FLAG_LOCKED is set then no values are copied.
 *
 * If X509_VP_FLAG_ONCE is set then the current inh_flags setting is zeroed
 * after the next call.
 */

/* Macro to test if a field should be copied from src to dest */

#define test_x509_verify_param_copy(field, def) \
	(to_overwrite || \
		((src->field != def) && (to_default || (dest->field == def))))

/* As above but for ID fields */

#define test_x509_verify_param_copy_id(idf, def) \
	test_x509_verify_param_copy(id->idf, def)

/* Macro to test and copy a field if necessary */

#define x509_verify_param_copy(field, def) \
	if (test_x509_verify_param_copy(field, def)) \
		dest->field = src->field

int
X509_VERIFY_PARAM_inherit(X509_VERIFY_PARAM *dest, const X509_VERIFY_PARAM *src)
{
	unsigned long inh_flags;
	int to_default, to_overwrite;
	X509_VERIFY_PARAM_ID *id;

	if (!src)
		return 1;
	id = src->id;
	inh_flags = dest->inh_flags | src->inh_flags;

	if (inh_flags & X509_VP_FLAG_ONCE)
		dest->inh_flags = 0;

	if (inh_flags & X509_VP_FLAG_LOCKED)
		return 1;

	if (inh_flags & X509_VP_FLAG_DEFAULT)
		to_default = 1;
	else
		to_default = 0;

	if (inh_flags & X509_VP_FLAG_OVERWRITE)
		to_overwrite = 1;
	else
		to_overwrite = 0;

	x509_verify_param_copy(purpose, 0);
	x509_verify_param_copy(trust, 0);
	x509_verify_param_copy(depth, -1);

	/* If overwrite or check time not set, copy across */

	if (to_overwrite || !(dest->flags & X509_V_FLAG_USE_CHECK_TIME)) {
		dest->check_time = src->check_time;
		dest->flags &= ~X509_V_FLAG_USE_CHECK_TIME;
		/* Don't need to copy flag: that is done below */
	}

	if (inh_flags & X509_VP_FLAG_RESET_FLAGS)
		dest->flags = 0;

	dest->flags |= src->flags;

	if (test_x509_verify_param_copy(policies, NULL)) {
		if (!X509_VERIFY_PARAM_set1_policies(dest, src->policies))
			return 0;
	}

	/* Copy the host flags if and only if we're copying the host list */
	if (test_x509_verify_param_copy_id(hosts, NULL)) {
		if (dest->id->hosts) {
			string_stack_free(dest->id->hosts);
			dest->id->hosts = NULL;
		}
		if (id->hosts) {
			dest->id->hosts =
			    sk_deep_copy(id->hosts, strdup, str_free);
			if (dest->id->hosts == NULL)
				return 0;
			dest->id->hostflags = id->hostflags;
		}
	}

	if (test_x509_verify_param_copy_id(email, NULL)) {
		if (!X509_VERIFY_PARAM_set1_email(dest, id->email,
		    id->emaillen))
			return 0;
	}

	if (test_x509_verify_param_copy_id(ip, NULL)) {
		if (!X509_VERIFY_PARAM_set1_ip(dest, id->ip, id->iplen))
			return 0;
	}

	return 1;
}

int
X509_VERIFY_PARAM_set1(X509_VERIFY_PARAM *to, const X509_VERIFY_PARAM *from)
{
	unsigned long save_flags = to->inh_flags;
	int ret;

	to->inh_flags |= X509_VP_FLAG_DEFAULT;
	ret = X509_VERIFY_PARAM_inherit(to, from);
	to->inh_flags = save_flags;
	return ret;
}

static int
int_x509_param_set1(char **pdest, size_t *pdestlen,  const char *src,
    size_t srclen)
{
	char *tmp;
	if (src) {
		if (srclen == 0) {
			if ((tmp = strdup(src)) == NULL)
				return 0;
			srclen = strlen(src);
		} else {
			if ((tmp = malloc(srclen)) == NULL)
				return 0;
			memcpy(tmp, src, srclen);
		}
	} else {
		tmp = NULL;
		srclen = 0;
	}
	if (*pdest)
		free(*pdest);
	*pdest = tmp;
	if (pdestlen)
		*pdestlen = srclen;
	return 1;
}

int
X509_VERIFY_PARAM_set1_name(X509_VERIFY_PARAM *param, const char *name)
{
	free(param->name);
	param->name = NULL;
	if (name == NULL)
		return 1;
	param->name = strdup(name);
	if (param->name)
		return 1;
	return 0;
}

int
X509_VERIFY_PARAM_set_flags(X509_VERIFY_PARAM *param, unsigned long flags)
{
	param->flags |= flags;
	if (flags & X509_V_FLAG_POLICY_MASK)
		param->flags |= X509_V_FLAG_POLICY_CHECK;
	return 1;
}

int
X509_VERIFY_PARAM_clear_flags(X509_VERIFY_PARAM *param, unsigned long flags)
{
	param->flags &= ~flags;
	return 1;
}

unsigned long
X509_VERIFY_PARAM_get_flags(X509_VERIFY_PARAM *param)
{
	return param->flags;
}

int
X509_VERIFY_PARAM_set_purpose(X509_VERIFY_PARAM *param, int purpose)
{
	return X509_PURPOSE_set(&param->purpose, purpose);
}

int
X509_VERIFY_PARAM_set_trust(X509_VERIFY_PARAM *param, int trust)
{
	return X509_TRUST_set(&param->trust, trust);
}

void
X509_VERIFY_PARAM_set_depth(X509_VERIFY_PARAM *param, int depth)
{
	param->depth = depth;
}

void
X509_VERIFY_PARAM_set_time(X509_VERIFY_PARAM *param, time_t t)
{
	param->check_time = t;
	param->flags |= X509_V_FLAG_USE_CHECK_TIME;
}

int
X509_VERIFY_PARAM_add0_policy(X509_VERIFY_PARAM *param, ASN1_OBJECT *policy)
{
	if (!param->policies) {
		param->policies = sk_ASN1_OBJECT_new_null();
		if (!param->policies)
			return 0;
	}
	if (!sk_ASN1_OBJECT_push(param->policies, policy))
		return 0;
	return 1;
}

int
X509_VERIFY_PARAM_set1_policies(X509_VERIFY_PARAM *param,
    STACK_OF(ASN1_OBJECT) *policies)
{
	int i;
	ASN1_OBJECT *oid, *doid;

	if (!param)
		return 0;
	if (param->policies)
		sk_ASN1_OBJECT_pop_free(param->policies, ASN1_OBJECT_free);

	if (!policies) {
		param->policies = NULL;
		return 1;
	}

	param->policies = sk_ASN1_OBJECT_new_null();
	if (!param->policies)
		return 0;

	for (i = 0; i < sk_ASN1_OBJECT_num(policies); i++) {
		oid = sk_ASN1_OBJECT_value(policies, i);
		doid = OBJ_dup(oid);
		if (!doid)
			return 0;
		if (!sk_ASN1_OBJECT_push(param->policies, doid)) {
			ASN1_OBJECT_free(doid);
			return 0;
		}
	}
	param->flags |= X509_V_FLAG_POLICY_CHECK;
	return 1;
}

int
X509_VERIFY_PARAM_set1_host(X509_VERIFY_PARAM *param,
    const char *name, size_t namelen)
{
	return int_x509_param_set_hosts(param->id, SET_HOST, name, namelen);
}

int
X509_VERIFY_PARAM_add1_host(X509_VERIFY_PARAM *param,
    const char *name, size_t namelen)
{
	return int_x509_param_set_hosts(param->id, ADD_HOST, name, namelen);
}

void
X509_VERIFY_PARAM_set_hostflags(X509_VERIFY_PARAM *param, unsigned int flags)
{
	param->id->hostflags = flags;
}

char *
X509_VERIFY_PARAM_get0_peername(X509_VERIFY_PARAM *param)
{
	return param->id->peername;
}

int
X509_VERIFY_PARAM_set1_email(X509_VERIFY_PARAM *param,  const char *email,
    size_t emaillen)
{
	return int_x509_param_set1(&param->id->email, &param->id->emaillen,
	    email, emaillen);
}

int
X509_VERIFY_PARAM_set1_ip(X509_VERIFY_PARAM *param, const unsigned char *ip,
    size_t iplen)
{
	if (iplen != 0 && iplen != 4 && iplen != 16)
		return 0;
	return int_x509_param_set1((char **)&param->id->ip, &param->id->iplen,
	    (char *)ip, iplen);
}

int
X509_VERIFY_PARAM_set1_ip_asc(X509_VERIFY_PARAM *param, const char *ipasc)
{
	unsigned char ipout[16];
	size_t iplen;

	iplen = (size_t)a2i_ipadd(ipout, ipasc);
	if (iplen == 0)
		return 0;
	return X509_VERIFY_PARAM_set1_ip(param, ipout, iplen);
}

int
X509_VERIFY_PARAM_get_depth(const X509_VERIFY_PARAM *param)
{
	return param->depth;
}

const char *
X509_VERIFY_PARAM_get0_name(const X509_VERIFY_PARAM *param)
{
	return param->name;
}

static const X509_VERIFY_PARAM_ID _empty_id = { NULL };

#define vpm_empty_id (X509_VERIFY_PARAM_ID *)&_empty_id

/*
 * Default verify parameters: these are used for various applications and can
 * be overridden by the user specified table.
 */

static const X509_VERIFY_PARAM default_table[] = {
	{
		.name = "default",
		.depth = 100,
		.trust = 0,  /* XXX This is not the default trust value */
		.id = vpm_empty_id
	},
	{
		.name = "pkcs7",
		.purpose = X509_PURPOSE_SMIME_SIGN,
		.trust = X509_TRUST_EMAIL,
		.depth = -1,
		.id = vpm_empty_id
	},
	{
		.name = "smime_sign",
		.purpose = X509_PURPOSE_SMIME_SIGN,
		.trust = X509_TRUST_EMAIL,
		.depth =  -1,
		.id = vpm_empty_id
	},
	{
		.name = "ssl_client",
		.purpose = X509_PURPOSE_SSL_CLIENT,
		.trust = X509_TRUST_SSL_CLIENT,
		.depth = -1,
		.id = vpm_empty_id
	},
	{
		.name = "ssl_server",
		.purpose = X509_PURPOSE_SSL_SERVER,
		.trust = X509_TRUST_SSL_SERVER,
		.depth = -1,
		.id = vpm_empty_id
	}
};

static STACK_OF(X509_VERIFY_PARAM) *param_table = NULL;

static int
param_cmp(const X509_VERIFY_PARAM * const *a,
    const X509_VERIFY_PARAM * const *b)
{
	return strcmp((*a)->name, (*b)->name);
}

int
X509_VERIFY_PARAM_add0_table(X509_VERIFY_PARAM *param)
{
	X509_VERIFY_PARAM *ptmp;
	if (!param_table) {
		param_table = sk_X509_VERIFY_PARAM_new(param_cmp);
		if (!param_table)
			return 0;
	} else {
		size_t idx;

		if ((idx = sk_X509_VERIFY_PARAM_find(param_table, param))
		    != -1) {
			ptmp = sk_X509_VERIFY_PARAM_value(param_table,
			    idx);
			X509_VERIFY_PARAM_free(ptmp);
			(void)sk_X509_VERIFY_PARAM_delete(param_table,
			    idx);
		}
	}
	if (!sk_X509_VERIFY_PARAM_push(param_table, param))
		return 0;
	return 1;
}

int
X509_VERIFY_PARAM_get_count(void)
{
	int num = sizeof(default_table) / sizeof(X509_VERIFY_PARAM);
	if (param_table)
		num += sk_X509_VERIFY_PARAM_num(param_table);
	return num;
}

const
X509_VERIFY_PARAM *X509_VERIFY_PARAM_get0(int id)
{
	int num = sizeof(default_table) / sizeof(X509_VERIFY_PARAM);
	if (id < num)
		return default_table + id;
	return sk_X509_VERIFY_PARAM_value(param_table, id - num);
}

const
X509_VERIFY_PARAM *X509_VERIFY_PARAM_lookup(const char *name)
{
	X509_VERIFY_PARAM pm;
	unsigned int i, limit;

	pm.name = (char *)name;
	if (param_table) {
		size_t idx;
		if ((idx = sk_X509_VERIFY_PARAM_find(param_table, &pm)) != -1)
			return sk_X509_VERIFY_PARAM_value(param_table, idx);
	}

	limit = sizeof(default_table) / sizeof(X509_VERIFY_PARAM);
	for (i = 0; i < limit; i++) {
		if (strcmp(default_table[i].name, name) == 0) {
			return &default_table[i];
		}
	}
	return NULL;
}

void
X509_VERIFY_PARAM_table_cleanup(void)
{
	if (param_table)
		sk_X509_VERIFY_PARAM_pop_free(param_table,
		    X509_VERIFY_PARAM_free);
	param_table = NULL;
}
