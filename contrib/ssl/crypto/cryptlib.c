/* $OpenBSD: cryptlib.c,v 1.41 2017/04/29 21:48:43 jsing Exp $ */
/* ====================================================================
 * Copyright (c) 1998-2006 The OpenSSL Project.  All rights reserved.
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
 *
 * This product includes cryptographic software written by Eric Young
 * (eay@cryptsoft.com).  This product includes software written by Tim
 * Hudson (tjh@cryptsoft.com).
 *
 */
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
/* ====================================================================
 * Copyright 2002 Sun Microsystems, Inc. ALL RIGHTS RESERVED.
 * ECDH support in OpenSSL originally developed by
 * SUN MICROSYSTEMS, INC., and contributed to the OpenSSL project.
 */

#include <limits.h>
#include <stdarg.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include <openssl/opensslconf.h>

#include <openssl/crypto.h>
#include <openssl/buffer.h>
#include <openssl/err.h>
#include <openssl/safestack.h>
#include <openssl/sha.h>

DECLARE_STACK_OF(CRYPTO_dynlock)

/* real #defines in crypto.h, keep these upto date */
static const char* const lock_names[CRYPTO_NUM_LOCKS] = {
	"<<ERROR>>",
	"err",
	"ex_data",
	"x509",
	"x509_info",
	"x509_pkey",
	"x509_crl",
	"x509_req",
	"dsa",
	"rsa",
	"evp_pkey",
	"x509_store",
	"ssl_ctx",
	"ssl_cert",
	"ssl_session",
	"ssl_sess_cert",
	"ssl",
	"ssl_method",
	"rand",
	"rand2",
	"debug_malloc",
	"BIO",
	"gethostbyname",
	"getservbyname",
	"readdir",
	"RSA_blinding",
	"dh",
	"debug_malloc2",
	"dso",
	"dynlock",
	"engine",
	"ui",
	"ecdsa",
	"ec",
	"ecdh",
	"bn",
	"ec_pre_comp",
	"store",
	"comp",
	"fips",
	"fips2",
#if CRYPTO_NUM_LOCKS != 41
# error "Inconsistency between crypto.h and cryptlib.c"
#endif
};

/* This is for applications to allocate new type names in the non-dynamic
   array of lock names.  These are numbered with positive numbers.  */
static STACK_OF(OPENSSL_STRING) *app_locks = NULL;

/* For applications that want a more dynamic way of handling threads, the
   following stack is used.  These are externally numbered with negative
   numbers.  */
static STACK_OF(CRYPTO_dynlock) *dyn_locks = NULL;

static void (*locking_callback)(int mode, int type,
    const char *file, int line) = 0;
static int (*add_lock_callback)(int *pointer, int amount,
    int type, const char *file, int line) = 0;
#ifndef OPENSSL_NO_DEPRECATED
static unsigned long (*id_callback)(void) = 0;
#endif
static void (*threadid_callback)(CRYPTO_THREADID *) = 0;
static struct CRYPTO_dynlock_value *(*dynlock_create_callback)(
    const char *file, int line) = 0;
static void (*dynlock_lock_callback)(int mode,
    struct CRYPTO_dynlock_value *l, const char *file, int line) = 0;
static void (*dynlock_destroy_callback)(struct CRYPTO_dynlock_value *l,
    const char *file, int line) = 0;

int
CRYPTO_get_new_lockid(char *name)
{
	char *str;
	int i;

	if ((app_locks == NULL) &&
	    ((app_locks = sk_OPENSSL_STRING_new_null()) == NULL)) {
		CRYPTOerror(ERR_R_MALLOC_FAILURE);
		return (0);
	}
	if (name == NULL || (str = strdup(name)) == NULL) {
		CRYPTOerror(ERR_R_MALLOC_FAILURE);
		return (0);
	}
	i = sk_OPENSSL_STRING_push(app_locks, str);
	if (!i)
		free(str);
	else
		i += CRYPTO_NUM_LOCKS; /* gap of one :-) */
	return (i);
}

int
CRYPTO_num_locks(void)
{
	return CRYPTO_NUM_LOCKS;
}

int
CRYPTO_get_new_dynlockid(void)
{
	int i = 0;
	CRYPTO_dynlock *pointer = NULL;

	if (dynlock_create_callback == NULL) {
		CRYPTOerror(CRYPTO_R_NO_DYNLOCK_CREATE_CALLBACK);
		return (0);
	}
	CRYPTO_w_lock(CRYPTO_LOCK_DYNLOCK);
	if ((dyn_locks == NULL) &&
	    ((dyn_locks = sk_CRYPTO_dynlock_new_null()) == NULL)) {
		CRYPTO_w_unlock(CRYPTO_LOCK_DYNLOCK);
		CRYPTOerror(ERR_R_MALLOC_FAILURE);
		return (0);
	}
	CRYPTO_w_unlock(CRYPTO_LOCK_DYNLOCK);

	pointer = malloc(sizeof(CRYPTO_dynlock));
	if (pointer == NULL) {
		CRYPTOerror(ERR_R_MALLOC_FAILURE);
		return (0);
	}
	pointer->references = 1;
	pointer->data = dynlock_create_callback(__FILE__, __LINE__);
	if (pointer->data == NULL) {
		free(pointer);
		CRYPTOerror(ERR_R_MALLOC_FAILURE);
		return (0);
	}

	CRYPTO_w_lock(CRYPTO_LOCK_DYNLOCK);
	/* First, try to find an existing empty slot */
	i = sk_CRYPTO_dynlock_find(dyn_locks, NULL);
	/* If there was none, push, thereby creating a new one */
	if (i == -1)
		/* Since sk_push() returns the number of items on the
		   stack, not the location of the pushed item, we need
		   to transform the returned number into a position,
		   by decreasing it.  */
		i = sk_CRYPTO_dynlock_push(dyn_locks, pointer) - 1;
	else
		/* If we found a place with a NULL pointer, put our pointer
		   in it.  */
		(void)sk_CRYPTO_dynlock_set(dyn_locks, i, pointer);
	CRYPTO_w_unlock(CRYPTO_LOCK_DYNLOCK);

	if (i == -1) {
		dynlock_destroy_callback(pointer->data, __FILE__, __LINE__);
		free(pointer);
	} else
		i += 1; /* to avoid 0 */
	return -i;
}

void
CRYPTO_destroy_dynlockid(int i)
{
	CRYPTO_dynlock *pointer = NULL;

	if (i)
		i = -i - 1;
	if (dynlock_destroy_callback == NULL)
		return;

	CRYPTO_w_lock(CRYPTO_LOCK_DYNLOCK);

	if (dyn_locks == NULL || i >= sk_CRYPTO_dynlock_num(dyn_locks)) {
		CRYPTO_w_unlock(CRYPTO_LOCK_DYNLOCK);
		return;
	}
	pointer = sk_CRYPTO_dynlock_value(dyn_locks, i);
	if (pointer != NULL) {
		--pointer->references;
		if (pointer->references <= 0) {
			(void)sk_CRYPTO_dynlock_set(dyn_locks, i, NULL);
		} else
			pointer = NULL;
	}
	CRYPTO_w_unlock(CRYPTO_LOCK_DYNLOCK);

	if (pointer) {
		dynlock_destroy_callback(pointer->data, __FILE__, __LINE__);
		free(pointer);
	}
}

struct CRYPTO_dynlock_value *
CRYPTO_get_dynlock_value(int i)
{
	CRYPTO_dynlock *pointer = NULL;

	if (i)
		i = -i - 1;

	CRYPTO_w_lock(CRYPTO_LOCK_DYNLOCK);

	if (dyn_locks != NULL && i < sk_CRYPTO_dynlock_num(dyn_locks))
		pointer = sk_CRYPTO_dynlock_value(dyn_locks, i);
	if (pointer)
		pointer->references++;

	CRYPTO_w_unlock(CRYPTO_LOCK_DYNLOCK);

	if (pointer)
		return pointer->data;
	return NULL;
}

struct CRYPTO_dynlock_value *
(*CRYPTO_get_dynlock_create_callback(void))(const char *file, int line)
{
	return (dynlock_create_callback);
}

void
(*CRYPTO_get_dynlock_lock_callback(void))(int mode,
    struct CRYPTO_dynlock_value *l, const char *file, int line)
{
	return (dynlock_lock_callback);
}

void
(*CRYPTO_get_dynlock_destroy_callback(void))(struct CRYPTO_dynlock_value *l,
    const char *file, int line)
{
	return (dynlock_destroy_callback);
}

void
CRYPTO_set_dynlock_create_callback(
    struct CRYPTO_dynlock_value *(*func)(const char *file, int line))
{
	dynlock_create_callback = func;
}

void
CRYPTO_set_dynlock_lock_callback(void (*func)(int mode,
    struct CRYPTO_dynlock_value *l, const char *file, int line))
{
	dynlock_lock_callback = func;
}

void
CRYPTO_set_dynlock_destroy_callback(
    void (*func)(struct CRYPTO_dynlock_value *l, const char *file, int line))
{
	dynlock_destroy_callback = func;
}

void
(*CRYPTO_get_locking_callback(void))(int mode, int type, const char *file,
    int line)
{
	return (locking_callback);
}

int
(*CRYPTO_get_add_lock_callback(void))(int *num, int mount, int type,
    const char *file, int line)
{
	return (add_lock_callback);
}

void
CRYPTO_set_locking_callback(void (*func)(int mode, int type,
    const char *file, int line))
{
	/* Calling this here ensures initialisation before any threads
	 * are started.
	 */
	locking_callback = func;
}

void
CRYPTO_set_add_lock_callback(int (*func)(int *num, int mount, int type,
    const char *file, int line))
{
	add_lock_callback = func;
}

/* the memset() here and in set_pointer() seem overkill, but for the sake of
 * CRYPTO_THREADID_cmp() this avoids any platform silliness that might cause two
 * "equal" THREADID structs to not be memcmp()-identical. */
void
CRYPTO_THREADID_set_numeric(CRYPTO_THREADID *id, unsigned long val)
{
	memset(id, 0, sizeof(*id));
	id->val = val;
}

void
CRYPTO_THREADID_set_pointer(CRYPTO_THREADID *id, void *ptr)
{
	memset(id, 0, sizeof(*id));
	id->ptr = ptr;
#if ULONG_MAX >= UINTPTR_MAX
	/*s u 'ptr' can be embedded in 'val' without loss of uniqueness */
	id->val = (uintptr_t)id->ptr;
#else
	{
		SHA256_CTX ctx;
		uint8_t results[SHA256_DIGEST_LENGTH];

		SHA256_Init(&ctx);
		SHA256_Update(&ctx, (char *)(&id->ptr), sizeof(id->ptr));
		SHA256_Final(results, &ctx);
		memcpy(&id->val, results, sizeof(id->val));
	}
#endif
}

int
CRYPTO_THREADID_set_callback(void (*func)(CRYPTO_THREADID *))
{
	if (threadid_callback)
		return 0;
	threadid_callback = func;
	return 1;
}

void (*CRYPTO_THREADID_get_callback(void))(CRYPTO_THREADID *)
{
	return threadid_callback;
}

void
CRYPTO_THREADID_current(CRYPTO_THREADID *id)
{
	if (threadid_callback) {
		threadid_callback(id);
		return;
	}
#ifndef OPENSSL_NO_DEPRECATED
	/* If the deprecated callback was set, fall back to that */
	if (id_callback) {
		CRYPTO_THREADID_set_numeric(id, id_callback());
		return;
	}
#endif
	/* Else pick a backup */
	/* For everything else, default to using the address of 'errno' */
	CRYPTO_THREADID_set_pointer(id, (void*)&errno);
}

int
CRYPTO_THREADID_cmp(const CRYPTO_THREADID *a, const CRYPTO_THREADID *b)
{
	return memcmp(a, b, sizeof(*a));
}

void
CRYPTO_THREADID_cpy(CRYPTO_THREADID *dest, const CRYPTO_THREADID *src)
{
	memcpy(dest, src, sizeof(*src));
}

unsigned long
CRYPTO_THREADID_hash(const CRYPTO_THREADID *id)
{
	return id->val;
}

#ifndef OPENSSL_NO_DEPRECATED
unsigned long (*CRYPTO_get_id_callback(void))(void)
{
	return (id_callback);
}

void
CRYPTO_set_id_callback(unsigned long (*func)(void))
{
	id_callback = func;
}

unsigned long
CRYPTO_thread_id(void)
{
	unsigned long ret = 0;

	if (id_callback == NULL) {
		ret = (unsigned long)getpid();
	} else
		ret = id_callback();
	return (ret);
}
#endif

void
CRYPTO_lock(int mode, int type, const char *file, int line)
{
#ifdef LOCK_DEBUG
	{
		CRYPTO_THREADID id;
		char *rw_text, *operation_text;

		if (mode & CRYPTO_LOCK)
			operation_text = "lock  ";
		else if (mode & CRYPTO_UNLOCK)
			operation_text = "unlock";
		else
			operation_text = "ERROR ";

		if (mode & CRYPTO_READ)
			rw_text = "r";
		else if (mode & CRYPTO_WRITE)
			rw_text = "w";
		else
			rw_text = "ERROR";

		CRYPTO_THREADID_current(&id);
		fprintf(stderr, "lock:%08lx:(%s)%s %-18s %s:%d\n",
		    CRYPTO_THREADID_hash(&id), rw_text, operation_text,
		    CRYPTO_get_lock_name(type), file, line);
	}
#endif
	if (type < 0) {
		if (dynlock_lock_callback != NULL) {
			struct CRYPTO_dynlock_value *pointer =
			    CRYPTO_get_dynlock_value(type);

			OPENSSL_assert(pointer != NULL);

			dynlock_lock_callback(mode, pointer, file, line);

			CRYPTO_destroy_dynlockid(type);
		}
	} else if (locking_callback != NULL)
		locking_callback(mode, type, file, line);
}

int
CRYPTO_add_lock(int *pointer, int amount, int type, const char *file,
    int line)
{
	int ret = 0;

	if (add_lock_callback != NULL) {
#ifdef LOCK_DEBUG
		int before= *pointer;
#endif

		ret = add_lock_callback(pointer, amount, type, file, line);
#ifdef LOCK_DEBUG
		{
			CRYPTO_THREADID id;
			CRYPTO_THREADID_current(&id);
			fprintf(stderr, "ladd:%08lx:%2d+%2d->%2d %-18s %s:%d\n",
			    CRYPTO_THREADID_hash(&id), before, amount, ret,
			    CRYPTO_get_lock_name(type),
			    file, line);
		}
#endif
	} else {
		CRYPTO_lock(CRYPTO_LOCK|CRYPTO_WRITE, type, file, line);

		ret= *pointer + amount;
#ifdef LOCK_DEBUG
		{
			CRYPTO_THREADID id;
			CRYPTO_THREADID_current(&id);
			fprintf(stderr, "ladd:%08lx:%2d+%2d->%2d %-18s %s:%d\n",
			    CRYPTO_THREADID_hash(&id), *pointer, amount, ret,
			    CRYPTO_get_lock_name(type), file, line);
		}
#endif
		*pointer = ret;
		CRYPTO_lock(CRYPTO_UNLOCK|CRYPTO_WRITE, type, file, line);
	}
	return (ret);
}

const char *
CRYPTO_get_lock_name(int type)
{
	if (type < 0)
		return("dynamic");
	else if (type < CRYPTO_NUM_LOCKS)
		return (lock_names[type]);
	else if (type - CRYPTO_NUM_LOCKS > sk_OPENSSL_STRING_num(app_locks))
		return("ERROR");
	else
		return (sk_OPENSSL_STRING_value(app_locks,
		    type - CRYPTO_NUM_LOCKS));
}

#if	defined(__i386)   || defined(__i386__)   || defined(_M_IX86) || \
	defined(__INTEL__) || \
	defined(__x86_64) || defined(__x86_64__) || defined(_M_AMD64) || defined(_M_X64)

uint64_t OPENSSL_ia32cap_P;

uint64_t
OPENSSL_cpu_caps(void)
{
	return OPENSSL_ia32cap_P;
}

#if defined(OPENSSL_CPUID_OBJ) && !defined(OPENSSL_NO_ASM)
#define OPENSSL_CPUID_SETUP
void
OPENSSL_cpuid_setup(void)
{
	static int trigger = 0;
	uint64_t OPENSSL_ia32_cpuid(void);

	if (trigger)
		return;
	trigger = 1;
	OPENSSL_ia32cap_P = OPENSSL_ia32_cpuid();
}
#endif

#else
uint64_t
OPENSSL_cpu_caps(void)
{
	return 0;
}
#endif

#if !defined(OPENSSL_CPUID_SETUP) && !defined(OPENSSL_CPUID_OBJ)
void
OPENSSL_cpuid_setup(void)
{
}
#endif

static void
OPENSSL_showfatal(const char *fmta, ...)
{
	va_list ap;

	va_start(ap, fmta);
	vfprintf(stderr, fmta, ap);
	va_end(ap);
}

void
OpenSSLDie(const char *file, int line, const char *assertion)
{
	OPENSSL_showfatal(
	    "%s(%d): OpenSSL internal error, assertion failed: %s\n",
	    file, line, assertion);
	abort();
}

int
CRYPTO_memcmp(const void *in_a, const void *in_b, size_t len)
{
	size_t i;
	const unsigned char *a = in_a;
	const unsigned char *b = in_b;
	unsigned char x = 0;

	for (i = 0; i < len; i++)
		x |= a[i] ^ b[i];

	return x;
}
