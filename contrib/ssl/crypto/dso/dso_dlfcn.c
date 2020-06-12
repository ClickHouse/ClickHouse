/* $OpenBSD: dso_dlfcn.c,v 1.29 2017/01/29 17:49:23 beck Exp $ */
/* Written by Geoff Thorpe (geoff@geoffthorpe.net) for the OpenSSL
 * project 2000.
 */
/* ====================================================================
 * Copyright (c) 2000 The OpenSSL Project.  All rights reserved.
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

#include <openssl/dso.h>
#include <openssl/err.h>

#ifndef DSO_DLFCN
DSO_METHOD *
DSO_METHOD_dlfcn(void)
{
	return NULL;
}
#else

#ifdef HAVE_DLFCN_H
# include <dlfcn.h>
# define HAVE_DLINFO 1
#endif

/* Part of the hack in "dlfcn_load" ... */
#define DSO_MAX_TRANSLATED_SIZE 256

static int dlfcn_load(DSO *dso);
static int dlfcn_unload(DSO *dso);
static void *dlfcn_bind_var(DSO *dso, const char *symname);
static DSO_FUNC_TYPE dlfcn_bind_func(DSO *dso, const char *symname);
static char *dlfcn_name_converter(DSO *dso, const char *filename);
static char *dlfcn_merger(DSO *dso, const char *filespec1,
    const char *filespec2);
static int dlfcn_pathbyaddr(void *addr, char *path, int sz);
static void *dlfcn_globallookup(const char *name);

static DSO_METHOD dso_meth_dlfcn = {
	.name = "OpenSSL 'dlfcn' shared library method",
	.dso_load = dlfcn_load,
	.dso_unload = dlfcn_unload,
	.dso_bind_var = dlfcn_bind_var,
	.dso_bind_func = dlfcn_bind_func,
	.dso_name_converter = dlfcn_name_converter,
	.dso_merger = dlfcn_merger,
	.pathbyaddr = dlfcn_pathbyaddr,
	.globallookup = dlfcn_globallookup
};

DSO_METHOD *
DSO_METHOD_dlfcn(void)
{
	return (&dso_meth_dlfcn);
}

/* For this DSO_METHOD, our meth_data STACK will contain;
 * (i) the handle (void*) returned from dlopen().
 */

static int
dlfcn_load(DSO *dso)
{
	void *ptr = NULL;
	/* See applicable comments in dso_dl.c */
	char *filename = DSO_convert_filename(dso, NULL);
	int flags = RTLD_LAZY;

	if (filename == NULL) {
		DSOerror(DSO_R_NO_FILENAME);
		goto err;
	}

	if (dso->flags & DSO_FLAG_GLOBAL_SYMBOLS)
		flags |= RTLD_GLOBAL;
	ptr = dlopen(filename, flags);
	if (ptr == NULL) {
		DSOerror(DSO_R_LOAD_FAILED);
		ERR_asprintf_error_data("filename(%s): %s", filename,
		    dlerror());
		goto err;
	}
	if (!sk_void_push(dso->meth_data, (char *)ptr)) {
		DSOerror(DSO_R_STACK_ERROR);
		goto err;
	}
	/* Success */
	dso->loaded_filename = filename;
	return (1);

err:
	/* Cleanup! */
	free(filename);
	if (ptr != NULL)
		dlclose(ptr);
	return (0);
}

static int
dlfcn_unload(DSO *dso)
{
	void *ptr;
	if (dso == NULL) {
		DSOerror(ERR_R_PASSED_NULL_PARAMETER);
		return (0);
	}
	if (sk_void_num(dso->meth_data) < 1)
		return (1);
	ptr = sk_void_pop(dso->meth_data);
	if (ptr == NULL) {
		DSOerror(DSO_R_NULL_HANDLE);
		/* Should push the value back onto the stack in
		 * case of a retry. */
		sk_void_push(dso->meth_data, ptr);
		return (0);
	}
	/* For now I'm not aware of any errors associated with dlclose() */
	dlclose(ptr);
	return (1);
}

static void *
dlfcn_bind_var(DSO *dso, const char *symname)
{
	void *ptr, *sym;

	if ((dso == NULL) || (symname == NULL)) {
		DSOerror(ERR_R_PASSED_NULL_PARAMETER);
		return (NULL);
	}
	if (sk_void_num(dso->meth_data) < 1) {
		DSOerror(DSO_R_STACK_ERROR);
		return (NULL);
	}
	ptr = sk_void_value(dso->meth_data, sk_void_num(dso->meth_data) - 1);
	if (ptr == NULL) {
		DSOerror(DSO_R_NULL_HANDLE);
		return (NULL);
	}
	sym = dlsym(ptr, symname);
	if (sym == NULL) {
		DSOerror(DSO_R_SYM_FAILURE);
		ERR_asprintf_error_data("symname(%s): %s", symname, dlerror());
		return (NULL);
	}
	return (sym);
}

static DSO_FUNC_TYPE
dlfcn_bind_func(DSO *dso, const char *symname)
{
	void *ptr;
	union {
		DSO_FUNC_TYPE sym;
		void *dlret;
	} u;

	if ((dso == NULL) || (symname == NULL)) {
		DSOerror(ERR_R_PASSED_NULL_PARAMETER);
		return (NULL);
	}
	if (sk_void_num(dso->meth_data) < 1) {
		DSOerror(DSO_R_STACK_ERROR);
		return (NULL);
	}
	ptr = sk_void_value(dso->meth_data, sk_void_num(dso->meth_data) - 1);
	if (ptr == NULL) {
		DSOerror(DSO_R_NULL_HANDLE);
		return (NULL);
	}
	u.dlret = dlsym(ptr, symname);
	if (u.dlret == NULL) {
		DSOerror(DSO_R_SYM_FAILURE);
		ERR_asprintf_error_data("symname(%s): %s", symname, dlerror());
		return (NULL);
	}
	return u.sym;
}

static char *
dlfcn_merger(DSO *dso, const char *filespec1, const char *filespec2)
{
	char *merged;

	if (!filespec1 && !filespec2) {
		DSOerror(ERR_R_PASSED_NULL_PARAMETER);
		return (NULL);
	}
	/* If the first file specification is a rooted path, it rules.
	   same goes if the second file specification is missing. */
	if (!filespec2 || (filespec1 != NULL && filespec1[0] == '/')) {
		merged = strdup(filespec1);
		if (!merged) {
			DSOerror(ERR_R_MALLOC_FAILURE);
			return (NULL);
		}
	}
	/* If the first file specification is missing, the second one rules. */
	else if (!filespec1) {
		merged = strdup(filespec2);
		if (!merged) {
			DSOerror(ERR_R_MALLOC_FAILURE);
			return (NULL);
		}
	} else
		/* This part isn't as trivial as it looks.  It assumes that
		   the second file specification really is a directory, and
		   makes no checks whatsoever.  Therefore, the result becomes
		   the concatenation of filespec2 followed by a slash followed
		   by filespec1. */
	{
		size_t spec2len, len;

		spec2len = strlen(filespec2);
		len = spec2len + (filespec1 ? strlen(filespec1) : 0);

		if (filespec2 && filespec2[spec2len - 1] == '/') {
			spec2len--;
			len--;
		}
		merged = malloc(len + 2);
		if (!merged) {
			DSOerror(ERR_R_MALLOC_FAILURE);
			return (NULL);
		}
		strlcpy(merged, filespec2, len + 2);
		merged[spec2len] = '/';
		strlcpy(&merged[spec2len + 1], filespec1, len + 1 - spec2len);
	}
	return (merged);
}

#define DSO_ext	".so"
#define DSO_extlen 3

static char *
dlfcn_name_converter(DSO *dso, const char *filename)
{
	char *translated;
	int ret;

	if (strchr(filename, '/') == NULL) {
		/* Bare name, so convert to "%s.so" or "lib%s.so" */
		if ((DSO_flags(dso) & DSO_FLAG_NAME_TRANSLATION_EXT_ONLY) == 0)
			ret = asprintf(&translated, "lib%s" DSO_ext, filename);
		else
			ret = asprintf(&translated, "%s" DSO_ext, filename);
		if (ret == -1)
			translated = NULL;
	} else {
		/* Full path, so just duplicate it */
		translated = strdup(filename);
	}

	if (translated == NULL)
		DSOerror(DSO_R_NAME_TRANSLATION_FAILED);
	return (translated);
}

static int
dlfcn_pathbyaddr(void *addr, char *path, int sz)
{
	Dl_info dli;
	int len;

	if (addr == NULL) {
		union{
			int(*f)(void*, char*, int);
			void *p;
		} t = { dlfcn_pathbyaddr };
		addr = t.p;
	}

	if (dladdr(addr, &dli)) {
		len = (int)strlen(dli.dli_fname);
		if (sz <= 0)
			return len + 1;
		if (len >= sz)
			len = sz - 1;
		memcpy(path, dli.dli_fname, len);
		path[len++] = 0;
		return len;
	}

	ERR_asprintf_error_data("dlfcn_pathbyaddr(): %s", dlerror());
	return -1;
}

static void *
dlfcn_globallookup(const char *name)
{
	void *ret = NULL, *handle = dlopen(NULL, RTLD_LAZY);

	if (handle) {
		ret = dlsym(handle, name);
		dlclose(handle);
	}

	return ret;
}
#endif /* DSO_DLFCN */
