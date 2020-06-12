/* $OpenBSD: conf_mod.c,v 1.27 2017/01/29 17:49:22 beck Exp $ */
/* Written by Stephen Henson (steve@openssl.org) for the OpenSSL
 * project 2001.
 */
/* ====================================================================
 * Copyright (c) 2001 The OpenSSL Project.  All rights reserved.
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

#include <ctype.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <openssl/conf.h>
#include <openssl/crypto.h>
#include <openssl/dso.h>
#include <openssl/err.h>
#include <openssl/x509.h>

#define DSO_mod_init_name "OPENSSL_init"
#define DSO_mod_finish_name "OPENSSL_finish"

/* This structure contains a data about supported modules.
 * entries in this table correspond to either dynamic or
 * static modules.
 */

struct conf_module_st {
	/* DSO of this module or NULL if static */
	DSO *dso;
	/* Name of the module */
	char *name;
	/* Init function */
	conf_init_func *init;
	/* Finish function */
	conf_finish_func *finish;
	/* Number of successfully initialized modules */
	int links;
	void *usr_data;
};


/* This structure contains information about modules that have been
 * successfully initialized. There may be more than one entry for a
 * given module.
 */

struct conf_imodule_st {
	CONF_MODULE *pmod;
	char *name;
	char *value;
	unsigned long flags;
	void *usr_data;
};

static STACK_OF(CONF_MODULE) *supported_modules = NULL;
static STACK_OF(CONF_IMODULE) *initialized_modules = NULL;

static void module_free(CONF_MODULE *md);
static void module_finish(CONF_IMODULE *imod);
static int module_run(const CONF *cnf, char *name, char *value,
    unsigned long flags);
static CONF_MODULE *module_add(DSO *dso, const char *name,
    conf_init_func *ifunc, conf_finish_func *ffunc);
static CONF_MODULE *module_find(char *name);
static int module_init(CONF_MODULE *pmod, char *name, char *value,
    const CONF *cnf);
static CONF_MODULE *module_load_dso(const CONF *cnf, char *name, char *value,
    unsigned long flags);

/* Main function: load modules from a CONF structure */

int
CONF_modules_load(const CONF *cnf, const char *appname, unsigned long flags)
{
	STACK_OF(CONF_VALUE) *values;
	CONF_VALUE *vl;
	char *vsection = NULL;

	int ret, i;

	if (!cnf)
		return 1;

	if (appname)
		vsection = NCONF_get_string(cnf, NULL, appname);

	if (!appname || (!vsection && (flags & CONF_MFLAGS_DEFAULT_SECTION)))
		vsection = NCONF_get_string(cnf, NULL, "openssl_conf");

	if (!vsection) {
		ERR_clear_error();
		return 1;
	}

	values = NCONF_get_section(cnf, vsection);

	if (!values)
		return 0;

	for (i = 0; i < sk_CONF_VALUE_num(values); i++) {
		vl = sk_CONF_VALUE_value(values, i);
		ret = module_run(cnf, vl->name, vl->value, flags);
		if (ret <= 0)
			if (!(flags & CONF_MFLAGS_IGNORE_ERRORS))
				return ret;
	}

	return 1;
}

int
CONF_modules_load_file(const char *filename, const char *appname,
    unsigned long flags)
{
	char *file = NULL;
	CONF *conf = NULL;
	int ret = 0;
	conf = NCONF_new(NULL);
	if (!conf)
		goto err;

	if (filename == NULL) {
		file = CONF_get1_default_config_file();
		if (!file)
			goto err;
	} else
		file = (char *)filename;

	if (NCONF_load(conf, file, NULL) <= 0) {
		if ((flags & CONF_MFLAGS_IGNORE_MISSING_FILE) &&
		    (ERR_GET_REASON(ERR_peek_last_error()) ==
		    CONF_R_NO_SUCH_FILE)) {
			ERR_clear_error();
			ret = 1;
		}
		goto err;
	}

	ret = CONF_modules_load(conf, appname, flags);

err:
	if (filename == NULL)
		free(file);
	NCONF_free(conf);

	return ret;
}

static int
module_run(const CONF *cnf, char *name, char *value, unsigned long flags)
{
	CONF_MODULE *md;
	int ret;

	md = module_find(name);

	/* Module not found: try to load DSO */
	if (!md && !(flags & CONF_MFLAGS_NO_DSO))
		md = module_load_dso(cnf, name, value, flags);

	if (!md) {
		if (!(flags & CONF_MFLAGS_SILENT)) {
			CONFerror(CONF_R_UNKNOWN_MODULE_NAME);
			ERR_asprintf_error_data("module=%s", name);
		}
		return -1;
	}

	ret = module_init(md, name, value, cnf);

	if (ret <= 0) {
		if (!(flags & CONF_MFLAGS_SILENT)) {
			CONFerror(CONF_R_MODULE_INITIALIZATION_ERROR);
			ERR_asprintf_error_data
			    ("module=%s, value=%s, retcode=%-8d",
			    name, value, ret);
		}
	}

	return ret;
}

/* Load a module from a DSO */
static CONF_MODULE *
module_load_dso(const CONF *cnf, char *name, char *value, unsigned long flags)
{
	DSO *dso = NULL;
	conf_init_func *ifunc;
	conf_finish_func *ffunc;
	char *path = NULL;
	int errcode = 0;
	CONF_MODULE *md;

	/* Look for alternative path in module section */
	path = NCONF_get_string(cnf, value, "path");
	if (!path) {
		ERR_clear_error();
		path = name;
	}
	dso = DSO_load(NULL, path, NULL, 0);
	if (!dso) {
		errcode = CONF_R_ERROR_LOADING_DSO;
		goto err;
	}
	ifunc = (conf_init_func *)DSO_bind_func(dso, DSO_mod_init_name);
	if (!ifunc) {
		errcode = CONF_R_MISSING_INIT_FUNCTION;
		goto err;
	}
	ffunc = (conf_finish_func *)DSO_bind_func(dso, DSO_mod_finish_name);
	/* All OK, add module */
	md = module_add(dso, name, ifunc, ffunc);

	if (!md)
		goto err;

	return md;

err:
	if (dso)
		DSO_free(dso);
	CONFerror(errcode);
	ERR_asprintf_error_data("module=%s, path=%s", name, path);
	return NULL;
}

/* add module to list */
static CONF_MODULE *
module_add(DSO *dso, const char *name, conf_init_func *ifunc,
    conf_finish_func *ffunc)
{
	CONF_MODULE *tmod = NULL;

	if (name == NULL)
		return NULL;
	if (supported_modules == NULL)
		supported_modules = sk_CONF_MODULE_new_null();
	if (supported_modules == NULL)
		return NULL;
	tmod = malloc(sizeof(CONF_MODULE));
	if (tmod == NULL)
		return NULL;

	tmod->dso = dso;
	tmod->name = strdup(name);
	tmod->init = ifunc;
	tmod->finish = ffunc;
	tmod->links = 0;

	if (!sk_CONF_MODULE_push(supported_modules, tmod)) {
		free(tmod);
		return NULL;
	}

	return tmod;
}

/* Find a module from the list. We allow module names of the
 * form modname.XXXX to just search for modname to allow the
 * same module to be initialized more than once.
 */

static CONF_MODULE *
module_find(char *name)
{
	CONF_MODULE *tmod;
	int i, nchar;
	char *p;

	p = strrchr(name, '.');

	if (p)
		nchar = p - name;
	else
		nchar = strlen(name);

	for (i = 0; i < sk_CONF_MODULE_num(supported_modules); i++) {
		tmod = sk_CONF_MODULE_value(supported_modules, i);
		if (!strncmp(tmod->name, name, nchar))
			return tmod;
	}

	return NULL;
}

/* initialize a module */
static int
module_init(CONF_MODULE *pmod, char *name, char *value, const CONF *cnf)
{
	int ret = 1;
	int init_called = 0;
	CONF_IMODULE *imod = NULL;

	/* Otherwise add initialized module to list */
	imod = malloc(sizeof(CONF_IMODULE));
	if (!imod)
		goto err;

	imod->pmod = pmod;
	imod->name = name ? strdup(name) : NULL;
	imod->value = value ? strdup(value) : NULL;
	imod->usr_data = NULL;

	if (!imod->name || !imod->value)
		goto memerr;

	/* Try to initialize module */
	if (pmod->init) {
		ret = pmod->init(imod, cnf);
		init_called = 1;
		/* Error occurred, exit */
		if (ret <= 0)
			goto err;
	}

	if (initialized_modules == NULL) {
		initialized_modules = sk_CONF_IMODULE_new_null();
		if (!initialized_modules) {
			CONFerror(ERR_R_MALLOC_FAILURE);
			goto err;
		}
	}

	if (!sk_CONF_IMODULE_push(initialized_modules, imod)) {
		CONFerror(ERR_R_MALLOC_FAILURE);
		goto err;
	}

	pmod->links++;

	return ret;

err:
	/* We've started the module so we'd better finish it */
	if (pmod->finish && init_called)
		pmod->finish(imod);

memerr:
	if (imod) {
		free(imod->name);
		free(imod->value);
		free(imod);
	}

	return -1;
}

/* Unload any dynamic modules that have a link count of zero:
 * i.e. have no active initialized modules. If 'all' is set
 * then all modules are unloaded including static ones.
 */

void
CONF_modules_unload(int all)
{
	int i;
	CONF_MODULE *md;

	CONF_modules_finish();

	/* unload modules in reverse order */
	for (i = sk_CONF_MODULE_num(supported_modules) - 1; i >= 0; i--) {
		md = sk_CONF_MODULE_value(supported_modules, i);
		/* If static or in use and 'all' not set ignore it */
		if (((md->links > 0) || !md->dso) && !all)
			continue;
		/* Since we're working in reverse this is OK */
		(void)sk_CONF_MODULE_delete(supported_modules, i);
		module_free(md);
	}
	if (sk_CONF_MODULE_num(supported_modules) == 0) {
		sk_CONF_MODULE_free(supported_modules);
		supported_modules = NULL;
	}
}

/* unload a single module */
static void
module_free(CONF_MODULE *md)
{
	if (md->dso)
		DSO_free(md->dso);
	free(md->name);
	free(md);
}

/* finish and free up all modules instances */

void
CONF_modules_finish(void)
{
	CONF_IMODULE *imod;

	while (sk_CONF_IMODULE_num(initialized_modules) > 0) {
		imod = sk_CONF_IMODULE_pop(initialized_modules);
		module_finish(imod);
	}
	sk_CONF_IMODULE_free(initialized_modules);
	initialized_modules = NULL;
}

/* finish a module instance */

static void
module_finish(CONF_IMODULE *imod)
{
	if (imod->pmod->finish)
		imod->pmod->finish(imod);
	imod->pmod->links--;
	free(imod->name);
	free(imod->value);
	free(imod);
}

/* Add a static module to OpenSSL */

int
CONF_module_add(const char *name, conf_init_func *ifunc,
    conf_finish_func *ffunc)
{
	if (module_add(NULL, name, ifunc, ffunc))
		return 1;
	else
		return 0;
}

void
CONF_modules_free(void)
{
	CONF_modules_finish();
	CONF_modules_unload(1);
}

/* Utility functions */

const char *
CONF_imodule_get_name(const CONF_IMODULE *md)
{
	return md->name;
}

const char *
CONF_imodule_get_value(const CONF_IMODULE *md)
{
	return md->value;
}

void *
CONF_imodule_get_usr_data(const CONF_IMODULE *md)
{
	return md->usr_data;
}

void
CONF_imodule_set_usr_data(CONF_IMODULE *md, void *usr_data)
{
	md->usr_data = usr_data;
}

CONF_MODULE *
CONF_imodule_get_module(const CONF_IMODULE *md)
{
	return md->pmod;
}

unsigned long
CONF_imodule_get_flags(const CONF_IMODULE *md)
{
	return md->flags;
}

void
CONF_imodule_set_flags(CONF_IMODULE *md, unsigned long flags)
{
	md->flags = flags;
}

void *
CONF_module_get_usr_data(CONF_MODULE *pmod)
{
	return pmod->usr_data;
}

void
CONF_module_set_usr_data(CONF_MODULE *pmod, void *usr_data)
{
	pmod->usr_data = usr_data;
}

/* Return default config file name */

char *
CONF_get1_default_config_file(void)
{
	char *file = NULL;

	if (asprintf(&file, "%s/openssl.cnf",
	    X509_get_default_cert_area()) == -1)
		return (NULL);
	return file;
}

/* This function takes a list separated by 'sep' and calls the
 * callback function giving the start and length of each member
 * optionally stripping leading and trailing whitespace. This can
 * be used to parse comma separated lists for example.
 */

int
CONF_parse_list(const char *list_, int sep, int nospc,
    int (*list_cb)(const char *elem, int len, void *usr), void *arg)
{
	int ret;
	const char *lstart, *tmpend, *p;

	if (list_ == NULL) {
		CONFerror(CONF_R_LIST_CANNOT_BE_NULL);
		return 0;
	}

	lstart = list_;
	for (;;) {
		if (nospc) {
			while (*lstart && isspace((unsigned char)*lstart))
				lstart++;
		}
		p = strchr(lstart, sep);
		if (p == lstart || !*lstart)
			ret = list_cb(NULL, 0, arg);
		else {
			if (p)
				tmpend = p - 1;
			else
				tmpend = lstart + strlen(lstart) - 1;
			if (nospc) {
				while (isspace((unsigned char)*tmpend))
					tmpend--;
			}
			ret = list_cb(lstart, tmpend - lstart + 1, arg);
		}
		if (ret <= 0)
			return ret;
		if (p == NULL)
			return 1;
		lstart = p + 1;
	}
}
