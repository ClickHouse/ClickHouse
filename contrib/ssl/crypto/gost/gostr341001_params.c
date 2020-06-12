/* $OpenBSD: gostr341001_params.c,v 1.3 2015/07/20 22:42:56 bcook Exp $ */
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

#include <string.h>

#include <openssl/opensslconf.h>

#ifndef OPENSSL_NO_GOST
#include <openssl/objects.h>
#include <openssl/gost.h>

#include "gost_locl.h"

int
GostR3410_get_md_digest(int nid)
{
	if (nid == NID_id_GostR3411_94_CryptoProParamSet)
		return NID_id_GostR3411_94;
	return nid;
}

int
GostR3410_get_pk_digest(int nid)
{
	switch (nid) {
	case NID_id_GostR3411_94_CryptoProParamSet:
		return NID_id_GostR3410_2001;
	case NID_id_tc26_gost3411_2012_256:
		return NID_id_tc26_gost3410_2012_256;
	case NID_id_tc26_gost3411_2012_512:
		return NID_id_tc26_gost3410_2012_512;
	default:
		return NID_undef;
	}
}

typedef struct GostR3410_params {
	const char *name;
	int nid;
} GostR3410_params;

static const GostR3410_params GostR3410_256_params[] = {
	{ "A",  NID_id_GostR3410_2001_CryptoPro_A_ParamSet },
	{ "B",  NID_id_GostR3410_2001_CryptoPro_B_ParamSet },
	{ "C",  NID_id_GostR3410_2001_CryptoPro_C_ParamSet },
	{ "0",  NID_id_GostR3410_2001_TestParamSet },
	{ "XA", NID_id_GostR3410_2001_CryptoPro_XchA_ParamSet },
	{ "XB", NID_id_GostR3410_2001_CryptoPro_XchB_ParamSet },
	{ NULL, NID_undef },
};

static const GostR3410_params GostR3410_512_params[] = {
	{ "A",  NID_id_tc26_gost_3410_2012_512_paramSetA },
	{ "B",  NID_id_tc26_gost_3410_2012_512_paramSetB },
	{ NULL, NID_undef },
};

int
GostR3410_256_param_id(const char *value)
{
	int i;

	for (i = 0; GostR3410_256_params[i].nid != NID_undef; i++) {
		if (strcasecmp(GostR3410_256_params[i].name, value) == 0)
			return GostR3410_256_params[i].nid;
	}

	return NID_undef;
}

int
GostR3410_512_param_id(const char *value)
{
	int i;

	for (i = 0; GostR3410_512_params[i].nid != NID_undef; i++) {
		if (strcasecmp(GostR3410_512_params[i].name, value) == 0)
			return GostR3410_512_params[i].nid;
	}

	return NID_undef;
}

#endif
