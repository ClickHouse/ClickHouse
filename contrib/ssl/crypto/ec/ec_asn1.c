/* $OpenBSD: ec_asn1.c,v 1.24 2017/05/26 16:32:14 jsing Exp $ */
/*
 * Written by Nils Larsch for the OpenSSL project.
 */
/* ====================================================================
 * Copyright (c) 2000-2003 The OpenSSL Project.  All rights reserved.
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

#include <string.h>

#include <openssl/opensslconf.h>

#include "ec_lcl.h"
#include <openssl/err.h>
#include <openssl/asn1t.h>
#include <openssl/objects.h>

int 
EC_GROUP_get_basis_type(const EC_GROUP * group)
{
	int i = 0;

	if (EC_METHOD_get_field_type(EC_GROUP_method_of(group)) !=
	    NID_X9_62_characteristic_two_field)
		/* everything else is currently not supported */
		return 0;

	while (group->poly[i] != 0)
		i++;

	if (i == 4)
		return NID_X9_62_ppBasis;
	else if (i == 2)
		return NID_X9_62_tpBasis;
	else
		/* everything else is currently not supported */
		return 0;
}
#ifndef OPENSSL_NO_EC2M
int 
EC_GROUP_get_trinomial_basis(const EC_GROUP * group, unsigned int *k)
{
	if (group == NULL)
		return 0;

	if (EC_METHOD_get_field_type(EC_GROUP_method_of(group)) !=
	    NID_X9_62_characteristic_two_field
	    || !((group->poly[0] != 0) && (group->poly[1] != 0) && (group->poly[2] == 0))) {
		ECerror(ERR_R_SHOULD_NOT_HAVE_BEEN_CALLED);
		return 0;
	}
	if (k)
		*k = group->poly[1];

	return 1;
}
int 
EC_GROUP_get_pentanomial_basis(const EC_GROUP * group, unsigned int *k1,
    unsigned int *k2, unsigned int *k3)
{
	if (group == NULL)
		return 0;

	if (EC_METHOD_get_field_type(EC_GROUP_method_of(group)) !=
	    NID_X9_62_characteristic_two_field
	    || !((group->poly[0] != 0) && (group->poly[1] != 0) && (group->poly[2] != 0) && (group->poly[3] != 0) && (group->poly[4] == 0))) {
		ECerror(ERR_R_SHOULD_NOT_HAVE_BEEN_CALLED);
		return 0;
	}
	if (k1)
		*k1 = group->poly[3];
	if (k2)
		*k2 = group->poly[2];
	if (k3)
		*k3 = group->poly[1];

	return 1;
}
#endif


/* some structures needed for the asn1 encoding */
typedef struct x9_62_pentanomial_st {
	long k1;
	long k2;
	long k3;
} X9_62_PENTANOMIAL;

typedef struct x9_62_characteristic_two_st {
	long m;
	ASN1_OBJECT *type;
	union {
		char *ptr;
		/* NID_X9_62_onBasis */
		ASN1_NULL *onBasis;
		/* NID_X9_62_tpBasis */
		ASN1_INTEGER *tpBasis;
		/* NID_X9_62_ppBasis */
		X9_62_PENTANOMIAL *ppBasis;
		/* anything else */
		ASN1_TYPE *other;
	} p;
} X9_62_CHARACTERISTIC_TWO;

typedef struct x9_62_fieldid_st {
	ASN1_OBJECT *fieldType;
	union {
		char *ptr;
		/* NID_X9_62_prime_field */
		ASN1_INTEGER *prime;
		/* NID_X9_62_characteristic_two_field */
		X9_62_CHARACTERISTIC_TWO *char_two;
		/* anything else */
		ASN1_TYPE *other;
	} p;
} X9_62_FIELDID;

typedef struct x9_62_curve_st {
	ASN1_OCTET_STRING *a;
	ASN1_OCTET_STRING *b;
	ASN1_BIT_STRING *seed;
} X9_62_CURVE;

typedef struct ec_parameters_st {
	long version;
	X9_62_FIELDID *fieldID;
	X9_62_CURVE *curve;
	ASN1_OCTET_STRING *base;
	ASN1_INTEGER *order;
	ASN1_INTEGER *cofactor;
} ECPARAMETERS;

struct ecpk_parameters_st {
	int type;
	union {
		ASN1_OBJECT *named_curve;
		ECPARAMETERS *parameters;
		ASN1_NULL *implicitlyCA;
	} value;
} /* ECPKPARAMETERS */ ;

/* SEC1 ECPrivateKey */
typedef struct ec_privatekey_st {
	long version;
	ASN1_OCTET_STRING *privateKey;
	ECPKPARAMETERS *parameters;
	ASN1_BIT_STRING *publicKey;
} EC_PRIVATEKEY;

/* the OpenSSL ASN.1 definitions */
static const ASN1_TEMPLATE X9_62_PENTANOMIAL_seq_tt[] = {
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(X9_62_PENTANOMIAL, k1),
		.field_name = "k1",
		.item = &LONG_it,
	},
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(X9_62_PENTANOMIAL, k2),
		.field_name = "k2",
		.item = &LONG_it,
	},
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(X9_62_PENTANOMIAL, k3),
		.field_name = "k3",
		.item = &LONG_it,
	},
};

const ASN1_ITEM X9_62_PENTANOMIAL_it = {
	.itype = ASN1_ITYPE_SEQUENCE,
	.utype = V_ASN1_SEQUENCE,
	.templates = X9_62_PENTANOMIAL_seq_tt,
	.tcount = sizeof(X9_62_PENTANOMIAL_seq_tt) / sizeof(ASN1_TEMPLATE),
	.funcs = NULL,
	.size = sizeof(X9_62_PENTANOMIAL),
	.sname = "X9_62_PENTANOMIAL",
};

X9_62_PENTANOMIAL *X9_62_PENTANOMIAL_new(void);
void X9_62_PENTANOMIAL_free(X9_62_PENTANOMIAL *a);

X9_62_PENTANOMIAL *
X9_62_PENTANOMIAL_new(void)
{
	return (X9_62_PENTANOMIAL*)ASN1_item_new(&X9_62_PENTANOMIAL_it);
}

void
X9_62_PENTANOMIAL_free(X9_62_PENTANOMIAL *a)
{
	ASN1_item_free((ASN1_VALUE *)a, &X9_62_PENTANOMIAL_it);
}

static const ASN1_TEMPLATE char_two_def_tt = {
	.flags = 0,
	.tag = 0,
	.offset = offsetof(X9_62_CHARACTERISTIC_TWO, p.other),
	.field_name = "p.other",
	.item = &ASN1_ANY_it,
};

static const ASN1_ADB_TABLE X9_62_CHARACTERISTIC_TWO_adbtbl[] = {
	{
		.value = NID_X9_62_onBasis,
		.tt = {
			.flags = 0,
			.tag = 0,
			.offset = offsetof(X9_62_CHARACTERISTIC_TWO, p.onBasis),
			.field_name = "p.onBasis",
			.item = &ASN1_NULL_it,
		},
	
	},
	{
		.value = NID_X9_62_tpBasis,
		.tt = {
			.flags = 0,
			.tag = 0,
			.offset = offsetof(X9_62_CHARACTERISTIC_TWO, p.tpBasis),
			.field_name = "p.tpBasis",
			.item = &ASN1_INTEGER_it,
		},
	
	},
	{
		.value = NID_X9_62_ppBasis,
		.tt = {
			.flags = 0,
			.tag = 0,
			.offset = offsetof(X9_62_CHARACTERISTIC_TWO, p.ppBasis),
			.field_name = "p.ppBasis",
			.item = &X9_62_PENTANOMIAL_it,
		},
	
	},
};

static const ASN1_ADB X9_62_CHARACTERISTIC_TWO_adb = {
	.flags = 0,
	.offset = offsetof(X9_62_CHARACTERISTIC_TWO, type),
	.app_items = 0,
	.tbl = X9_62_CHARACTERISTIC_TWO_adbtbl,
	.tblcount = sizeof(X9_62_CHARACTERISTIC_TWO_adbtbl) / sizeof(ASN1_ADB_TABLE),
	.default_tt = &char_two_def_tt,
	.null_tt = NULL,
};

static const ASN1_TEMPLATE X9_62_CHARACTERISTIC_TWO_seq_tt[] = {
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(X9_62_CHARACTERISTIC_TWO, m),
		.field_name = "m",
		.item = &LONG_it,
	},
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(X9_62_CHARACTERISTIC_TWO, type),
		.field_name = "type",
		.item = &ASN1_OBJECT_it,
	},
	{
		.flags = ASN1_TFLG_ADB_OID,
		.tag = -1,
		.offset = 0,
		.field_name = "X9_62_CHARACTERISTIC_TWO",
		.item = (const ASN1_ITEM *)&X9_62_CHARACTERISTIC_TWO_adb,
	},
};

const ASN1_ITEM X9_62_CHARACTERISTIC_TWO_it = {
	.itype = ASN1_ITYPE_SEQUENCE,
	.utype = V_ASN1_SEQUENCE,
	.templates = X9_62_CHARACTERISTIC_TWO_seq_tt,
	.tcount = sizeof(X9_62_CHARACTERISTIC_TWO_seq_tt) / sizeof(ASN1_TEMPLATE),
	.funcs = NULL,
	.size = sizeof(X9_62_CHARACTERISTIC_TWO),
	.sname = "X9_62_CHARACTERISTIC_TWO",
};
X9_62_CHARACTERISTIC_TWO *X9_62_CHARACTERISTIC_TWO_new(void);
void X9_62_CHARACTERISTIC_TWO_free(X9_62_CHARACTERISTIC_TWO *a);

X9_62_CHARACTERISTIC_TWO *
X9_62_CHARACTERISTIC_TWO_new(void)
{
	return (X9_62_CHARACTERISTIC_TWO*)ASN1_item_new(&X9_62_CHARACTERISTIC_TWO_it);
}

void
X9_62_CHARACTERISTIC_TWO_free(X9_62_CHARACTERISTIC_TWO *a)
{
	ASN1_item_free((ASN1_VALUE *)a, &X9_62_CHARACTERISTIC_TWO_it);
}
static const ASN1_TEMPLATE fieldID_def_tt = {
	.flags = 0,
	.tag = 0,
	.offset = offsetof(X9_62_FIELDID, p.other),
	.field_name = "p.other",
	.item = &ASN1_ANY_it,
};

static const ASN1_ADB_TABLE X9_62_FIELDID_adbtbl[] = {
	{
		.value = NID_X9_62_prime_field,
		.tt = {
			.flags = 0,
			.tag = 0,
			.offset = offsetof(X9_62_FIELDID, p.prime),
			.field_name = "p.prime",
			.item = &ASN1_INTEGER_it,
		},
	
	},
	{
		.value = NID_X9_62_characteristic_two_field,
		.tt = {
			.flags = 0,
			.tag = 0,
			.offset = offsetof(X9_62_FIELDID, p.char_two),
			.field_name = "p.char_two",
			.item = &X9_62_CHARACTERISTIC_TWO_it,
		},
	
	},
};

static const ASN1_ADB X9_62_FIELDID_adb = {
	.flags = 0,
	.offset = offsetof(X9_62_FIELDID, fieldType),
	.app_items = 0,
	.tbl = X9_62_FIELDID_adbtbl,
	.tblcount = sizeof(X9_62_FIELDID_adbtbl) / sizeof(ASN1_ADB_TABLE),
	.default_tt = &fieldID_def_tt,
	.null_tt = NULL,
};

static const ASN1_TEMPLATE X9_62_FIELDID_seq_tt[] = {
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(X9_62_FIELDID, fieldType),
		.field_name = "fieldType",
		.item = &ASN1_OBJECT_it,
	},
	{
		.flags = ASN1_TFLG_ADB_OID,
		.tag = -1,
		.offset = 0,
		.field_name = "X9_62_FIELDID",
		.item = (const ASN1_ITEM *)&X9_62_FIELDID_adb,
	},
};

const ASN1_ITEM X9_62_FIELDID_it = {
	.itype = ASN1_ITYPE_SEQUENCE,
	.utype = V_ASN1_SEQUENCE,
	.templates = X9_62_FIELDID_seq_tt,
	.tcount = sizeof(X9_62_FIELDID_seq_tt) / sizeof(ASN1_TEMPLATE),
	.funcs = NULL,
	.size = sizeof(X9_62_FIELDID),
	.sname = "X9_62_FIELDID",
};

static const ASN1_TEMPLATE X9_62_CURVE_seq_tt[] = {
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(X9_62_CURVE, a),
		.field_name = "a",
		.item = &ASN1_OCTET_STRING_it,
	},
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(X9_62_CURVE, b),
		.field_name = "b",
		.item = &ASN1_OCTET_STRING_it,
	},
	{
		.flags = ASN1_TFLG_OPTIONAL,
		.tag = 0,
		.offset = offsetof(X9_62_CURVE, seed),
		.field_name = "seed",
		.item = &ASN1_BIT_STRING_it,
	},
};

const ASN1_ITEM X9_62_CURVE_it = {
	.itype = ASN1_ITYPE_SEQUENCE,
	.utype = V_ASN1_SEQUENCE,
	.templates = X9_62_CURVE_seq_tt,
	.tcount = sizeof(X9_62_CURVE_seq_tt) / sizeof(ASN1_TEMPLATE),
	.funcs = NULL,
	.size = sizeof(X9_62_CURVE),
	.sname = "X9_62_CURVE",
};

static const ASN1_TEMPLATE ECPARAMETERS_seq_tt[] = {
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(ECPARAMETERS, version),
		.field_name = "version",
		.item = &LONG_it,
	},
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(ECPARAMETERS, fieldID),
		.field_name = "fieldID",
		.item = &X9_62_FIELDID_it,
	},
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(ECPARAMETERS, curve),
		.field_name = "curve",
		.item = &X9_62_CURVE_it,
	},
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(ECPARAMETERS, base),
		.field_name = "base",
		.item = &ASN1_OCTET_STRING_it,
	},
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(ECPARAMETERS, order),
		.field_name = "order",
		.item = &ASN1_INTEGER_it,
	},
	{
		.flags = ASN1_TFLG_OPTIONAL,
		.tag = 0,
		.offset = offsetof(ECPARAMETERS, cofactor),
		.field_name = "cofactor",
		.item = &ASN1_INTEGER_it,
	},
};

const ASN1_ITEM ECPARAMETERS_it = {
	.itype = ASN1_ITYPE_SEQUENCE,
	.utype = V_ASN1_SEQUENCE,
	.templates = ECPARAMETERS_seq_tt,
	.tcount = sizeof(ECPARAMETERS_seq_tt) / sizeof(ASN1_TEMPLATE),
	.funcs = NULL,
	.size = sizeof(ECPARAMETERS),
	.sname = "ECPARAMETERS",
};
ECPARAMETERS *ECPARAMETERS_new(void);
void ECPARAMETERS_free(ECPARAMETERS *a);

ECPARAMETERS *
ECPARAMETERS_new(void)
{
	return (ECPARAMETERS*)ASN1_item_new(&ECPARAMETERS_it);
}

void
ECPARAMETERS_free(ECPARAMETERS *a)
{
	ASN1_item_free((ASN1_VALUE *)a, &ECPARAMETERS_it);
}

static const ASN1_TEMPLATE ECPKPARAMETERS_ch_tt[] = {
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(ECPKPARAMETERS, value.named_curve),
		.field_name = "value.named_curve",
		.item = &ASN1_OBJECT_it,
	},
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(ECPKPARAMETERS, value.parameters),
		.field_name = "value.parameters",
		.item = &ECPARAMETERS_it,
	},
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(ECPKPARAMETERS, value.implicitlyCA),
		.field_name = "value.implicitlyCA",
		.item = &ASN1_NULL_it,
	},
};

const ASN1_ITEM ECPKPARAMETERS_it = {
	.itype = ASN1_ITYPE_CHOICE,
	.utype = offsetof(ECPKPARAMETERS, type),
	.templates = ECPKPARAMETERS_ch_tt,
	.tcount = sizeof(ECPKPARAMETERS_ch_tt) / sizeof(ASN1_TEMPLATE),
	.funcs = NULL,
	.size = sizeof(ECPKPARAMETERS),
	.sname = "ECPKPARAMETERS",
};

ECPKPARAMETERS *ECPKPARAMETERS_new(void);
void ECPKPARAMETERS_free(ECPKPARAMETERS *a);
ECPKPARAMETERS *d2i_ECPKPARAMETERS(ECPKPARAMETERS **a, const unsigned char **in, long len);
int i2d_ECPKPARAMETERS(const ECPKPARAMETERS *a, unsigned char **out);

ECPKPARAMETERS *
d2i_ECPKPARAMETERS(ECPKPARAMETERS **a, const unsigned char **in, long len)
{
	return (ECPKPARAMETERS *)ASN1_item_d2i((ASN1_VALUE **)a, in, len,
	    &ECPKPARAMETERS_it);
}

int
i2d_ECPKPARAMETERS(const ECPKPARAMETERS *a, unsigned char **out)
{
	return ASN1_item_i2d((ASN1_VALUE *)a, out, &ECPKPARAMETERS_it);
}

ECPKPARAMETERS *
ECPKPARAMETERS_new(void)
{
	return (ECPKPARAMETERS *)ASN1_item_new(&ECPKPARAMETERS_it);
}

void
ECPKPARAMETERS_free(ECPKPARAMETERS *a)
{
	ASN1_item_free((ASN1_VALUE *)a, &ECPKPARAMETERS_it);
}

static const ASN1_TEMPLATE EC_PRIVATEKEY_seq_tt[] = {
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(EC_PRIVATEKEY, version),
		.field_name = "version",
		.item = &LONG_it,
	},
	{
		.flags = 0,
		.tag = 0,
		.offset = offsetof(EC_PRIVATEKEY, privateKey),
		.field_name = "privateKey",
		.item = &ASN1_OCTET_STRING_it,
	},
	{
		.flags = ASN1_TFLG_EXPLICIT | ASN1_TFLG_OPTIONAL,
		.tag = 0,
		.offset = offsetof(EC_PRIVATEKEY, parameters),
		.field_name = "parameters",
		.item = &ECPKPARAMETERS_it,
	},
	{
		.flags = ASN1_TFLG_EXPLICIT | ASN1_TFLG_OPTIONAL,
		.tag = 1,
		.offset = offsetof(EC_PRIVATEKEY, publicKey),
		.field_name = "publicKey",
		.item = &ASN1_BIT_STRING_it,
	},
};

const ASN1_ITEM EC_PRIVATEKEY_it = {
	.itype = ASN1_ITYPE_SEQUENCE,
	.utype = V_ASN1_SEQUENCE,
	.templates = EC_PRIVATEKEY_seq_tt,
	.tcount = sizeof(EC_PRIVATEKEY_seq_tt) / sizeof(ASN1_TEMPLATE),
	.funcs = NULL,
	.size = sizeof(EC_PRIVATEKEY),
	.sname = "EC_PRIVATEKEY",
};

EC_PRIVATEKEY *EC_PRIVATEKEY_new(void);
void EC_PRIVATEKEY_free(EC_PRIVATEKEY *a);
EC_PRIVATEKEY *d2i_EC_PRIVATEKEY(EC_PRIVATEKEY **a, const unsigned char **in, long len);
int i2d_EC_PRIVATEKEY(const EC_PRIVATEKEY *a, unsigned char **out);

EC_PRIVATEKEY *
d2i_EC_PRIVATEKEY(EC_PRIVATEKEY **a, const unsigned char **in, long len)
{
	return (EC_PRIVATEKEY *)ASN1_item_d2i((ASN1_VALUE **)a, in, len,
	    &EC_PRIVATEKEY_it);
}

int
i2d_EC_PRIVATEKEY(const EC_PRIVATEKEY *a, unsigned char **out)
{
	return ASN1_item_i2d((ASN1_VALUE *)a, out, &EC_PRIVATEKEY_it);
}

EC_PRIVATEKEY *
EC_PRIVATEKEY_new(void)
{
	return (EC_PRIVATEKEY *)ASN1_item_new(&EC_PRIVATEKEY_it);
}

void
EC_PRIVATEKEY_free(EC_PRIVATEKEY *a)
{
	ASN1_item_free((ASN1_VALUE *)a, &EC_PRIVATEKEY_it);
}
/* some declarations of internal function */

/* ec_asn1_group2field() sets the values in a X9_62_FIELDID object */
static int ec_asn1_group2fieldid(const EC_GROUP *, X9_62_FIELDID *);
/* ec_asn1_group2curve() sets the values in a X9_62_CURVE object */
static int ec_asn1_group2curve(const EC_GROUP *, X9_62_CURVE *);
/* ec_asn1_parameters2group() creates a EC_GROUP object from a
 * ECPARAMETERS object */
static EC_GROUP *ec_asn1_parameters2group(const ECPARAMETERS *);
/* ec_asn1_group2parameters() creates a ECPARAMETERS object from a
 * EC_GROUP object */
static ECPARAMETERS *ec_asn1_group2parameters(const EC_GROUP *, ECPARAMETERS *);
/* ec_asn1_pkparameters2group() creates a EC_GROUP object from a
 * ECPKPARAMETERS object */
static EC_GROUP *ec_asn1_pkparameters2group(const ECPKPARAMETERS *);
/* ec_asn1_group2pkparameters() creates a ECPKPARAMETERS object from a
 * EC_GROUP object */
static ECPKPARAMETERS *ec_asn1_group2pkparameters(const EC_GROUP *,
    ECPKPARAMETERS *);


/* the function definitions */

static int
ec_asn1_group2fieldid(const EC_GROUP * group, X9_62_FIELDID * field)
{
	int ok = 0, nid;
	BIGNUM *tmp = NULL;

	if (group == NULL || field == NULL)
		return 0;

	/* clear the old values (if necessary) */
	if (field->fieldType != NULL)
		ASN1_OBJECT_free(field->fieldType);
	if (field->p.other != NULL)
		ASN1_TYPE_free(field->p.other);

	nid = EC_METHOD_get_field_type(EC_GROUP_method_of(group));
	/* set OID for the field */
	if ((field->fieldType = OBJ_nid2obj(nid)) == NULL) {
		ECerror(ERR_R_OBJ_LIB);
		goto err;
	}
	if (nid == NID_X9_62_prime_field) {
		if ((tmp = BN_new()) == NULL) {
			ECerror(ERR_R_MALLOC_FAILURE);
			goto err;
		}
		/* the parameters are specified by the prime number p */
		if (!EC_GROUP_get_curve_GFp(group, tmp, NULL, NULL, NULL)) {
			ECerror(ERR_R_EC_LIB);
			goto err;
		}
		/* set the prime number */
		field->p.prime = BN_to_ASN1_INTEGER(tmp, NULL);
		if (field->p.prime == NULL) {
			ECerror(ERR_R_ASN1_LIB);
			goto err;
		}
	} else			/* nid == NID_X9_62_characteristic_two_field */
#ifdef OPENSSL_NO_EC2M
	{
		ECerror(EC_R_GF2M_NOT_SUPPORTED);
		goto err;
	}
#else
	{
		int field_type;
		X9_62_CHARACTERISTIC_TWO *char_two;

		field->p.char_two = X9_62_CHARACTERISTIC_TWO_new();
		char_two = field->p.char_two;

		if (char_two == NULL) {
			ECerror(ERR_R_MALLOC_FAILURE);
			goto err;
		}
		char_two->m = (long) EC_GROUP_get_degree(group);

		field_type = EC_GROUP_get_basis_type(group);

		if (field_type == 0) {
			ECerror(ERR_R_EC_LIB);
			goto err;
		}
		/* set base type OID */
		if ((char_two->type = OBJ_nid2obj(field_type)) == NULL) {
			ECerror(ERR_R_OBJ_LIB);
			goto err;
		}
		if (field_type == NID_X9_62_tpBasis) {
			unsigned int k;

			if (!EC_GROUP_get_trinomial_basis(group, &k))
				goto err;

			char_two->p.tpBasis = ASN1_INTEGER_new();
			if (!char_two->p.tpBasis) {
				ECerror(ERR_R_MALLOC_FAILURE);
				goto err;
			}
			if (!ASN1_INTEGER_set(char_two->p.tpBasis, (long) k)) {
				ECerror(ERR_R_ASN1_LIB);
				goto err;
			}
		} else if (field_type == NID_X9_62_ppBasis) {
			unsigned int k1, k2, k3;

			if (!EC_GROUP_get_pentanomial_basis(group, &k1, &k2, &k3))
				goto err;

			char_two->p.ppBasis = X9_62_PENTANOMIAL_new();
			if (!char_two->p.ppBasis) {
				ECerror(ERR_R_MALLOC_FAILURE);
				goto err;
			}
			/* set k? values */
			char_two->p.ppBasis->k1 = (long) k1;
			char_two->p.ppBasis->k2 = (long) k2;
			char_two->p.ppBasis->k3 = (long) k3;
		} else {	/* field_type == NID_X9_62_onBasis */
			/* for ONB the parameters are (asn1) NULL */
			char_two->p.onBasis = ASN1_NULL_new();
			if (!char_two->p.onBasis) {
				ECerror(ERR_R_MALLOC_FAILURE);
				goto err;
			}
		}
	}
#endif

	ok = 1;

err:
	BN_free(tmp);
	return (ok);
}

static int 
ec_asn1_group2curve(const EC_GROUP * group, X9_62_CURVE * curve)
{
	int ok = 0, nid;
	BIGNUM *tmp_1 = NULL, *tmp_2 = NULL;
	unsigned char *buffer_1 = NULL, *buffer_2 = NULL, *a_buf = NULL,
	*b_buf = NULL;
	size_t len_1, len_2;
	unsigned char char_zero = 0;

	if (!group || !curve || !curve->a || !curve->b)
		return 0;

	if ((tmp_1 = BN_new()) == NULL || (tmp_2 = BN_new()) == NULL) {
		ECerror(ERR_R_MALLOC_FAILURE);
		goto err;
	}
	nid = EC_METHOD_get_field_type(EC_GROUP_method_of(group));

	/* get a and b */
	if (nid == NID_X9_62_prime_field) {
		if (!EC_GROUP_get_curve_GFp(group, NULL, tmp_1, tmp_2, NULL)) {
			ECerror(ERR_R_EC_LIB);
			goto err;
		}
	}
#ifndef OPENSSL_NO_EC2M
	else {			/* nid == NID_X9_62_characteristic_two_field */
		if (!EC_GROUP_get_curve_GF2m(group, NULL, tmp_1, tmp_2, NULL)) {
			ECerror(ERR_R_EC_LIB);
			goto err;
		}
	}
#endif
	len_1 = (size_t) BN_num_bytes(tmp_1);
	len_2 = (size_t) BN_num_bytes(tmp_2);

	if (len_1 == 0) {
		/* len_1 == 0 => a == 0 */
		a_buf = &char_zero;
		len_1 = 1;
	} else {
		if ((buffer_1 = malloc(len_1)) == NULL) {
			ECerror(ERR_R_MALLOC_FAILURE);
			goto err;
		}
		if ((len_1 = BN_bn2bin(tmp_1, buffer_1)) == 0) {
			ECerror(ERR_R_BN_LIB);
			goto err;
		}
		a_buf = buffer_1;
	}

	if (len_2 == 0) {
		/* len_2 == 0 => b == 0 */
		b_buf = &char_zero;
		len_2 = 1;
	} else {
		if ((buffer_2 = malloc(len_2)) == NULL) {
			ECerror(ERR_R_MALLOC_FAILURE);
			goto err;
		}
		if ((len_2 = BN_bn2bin(tmp_2, buffer_2)) == 0) {
			ECerror(ERR_R_BN_LIB);
			goto err;
		}
		b_buf = buffer_2;
	}

	/* set a and b */
	if (!ASN1_STRING_set(curve->a, a_buf, len_1) ||
	    !ASN1_STRING_set(curve->b, b_buf, len_2)) {
		ECerror(ERR_R_ASN1_LIB);
		goto err;
	}
	/* set the seed (optional) */
	if (group->seed) {
		if (!curve->seed)
			if ((curve->seed = ASN1_BIT_STRING_new()) == NULL) {
				ECerror(ERR_R_MALLOC_FAILURE);
				goto err;
			}
		curve->seed->flags &= ~(ASN1_STRING_FLAG_BITS_LEFT | 0x07);
		curve->seed->flags |= ASN1_STRING_FLAG_BITS_LEFT;
		if (!ASN1_BIT_STRING_set(curve->seed, group->seed,
			(int) group->seed_len)) {
			ECerror(ERR_R_ASN1_LIB);
			goto err;
		}
	} else {
		if (curve->seed) {
			ASN1_BIT_STRING_free(curve->seed);
			curve->seed = NULL;
		}
	}

	ok = 1;

err:
	free(buffer_1);
	free(buffer_2);
	BN_free(tmp_1);
	BN_free(tmp_2);
	return (ok);
}

static ECPARAMETERS *
ec_asn1_group2parameters(const EC_GROUP * group, ECPARAMETERS * param)
{
	int ok = 0;
	size_t len = 0;
	ECPARAMETERS *ret = NULL;
	BIGNUM *tmp = NULL;
	unsigned char *buffer = NULL;
	const EC_POINT *point = NULL;
	point_conversion_form_t form;

	if ((tmp = BN_new()) == NULL) {
		ECerror(ERR_R_MALLOC_FAILURE);
		goto err;
	}
	if (param == NULL) {
		if ((ret = ECPARAMETERS_new()) == NULL) {
			ECerror(ERR_R_MALLOC_FAILURE);
			goto err;
		}
	} else
		ret = param;

	/* set the version (always one) */
	ret->version = (long) 0x1;

	/* set the fieldID */
	if (!ec_asn1_group2fieldid(group, ret->fieldID)) {
		ECerror(ERR_R_EC_LIB);
		goto err;
	}
	/* set the curve */
	if (!ec_asn1_group2curve(group, ret->curve)) {
		ECerror(ERR_R_EC_LIB);
		goto err;
	}
	/* set the base point */
	if ((point = EC_GROUP_get0_generator(group)) == NULL) {
		ECerror(EC_R_UNDEFINED_GENERATOR);
		goto err;
	}
	form = EC_GROUP_get_point_conversion_form(group);

	len = EC_POINT_point2oct(group, point, form, NULL, len, NULL);
	if (len == 0) {
		ECerror(ERR_R_EC_LIB);
		goto err;
	}
	if ((buffer = malloc(len)) == NULL) {
		ECerror(ERR_R_MALLOC_FAILURE);
		goto err;
	}
	if (!EC_POINT_point2oct(group, point, form, buffer, len, NULL)) {
		ECerror(ERR_R_EC_LIB);
		goto err;
	}
	if (ret->base == NULL && (ret->base = ASN1_OCTET_STRING_new()) == NULL) {
		ECerror(ERR_R_MALLOC_FAILURE);
		goto err;
	}
	if (!ASN1_OCTET_STRING_set(ret->base, buffer, len)) {
		ECerror(ERR_R_ASN1_LIB);
		goto err;
	}
	/* set the order */
	if (!EC_GROUP_get_order(group, tmp, NULL)) {
		ECerror(ERR_R_EC_LIB);
		goto err;
	}
	ret->order = BN_to_ASN1_INTEGER(tmp, ret->order);
	if (ret->order == NULL) {
		ECerror(ERR_R_ASN1_LIB);
		goto err;
	}
	/* set the cofactor (optional) */
	if (EC_GROUP_get_cofactor(group, tmp, NULL)) {
		ret->cofactor = BN_to_ASN1_INTEGER(tmp, ret->cofactor);
		if (ret->cofactor == NULL) {
			ECerror(ERR_R_ASN1_LIB);
			goto err;
		}
	}
	ok = 1;

err:	if (!ok) {
		if (ret && !param)
			ECPARAMETERS_free(ret);
		ret = NULL;
	}
	BN_free(tmp);
	free(buffer);
	return (ret);
}

ECPKPARAMETERS *
ec_asn1_group2pkparameters(const EC_GROUP * group, ECPKPARAMETERS * params)
{
	int ok = 1, tmp;
	ECPKPARAMETERS *ret = params;

	if (ret == NULL) {
		if ((ret = ECPKPARAMETERS_new()) == NULL) {
			ECerror(ERR_R_MALLOC_FAILURE);
			return NULL;
		}
	} else {
		if (ret->type == 0 && ret->value.named_curve)
			ASN1_OBJECT_free(ret->value.named_curve);
		else if (ret->type == 1 && ret->value.parameters)
			ECPARAMETERS_free(ret->value.parameters);
	}

	if (EC_GROUP_get_asn1_flag(group)) {
		/*
		 * use the asn1 OID to describe the elliptic curve
		 * parameters
		 */
		tmp = EC_GROUP_get_curve_name(group);
		if (tmp) {
			ret->type = 0;
			if ((ret->value.named_curve = OBJ_nid2obj(tmp)) == NULL)
				ok = 0;
		} else
			/* we don't kmow the nid => ERROR */
			ok = 0;
	} else {
		/* use the ECPARAMETERS structure */
		ret->type = 1;
		if ((ret->value.parameters = ec_asn1_group2parameters(
			    group, NULL)) == NULL)
			ok = 0;
	}

	if (!ok) {
		ECPKPARAMETERS_free(ret);
		return NULL;
	}
	return ret;
}

static EC_GROUP *
ec_asn1_parameters2group(const ECPARAMETERS * params)
{
	int ok = 0, tmp;
	EC_GROUP *ret = NULL;
	BIGNUM *p = NULL, *a = NULL, *b = NULL;
	EC_POINT *point = NULL;
	long field_bits;

	if (!params->fieldID || !params->fieldID->fieldType ||
	    !params->fieldID->p.ptr) {
		ECerror(EC_R_ASN1_ERROR);
		goto err;
	}
	/* now extract the curve parameters a and b */
	if (!params->curve || !params->curve->a ||
	    !params->curve->a->data || !params->curve->b ||
	    !params->curve->b->data) {
		ECerror(EC_R_ASN1_ERROR);
		goto err;
	}
	a = BN_bin2bn(params->curve->a->data, params->curve->a->length, NULL);
	if (a == NULL) {
		ECerror(ERR_R_BN_LIB);
		goto err;
	}
	b = BN_bin2bn(params->curve->b->data, params->curve->b->length, NULL);
	if (b == NULL) {
		ECerror(ERR_R_BN_LIB);
		goto err;
	}
	/* get the field parameters */
	tmp = OBJ_obj2nid(params->fieldID->fieldType);
	if (tmp == NID_X9_62_characteristic_two_field)
#ifdef OPENSSL_NO_EC2M
	{
		ECerror(EC_R_GF2M_NOT_SUPPORTED);
		goto err;
	}
#else
	{
		X9_62_CHARACTERISTIC_TWO *char_two;

		char_two = params->fieldID->p.char_two;

		field_bits = char_two->m;
		if (field_bits > OPENSSL_ECC_MAX_FIELD_BITS) {
			ECerror(EC_R_FIELD_TOO_LARGE);
			goto err;
		}
		if ((p = BN_new()) == NULL) {
			ECerror(ERR_R_MALLOC_FAILURE);
			goto err;
		}
		/* get the base type */
		tmp = OBJ_obj2nid(char_two->type);

		if (tmp == NID_X9_62_tpBasis) {
			long tmp_long;

			if (!char_two->p.tpBasis) {
				ECerror(EC_R_ASN1_ERROR);
				goto err;
			}
			tmp_long = ASN1_INTEGER_get(char_two->p.tpBasis);

			if (!(char_two->m > tmp_long && tmp_long > 0)) {
				ECerror(EC_R_INVALID_TRINOMIAL_BASIS);
				goto err;
			}
			/* create the polynomial */
			if (!BN_set_bit(p, (int) char_two->m))
				goto err;
			if (!BN_set_bit(p, (int) tmp_long))
				goto err;
			if (!BN_set_bit(p, 0))
				goto err;
		} else if (tmp == NID_X9_62_ppBasis) {
			X9_62_PENTANOMIAL *penta;

			penta = char_two->p.ppBasis;
			if (!penta) {
				ECerror(EC_R_ASN1_ERROR);
				goto err;
			}
			if (!(char_two->m > penta->k3 && penta->k3 > penta->k2 && penta->k2 > penta->k1 && penta->k1 > 0)) {
				ECerror(EC_R_INVALID_PENTANOMIAL_BASIS);
				goto err;
			}
			/* create the polynomial */
			if (!BN_set_bit(p, (int) char_two->m))
				goto err;
			if (!BN_set_bit(p, (int) penta->k1))
				goto err;
			if (!BN_set_bit(p, (int) penta->k2))
				goto err;
			if (!BN_set_bit(p, (int) penta->k3))
				goto err;
			if (!BN_set_bit(p, 0))
				goto err;
		} else if (tmp == NID_X9_62_onBasis) {
			ECerror(EC_R_NOT_IMPLEMENTED);
			goto err;
		} else {	/* error */
			ECerror(EC_R_ASN1_ERROR);
			goto err;
		}

		/* create the EC_GROUP structure */
		ret = EC_GROUP_new_curve_GF2m(p, a, b, NULL);
	}
#endif
	else if (tmp == NID_X9_62_prime_field) {
		/* we have a curve over a prime field */
		/* extract the prime number */
		if (!params->fieldID->p.prime) {
			ECerror(EC_R_ASN1_ERROR);
			goto err;
		}
		p = ASN1_INTEGER_to_BN(params->fieldID->p.prime, NULL);
		if (p == NULL) {
			ECerror(ERR_R_ASN1_LIB);
			goto err;
		}
		if (BN_is_negative(p) || BN_is_zero(p)) {
			ECerror(EC_R_INVALID_FIELD);
			goto err;
		}
		field_bits = BN_num_bits(p);
		if (field_bits > OPENSSL_ECC_MAX_FIELD_BITS) {
			ECerror(EC_R_FIELD_TOO_LARGE);
			goto err;
		}
		/* create the EC_GROUP structure */
		ret = EC_GROUP_new_curve_GFp(p, a, b, NULL);
	} else {
		ECerror(EC_R_INVALID_FIELD);
		goto err;
	}

	if (ret == NULL) {
		ECerror(ERR_R_EC_LIB);
		goto err;
	}
	/* extract seed (optional) */
	if (params->curve->seed != NULL) {
		free(ret->seed);
		if (!(ret->seed = malloc(params->curve->seed->length))) {
			ECerror(ERR_R_MALLOC_FAILURE);
			goto err;
		}
		memcpy(ret->seed, params->curve->seed->data,
		    params->curve->seed->length);
		ret->seed_len = params->curve->seed->length;
	}
	if (!params->order || !params->base || !params->base->data) {
		ECerror(EC_R_ASN1_ERROR);
		goto err;
	}
	if ((point = EC_POINT_new(ret)) == NULL)
		goto err;

	/* set the point conversion form */
	EC_GROUP_set_point_conversion_form(ret, (point_conversion_form_t)
	    (params->base->data[0] & ~0x01));

	/* extract the ec point */
	if (!EC_POINT_oct2point(ret, point, params->base->data,
		params->base->length, NULL)) {
		ECerror(ERR_R_EC_LIB);
		goto err;
	}
	/* extract the order */
	if ((a = ASN1_INTEGER_to_BN(params->order, a)) == NULL) {
		ECerror(ERR_R_ASN1_LIB);
		goto err;
	}
	if (BN_is_negative(a) || BN_is_zero(a)) {
		ECerror(EC_R_INVALID_GROUP_ORDER);
		goto err;
	}
	if (BN_num_bits(a) > (int) field_bits + 1) {	/* Hasse bound */
		ECerror(EC_R_INVALID_GROUP_ORDER);
		goto err;
	}
	/* extract the cofactor (optional) */
	if (params->cofactor == NULL) {
		BN_free(b);
		b = NULL;
	} else if ((b = ASN1_INTEGER_to_BN(params->cofactor, b)) == NULL) {
		ECerror(ERR_R_ASN1_LIB);
		goto err;
	}
	/* set the generator, order and cofactor (if present) */
	if (!EC_GROUP_set_generator(ret, point, a, b)) {
		ECerror(ERR_R_EC_LIB);
		goto err;
	}
	ok = 1;

err:	if (!ok) {
		EC_GROUP_clear_free(ret);
		ret = NULL;
	}
	BN_free(p);
	BN_free(a);
	BN_free(b);
	EC_POINT_free(point);
	return (ret);
}

EC_GROUP *
ec_asn1_pkparameters2group(const ECPKPARAMETERS * params)
{
	EC_GROUP *ret = NULL;
	int tmp = 0;

	if (params == NULL) {
		ECerror(EC_R_MISSING_PARAMETERS);
		return NULL;
	}
	if (params->type == 0) {/* the curve is given by an OID */
		tmp = OBJ_obj2nid(params->value.named_curve);
		if ((ret = EC_GROUP_new_by_curve_name(tmp)) == NULL) {
			ECerror(EC_R_EC_GROUP_NEW_BY_NAME_FAILURE);
			return NULL;
		}
		EC_GROUP_set_asn1_flag(ret, OPENSSL_EC_NAMED_CURVE);
	} else if (params->type == 1) {	/* the parameters are given by a
					 * ECPARAMETERS structure */
		ret = ec_asn1_parameters2group(params->value.parameters);
		if (!ret) {
			ECerror(ERR_R_EC_LIB);
			return NULL;
		}
		EC_GROUP_set_asn1_flag(ret, 0x0);
	} else if (params->type == 2) {	/* implicitlyCA */
		return NULL;
	} else {
		ECerror(EC_R_ASN1_ERROR);
		return NULL;
	}

	return ret;
}

/* EC_GROUP <-> DER encoding of ECPKPARAMETERS */

EC_GROUP *
d2i_ECPKParameters(EC_GROUP ** a, const unsigned char **in, long len)
{
	EC_GROUP *group = NULL;
	ECPKPARAMETERS *params = NULL;

	if ((params = d2i_ECPKPARAMETERS(NULL, in, len)) == NULL) {
		ECerror(EC_R_D2I_ECPKPARAMETERS_FAILURE);
		goto err;
	}
	if ((group = ec_asn1_pkparameters2group(params)) == NULL) {
		ECerror(EC_R_PKPARAMETERS2GROUP_FAILURE);
		goto err;
	}

	if (a != NULL) {
		EC_GROUP_clear_free(*a);
		*a = group;
	}

err:
	ECPKPARAMETERS_free(params);
	return (group);
}

int 
i2d_ECPKParameters(const EC_GROUP * a, unsigned char **out)
{
	int ret = 0;
	ECPKPARAMETERS *tmp = ec_asn1_group2pkparameters(a, NULL);
	if (tmp == NULL) {
		ECerror(EC_R_GROUP2PKPARAMETERS_FAILURE);
		return 0;
	}
	if ((ret = i2d_ECPKPARAMETERS(tmp, out)) == 0) {
		ECerror(EC_R_I2D_ECPKPARAMETERS_FAILURE);
		ECPKPARAMETERS_free(tmp);
		return 0;
	}
	ECPKPARAMETERS_free(tmp);
	return (ret);
}

/* some EC_KEY functions */

EC_KEY *
d2i_ECPrivateKey(EC_KEY ** a, const unsigned char **in, long len)
{
	EC_KEY *ret = NULL;
	EC_PRIVATEKEY *priv_key = NULL;

	if ((priv_key = EC_PRIVATEKEY_new()) == NULL) {
		ECerror(ERR_R_MALLOC_FAILURE);
		return NULL;
	}
	if ((priv_key = d2i_EC_PRIVATEKEY(&priv_key, in, len)) == NULL) {
		ECerror(ERR_R_EC_LIB);
		EC_PRIVATEKEY_free(priv_key);
		return NULL;
	}
	if (a == NULL || *a == NULL) {
		if ((ret = EC_KEY_new()) == NULL) {
			ECerror(ERR_R_MALLOC_FAILURE);
			goto err;
		}
	} else
		ret = *a;

	if (priv_key->parameters) {
		EC_GROUP_clear_free(ret->group);
		ret->group = ec_asn1_pkparameters2group(priv_key->parameters);
	}
	if (ret->group == NULL) {
		ECerror(ERR_R_EC_LIB);
		goto err;
	}
	ret->version = priv_key->version;

	if (priv_key->privateKey) {
		ret->priv_key = BN_bin2bn(
		    ASN1_STRING_data(priv_key->privateKey),
		    ASN1_STRING_length(priv_key->privateKey),
		    ret->priv_key);
		if (ret->priv_key == NULL) {
			ECerror(ERR_R_BN_LIB);
			goto err;
		}
	} else {
		ECerror(EC_R_MISSING_PRIVATE_KEY);
		goto err;
	}

	if (priv_key->publicKey) {
		const unsigned char *pub_oct;
		size_t pub_oct_len;

		EC_POINT_clear_free(ret->pub_key);
		ret->pub_key = EC_POINT_new(ret->group);
		if (ret->pub_key == NULL) {
			ECerror(ERR_R_EC_LIB);
			goto err;
		}

		pub_oct = ASN1_STRING_data(priv_key->publicKey);
		pub_oct_len = ASN1_STRING_length(priv_key->publicKey);
		if (pub_oct == NULL || pub_oct_len <= 0) {
			ECerror(EC_R_BUFFER_TOO_SMALL);
			goto err;
		}

		/* save the point conversion form */
		ret->conv_form = (point_conversion_form_t) (pub_oct[0] & ~0x01);
		if (!EC_POINT_oct2point(ret->group, ret->pub_key,
			pub_oct, pub_oct_len, NULL)) {
			ECerror(ERR_R_EC_LIB);
			goto err;
		}
	}

	EC_PRIVATEKEY_free(priv_key);
	if (a != NULL)
		*a = ret;
	return (ret);

err:
	if (a == NULL || *a != ret)
		EC_KEY_free(ret);
	if (priv_key)
		EC_PRIVATEKEY_free(priv_key);

	return (NULL);
}

int 
i2d_ECPrivateKey(EC_KEY * a, unsigned char **out)
{
	int ret = 0, ok = 0;
	unsigned char *buffer = NULL;
	size_t buf_len = 0, tmp_len;
	EC_PRIVATEKEY *priv_key = NULL;

	if (a == NULL || a->group == NULL || a->priv_key == NULL) {
		ECerror(ERR_R_PASSED_NULL_PARAMETER);
		goto err;
	}
	if ((priv_key = EC_PRIVATEKEY_new()) == NULL) {
		ECerror(ERR_R_MALLOC_FAILURE);
		goto err;
	}
	priv_key->version = a->version;

	buf_len = (size_t) BN_num_bytes(a->priv_key);
	buffer = malloc(buf_len);
	if (buffer == NULL) {
		ECerror(ERR_R_MALLOC_FAILURE);
		goto err;
	}
	if (!BN_bn2bin(a->priv_key, buffer)) {
		ECerror(ERR_R_BN_LIB);
		goto err;
	}
	if (!ASN1_STRING_set(priv_key->privateKey, buffer, buf_len)) {
		ECerror(ERR_R_ASN1_LIB);
		goto err;
	}
	if (!(a->enc_flag & EC_PKEY_NO_PARAMETERS)) {
		if ((priv_key->parameters = ec_asn1_group2pkparameters(
			    a->group, priv_key->parameters)) == NULL) {
			ECerror(ERR_R_EC_LIB);
			goto err;
		}
	}
	if (!(a->enc_flag & EC_PKEY_NO_PUBKEY) && a->pub_key != NULL) {
		priv_key->publicKey = ASN1_BIT_STRING_new();
		if (priv_key->publicKey == NULL) {
			ECerror(ERR_R_MALLOC_FAILURE);
			goto err;
		}
		tmp_len = EC_POINT_point2oct(a->group, a->pub_key,
		    a->conv_form, NULL, 0, NULL);

		if (tmp_len > buf_len) {
			unsigned char *tmp_buffer = realloc(buffer, tmp_len);
			if (!tmp_buffer) {
				ECerror(ERR_R_MALLOC_FAILURE);
				goto err;
			}
			buffer = tmp_buffer;
			buf_len = tmp_len;
		}
		if (!EC_POINT_point2oct(a->group, a->pub_key,
			a->conv_form, buffer, buf_len, NULL)) {
			ECerror(ERR_R_EC_LIB);
			goto err;
		}
		priv_key->publicKey->flags &= ~(ASN1_STRING_FLAG_BITS_LEFT | 0x07);
		priv_key->publicKey->flags |= ASN1_STRING_FLAG_BITS_LEFT;
		if (!ASN1_STRING_set(priv_key->publicKey, buffer,
			buf_len)) {
			ECerror(ERR_R_ASN1_LIB);
			goto err;
		}
	}
	if ((ret = i2d_EC_PRIVATEKEY(priv_key, out)) == 0) {
		ECerror(ERR_R_EC_LIB);
		goto err;
	}
	ok = 1;
err:
	free(buffer);
	if (priv_key)
		EC_PRIVATEKEY_free(priv_key);
	return (ok ? ret : 0);
}

int 
i2d_ECParameters(EC_KEY * a, unsigned char **out)
{
	if (a == NULL) {
		ECerror(ERR_R_PASSED_NULL_PARAMETER);
		return 0;
	}
	return i2d_ECPKParameters(a->group, out);
}

EC_KEY *
d2i_ECParameters(EC_KEY ** a, const unsigned char **in, long len)
{
	EC_KEY *ret;

	if (in == NULL || *in == NULL) {
		ECerror(ERR_R_PASSED_NULL_PARAMETER);
		return NULL;
	}
	if (a == NULL || *a == NULL) {
		if ((ret = EC_KEY_new()) == NULL) {
			ECerror(ERR_R_MALLOC_FAILURE);
			return NULL;
		}
	} else
		ret = *a;

	if (!d2i_ECPKParameters(&ret->group, in, len)) {
		ECerror(ERR_R_EC_LIB);
		if (a == NULL || *a != ret)
			EC_KEY_free(ret);
		return NULL;
	}

	if (a != NULL)
		*a = ret;
	return ret;
}

EC_KEY *
o2i_ECPublicKey(EC_KEY ** a, const unsigned char **in, long len)
{
	EC_KEY *ret = NULL;

	if (a == NULL || (*a) == NULL || (*a)->group == NULL) {
		/*
		 * sorry, but a EC_GROUP-structur is necessary to set the
		 * public key
		 */
		ECerror(ERR_R_PASSED_NULL_PARAMETER);
		return 0;
	}
	ret = *a;
	if (ret->pub_key == NULL &&
	    (ret->pub_key = EC_POINT_new(ret->group)) == NULL) {
		ECerror(ERR_R_MALLOC_FAILURE);
		return 0;
	}
	if (!EC_POINT_oct2point(ret->group, ret->pub_key, *in, len, NULL)) {
		ECerror(ERR_R_EC_LIB);
		return 0;
	}
	/* save the point conversion form */
	ret->conv_form = (point_conversion_form_t) (*in[0] & ~0x01);
	*in += len;
	return ret;
}

int 
i2o_ECPublicKey(EC_KEY * a, unsigned char **out)
{
	size_t buf_len = 0;
	int new_buffer = 0;

	if (a == NULL) {
		ECerror(ERR_R_PASSED_NULL_PARAMETER);
		return 0;
	}
	buf_len = EC_POINT_point2oct(a->group, a->pub_key,
	    a->conv_form, NULL, 0, NULL);

	if (out == NULL || buf_len == 0)
		/* out == NULL => just return the length of the octet string */
		return buf_len;

	if (*out == NULL) {
		if ((*out = malloc(buf_len)) == NULL) {
			ECerror(ERR_R_MALLOC_FAILURE);
			return 0;
		}
		new_buffer = 1;
	}
	if (!EC_POINT_point2oct(a->group, a->pub_key, a->conv_form,
		*out, buf_len, NULL)) {
		ECerror(ERR_R_EC_LIB);
		if (new_buffer) {
			free(*out);
			*out = NULL;
		}
		return 0;
	}
	if (!new_buffer)
		*out += buf_len;
	return buf_len;
}
