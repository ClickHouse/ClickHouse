/* $OpenBSD: v3_pci.c,v 1.13 2017/05/02 04:11:08 deraadt Exp $ */
/* Contributed to the OpenSSL Project 2004
 * by Richard Levitte (richard@levitte.org)
 */
/* Copyright (c) 2004 Kungliga Tekniska H�gskolan
 * (Royal Institute of Technology, Stockholm, Sweden).
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the Institute nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE INSTITUTE AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE INSTITUTE OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <stdio.h>
#include <string.h>

#include <openssl/conf.h>
#include <openssl/err.h>
#include <openssl/x509v3.h>

static int i2r_pci(X509V3_EXT_METHOD *method, PROXY_CERT_INFO_EXTENSION *ext,
    BIO *out, int indent);
static PROXY_CERT_INFO_EXTENSION *r2i_pci(X509V3_EXT_METHOD *method,
    X509V3_CTX *ctx, char *str);

const X509V3_EXT_METHOD v3_pci = {
	.ext_nid = NID_proxyCertInfo,
	.ext_flags = 0,
	.it = &PROXY_CERT_INFO_EXTENSION_it,
	.ext_new = NULL,
	.ext_free = NULL,
	.d2i = NULL,
	.i2d = NULL,
	.i2s = NULL,
	.s2i = NULL,
	.i2v = NULL,
	.v2i = NULL,
	.i2r = (X509V3_EXT_I2R)i2r_pci,
	.r2i = (X509V3_EXT_R2I)r2i_pci,
	.usr_data = NULL,
};

static int
i2r_pci(X509V3_EXT_METHOD *method, PROXY_CERT_INFO_EXTENSION *pci, BIO *out,
    int indent)
{
	BIO_printf(out, "%*sPath Length Constraint: ", indent, "");
	if (pci->pcPathLengthConstraint)
		i2a_ASN1_INTEGER(out, pci->pcPathLengthConstraint);
	else
		BIO_printf(out, "infinite");
	BIO_puts(out, "\n");
	BIO_printf(out, "%*sPolicy Language: ", indent, "");
	i2a_ASN1_OBJECT(out, pci->proxyPolicy->policyLanguage);
	BIO_puts(out, "\n");
	if (pci->proxyPolicy->policy && pci->proxyPolicy->policy->data)
		BIO_printf(out, "%*sPolicy Text: %s\n", indent, "",
		    pci->proxyPolicy->policy->data);
	return 1;
}

static int
process_pci_value(CONF_VALUE *val, ASN1_OBJECT **language,
    ASN1_INTEGER **pathlen, ASN1_OCTET_STRING **policy)
{
	int free_policy = 0;

	if (strcmp(val->name, "language") == 0) {
		if (*language) {
			X509V3error(X509V3_R_POLICY_LANGUAGE_ALREADY_DEFINED);
			X509V3_conf_err(val);
			return 0;
		}
		if (!(*language = OBJ_txt2obj(val->value, 0))) {
			X509V3error(X509V3_R_INVALID_OBJECT_IDENTIFIER);
			X509V3_conf_err(val);
			return 0;
		}
	}
	else if (strcmp(val->name, "pathlen") == 0) {
		if (*pathlen) {
			X509V3error(X509V3_R_POLICY_PATH_LENGTH_ALREADY_DEFINED);
			X509V3_conf_err(val);
			return 0;
		}
		if (!X509V3_get_value_int(val, pathlen)) {
			X509V3error(X509V3_R_POLICY_PATH_LENGTH);
			X509V3_conf_err(val);
			return 0;
		}
	}
	else if (strcmp(val->name, "policy") == 0) {
		unsigned char *tmp_data = NULL;
		long val_len;
		if (!*policy) {
			*policy = ASN1_OCTET_STRING_new();
			if (!*policy) {
				X509V3error(ERR_R_MALLOC_FAILURE);
				X509V3_conf_err(val);
				return 0;
			}
			free_policy = 1;
		}
		if (strncmp(val->value, "hex:", 4) == 0) {
			unsigned char *tmp_data2 =
			    string_to_hex(val->value + 4, &val_len);

			if (!tmp_data2) {
				X509V3error(X509V3_R_ILLEGAL_HEX_DIGIT);
				X509V3_conf_err(val);
				goto err;
			}

			tmp_data = realloc((*policy)->data,
			    (*policy)->length + val_len + 1);
			if (tmp_data) {
				(*policy)->data = tmp_data;
				memcpy(&(*policy)->data[(*policy)->length],
				    tmp_data2, val_len);
				(*policy)->length += val_len;
				(*policy)->data[(*policy)->length] = '\0';
			} else {
				free(tmp_data2);
				free((*policy)->data);
				(*policy)->data = NULL;
				(*policy)->length = 0;
				X509V3error(ERR_R_MALLOC_FAILURE);
				X509V3_conf_err(val);
				goto err;
			}
			free(tmp_data2);
		}
		else if (strncmp(val->value, "file:", 5) == 0) {
			unsigned char buf[2048];
			int n;
			BIO *b = BIO_new_file(val->value + 5, "r");
			if (!b) {
				X509V3error(ERR_R_BIO_LIB);
				X509V3_conf_err(val);
				goto err;
			}
			while ((n = BIO_read(b, buf, sizeof(buf))) > 0 ||
			    (n == 0 && BIO_should_retry(b))) {
				if (!n)
					continue;

				tmp_data = realloc((*policy)->data,
				    (*policy)->length + n + 1);

				if (!tmp_data)
					break;

				(*policy)->data = tmp_data;
				memcpy(&(*policy)->data[(*policy)->length],
				    buf, n);
				(*policy)->length += n;
				(*policy)->data[(*policy)->length] = '\0';
			}
			BIO_free_all(b);

			if (n < 0) {
				X509V3error(ERR_R_BIO_LIB);
				X509V3_conf_err(val);
				goto err;
			}
		}
		else if (strncmp(val->value, "text:", 5) == 0) {
			val_len = strlen(val->value + 5);
			tmp_data = realloc((*policy)->data,
			    (*policy)->length + val_len + 1);
			if (tmp_data) {
				(*policy)->data = tmp_data;
				memcpy(&(*policy)->data[(*policy)->length],
				    val->value + 5, val_len);
				(*policy)->length += val_len;
				(*policy)->data[(*policy)->length] = '\0';
			} else {
				free((*policy)->data);
				(*policy)->data = NULL;
				(*policy)->length = 0;
				X509V3error(ERR_R_MALLOC_FAILURE);
				X509V3_conf_err(val);
				goto err;
			}
		} else {
			X509V3error(X509V3_R_INCORRECT_POLICY_SYNTAX_TAG);
			X509V3_conf_err(val);
			goto err;
		}
		if (!tmp_data) {
			X509V3error(ERR_R_MALLOC_FAILURE);
			X509V3_conf_err(val);
			goto err;
		}
	}
	return 1;

err:
	if (free_policy) {
		ASN1_OCTET_STRING_free(*policy);
		*policy = NULL;
	}
	return 0;
}

static PROXY_CERT_INFO_EXTENSION *
r2i_pci(X509V3_EXT_METHOD *method, X509V3_CTX *ctx, char *value)
{
	PROXY_CERT_INFO_EXTENSION *pci = NULL;
	STACK_OF(CONF_VALUE) *vals;
	ASN1_OBJECT *language = NULL;
	ASN1_INTEGER *pathlen = NULL;
	ASN1_OCTET_STRING *policy = NULL;
	int i, j;

	vals = X509V3_parse_list(value);
	for (i = 0; i < sk_CONF_VALUE_num(vals); i++) {
		CONF_VALUE *cnf = sk_CONF_VALUE_value(vals, i);
		if (!cnf->name || (*cnf->name != '@' && !cnf->value)) {
			X509V3error(X509V3_R_INVALID_PROXY_POLICY_SETTING);
			X509V3_conf_err(cnf);
			goto err;
		}
		if (*cnf->name == '@') {
			STACK_OF(CONF_VALUE) *sect;
			int success_p = 1;

			sect = X509V3_get_section(ctx, cnf->name + 1);
			if (!sect) {
				X509V3error(X509V3_R_INVALID_SECTION);
				X509V3_conf_err(cnf);
				goto err;
			}
			for (j = 0; success_p &&
			    j < sk_CONF_VALUE_num(sect); j++) {
				success_p = process_pci_value(
				    sk_CONF_VALUE_value(sect, j),
				    &language, &pathlen, &policy);
			}
			X509V3_section_free(ctx, sect);
			if (!success_p)
				goto err;
		} else {
			if (!process_pci_value(cnf,
			    &language, &pathlen, &policy)) {
				X509V3_conf_err(cnf);
				goto err;
			}
		}
	}

	/* Language is mandatory */
	if (!language) {
		X509V3error(X509V3_R_NO_PROXY_CERT_POLICY_LANGUAGE_DEFINED);
		goto err;
	}
	i = OBJ_obj2nid(language);
	if ((i == NID_Independent || i == NID_id_ppl_inheritAll) && policy) {
		X509V3error(X509V3_R_POLICY_WHEN_PROXY_LANGUAGE_REQUIRES_NO_POLICY);
		goto err;
	}

	pci = PROXY_CERT_INFO_EXTENSION_new();
	if (!pci) {
		X509V3error(ERR_R_MALLOC_FAILURE);
		goto err;
	}

	pci->proxyPolicy->policyLanguage = language;
	language = NULL;
	pci->proxyPolicy->policy = policy;
	policy = NULL;
	pci->pcPathLengthConstraint = pathlen;
	pathlen = NULL;
	goto end;

err:
	ASN1_OBJECT_free(language);
	language = NULL;
	ASN1_INTEGER_free(pathlen);
	pathlen = NULL;
	ASN1_OCTET_STRING_free(policy);
	policy = NULL;
end:
	sk_CONF_VALUE_pop_free(vals, X509V3_conf_free);
	return pci;
}
