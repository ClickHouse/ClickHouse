/* $OpenBSD: tls_verify.c,v 1.19 2017/04/10 17:11:13 jsing Exp $ */
/*
 * Copyright (c) 2014 Jeremie Courreges-Anglas <jca@openbsd.org>
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

#include <sys/socket.h>

#include <arpa/inet.h>
#include <netinet/in.h>

#include <string.h>

#include <openssl/x509v3.h>

#include <tls.h>
#include "tls_internal.h"

static int
tls_match_name(const char *cert_name, const char *name)
{
	const char *cert_domain, *domain, *next_dot;

	if (strcasecmp(cert_name, name) == 0)
		return 0;

	/* Wildcard match? */
	if (cert_name[0] == '*') {
		/*
		 * Valid wildcards:
		 * - "*.domain.tld"
		 * - "*.sub.domain.tld"
		 * - etc.
		 * Reject "*.tld".
		 * No attempt to prevent the use of eg. "*.co.uk".
		 */
		cert_domain = &cert_name[1];
		/* Disallow "*"  */
		if (cert_domain[0] == '\0')
			return -1;
		/* Disallow "*foo" */
		if (cert_domain[0] != '.')
			return -1;
		/* Disallow "*.." */
		if (cert_domain[1] == '.')
			return -1;
		next_dot = strchr(&cert_domain[1], '.');
		/* Disallow "*.bar" */
		if (next_dot == NULL)
			return -1;
		/* Disallow "*.bar.." */
		if (next_dot[1] == '.')
			return -1;

		domain = strchr(name, '.');

		/* No wildcard match against a name with no host part. */
		if (name[0] == '.')
			return -1;
		/* No wildcard match against a name with no domain part. */
		if (domain == NULL || strlen(domain) == 1)
			return -1;

		if (strcasecmp(cert_domain, domain) == 0)
			return 0;
	}

	return -1;
}

/*
 * See RFC 5280 section 4.2.1.6 for SubjectAltName details.
 * alt_match is set to 1 if a matching alternate name is found.
 * alt_exists is set to 1 if any known alternate name exists in the certificate.
 */
static int
tls_check_subject_altname(struct tls *ctx, X509 *cert, const char *name,
    int *alt_match, int *alt_exists)
{
	STACK_OF(GENERAL_NAME) *altname_stack = NULL;
	union tls_addr addrbuf;
	int addrlen, type;
	int count, i;
	int rv = 0;

	*alt_match = 0;
	*alt_exists = 0;

	altname_stack = X509_get_ext_d2i(cert, NID_subject_alt_name,
	    NULL, NULL);
	if (altname_stack == NULL)
		return 0;

	if (inet_pton(AF_INET, name, &addrbuf) == 1) {
		type = GEN_IPADD;
		addrlen = 4;
	} else if (inet_pton(AF_INET6, name, &addrbuf) == 1) {
		type = GEN_IPADD;
		addrlen = 16;
	} else {
		type = GEN_DNS;
		addrlen = 0;
	}

	count = sk_GENERAL_NAME_num(altname_stack);
	for (i = 0; i < count; i++) {
		GENERAL_NAME	*altname;

		altname = sk_GENERAL_NAME_value(altname_stack, i);

		if (altname->type == GEN_DNS || altname->type == GEN_IPADD)
			*alt_exists = 1;

		if (altname->type != type)
			continue;

		if (type == GEN_DNS) {
			unsigned char	*data;
			int		 format, len;

			format = ASN1_STRING_type(altname->d.dNSName);
			if (format == V_ASN1_IA5STRING) {
				data = ASN1_STRING_data(altname->d.dNSName);
				len = ASN1_STRING_length(altname->d.dNSName);

				if (len < 0 || (size_t)len != strlen(data)) {
					tls_set_errorx(ctx,
					    "error verifying name '%s': "
					    "NUL byte in subjectAltName, "
					    "probably a malicious certificate",
					    name);
					rv = -1;
					break;
				}

				/*
				 * Per RFC 5280 section 4.2.1.6:
				 * " " is a legal domain name, but that
				 * dNSName must be rejected.
				 */
				if (strcmp(data, " ") == 0) {
					tls_set_errorx(ctx,
					    "error verifying name '%s': "
					    "a dNSName of \" \" must not be "
					    "used", name);
					rv = -1;
					break;
				}

				if (tls_match_name(data, name) == 0) {
					*alt_match = 1;
					break;
				}
			} else {
#ifdef DEBUG
				fprintf(stdout, "%s: unhandled subjectAltName "
				    "dNSName encoding (%d)\n", getprogname(),
				    format);
#endif
			}

		} else if (type == GEN_IPADD) {
			unsigned char	*data;
			int		 datalen;

			datalen = ASN1_STRING_length(altname->d.iPAddress);
			data = ASN1_STRING_data(altname->d.iPAddress);

			if (datalen < 0) {
				tls_set_errorx(ctx,
				    "Unexpected negative length for an "
				    "IP address: %d", datalen);
				rv = -1;
				break;
			}

			/*
			 * Per RFC 5280 section 4.2.1.6:
			 * IPv4 must use 4 octets and IPv6 must use 16 octets.
			 */
			if (datalen == addrlen &&
			    memcmp(data, &addrbuf, addrlen) == 0) {
				*alt_match = 1;
				break;
			}
		}
	}

	sk_GENERAL_NAME_pop_free(altname_stack, GENERAL_NAME_free);
	return rv;
}

static int
tls_check_common_name(struct tls *ctx, X509 *cert, const char *name,
    int *cn_match)
{
	X509_NAME *subject_name;
	char *common_name = NULL;
	union tls_addr addrbuf;
	int common_name_len;
	int rv = 0;

	*cn_match = 0;

	subject_name = X509_get_subject_name(cert);
	if (subject_name == NULL)
		goto out;

	common_name_len = X509_NAME_get_text_by_NID(subject_name,
	    NID_commonName, NULL, 0);
	if (common_name_len < 0)
		goto out;

	common_name = calloc(common_name_len + 1, 1);
	if (common_name == NULL)
		goto out;

	X509_NAME_get_text_by_NID(subject_name, NID_commonName, common_name,
	    common_name_len + 1);

	/* NUL bytes in CN? */
	if (common_name_len < 0 ||
	    (size_t)common_name_len != strlen(common_name)) {
		tls_set_errorx(ctx, "error verifying name '%s': "
		    "NUL byte in Common Name field, "
		    "probably a malicious certificate", name);
		rv = -1;
		goto out;
	}

	/*
	 * We don't want to attempt wildcard matching against IP addresses,
	 * so perform a simple comparison here.
	 */
	if (inet_pton(AF_INET,  name, &addrbuf) == 1 ||
	    inet_pton(AF_INET6, name, &addrbuf) == 1) {
		if (strcmp(common_name, name) == 0)
			*cn_match = 1;
		goto out;
	}

	if (tls_match_name(common_name, name) == 0)
		*cn_match = 1;

 out:
	free(common_name);
	return rv;
}

int
tls_check_name(struct tls *ctx, X509 *cert, const char *name, int *match)
{
	int alt_exists;

	*match = 0;

	if (tls_check_subject_altname(ctx, cert, name, match,
	    &alt_exists) == -1)
		return -1;

	/*
	 * As per RFC 6125 section 6.4.4, if any known alternate name existed
	 * in the certificate, we do not attempt to match on the CN.
	 */
	if (*match || alt_exists)
		return 0;

	return tls_check_common_name(ctx, cert, name, match);
}
