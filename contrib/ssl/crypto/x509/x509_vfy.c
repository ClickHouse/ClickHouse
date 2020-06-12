/* $OpenBSD: x509_vfy.c,v 1.66 2017/08/27 01:39:26 beck Exp $ */
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

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <openssl/opensslconf.h>

#include <openssl/asn1.h>
#include <openssl/buffer.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/lhash.h>
#include <openssl/objects.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include "asn1_locl.h"
#include "vpm_int.h"
#include "x509_lcl.h"

/* CRL score values */

/* No unhandled critical extensions */

#define CRL_SCORE_NOCRITICAL	0x100

/* certificate is within CRL scope */

#define CRL_SCORE_SCOPE		0x080

/* CRL times valid */

#define CRL_SCORE_TIME		0x040

/* Issuer name matches certificate */

#define CRL_SCORE_ISSUER_NAME	0x020

/* If this score or above CRL is probably valid */

#define CRL_SCORE_VALID (CRL_SCORE_NOCRITICAL|CRL_SCORE_TIME|CRL_SCORE_SCOPE)

/* CRL issuer is certificate issuer */

#define CRL_SCORE_ISSUER_CERT	0x018

/* CRL issuer is on certificate path */

#define CRL_SCORE_SAME_PATH	0x008

/* CRL issuer matches CRL AKID */

#define CRL_SCORE_AKID		0x004

/* Have a delta CRL with valid times */

#define CRL_SCORE_TIME_DELTA	0x002

static int null_callback(int ok, X509_STORE_CTX *e);
static int check_issued(X509_STORE_CTX *ctx, X509 *x, X509 *issuer);
static X509 *find_issuer(X509_STORE_CTX *ctx, STACK_OF(X509) *sk, X509 *x);
static int check_chain_extensions(X509_STORE_CTX *ctx);
static int check_name_constraints(X509_STORE_CTX *ctx);
static int check_trust(X509_STORE_CTX *ctx);
static int check_revocation(X509_STORE_CTX *ctx);
static int check_cert(X509_STORE_CTX *ctx);
static int check_policy(X509_STORE_CTX *ctx);

static int get_crl_score(X509_STORE_CTX *ctx, X509 **pissuer,
    unsigned int *preasons, X509_CRL *crl, X509 *x);
static int get_crl_delta(X509_STORE_CTX *ctx,
    X509_CRL **pcrl, X509_CRL **pdcrl, X509 *x);
static void get_delta_sk(X509_STORE_CTX *ctx, X509_CRL **dcrl, int *pcrl_score,
    X509_CRL *base, STACK_OF(X509_CRL) *crls);
static void crl_akid_check(X509_STORE_CTX *ctx, X509_CRL *crl, X509 **pissuer,
    int *pcrl_score);
static int crl_crldp_check(X509 *x, X509_CRL *crl, int crl_score,
    unsigned int *preasons);
static int check_crl_path(X509_STORE_CTX *ctx, X509 *x);
static int check_crl_chain(X509_STORE_CTX *ctx, STACK_OF(X509) *cert_path,
    STACK_OF(X509) *crl_path);
static int X509_cmp_time_internal(const ASN1_TIME *ctm, time_t *cmp_time,
    int clamp_notafter);

static int internal_verify(X509_STORE_CTX *ctx);

int ASN1_time_tm_clamp_notafter(struct tm *tm);

static int
null_callback(int ok, X509_STORE_CTX *e)
{
	return ok;
}

#if 0
static int
x509_subject_cmp(X509 **a, X509 **b)
{
	return X509_subject_name_cmp(*a, *b);
}
#endif

/* Return 1 is a certificate is self signed */
static int
cert_self_signed(X509 *x)
{
	X509_check_purpose(x, -1, 0);
	if (x->ex_flags & EXFLAG_SS)
		return 1;
	else
		return 0;
}

static int
check_id_error(X509_STORE_CTX *ctx, int errcode)
{
	ctx->error = errcode;
	ctx->current_cert = ctx->cert;
	ctx->error_depth = 0;
	return ctx->verify_cb(0, ctx);
}

static int
check_hosts(X509 *x, X509_VERIFY_PARAM_ID *id)
{
	size_t i;
	size_t n = sk_OPENSSL_STRING_num(id->hosts);
	char *name;

	free(id->peername);
	id->peername = NULL;

	for (i = 0; i < n; ++i) {
		name = sk_OPENSSL_STRING_value(id->hosts, i);
		if (X509_check_host(x, name, strlen(name), id->hostflags,
		    &id->peername) > 0)
			return 1;
	}
	return n == 0;
}

static int
check_id(X509_STORE_CTX *ctx)
{
	X509_VERIFY_PARAM *vpm = ctx->param;
	X509_VERIFY_PARAM_ID *id = vpm->id;
	X509 *x = ctx->cert;

	if (id->hosts && check_hosts(x, id) <= 0) {
		if (!check_id_error(ctx, X509_V_ERR_HOSTNAME_MISMATCH))
			return 0;
	}
	if (id->email != NULL && X509_check_email(x, id->email, id->emaillen, 0)
	    <= 0) {
		if (!check_id_error(ctx, X509_V_ERR_EMAIL_MISMATCH))
			return 0;
	}
	if (id->ip != NULL && X509_check_ip(x, id->ip, id->iplen, 0) <= 0) {
		if (!check_id_error(ctx, X509_V_ERR_IP_ADDRESS_MISMATCH))
			return 0;
	}
	return 1;
}

int
X509_verify_cert(X509_STORE_CTX *ctx)
{
	X509 *x, *xtmp, *xtmp2, *chain_ss = NULL;
	int bad_chain = 0;
	X509_VERIFY_PARAM *param = ctx->param;
	int depth, i, ok = 0;
	int num, j, retry, trust;
	int (*cb) (int xok, X509_STORE_CTX *xctx);
	STACK_OF(X509) *sktmp = NULL;

	if (ctx->cert == NULL) {
		X509error(X509_R_NO_CERT_SET_FOR_US_TO_VERIFY);
		ctx->error = X509_V_ERR_INVALID_CALL;
		return -1;
	}
	if (ctx->chain != NULL) {
		/*
		 * This X509_STORE_CTX has already been used to verify
		 * a cert. We cannot do another one.
		 */
		X509error(ERR_R_SHOULD_NOT_HAVE_BEEN_CALLED);
		ctx->error = X509_V_ERR_INVALID_CALL;
		return -1;
	}
	if (ctx->error != X509_V_ERR_INVALID_CALL) {
		/*
		 * This X509_STORE_CTX has not been properly initialized.
		 */
		X509error(ERR_R_SHOULD_NOT_HAVE_BEEN_CALLED);
		ctx->error = X509_V_ERR_INVALID_CALL;
		return -1;
	}
	ctx->error = X509_V_OK; /* Initialize to OK */

	cb = ctx->verify_cb;

	/*
	 * First we make sure the chain we are going to build is
	 * present and that the first entry is in place.
	 */
	ctx->chain = sk_X509_new_null();
	if (ctx->chain == NULL || !sk_X509_push(ctx->chain, ctx->cert)) {
		X509error(ERR_R_MALLOC_FAILURE);
		ctx->error = X509_V_ERR_OUT_OF_MEM;
		goto end;
	}
	X509_up_ref(ctx->cert);
	ctx->last_untrusted = 1;

	/* We use a temporary STACK so we can chop and hack at it */
	if (ctx->untrusted != NULL &&
	    (sktmp = sk_X509_dup(ctx->untrusted)) == NULL) {
		X509error(ERR_R_MALLOC_FAILURE);
		ctx->error = X509_V_ERR_OUT_OF_MEM;
		goto end;
	}

	num = sk_X509_num(ctx->chain);
	x = sk_X509_value(ctx->chain, num - 1);
	depth = param->depth;

	for (;;) {
		/* If we have enough, we break */
		/* FIXME: If this happens, we should take
		 * note of it and, if appropriate, use the
		 * X509_V_ERR_CERT_CHAIN_TOO_LONG error code
		 * later.
		 */
		if (depth < num)
			break;
		/* If we are self signed, we break */
		if (cert_self_signed(x))
			break;
		/*
		 * If asked see if we can find issuer in trusted store first
		 */
		if (ctx->param->flags & X509_V_FLAG_TRUSTED_FIRST) {
			ok = ctx->get_issuer(&xtmp, ctx, x);
			if (ok < 0) {
				ctx->error = X509_V_ERR_STORE_LOOKUP;
				goto end;
			}
			/*
			 * If successful for now free up cert so it
			 * will be picked up again later.
			 */
			if (ok > 0) {
				X509_free(xtmp);
				break;
			}
		}
		/* If we were passed a cert chain, use it first */
		if (ctx->untrusted != NULL) {
			xtmp = find_issuer(ctx, sktmp, x);
			if (xtmp != NULL) {
				if (!sk_X509_push(ctx->chain, xtmp)) {
					X509error(ERR_R_MALLOC_FAILURE);
					ctx->error = X509_V_ERR_OUT_OF_MEM;
					ok = 0;
					goto end;
				}
				X509_up_ref(xtmp);
				(void)sk_X509_delete_ptr(sktmp, xtmp);
				ctx->last_untrusted++;
				x = xtmp;
				num++;
				/*
				 * reparse the full chain for the next one
				 */
				continue;
			}
		}
		break;
	}
	/* Remember how many untrusted certs we have */
	j = num;

	/*
	 * At this point, chain should contain a list of untrusted
	 * certificates.  We now need to add at least one trusted one,
	 * if possible, otherwise we complain.
	 */

	do {
		/*
		 * Examine last certificate in chain and see if it is
		 * self signed.
		 */
		i = sk_X509_num(ctx->chain);
		x = sk_X509_value(ctx->chain, i - 1);
		if (cert_self_signed(x)) {
			/* we have a self signed certificate */
			if (i == 1) {
				/*
				 * We have a single self signed
				 * certificate: see if we can find it
				 * in the store. We must have an exact
				 * match to avoid possible
				 * impersonation.
				 */
				ok = ctx->get_issuer(&xtmp, ctx, x);
				if ((ok <= 0) || X509_cmp(x, xtmp)) {
					ctx->error = X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT;
					ctx->current_cert = x;
					ctx->error_depth = i - 1;
					if (ok == 1)
						X509_free(xtmp);
					bad_chain = 1;
					ok = cb(0, ctx);
					if (!ok)
						goto end;
				} else {
					/*
					 * We have a match: replace
					 * certificate with store
					 * version so we get any trust
					 * settings.
					 */
					X509_free(x);
					x = xtmp;
					(void)sk_X509_set(ctx->chain, i - 1, x);
					ctx->last_untrusted = 0;
				}
			} else {
				/*
				 * extract and save self signed
				 * certificate for later use
				 */
				chain_ss = sk_X509_pop(ctx->chain);
				ctx->last_untrusted--;
				num--;
				j--;
				x = sk_X509_value(ctx->chain, num - 1);
			}
		}
		/* We now lookup certs from the certificate store */
		for (;;) {
			/* If we have enough, we break */
			if (depth < num)
				break;
			/* If we are self signed, we break */
			if (cert_self_signed(x))
				break;
			ok = ctx->get_issuer(&xtmp, ctx, x);

			if (ok < 0) {
				ctx->error = X509_V_ERR_STORE_LOOKUP;
				goto end;
			}
			if (ok == 0)
				break;
			x = xtmp;
			if (!sk_X509_push(ctx->chain, x)) {
				X509_free(xtmp);
				X509error(ERR_R_MALLOC_FAILURE);
				ctx->error = X509_V_ERR_OUT_OF_MEM;
				ok = 0;
				goto end;
			}
			num++;
		}

		/* we now have our chain, lets check it... */
		trust = check_trust(ctx);

		/* If explicitly rejected error */
		if (trust == X509_TRUST_REJECTED) {
			ok = 0;
			goto end;
		}
		/*
		 * If it's not explicitly trusted then check if there
		 * is an alternative chain that could be used. We only
		 * do this if we haven't already checked via
		 * TRUSTED_FIRST and the user hasn't switched off
		 * alternate chain checking
		 */
		retry = 0;
		if (trust != X509_TRUST_TRUSTED &&
		    !(ctx->param->flags & X509_V_FLAG_TRUSTED_FIRST) &&
		    !(ctx->param->flags & X509_V_FLAG_NO_ALT_CHAINS)) {
			while (j-- > 1) {
				xtmp2 = sk_X509_value(ctx->chain, j - 1);
				ok = ctx->get_issuer(&xtmp, ctx, xtmp2);
				if (ok < 0)
					goto end;
				/* Check if we found an alternate chain */
				if (ok > 0) {
					/*
					 * Free up the found cert
					 * we'll add it again later
					 */
					X509_free(xtmp);
					/*
					 * Dump all the certs above
					 * this point - we've found an
					 * alternate chain
					 */
					while (num > j) {
						xtmp = sk_X509_pop(ctx->chain);
						X509_free(xtmp);
						num--;
					}
					ctx->last_untrusted = sk_X509_num(ctx->chain);
					retry = 1;
					break;
				}
			}
		}
	} while (retry);

	/*
	 * If not explicitly trusted then indicate error unless it's a single
	 * self signed certificate in which case we've indicated an error already
	 * and set bad_chain == 1
	 */
	if (trust != X509_TRUST_TRUSTED && !bad_chain) {
		if ((chain_ss == NULL) || !ctx->check_issued(ctx, x, chain_ss)) {
			if (ctx->last_untrusted >= num)
				ctx->error = X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY;
			else
				ctx->error = X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT;
			ctx->current_cert = x;
		} else {
			if (!sk_X509_push(ctx->chain, chain_ss)) {
				X509_free(chain_ss);
				X509error(ERR_R_MALLOC_FAILURE);
				return 0;
			}
			num++;
			ctx->last_untrusted = num;
			ctx->current_cert = chain_ss;
			ctx->error = X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN;
			chain_ss = NULL;
		}

		ctx->error_depth = num - 1;
		bad_chain = 1;
		ok = cb(0, ctx);
		if (!ok)
			goto end;
	}

	/* We have the chain complete: now we need to check its purpose */
	ok = check_chain_extensions(ctx);
	if (!ok)
		goto end;

	/* Check name constraints */
	ok = check_name_constraints(ctx);
	if (!ok)
		goto end;

	ok = check_id(ctx);
	if (!ok)
		goto end;
	/*
	 * Check revocation status: we do this after copying parameters because
	 * they may be needed for CRL signature verification.
	 */
	ok = ctx->check_revocation(ctx);
	if (!ok)
		goto end;

	/* At this point, we have a chain and need to verify it */
	if (ctx->verify != NULL)
		ok = ctx->verify(ctx);
	else
		ok = internal_verify(ctx);
	if (!ok)
		goto end;

	/* If we get this far evaluate policies */
	if (!bad_chain && (ctx->param->flags & X509_V_FLAG_POLICY_CHECK))
		ok = ctx->check_policy(ctx);

 end:
	if (sktmp != NULL)
		sk_X509_free(sktmp);
	X509_free(chain_ss);

	/* Safety net, error returns must set ctx->error */
	if (ok <= 0 && ctx->error == X509_V_OK)
		ctx->error = X509_V_ERR_UNSPECIFIED;
	return ok;
}

/* Given a STACK_OF(X509) find the issuer of cert (if any)
 */

static X509 *
find_issuer(X509_STORE_CTX *ctx, STACK_OF(X509) *sk, X509 *x)
{
	int i;
	X509 *issuer, *rv = NULL;

	for (i = 0; i < sk_X509_num(sk); i++) {
		issuer = sk_X509_value(sk, i);
		if (ctx->check_issued(ctx, x, issuer)) {
			rv = issuer;
			if (x509_check_cert_time(ctx, rv, -1))
				break;
		}
	}
	return rv;
}

/* Given a possible certificate and issuer check them */

static int
check_issued(X509_STORE_CTX *ctx, X509 *x, X509 *issuer)
{
	int ret;

	ret = X509_check_issued(issuer, x);
	if (ret == X509_V_OK)
		return 1;
	/* If we haven't asked for issuer errors don't set ctx */
	if (!(ctx->param->flags & X509_V_FLAG_CB_ISSUER_CHECK))
		return 0;

	ctx->error = ret;
	ctx->current_cert = x;
	ctx->current_issuer = issuer;
	return ctx->verify_cb(0, ctx);
}

/* Alternative lookup method: look from a STACK stored in other_ctx */

static int
get_issuer_sk(X509 **issuer, X509_STORE_CTX *ctx, X509 *x)
{
	*issuer = find_issuer(ctx, ctx->other_ctx, x);
	if (*issuer) {
		CRYPTO_add(&(*issuer)->references, 1, CRYPTO_LOCK_X509);
		return 1;
	} else
		return 0;
}

/* Check a certificate chains extensions for consistency
 * with the supplied purpose
 */

static int
check_chain_extensions(X509_STORE_CTX *ctx)
{
#ifdef OPENSSL_NO_CHAIN_VERIFY
	return 1;
#else
	int i, ok = 0, must_be_ca, plen = 0;
	X509 *x;
	int (*cb)(int xok, X509_STORE_CTX *xctx);
	int proxy_path_length = 0;
	int purpose;
	int allow_proxy_certs;

	cb = ctx->verify_cb;

	/* must_be_ca can have 1 of 3 values:
	   -1: we accept both CA and non-CA certificates, to allow direct
	       use of self-signed certificates (which are marked as CA).
	   0:  we only accept non-CA certificates.  This is currently not
	       used, but the possibility is present for future extensions.
	   1:  we only accept CA certificates.  This is currently used for
	       all certificates in the chain except the leaf certificate.
	*/
	must_be_ca = -1;

	/* CRL path validation */
	if (ctx->parent) {
		allow_proxy_certs = 0;
		purpose = X509_PURPOSE_CRL_SIGN;
	} else {
		allow_proxy_certs =
		    !!(ctx->param->flags & X509_V_FLAG_ALLOW_PROXY_CERTS);
		purpose = ctx->param->purpose;
	}

	/* Check all untrusted certificates */
	for (i = 0; i < ctx->last_untrusted; i++) {
		int ret;
		x = sk_X509_value(ctx->chain, i);
		if (!(ctx->param->flags & X509_V_FLAG_IGNORE_CRITICAL) &&
		    (x->ex_flags & EXFLAG_CRITICAL)) {
			ctx->error = X509_V_ERR_UNHANDLED_CRITICAL_EXTENSION;
			ctx->error_depth = i;
			ctx->current_cert = x;
			ok = cb(0, ctx);
			if (!ok)
				goto end;
		}
		if (!allow_proxy_certs && (x->ex_flags & EXFLAG_PROXY)) {
			ctx->error = X509_V_ERR_PROXY_CERTIFICATES_NOT_ALLOWED;
			ctx->error_depth = i;
			ctx->current_cert = x;
			ok = cb(0, ctx);
			if (!ok)
				goto end;
		}
		ret = X509_check_ca(x);
		switch (must_be_ca) {
		case -1:
			if ((ctx->param->flags & X509_V_FLAG_X509_STRICT) &&
			    (ret != 1) && (ret != 0)) {
				ret = 0;
				ctx->error = X509_V_ERR_INVALID_CA;
			} else
				ret = 1;
			break;
		case 0:
			if (ret != 0) {
				ret = 0;
				ctx->error = X509_V_ERR_INVALID_NON_CA;
			} else
				ret = 1;
			break;
		default:
			if ((ret == 0) ||
			    ((ctx->param->flags & X509_V_FLAG_X509_STRICT) &&
			    (ret != 1))) {
				ret = 0;
				ctx->error = X509_V_ERR_INVALID_CA;
			} else
				ret = 1;
			break;
		}
		if (ret == 0) {
			ctx->error_depth = i;
			ctx->current_cert = x;
			ok = cb(0, ctx);
			if (!ok)
				goto end;
		}
		if (ctx->param->purpose > 0) {
			ret = X509_check_purpose(x, purpose, must_be_ca > 0);
			if ((ret == 0) ||
			    ((ctx->param->flags & X509_V_FLAG_X509_STRICT) &&
			    (ret != 1))) {
				ctx->error = X509_V_ERR_INVALID_PURPOSE;
				ctx->error_depth = i;
				ctx->current_cert = x;
				ok = cb(0, ctx);
				if (!ok)
					goto end;
			}
		}
		/* Check pathlen if not self issued */
		if ((i > 1) && !(x->ex_flags & EXFLAG_SI) &&
		    (x->ex_pathlen != -1) &&
		    (plen > (x->ex_pathlen + proxy_path_length + 1))) {
			ctx->error = X509_V_ERR_PATH_LENGTH_EXCEEDED;
			ctx->error_depth = i;
			ctx->current_cert = x;
			ok = cb(0, ctx);
			if (!ok)
				goto end;
		}
		/* Increment path length if not self issued */
		if (!(x->ex_flags & EXFLAG_SI))
			plen++;
		/* If this certificate is a proxy certificate, the next
		   certificate must be another proxy certificate or a EE
		   certificate.  If not, the next certificate must be a
		   CA certificate.  */
		if (x->ex_flags & EXFLAG_PROXY) {
			if (x->ex_pcpathlen != -1 && i > x->ex_pcpathlen) {
				ctx->error =
				    X509_V_ERR_PROXY_PATH_LENGTH_EXCEEDED;
				ctx->error_depth = i;
				ctx->current_cert = x;
				ok = cb(0, ctx);
				if (!ok)
					goto end;
			}
			proxy_path_length++;
			must_be_ca = 0;
		} else
			must_be_ca = 1;
	}
	ok = 1;

end:
	return ok;
#endif
}

static int
check_name_constraints(X509_STORE_CTX *ctx)
{
	X509 *x;
	int i, j, rv;

	/* Check name constraints for all certificates */
	for (i = sk_X509_num(ctx->chain) - 1; i >= 0; i--) {
		x = sk_X509_value(ctx->chain, i);
		/* Ignore self issued certs unless last in chain */
		if (i && (x->ex_flags & EXFLAG_SI))
			continue;
		/* Check against constraints for all certificates higher in
		 * chain including trust anchor. Trust anchor not strictly
		 * speaking needed but if it includes constraints it is to be
		 * assumed it expects them to be obeyed.
		 */
		for (j = sk_X509_num(ctx->chain) - 1; j > i; j--) {
			NAME_CONSTRAINTS *nc = sk_X509_value(ctx->chain, j)->nc;
			if (nc) {
				rv = NAME_CONSTRAINTS_check(x, nc);
				if (rv != X509_V_OK) {
					ctx->error = rv;
					ctx->error_depth = i;
					ctx->current_cert = x;
					if (!ctx->verify_cb(0, ctx))
						return 0;
				}
			}
		}
	}
	return 1;
}

/* Given a certificate try and find an exact match in the store */

static X509 *lookup_cert_match(X509_STORE_CTX *ctx, X509 *x)
{
	STACK_OF(X509) *certs;
	X509 *xtmp = NULL;
	size_t i;

	/* Lookup all certs with matching subject name */
	certs = ctx->lookup_certs(ctx, X509_get_subject_name(x));
	if (certs == NULL)
		return NULL;

	/* Look for exact match */
	for (i = 0; i < sk_X509_num(certs); i++) {
		xtmp = sk_X509_value(certs, i);
		if (!X509_cmp(xtmp, x))
			break;
	}

	if (i < sk_X509_num(certs))
		X509_up_ref(xtmp);
	else
		xtmp = NULL;

	sk_X509_pop_free(certs, X509_free);
	return xtmp;
}

static int check_trust(X509_STORE_CTX *ctx)
{
	size_t i;
	int ok;
	X509 *x = NULL;
	int (*cb) (int xok, X509_STORE_CTX *xctx);

	cb = ctx->verify_cb;
	/* Check all trusted certificates in chain */
	for (i = ctx->last_untrusted; i < sk_X509_num(ctx->chain); i++) {
		x = sk_X509_value(ctx->chain, i);
		ok = X509_check_trust(x, ctx->param->trust, 0);

		/* If explicitly trusted return trusted */
		if (ok == X509_TRUST_TRUSTED)
			return X509_TRUST_TRUSTED;
		/*
		 * If explicitly rejected notify callback and reject if not
		 * overridden.
		 */
		if (ok == X509_TRUST_REJECTED) {
			ctx->error_depth = i;
			ctx->current_cert = x;
			ctx->error = X509_V_ERR_CERT_REJECTED;
			ok = cb(0, ctx);
			if (!ok)
				return X509_TRUST_REJECTED;
		}
	}
	/*
	 * If we accept partial chains and have at least one trusted certificate
	 * return success.
	 */
	if (ctx->param->flags & X509_V_FLAG_PARTIAL_CHAIN) {
		X509 *mx;
		if (ctx->last_untrusted < (int)sk_X509_num(ctx->chain))
			return X509_TRUST_TRUSTED;
		x = sk_X509_value(ctx->chain, 0);
		mx = lookup_cert_match(ctx, x);
		if (mx) {
			(void)sk_X509_set(ctx->chain, 0, mx);
			X509_free(x);
			ctx->last_untrusted = 0;
			return X509_TRUST_TRUSTED;
		}
	}

	/*
	 * If no trusted certs in chain at all return untrusted and allow
	 * standard (no issuer cert) etc errors to be indicated.
	 */
	return X509_TRUST_UNTRUSTED;
}

static int
check_revocation(X509_STORE_CTX *ctx)
{
	int i, last, ok;

	if (!(ctx->param->flags & X509_V_FLAG_CRL_CHECK))
		return 1;
	if (ctx->param->flags & X509_V_FLAG_CRL_CHECK_ALL)
		last = sk_X509_num(ctx->chain) - 1;
	else {
		/* If checking CRL paths this isn't the EE certificate */
		if (ctx->parent)
			return 1;
		last = 0;
	}
	for (i = 0; i <= last; i++) {
		ctx->error_depth = i;
		ok = check_cert(ctx);
		if (!ok)
			return ok;
	}
	return 1;
}

static int
check_cert(X509_STORE_CTX *ctx)
{
	X509_CRL *crl = NULL, *dcrl = NULL;
	X509 *x;
	int ok = 0, cnum;
	unsigned int last_reasons;

	cnum = ctx->error_depth;
	x = sk_X509_value(ctx->chain, cnum);
	ctx->current_cert = x;
	ctx->current_issuer = NULL;
	ctx->current_crl_score = 0;
	ctx->current_reasons = 0;
	while (ctx->current_reasons != CRLDP_ALL_REASONS) {
		last_reasons = ctx->current_reasons;
		/* Try to retrieve relevant CRL */
		if (ctx->get_crl)
			ok = ctx->get_crl(ctx, &crl, x);
		else
			ok = get_crl_delta(ctx, &crl, &dcrl, x);
		/* If error looking up CRL, nothing we can do except
		 * notify callback
		 */
		if (!ok) {
			ctx->error = X509_V_ERR_UNABLE_TO_GET_CRL;
			ok = ctx->verify_cb(0, ctx);
			goto err;
		}
		ctx->current_crl = crl;
		ok = ctx->check_crl(ctx, crl);
		if (!ok)
			goto err;

		if (dcrl) {
			ok = ctx->check_crl(ctx, dcrl);
			if (!ok)
				goto err;
			ok = ctx->cert_crl(ctx, dcrl, x);
			if (!ok)
				goto err;
		} else
			ok = 1;

		/* Don't look in full CRL if delta reason is removefromCRL */
		if (ok != 2) {
			ok = ctx->cert_crl(ctx, crl, x);
			if (!ok)
				goto err;
		}

		ctx->current_crl = NULL;
		X509_CRL_free(crl);
		X509_CRL_free(dcrl);
		crl = NULL;
		dcrl = NULL;
		/* If reasons not updated we wont get anywhere by
		 * another iteration, so exit loop.
		 */
		if (last_reasons == ctx->current_reasons) {
			ctx->error = X509_V_ERR_UNABLE_TO_GET_CRL;
			ok = ctx->verify_cb(0, ctx);
			goto err;
		}
	}

err:
	ctx->current_crl = NULL;
	X509_CRL_free(crl);
	X509_CRL_free(dcrl);
	return ok;
}

/* Check CRL times against values in X509_STORE_CTX */

static int
check_crl_time(X509_STORE_CTX *ctx, X509_CRL *crl, int notify)
{
	time_t *ptime = NULL;
	int i;

	if (ctx->param->flags & X509_V_FLAG_NO_CHECK_TIME)
		return (1);

	if (ctx->param->flags & X509_V_FLAG_USE_CHECK_TIME)
		ptime = &ctx->param->check_time;

	if (notify)
		ctx->current_crl = crl;

	i = X509_cmp_time(X509_CRL_get_lastUpdate(crl), ptime);
	if (i == 0) {
		if (!notify)
			return 0;
		ctx->error = X509_V_ERR_ERROR_IN_CRL_LAST_UPDATE_FIELD;
		if (!ctx->verify_cb(0, ctx))
			return 0;
	}

	if (i > 0) {
		if (!notify)
			return 0;
		ctx->error = X509_V_ERR_CRL_NOT_YET_VALID;
		if (!ctx->verify_cb(0, ctx))
			return 0;
	}

	if (X509_CRL_get_nextUpdate(crl)) {
		i = X509_cmp_time(X509_CRL_get_nextUpdate(crl), ptime);

		if (i == 0) {
			if (!notify)
				return 0;
			ctx->error = X509_V_ERR_ERROR_IN_CRL_NEXT_UPDATE_FIELD;
			if (!ctx->verify_cb(0, ctx))
				return 0;
		}
		/* Ignore expiry of base CRL is delta is valid */
		if ((i < 0) &&
		    !(ctx->current_crl_score & CRL_SCORE_TIME_DELTA)) {
			if (!notify)
				return 0;
			ctx->error = X509_V_ERR_CRL_HAS_EXPIRED;
			if (!ctx->verify_cb(0, ctx))
				return 0;
		}
	}

	if (notify)
		ctx->current_crl = NULL;

	return 1;
}

static int
get_crl_sk(X509_STORE_CTX *ctx, X509_CRL **pcrl, X509_CRL **pdcrl,
    X509 **pissuer, int *pscore, unsigned int *preasons,
    STACK_OF(X509_CRL) *crls)
{
	int i, crl_score, best_score = *pscore;
	unsigned int reasons, best_reasons = 0;
	X509 *x = ctx->current_cert;
	X509_CRL *crl, *best_crl = NULL;
	X509 *crl_issuer = NULL, *best_crl_issuer = NULL;

	for (i = 0; i < sk_X509_CRL_num(crls); i++) {
		crl = sk_X509_CRL_value(crls, i);
		reasons = *preasons;
		crl_score = get_crl_score(ctx, &crl_issuer, &reasons, crl, x);

		if (crl_score > best_score) {
			best_crl = crl;
			best_crl_issuer = crl_issuer;
			best_score = crl_score;
			best_reasons = reasons;
		}
	}

	if (best_crl) {
		if (*pcrl)
			X509_CRL_free(*pcrl);
		*pcrl = best_crl;
		*pissuer = best_crl_issuer;
		*pscore = best_score;
		*preasons = best_reasons;
		CRYPTO_add(&best_crl->references, 1, CRYPTO_LOCK_X509_CRL);
		if (*pdcrl) {
			X509_CRL_free(*pdcrl);
			*pdcrl = NULL;
		}
		get_delta_sk(ctx, pdcrl, pscore, best_crl, crls);
	}

	if (best_score >= CRL_SCORE_VALID)
		return 1;

	return 0;
}

/* Compare two CRL extensions for delta checking purposes. They should be
 * both present or both absent. If both present all fields must be identical.
 */

static int
crl_extension_match(X509_CRL *a, X509_CRL *b, int nid)
{
	ASN1_OCTET_STRING *exta, *extb;
	int i;

	i = X509_CRL_get_ext_by_NID(a, nid, -1);
	if (i >= 0) {
		/* Can't have multiple occurrences */
		if (X509_CRL_get_ext_by_NID(a, nid, i) != -1)
			return 0;
		exta = X509_EXTENSION_get_data(X509_CRL_get_ext(a, i));
	} else
		exta = NULL;

	i = X509_CRL_get_ext_by_NID(b, nid, -1);

	if (i >= 0) {
		if (X509_CRL_get_ext_by_NID(b, nid, i) != -1)
			return 0;
		extb = X509_EXTENSION_get_data(X509_CRL_get_ext(b, i));
	} else
		extb = NULL;

	if (!exta && !extb)
		return 1;

	if (!exta || !extb)
		return 0;

	if (ASN1_OCTET_STRING_cmp(exta, extb))
		return 0;

	return 1;
}

/* See if a base and delta are compatible */

static int
check_delta_base(X509_CRL *delta, X509_CRL *base)
{
	/* Delta CRL must be a delta */
	if (!delta->base_crl_number)
		return 0;
	/* Base must have a CRL number */
	if (!base->crl_number)
		return 0;
	/* Issuer names must match */
	if (X509_NAME_cmp(X509_CRL_get_issuer(base),
	    X509_CRL_get_issuer(delta)))
		return 0;
	/* AKID and IDP must match */
	if (!crl_extension_match(delta, base, NID_authority_key_identifier))
		return 0;
	if (!crl_extension_match(delta, base, NID_issuing_distribution_point))
		return 0;
	/* Delta CRL base number must not exceed Full CRL number. */
	if (ASN1_INTEGER_cmp(delta->base_crl_number, base->crl_number) > 0)
		return 0;
	/* Delta CRL number must exceed full CRL number */
	if (ASN1_INTEGER_cmp(delta->crl_number, base->crl_number) > 0)
		return 1;
	return 0;
}

/* For a given base CRL find a delta... maybe extend to delta scoring
 * or retrieve a chain of deltas...
 */

static void
get_delta_sk(X509_STORE_CTX *ctx, X509_CRL **dcrl, int *pscore, X509_CRL *base,
    STACK_OF(X509_CRL) *crls)
{
	X509_CRL *delta;
	int i;

	if (!(ctx->param->flags & X509_V_FLAG_USE_DELTAS))
		return;
	if (!((ctx->current_cert->ex_flags | base->flags) & EXFLAG_FRESHEST))
		return;
	for (i = 0; i < sk_X509_CRL_num(crls); i++) {
		delta = sk_X509_CRL_value(crls, i);
		if (check_delta_base(delta, base)) {
			if (check_crl_time(ctx, delta, 0))
				*pscore |= CRL_SCORE_TIME_DELTA;
			CRYPTO_add(&delta->references, 1, CRYPTO_LOCK_X509_CRL);
			*dcrl = delta;
			return;
		}
	}
	*dcrl = NULL;
}

/* For a given CRL return how suitable it is for the supplied certificate 'x'.
 * The return value is a mask of several criteria.
 * If the issuer is not the certificate issuer this is returned in *pissuer.
 * The reasons mask is also used to determine if the CRL is suitable: if
 * no new reasons the CRL is rejected, otherwise reasons is updated.
 */

static int
get_crl_score(X509_STORE_CTX *ctx, X509 **pissuer, unsigned int *preasons,
    X509_CRL *crl, X509 *x)
{
	int crl_score = 0;
	unsigned int tmp_reasons = *preasons, crl_reasons;

	/* First see if we can reject CRL straight away */

	/* Invalid IDP cannot be processed */
	if (crl->idp_flags & IDP_INVALID)
		return 0;
	/* Reason codes or indirect CRLs need extended CRL support */
	if (!(ctx->param->flags & X509_V_FLAG_EXTENDED_CRL_SUPPORT)) {
		if (crl->idp_flags & (IDP_INDIRECT | IDP_REASONS))
			return 0;
	} else if (crl->idp_flags & IDP_REASONS) {
		/* If no new reasons reject */
		if (!(crl->idp_reasons & ~tmp_reasons))
			return 0;
	}
	/* Don't process deltas at this stage */
	else if (crl->base_crl_number)
		return 0;
	/* If issuer name doesn't match certificate need indirect CRL */
	if (X509_NAME_cmp(X509_get_issuer_name(x), X509_CRL_get_issuer(crl))) {
		if (!(crl->idp_flags & IDP_INDIRECT))
			return 0;
	} else
		crl_score |= CRL_SCORE_ISSUER_NAME;

	if (!(crl->flags & EXFLAG_CRITICAL))
		crl_score |= CRL_SCORE_NOCRITICAL;

	/* Check expiry */
	if (check_crl_time(ctx, crl, 0))
		crl_score |= CRL_SCORE_TIME;

	/* Check authority key ID and locate certificate issuer */
	crl_akid_check(ctx, crl, pissuer, &crl_score);

	/* If we can't locate certificate issuer at this point forget it */

	if (!(crl_score & CRL_SCORE_AKID))
		return 0;

	/* Check cert for matching CRL distribution points */

	if (crl_crldp_check(x, crl, crl_score, &crl_reasons)) {
		/* If no new reasons reject */
		if (!(crl_reasons & ~tmp_reasons))
			return 0;
		tmp_reasons |= crl_reasons;
		crl_score |= CRL_SCORE_SCOPE;
	}

	*preasons = tmp_reasons;

	return crl_score;
}

static void
crl_akid_check(X509_STORE_CTX *ctx, X509_CRL *crl, X509 **pissuer,
    int *pcrl_score)
{
	X509 *crl_issuer = NULL;
	X509_NAME *cnm = X509_CRL_get_issuer(crl);
	int cidx = ctx->error_depth;
	int i;

	if (cidx != sk_X509_num(ctx->chain) - 1)
		cidx++;

	crl_issuer = sk_X509_value(ctx->chain, cidx);

	if (X509_check_akid(crl_issuer, crl->akid) == X509_V_OK) {
		if (*pcrl_score & CRL_SCORE_ISSUER_NAME) {
			*pcrl_score |= CRL_SCORE_AKID|CRL_SCORE_ISSUER_CERT;
			*pissuer = crl_issuer;
			return;
		}
	}

	for (cidx++; cidx < sk_X509_num(ctx->chain); cidx++) {
		crl_issuer = sk_X509_value(ctx->chain, cidx);
		if (X509_NAME_cmp(X509_get_subject_name(crl_issuer), cnm))
			continue;
		if (X509_check_akid(crl_issuer, crl->akid) == X509_V_OK) {
			*pcrl_score |= CRL_SCORE_AKID|CRL_SCORE_SAME_PATH;
			*pissuer = crl_issuer;
			return;
		}
	}

	/* Anything else needs extended CRL support */

	if (!(ctx->param->flags & X509_V_FLAG_EXTENDED_CRL_SUPPORT))
		return;

	/* Otherwise the CRL issuer is not on the path. Look for it in the
	 * set of untrusted certificates.
	 */
	for (i = 0; i < sk_X509_num(ctx->untrusted); i++) {
		crl_issuer = sk_X509_value(ctx->untrusted, i);
		if (X509_NAME_cmp(X509_get_subject_name(crl_issuer), cnm))
			continue;
		if (X509_check_akid(crl_issuer, crl->akid) == X509_V_OK) {
			*pissuer = crl_issuer;
			*pcrl_score |= CRL_SCORE_AKID;
			return;
		}
	}
}

/* Check the path of a CRL issuer certificate. This creates a new
 * X509_STORE_CTX and populates it with most of the parameters from the
 * parent. This could be optimised somewhat since a lot of path checking
 * will be duplicated by the parent, but this will rarely be used in
 * practice.
 */

static int
check_crl_path(X509_STORE_CTX *ctx, X509 *x)
{
	X509_STORE_CTX crl_ctx;
	int ret;

	/* Don't allow recursive CRL path validation */
	if (ctx->parent)
		return 0;
	if (!X509_STORE_CTX_init(&crl_ctx, ctx->ctx, x, ctx->untrusted)) {
		ret = -1;
		goto err;
	}

	crl_ctx.crls = ctx->crls;
	/* Copy verify params across */
	X509_STORE_CTX_set0_param(&crl_ctx, ctx->param);

	crl_ctx.parent = ctx;
	crl_ctx.verify_cb = ctx->verify_cb;

	/* Verify CRL issuer */
	ret = X509_verify_cert(&crl_ctx);

	if (ret <= 0)
		goto err;

	/* Check chain is acceptable */
	ret = check_crl_chain(ctx, ctx->chain, crl_ctx.chain);

err:
	X509_STORE_CTX_cleanup(&crl_ctx);
	return ret;
}

/* RFC3280 says nothing about the relationship between CRL path
 * and certificate path, which could lead to situations where a
 * certificate could be revoked or validated by a CA not authorised
 * to do so. RFC5280 is more strict and states that the two paths must
 * end in the same trust anchor, though some discussions remain...
 * until this is resolved we use the RFC5280 version
 */

static int
check_crl_chain(X509_STORE_CTX *ctx, STACK_OF(X509) *cert_path,
    STACK_OF(X509) *crl_path)
{
	X509 *cert_ta, *crl_ta;

	cert_ta = sk_X509_value(cert_path, sk_X509_num(cert_path) - 1);
	crl_ta = sk_X509_value(crl_path, sk_X509_num(crl_path) - 1);
	if (!X509_cmp(cert_ta, crl_ta))
		return 1;
	return 0;
}

/* Check for match between two dist point names: three separate cases.
 * 1. Both are relative names and compare X509_NAME types.
 * 2. One full, one relative. Compare X509_NAME to GENERAL_NAMES.
 * 3. Both are full names and compare two GENERAL_NAMES.
 * 4. One is NULL: automatic match.
 */

static int
idp_check_dp(DIST_POINT_NAME *a, DIST_POINT_NAME *b)
{
	X509_NAME *nm = NULL;
	GENERAL_NAMES *gens = NULL;
	GENERAL_NAME *gena, *genb;
	int i, j;

	if (!a || !b)
		return 1;
	if (a->type == 1) {
		if (!a->dpname)
			return 0;
		/* Case 1: two X509_NAME */
		if (b->type == 1) {
			if (!b->dpname)
				return 0;
			if (!X509_NAME_cmp(a->dpname, b->dpname))
				return 1;
			else
				return 0;
		}
		/* Case 2: set name and GENERAL_NAMES appropriately */
		nm = a->dpname;
		gens = b->name.fullname;
	} else if (b->type == 1) {
		if (!b->dpname)
			return 0;
		/* Case 2: set name and GENERAL_NAMES appropriately */
		gens = a->name.fullname;
		nm = b->dpname;
	}

	/* Handle case 2 with one GENERAL_NAMES and one X509_NAME */
	if (nm) {
		for (i = 0; i < sk_GENERAL_NAME_num(gens); i++) {
			gena = sk_GENERAL_NAME_value(gens, i);
			if (gena->type != GEN_DIRNAME)
				continue;
			if (!X509_NAME_cmp(nm, gena->d.directoryName))
				return 1;
		}
		return 0;
	}

	/* Else case 3: two GENERAL_NAMES */

	for (i = 0; i < sk_GENERAL_NAME_num(a->name.fullname); i++) {
		gena = sk_GENERAL_NAME_value(a->name.fullname, i);
		for (j = 0; j < sk_GENERAL_NAME_num(b->name.fullname); j++) {
			genb = sk_GENERAL_NAME_value(b->name.fullname, j);
			if (!GENERAL_NAME_cmp(gena, genb))
				return 1;
		}
	}

	return 0;
}

static int
crldp_check_crlissuer(DIST_POINT *dp, X509_CRL *crl, int crl_score)
{
	int i;
	X509_NAME *nm = X509_CRL_get_issuer(crl);

	/* If no CRLissuer return is successful iff don't need a match */
	if (!dp->CRLissuer)
		return !!(crl_score & CRL_SCORE_ISSUER_NAME);
	for (i = 0; i < sk_GENERAL_NAME_num(dp->CRLissuer); i++) {
		GENERAL_NAME *gen = sk_GENERAL_NAME_value(dp->CRLissuer, i);
		if (gen->type != GEN_DIRNAME)
			continue;
		if (!X509_NAME_cmp(gen->d.directoryName, nm))
			return 1;
	}
	return 0;
}

/* Check CRLDP and IDP */

static int
crl_crldp_check(X509 *x, X509_CRL *crl, int crl_score, unsigned int *preasons)
{
	int i;

	if (crl->idp_flags & IDP_ONLYATTR)
		return 0;
	if (x->ex_flags & EXFLAG_CA) {
		if (crl->idp_flags & IDP_ONLYUSER)
			return 0;
	} else {
		if (crl->idp_flags & IDP_ONLYCA)
			return 0;
	}
	*preasons = crl->idp_reasons;
	for (i = 0; i < sk_DIST_POINT_num(x->crldp); i++) {
		DIST_POINT *dp = sk_DIST_POINT_value(x->crldp, i);
		if (crldp_check_crlissuer(dp, crl, crl_score)) {
			if (!crl->idp ||
			    idp_check_dp(dp->distpoint, crl->idp->distpoint)) {
				*preasons &= dp->dp_reasons;
				return 1;
			}
		}
	}
	if ((!crl->idp || !crl->idp->distpoint) &&
	    (crl_score & CRL_SCORE_ISSUER_NAME))
		return 1;
	return 0;
}

/* Retrieve CRL corresponding to current certificate.
 * If deltas enabled try to find a delta CRL too
 */

static int
get_crl_delta(X509_STORE_CTX *ctx, X509_CRL **pcrl, X509_CRL **pdcrl, X509 *x)
{
	int ok;
	X509 *issuer = NULL;
	int crl_score = 0;
	unsigned int reasons;
	X509_CRL *crl = NULL, *dcrl = NULL;
	STACK_OF(X509_CRL) *skcrl;
	X509_NAME *nm = X509_get_issuer_name(x);

	reasons = ctx->current_reasons;
	ok = get_crl_sk(ctx, &crl, &dcrl, &issuer, &crl_score, &reasons,
	    ctx->crls);
	if (ok)
		goto done;

	/* Lookup CRLs from store */
	skcrl = ctx->lookup_crls(ctx, nm);

	/* If no CRLs found and a near match from get_crl_sk use that */
	if (!skcrl && crl)
		goto done;

	get_crl_sk(ctx, &crl, &dcrl, &issuer, &crl_score, &reasons, skcrl);

	sk_X509_CRL_pop_free(skcrl, X509_CRL_free);

done:

	/* If we got any kind of CRL use it and return success */
	if (crl) {
		ctx->current_issuer = issuer;
		ctx->current_crl_score = crl_score;
		ctx->current_reasons = reasons;
		*pcrl = crl;
		*pdcrl = dcrl;
		return 1;
	}

	return 0;
}

/* Check CRL validity */
static int
check_crl(X509_STORE_CTX *ctx, X509_CRL *crl)
{
	X509 *issuer = NULL;
	EVP_PKEY *ikey = NULL;
	int ok = 0, chnum, cnum;

	cnum = ctx->error_depth;
	chnum = sk_X509_num(ctx->chain) - 1;
	/* if we have an alternative CRL issuer cert use that */
	if (ctx->current_issuer) {
		issuer = ctx->current_issuer;
	} else if (cnum < chnum) {
		/* Else find CRL issuer: if not last certificate then issuer
	 	* is next certificate in chain.
	 	*/
		issuer = sk_X509_value(ctx->chain, cnum + 1);
	} else {
		issuer = sk_X509_value(ctx->chain, chnum);
		/* If not self signed, can't check signature */
		if (!ctx->check_issued(ctx, issuer, issuer)) {
			ctx->error = X509_V_ERR_UNABLE_TO_GET_CRL_ISSUER;
			ok = ctx->verify_cb(0, ctx);
			if (!ok)
				goto err;
		}
	}

	if (issuer) {
		/* Skip most tests for deltas because they have already
		 * been done
		 */
		if (!crl->base_crl_number) {
			/* Check for cRLSign bit if keyUsage present */
			if ((issuer->ex_flags & EXFLAG_KUSAGE) &&
			    !(issuer->ex_kusage & KU_CRL_SIGN)) {
				ctx->error = X509_V_ERR_KEYUSAGE_NO_CRL_SIGN;
				ok = ctx->verify_cb(0, ctx);
				if (!ok)
					goto err;
			}

			if (!(ctx->current_crl_score & CRL_SCORE_SCOPE)) {
				ctx->error = X509_V_ERR_DIFFERENT_CRL_SCOPE;
				ok = ctx->verify_cb(0, ctx);
				if (!ok)
					goto err;
			}

			if (!(ctx->current_crl_score & CRL_SCORE_SAME_PATH)) {
				if (check_crl_path(ctx,
				    ctx->current_issuer) <= 0) {
					ctx->error = X509_V_ERR_CRL_PATH_VALIDATION_ERROR;
					ok = ctx->verify_cb(0, ctx);
					if (!ok)
						goto err;
				}
			}

			if (crl->idp_flags & IDP_INVALID) {
				ctx->error = X509_V_ERR_INVALID_EXTENSION;
				ok = ctx->verify_cb(0, ctx);
				if (!ok)
					goto err;
			}


		}

		if (!(ctx->current_crl_score & CRL_SCORE_TIME)) {
			ok = check_crl_time(ctx, crl, 1);
			if (!ok)
				goto err;
		}

		/* Attempt to get issuer certificate public key */
		ikey = X509_get_pubkey(issuer);

		if (!ikey) {
			ctx->error = X509_V_ERR_UNABLE_TO_DECODE_ISSUER_PUBLIC_KEY;
			ok = ctx->verify_cb(0, ctx);
			if (!ok)
				goto err;
		} else {
			/* Verify CRL signature */
			if (X509_CRL_verify(crl, ikey) <= 0) {
				ctx->error = X509_V_ERR_CRL_SIGNATURE_FAILURE;
				ok = ctx->verify_cb(0, ctx);
				if (!ok)
					goto err;
			}
		}
	}

	ok = 1;

err:
	EVP_PKEY_free(ikey);
	return ok;
}

/* Check certificate against CRL */
static int
cert_crl(X509_STORE_CTX *ctx, X509_CRL *crl, X509 *x)
{
	int ok;
	X509_REVOKED *rev;

	/* The rules changed for this... previously if a CRL contained
	 * unhandled critical extensions it could still be used to indicate
	 * a certificate was revoked. This has since been changed since
	 * critical extension can change the meaning of CRL entries.
	 */
	if (!(ctx->param->flags & X509_V_FLAG_IGNORE_CRITICAL) &&
	    (crl->flags & EXFLAG_CRITICAL)) {
		ctx->error = X509_V_ERR_UNHANDLED_CRITICAL_CRL_EXTENSION;
		ok = ctx->verify_cb(0, ctx);
		if (!ok)
			return 0;
	}
	/* Look for serial number of certificate in CRL
	 * If found make sure reason is not removeFromCRL.
	 */
	if (X509_CRL_get0_by_cert(crl, &rev, x)) {
		if (rev->reason == CRL_REASON_REMOVE_FROM_CRL)
			return 2;
		ctx->error = X509_V_ERR_CERT_REVOKED;
		ok = ctx->verify_cb(0, ctx);
		if (!ok)
			return 0;
	}

	return 1;
}

static int
check_policy(X509_STORE_CTX *ctx)
{
	int ret;

	if (ctx->parent)
		return 1;
	ret = X509_policy_check(&ctx->tree, &ctx->explicit_policy, ctx->chain,
	    ctx->param->policies, ctx->param->flags);
	if (ret == 0) {
		X509error(ERR_R_MALLOC_FAILURE);
		return 0;
	}
	/* Invalid or inconsistent extensions */
	if (ret == -1) {
		/* Locate certificates with bad extensions and notify
		 * callback.
		 */
		X509 *x;
		int i;
		for (i = 1; i < sk_X509_num(ctx->chain); i++) {
			x = sk_X509_value(ctx->chain, i);
			if (!(x->ex_flags & EXFLAG_INVALID_POLICY))
				continue;
			ctx->current_cert = x;
			ctx->error = X509_V_ERR_INVALID_POLICY_EXTENSION;
			if (!ctx->verify_cb(0, ctx))
				return 0;
		}
		return 1;
	}
	if (ret == -2) {
		ctx->current_cert = NULL;
		ctx->error = X509_V_ERR_NO_EXPLICIT_POLICY;
		return ctx->verify_cb(0, ctx);
	}

	if (ctx->param->flags & X509_V_FLAG_NOTIFY_POLICY) {
		ctx->current_cert = NULL;
		ctx->error = X509_V_OK;
		if (!ctx->verify_cb(2, ctx))
			return 0;
	}

	return 1;
}

/*
 * Inform the verify callback of an error.
 *
 * If x is not NULL it is the error cert, otherwise use the chain cert
 * at depth.
 *
 * If err is not X509_V_OK, that's the error value, otherwise leave
 * unchanged (presumably set by the caller).
 *
 * Returns 0 to abort verification with an error, non-zero to continue.
 */
static int
verify_cb_cert(X509_STORE_CTX *ctx, X509 *x, int depth, int err)
{
	ctx->error_depth = depth;
	ctx->current_cert = (x != NULL) ? x : sk_X509_value(ctx->chain, depth);
	if (err != X509_V_OK)
		ctx->error = err;
	return ctx->verify_cb(0, ctx);
}

/*
 * Check certificate validity times.
 *
 * If depth >= 0, invoke verification callbacks on error, otherwise just return
 * the validation status.
 *
 * Return 1 on success, 0 otherwise.
 */
int
x509_check_cert_time(X509_STORE_CTX *ctx, X509 *x, int depth)
{
	time_t *ptime;
	int i;

	if (ctx->param->flags & X509_V_FLAG_USE_CHECK_TIME)
		ptime = &ctx->param->check_time;
	else if (ctx->param->flags & X509_V_FLAG_NO_CHECK_TIME)
		return 1;
	else
		ptime = NULL;

	i = X509_cmp_time(X509_get_notBefore(x), ptime);
	if (i >= 0 && depth < 0)
		return 0;
	if (i == 0 && !verify_cb_cert(ctx, x, depth,
	    X509_V_ERR_ERROR_IN_CERT_NOT_BEFORE_FIELD))
		return 0;
	if (i > 0 && !verify_cb_cert(ctx, x, depth,
		X509_V_ERR_CERT_NOT_YET_VALID))
		return 0;

	i = X509_cmp_time_internal(X509_get_notAfter(x), ptime, 1);
	if (i <= 0 && depth < 0)
		return 0;
	if (i == 0 && !verify_cb_cert(ctx, x, depth,
	    X509_V_ERR_ERROR_IN_CERT_NOT_AFTER_FIELD))
		return 0;
	if (i < 0 && !verify_cb_cert(ctx, x, depth,
	    X509_V_ERR_CERT_HAS_EXPIRED))
		return 0;
	return 1;
}

static int
internal_verify(X509_STORE_CTX *ctx)
{
	int n = sk_X509_num(ctx->chain) - 1;
	X509 *xi = sk_X509_value(ctx->chain, n);
	X509 *xs;

	if (ctx->check_issued(ctx, xi, xi))
		xs = xi;
	else {
		if (ctx->param->flags & X509_V_FLAG_PARTIAL_CHAIN) {
			xs = xi;
			goto check_cert;
		}
		if (n <= 0)
			return verify_cb_cert(ctx, xi, 0,
			    X509_V_ERR_UNABLE_TO_VERIFY_LEAF_SIGNATURE);
		n--;
		ctx->error_depth = n;
		xs = sk_X509_value(ctx->chain, n);
	}

	/*
	 * Do not clear ctx->error=0, it must be "sticky", only the
	 * user's callback is allowed to reset errors (at its own
	 * peril).
	 */
	while (n >= 0) {

		/*
		 * Skip signature check for self signed certificates
		 * unless explicitly asked for.  It doesn't add any
		 * security and just wastes time.  If the issuer's
		 * public key is unusable, report the issuer
		 * certificate and its depth (rather than the depth of
		 * the subject).
		 */
		if (xs != xi || (ctx->param->flags &
			X509_V_FLAG_CHECK_SS_SIGNATURE)) {
			EVP_PKEY *pkey;
			if ((pkey = X509_get_pubkey(xi)) == NULL) {
				if (!verify_cb_cert(ctx, xi, xi != xs ? n+1 : n,
					X509_V_ERR_UNABLE_TO_DECODE_ISSUER_PUBLIC_KEY))
					return 0;
			} else if (X509_verify(xs, pkey) <= 0) {
				if (!verify_cb_cert(ctx, xs, n,
					X509_V_ERR_CERT_SIGNATURE_FAILURE)) {
					EVP_PKEY_free(pkey);
					return 0;
				}
			}
			EVP_PKEY_free(pkey);
		}
check_cert:
		/* Calls verify callback as needed */
		if (!x509_check_cert_time(ctx, xs, n))
			return 0;

		/*
		 * Signal success at this depth.  However, the
		 * previous error (if any) is retained.
		 */
		ctx->current_issuer = xi;
		ctx->current_cert = xs;
		ctx->error_depth = n;
		if (!ctx->verify_cb(1, ctx))
			return 0;

		if (--n >= 0) {
			xi = xs;
			xs = sk_X509_value(ctx->chain, n);
		}
	}
	return 1;
}

int
X509_cmp_current_time(const ASN1_TIME *ctm)
{
	return X509_cmp_time(ctm, NULL);
}

/*
 * Compare a possibly unvalidated ASN1_TIME string against a time_t
 * using RFC 5280 rules for the time string. If *cmp_time is NULL
 * the current system time is used.
 *
 * XXX NOTE that unlike what you expect a "cmp" function to do in C,
 * XXX this one is "special", and returns 0 for error.
 *
 * Returns:
 * -1 if the ASN1_time is earlier than OR the same as *cmp_time.
 * 1 if the ASN1_time is later than *cmp_time.
 * 0 on error.
 */
static int
X509_cmp_time_internal(const ASN1_TIME *ctm, time_t *cmp_time, int clamp_notafter)
{
	time_t time1, time2;
	struct tm tm1, tm2;
	int ret = 0;
	int type;

	if (cmp_time == NULL)
		time2 = time(NULL);
	else
		time2 = *cmp_time;

	memset(&tm1, 0, sizeof(tm1));

	type = ASN1_time_parse(ctm->data, ctm->length, &tm1, ctm->type);
	if (type == -1)
		goto out; /* invalid time */

	/* RFC 5280 section 4.1.2.5 */
	if (tm1.tm_year < 150 && type != V_ASN1_UTCTIME)
		goto out;
	if (tm1.tm_year >= 150 && type != V_ASN1_GENERALIZEDTIME)
		goto out;

	if (clamp_notafter) {
		/* Allow for completely broken operating systems. */
		if (!ASN1_time_tm_clamp_notafter(&tm1))
			goto out;
	}

	/*
	 * Defensively fail if the time string is not representable as
	 * a time_t. A time_t must be sane if you care about times after
	 * Jan 19 2038.
	 */
	if ((time1 = timegm(&tm1)) == -1)
		goto out;

	if (gmtime_r(&time2, &tm2) == NULL)
		goto out;

	ret = ASN1_time_tm_cmp(&tm1, &tm2);
	if (ret == 0)
		ret = -1; /* 0 is used for error, so map same to less than */
 out:
	return (ret);
}

int
X509_cmp_time(const ASN1_TIME *ctm, time_t *cmp_time)
{
	return X509_cmp_time_internal(ctm, cmp_time, 0);
}


ASN1_TIME *
X509_gmtime_adj(ASN1_TIME *s, long adj)
{
	return X509_time_adj(s, adj, NULL);
}

ASN1_TIME *
X509_time_adj(ASN1_TIME *s, long offset_sec, time_t *in_time)
{
	return X509_time_adj_ex(s, 0, offset_sec, in_time);
}

ASN1_TIME *
X509_time_adj_ex(ASN1_TIME *s, int offset_day, long offset_sec, time_t *in_time)
{
	time_t t;
	if (in_time == NULL)
		t = time(NULL);
	else
		t = *in_time;

	return ASN1_TIME_adj(s, t, offset_day, offset_sec);
}

int
X509_get_pubkey_parameters(EVP_PKEY *pkey, STACK_OF(X509) *chain)
{
	EVP_PKEY *ktmp = NULL, *ktmp2;
	int i, j;

	if ((pkey != NULL) && !EVP_PKEY_missing_parameters(pkey))
		return 1;

	for (i = 0; i < sk_X509_num(chain); i++) {
		ktmp = X509_get_pubkey(sk_X509_value(chain, i));
		if (ktmp == NULL) {
			X509error(X509_R_UNABLE_TO_GET_CERTS_PUBLIC_KEY);
			return 0;
		}
		if (!EVP_PKEY_missing_parameters(ktmp))
			break;
		else {
			EVP_PKEY_free(ktmp);
			ktmp = NULL;
		}
	}
	if (ktmp == NULL) {
		X509error(X509_R_UNABLE_TO_FIND_PARAMETERS_IN_CHAIN);
		return 0;
	}

	/* first, populate the other certs */
	for (j = i - 1; j >= 0; j--) {
		ktmp2 = X509_get_pubkey(sk_X509_value(chain, j));
		EVP_PKEY_copy_parameters(ktmp2, ktmp);
		EVP_PKEY_free(ktmp2);
	}

	if (pkey != NULL)
		EVP_PKEY_copy_parameters(pkey, ktmp);
	EVP_PKEY_free(ktmp);
	return 1;
}

int
X509_STORE_CTX_get_ex_new_index(long argl, void *argp, CRYPTO_EX_new *new_func,
    CRYPTO_EX_dup *dup_func, CRYPTO_EX_free *free_func)
{
	/* This function is (usually) called only once, by
	 * SSL_get_ex_data_X509_STORE_CTX_idx (ssl/ssl_cert.c). */
	return CRYPTO_get_ex_new_index(CRYPTO_EX_INDEX_X509_STORE_CTX,
	    argl, argp, new_func, dup_func, free_func);
}

int
X509_STORE_CTX_set_ex_data(X509_STORE_CTX *ctx, int idx, void *data)
{
	return CRYPTO_set_ex_data(&ctx->ex_data, idx, data);
}

void *
X509_STORE_CTX_get_ex_data(X509_STORE_CTX *ctx, int idx)
{
	return CRYPTO_get_ex_data(&ctx->ex_data, idx);
}

int
X509_STORE_CTX_get_error(X509_STORE_CTX *ctx)
{
	return ctx->error;
}

void
X509_STORE_CTX_set_error(X509_STORE_CTX *ctx, int err)
{
	ctx->error = err;
}

int
X509_STORE_CTX_get_error_depth(X509_STORE_CTX *ctx)
{
	return ctx->error_depth;
}

X509 *
X509_STORE_CTX_get_current_cert(X509_STORE_CTX *ctx)
{
	return ctx->current_cert;
}

STACK_OF(X509) *X509_STORE_CTX_get_chain(X509_STORE_CTX *ctx)
{
	return ctx->chain;
}

STACK_OF(X509) *X509_STORE_CTX_get1_chain(X509_STORE_CTX *ctx)
{
	int i;
	X509 *x;
	STACK_OF(X509) *chain;

	if (!ctx->chain || !(chain = sk_X509_dup(ctx->chain)))
		return NULL;
	for (i = 0; i < sk_X509_num(chain); i++) {
		x = sk_X509_value(chain, i);
		CRYPTO_add(&x->references, 1, CRYPTO_LOCK_X509);
	}
	return chain;
}

X509 *
X509_STORE_CTX_get0_current_issuer(X509_STORE_CTX *ctx)
{
	return ctx->current_issuer;
}

X509_CRL *
X509_STORE_CTX_get0_current_crl(X509_STORE_CTX *ctx)
{
	return ctx->current_crl;
}

X509_STORE_CTX *
X509_STORE_CTX_get0_parent_ctx(X509_STORE_CTX *ctx)
{
	return ctx->parent;
}

void
X509_STORE_CTX_set_cert(X509_STORE_CTX *ctx, X509 *x)
{
	ctx->cert = x;
}

void
X509_STORE_CTX_set_chain(X509_STORE_CTX *ctx, STACK_OF(X509) *sk)
{
	ctx->untrusted = sk;
}

void
X509_STORE_CTX_set0_crls(X509_STORE_CTX *ctx, STACK_OF(X509_CRL) *sk)
{
	ctx->crls = sk;
}

int
X509_STORE_CTX_set_purpose(X509_STORE_CTX *ctx, int purpose)
{
	return X509_STORE_CTX_purpose_inherit(ctx, 0, purpose, 0);
}

int
X509_STORE_CTX_set_trust(X509_STORE_CTX *ctx, int trust)
{
	return X509_STORE_CTX_purpose_inherit(ctx, 0, 0, trust);
}

/* This function is used to set the X509_STORE_CTX purpose and trust
 * values. This is intended to be used when another structure has its
 * own trust and purpose values which (if set) will be inherited by
 * the ctx. If they aren't set then we will usually have a default
 * purpose in mind which should then be used to set the trust value.
 * An example of this is SSL use: an SSL structure will have its own
 * purpose and trust settings which the application can set: if they
 * aren't set then we use the default of SSL client/server.
 */

int
X509_STORE_CTX_purpose_inherit(X509_STORE_CTX *ctx, int def_purpose,
    int purpose, int trust)
{
	int idx;

	/* If purpose not set use default */
	if (!purpose)
		purpose = def_purpose;
	/* If we have a purpose then check it is valid */
	if (purpose) {
		X509_PURPOSE *ptmp;
		idx = X509_PURPOSE_get_by_id(purpose);
		if (idx == -1) {
			X509error(X509_R_UNKNOWN_PURPOSE_ID);
			return 0;
		}
		ptmp = X509_PURPOSE_get0(idx);
		if (ptmp->trust == X509_TRUST_DEFAULT) {
			idx = X509_PURPOSE_get_by_id(def_purpose);
			if (idx == -1) {
				X509error(X509_R_UNKNOWN_PURPOSE_ID);
				return 0;
			}
			ptmp = X509_PURPOSE_get0(idx);
		}
		/* If trust not set then get from purpose default */
		if (!trust)
			trust = ptmp->trust;
	}
	if (trust) {
		idx = X509_TRUST_get_by_id(trust);
		if (idx == -1) {
			X509error(X509_R_UNKNOWN_TRUST_ID);
			return 0;
		}
	}

	if (purpose && !ctx->param->purpose)
		ctx->param->purpose = purpose;
	if (trust && !ctx->param->trust)
		ctx->param->trust = trust;
	return 1;
}

X509_STORE_CTX *
X509_STORE_CTX_new(void)
{
	X509_STORE_CTX *ctx;

	ctx = calloc(1, sizeof(X509_STORE_CTX));
	if (!ctx) {
		X509error(ERR_R_MALLOC_FAILURE);
		return NULL;
	}
	return ctx;
}

void
X509_STORE_CTX_free(X509_STORE_CTX *ctx)
{
	if (ctx == NULL)
		return;

	X509_STORE_CTX_cleanup(ctx);
	free(ctx);
}

int
X509_STORE_CTX_init(X509_STORE_CTX *ctx, X509_STORE *store, X509 *x509,
    STACK_OF(X509) *chain)
{
	int param_ret = 1;

	/*
	 * Make sure everything is initialized properly even in case of an
	 * early return due to an error.
	 *
	 * While this 'ctx' can be reused, X509_STORE_CTX_cleanup() will have
	 * freed everything and memset ex_data anyway.  This also allows us
	 * to safely use X509_STORE_CTX variables from the stack which will
	 * have uninitialized data.
	 */
	memset(ctx, 0, sizeof(*ctx));

	/*
	 * Start with this set to not valid - it will be set to valid
	 * in X509_verify_cert.
	 */
	ctx->error = X509_V_ERR_INVALID_CALL;

	/*
	 * Set values other than 0.  Keep this in the same order as
	 * X509_STORE_CTX except for values that may fail.  All fields that
	 * may fail should go last to make sure 'ctx' is as consistent as
	 * possible even on early exits.
	 */
	ctx->ctx = store;
	ctx->cert = x509;
	ctx->untrusted = chain;

	if (store && store->verify)
		ctx->verify = store->verify;
	else
		ctx->verify = internal_verify;

	if (store && store->verify_cb)
		ctx->verify_cb = store->verify_cb;
	else
		ctx->verify_cb = null_callback;

	if (store && store->get_issuer)
		ctx->get_issuer = store->get_issuer;
	else
		ctx->get_issuer = X509_STORE_CTX_get1_issuer;

	if (store && store->check_issued)
		ctx->check_issued = store->check_issued;
	else
		ctx->check_issued = check_issued;

	if (store && store->check_revocation)
		ctx->check_revocation = store->check_revocation;
	else
		ctx->check_revocation = check_revocation;

	if (store && store->get_crl)
		ctx->get_crl = store->get_crl;
	else
		ctx->get_crl = NULL;

	if (store && store->check_crl)
		ctx->check_crl = store->check_crl;
	else
		ctx->check_crl = check_crl;

	if (store && store->cert_crl)
		ctx->cert_crl = store->cert_crl;
	else
		ctx->cert_crl = cert_crl;

	ctx->check_policy = check_policy;

	if (store && store->lookup_certs)
		ctx->lookup_certs = store->lookup_certs;
	else
		ctx->lookup_certs = X509_STORE_get1_certs;

	if (store && store->lookup_crls)
		ctx->lookup_crls = store->lookup_crls;
	else
		ctx->lookup_crls = X509_STORE_get1_crls;

	if (store && store->cleanup)
		ctx->cleanup = store->cleanup;
	else
		ctx->cleanup = NULL;

	ctx->param = X509_VERIFY_PARAM_new();
	if (!ctx->param) {
		X509error(ERR_R_MALLOC_FAILURE);
		return 0;
	}

	/* Inherit callbacks and flags from X509_STORE if not set
	 * use defaults.
	 */
	if (store)
		param_ret = X509_VERIFY_PARAM_inherit(ctx->param, store->param);
	else
		ctx->param->inh_flags |= X509_VP_FLAG_DEFAULT|X509_VP_FLAG_ONCE;

	if (param_ret)
		param_ret = X509_VERIFY_PARAM_inherit(ctx->param,
		    X509_VERIFY_PARAM_lookup("default"));

	if (param_ret == 0) {
		X509error(ERR_R_MALLOC_FAILURE);
		return 0;
	}

	if (CRYPTO_new_ex_data(CRYPTO_EX_INDEX_X509_STORE_CTX, ctx,
	    &(ctx->ex_data)) == 0) {
		X509error(ERR_R_MALLOC_FAILURE);
		return 0;
	}
	return 1;
}

/* Set alternative lookup method: just a STACK of trusted certificates.
 * This avoids X509_STORE nastiness where it isn't needed.
 */

void
X509_STORE_CTX_trusted_stack(X509_STORE_CTX *ctx, STACK_OF(X509) *sk)
{
	ctx->other_ctx = sk;
	ctx->get_issuer = get_issuer_sk;
}

void
X509_STORE_CTX_cleanup(X509_STORE_CTX *ctx)
{
	if (ctx->cleanup)
		ctx->cleanup(ctx);
	if (ctx->param != NULL) {
		if (ctx->parent == NULL)
			X509_VERIFY_PARAM_free(ctx->param);
		ctx->param = NULL;
	}
	if (ctx->tree != NULL) {
		X509_policy_tree_free(ctx->tree);
		ctx->tree = NULL;
	}
	if (ctx->chain != NULL) {
		sk_X509_pop_free(ctx->chain, X509_free);
		ctx->chain = NULL;
	}
	CRYPTO_free_ex_data(CRYPTO_EX_INDEX_X509_STORE_CTX,
	    ctx, &(ctx->ex_data));
	memset(&ctx->ex_data, 0, sizeof(CRYPTO_EX_DATA));
}

void
X509_STORE_CTX_set_depth(X509_STORE_CTX *ctx, int depth)
{
	X509_VERIFY_PARAM_set_depth(ctx->param, depth);
}

void
X509_STORE_CTX_set_flags(X509_STORE_CTX *ctx, unsigned long flags)
{
	X509_VERIFY_PARAM_set_flags(ctx->param, flags);
}

void
X509_STORE_CTX_set_time(X509_STORE_CTX *ctx, unsigned long flags, time_t t)
{
	X509_VERIFY_PARAM_set_time(ctx->param, t);
}

void
X509_STORE_CTX_set_verify_cb(X509_STORE_CTX *ctx,
    int (*verify_cb)(int, X509_STORE_CTX *))
{
	ctx->verify_cb = verify_cb;
}

X509_POLICY_TREE *
X509_STORE_CTX_get0_policy_tree(X509_STORE_CTX *ctx)
{
	return ctx->tree;
}

int
X509_STORE_CTX_get_explicit_policy(X509_STORE_CTX *ctx)
{
	return ctx->explicit_policy;
}

int
X509_STORE_CTX_set_default(X509_STORE_CTX *ctx, const char *name)
{
	const X509_VERIFY_PARAM *param;
	param = X509_VERIFY_PARAM_lookup(name);
	if (!param)
		return 0;
	return X509_VERIFY_PARAM_inherit(ctx->param, param);
}

X509_VERIFY_PARAM *
X509_STORE_CTX_get0_param(X509_STORE_CTX *ctx)
{
	return ctx->param;
}

void
X509_STORE_CTX_set0_param(X509_STORE_CTX *ctx, X509_VERIFY_PARAM *param)
{
	if (ctx->param)
		X509_VERIFY_PARAM_free(ctx->param);
	ctx->param = param;
}
