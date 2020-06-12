/* $OpenBSD: ssl_stat.c,v 1.14 2017/05/07 04:22:24 beck Exp $ */
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
 * Copyright 2005 Nokia. All rights reserved.
 *
 * The portions of the attached software ("Contribution") is developed by
 * Nokia Corporation and is licensed pursuant to the OpenSSL open source
 * license.
 *
 * The Contribution, originally written by Mika Kousa and Pasi Eronen of
 * Nokia Corporation, consists of the "PSK" (Pre-Shared Key) ciphersuites
 * support (see RFC 4279) to OpenSSL.
 *
 * No patent licenses or other rights except those expressly stated in
 * the OpenSSL open source license shall be deemed granted or received
 * expressly, by implication, estoppel, or otherwise.
 *
 * No assurances are provided by Nokia that the Contribution does not
 * infringe the patent or other intellectual property rights of any third
 * party or that the license provides you with all the necessary rights
 * to make use of the Contribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND. IN
 * ADDITION TO THE DISCLAIMERS INCLUDED IN THE LICENSE, NOKIA
 * SPECIFICALLY DISCLAIMS ANY LIABILITY FOR CLAIMS BROUGHT BY YOU OR ANY
 * OTHER ENTITY BASED ON INFRINGEMENT OF INTELLECTUAL PROPERTY RIGHTS OR
 * OTHERWISE.
 */

#include <stdio.h>

#include "ssl_locl.h"

const char *
SSL_state_string_long(const SSL *s)
{
	const char *str;

	switch (S3I(s)->hs.state) {
	case SSL_ST_BEFORE:
		str = "before SSL initialization";
		break;
	case SSL_ST_ACCEPT:
		str = "before accept initialization";
		break;
	case SSL_ST_CONNECT:
		str = "before connect initialization";
		break;
	case SSL_ST_OK:
		str = "SSL negotiation finished successfully";
		break;
	case SSL_ST_RENEGOTIATE:
		str = "SSL renegotiate ciphers";
		break;
	case SSL_ST_BEFORE|SSL_ST_CONNECT:
		str = "before/connect initialization";
		break;
	case SSL_ST_OK|SSL_ST_CONNECT:
		str = "ok/connect SSL initialization";
		break;
	case SSL_ST_BEFORE|SSL_ST_ACCEPT:
		str = "before/accept initialization";
		break;
	case SSL_ST_OK|SSL_ST_ACCEPT:
		str = "ok/accept SSL initialization";
		break;

	/* SSLv3 additions */
	case SSL3_ST_CW_CLNT_HELLO_A:
		str = "SSLv3 write client hello A";
		break;
	case SSL3_ST_CW_CLNT_HELLO_B:
		str = "SSLv3 write client hello B";
		break;
	case SSL3_ST_CR_SRVR_HELLO_A:
		str = "SSLv3 read server hello A";
		break;
	case SSL3_ST_CR_SRVR_HELLO_B:
		str = "SSLv3 read server hello B";
		break;
	case SSL3_ST_CR_CERT_A:
		str = "SSLv3 read server certificate A";
		break;
	case SSL3_ST_CR_CERT_B:
		str = "SSLv3 read server certificate B";
		break;
	case SSL3_ST_CR_KEY_EXCH_A:
		str = "SSLv3 read server key exchange A";
		break;
	case SSL3_ST_CR_KEY_EXCH_B:
		str = "SSLv3 read server key exchange B";
		break;
	case SSL3_ST_CR_CERT_REQ_A:
		str = "SSLv3 read server certificate request A";
		break;
	case SSL3_ST_CR_CERT_REQ_B:
		str = "SSLv3 read server certificate request B";
		break;
	case SSL3_ST_CR_SESSION_TICKET_A:
		str = "SSLv3 read server session ticket A";
		break;
	case SSL3_ST_CR_SESSION_TICKET_B:
		str = "SSLv3 read server session ticket B";
		break;
	case SSL3_ST_CR_SRVR_DONE_A:
		str = "SSLv3 read server done A";
		break;
	case SSL3_ST_CR_SRVR_DONE_B:
		str = "SSLv3 read server done B";
		break;
	case SSL3_ST_CW_CERT_A:
		str = "SSLv3 write client certificate A";
		break;
	case SSL3_ST_CW_CERT_B:
		str = "SSLv3 write client certificate B";
		break;
	case SSL3_ST_CW_CERT_C:
		str = "SSLv3 write client certificate C";
		break;
	case SSL3_ST_CW_CERT_D:
		str = "SSLv3 write client certificate D";
		break;
	case SSL3_ST_CW_KEY_EXCH_A:
		str = "SSLv3 write client key exchange A";
		break;
	case SSL3_ST_CW_KEY_EXCH_B:
		str = "SSLv3 write client key exchange B";
		break;
	case SSL3_ST_CW_CERT_VRFY_A:
		str = "SSLv3 write certificate verify A";
		break;
	case SSL3_ST_CW_CERT_VRFY_B:
		str = "SSLv3 write certificate verify B";
		break;

	case SSL3_ST_CW_CHANGE_A:
	case SSL3_ST_SW_CHANGE_A:
		str = "SSLv3 write change cipher spec A";
		break;
	case SSL3_ST_CW_CHANGE_B:
	case SSL3_ST_SW_CHANGE_B:
		str = "SSLv3 write change cipher spec B";
		break;
	case SSL3_ST_CW_FINISHED_A:
	case SSL3_ST_SW_FINISHED_A:
		str = "SSLv3 write finished A";
		break;
	case SSL3_ST_CW_FINISHED_B:
	case SSL3_ST_SW_FINISHED_B:
		str = "SSLv3 write finished B";
		break;
	case SSL3_ST_CR_CHANGE_A:
	case SSL3_ST_SR_CHANGE_A:
		str = "SSLv3 read change cipher spec A";
		break;
	case SSL3_ST_CR_CHANGE_B:
	case SSL3_ST_SR_CHANGE_B:
		str = "SSLv3 read change cipher spec B";
		break;
	case SSL3_ST_CR_FINISHED_A:
	case SSL3_ST_SR_FINISHED_A:
		str = "SSLv3 read finished A";
		break;
	case SSL3_ST_CR_FINISHED_B:
	case SSL3_ST_SR_FINISHED_B:
		str = "SSLv3 read finished B";
		break;

	case SSL3_ST_CW_FLUSH:
	case SSL3_ST_SW_FLUSH:
		str = "SSLv3 flush data";
		break;

	case SSL3_ST_SR_CLNT_HELLO_A:
		str = "SSLv3 read client hello A";
		break;
	case SSL3_ST_SR_CLNT_HELLO_B:
		str = "SSLv3 read client hello B";
		break;
	case SSL3_ST_SR_CLNT_HELLO_C:
		str = "SSLv3 read client hello C";
		break;
	case SSL3_ST_SW_HELLO_REQ_A:
		str = "SSLv3 write hello request A";
		break;
	case SSL3_ST_SW_HELLO_REQ_B:
		str = "SSLv3 write hello request B";
		break;
	case SSL3_ST_SW_HELLO_REQ_C:
		str = "SSLv3 write hello request C";
		break;
	case SSL3_ST_SW_SRVR_HELLO_A:
		str = "SSLv3 write server hello A";
		break;
	case SSL3_ST_SW_SRVR_HELLO_B:
		str = "SSLv3 write server hello B";
		break;
	case SSL3_ST_SW_CERT_A:
		str = "SSLv3 write certificate A";
		break;
	case SSL3_ST_SW_CERT_B:
		str = "SSLv3 write certificate B";
		break;
	case SSL3_ST_SW_KEY_EXCH_A:
		str = "SSLv3 write key exchange A";
		break;
	case SSL3_ST_SW_KEY_EXCH_B:
		str = "SSLv3 write key exchange B";
		break;
	case SSL3_ST_SW_CERT_REQ_A:
		str = "SSLv3 write certificate request A";
		break;
	case SSL3_ST_SW_CERT_REQ_B:
		str = "SSLv3 write certificate request B";
		break;
	case SSL3_ST_SW_SESSION_TICKET_A:
		str = "SSLv3 write session ticket A";
		break;
	case SSL3_ST_SW_SESSION_TICKET_B:
		str = "SSLv3 write session ticket B";
		break;
	case SSL3_ST_SW_SRVR_DONE_A:
		str = "SSLv3 write server done A";
		break;
	case SSL3_ST_SW_SRVR_DONE_B:
		str = "SSLv3 write server done B";
		break;
	case SSL3_ST_SR_CERT_A:
		str = "SSLv3 read client certificate A";
		break;
	case SSL3_ST_SR_CERT_B:
		str = "SSLv3 read client certificate B";
		break;
	case SSL3_ST_SR_KEY_EXCH_A:
		str = "SSLv3 read client key exchange A";
		break;
	case SSL3_ST_SR_KEY_EXCH_B:
		str = "SSLv3 read client key exchange B";
		break;
	case SSL3_ST_SR_CERT_VRFY_A:
		str = "SSLv3 read certificate verify A";
		break;
	case SSL3_ST_SR_CERT_VRFY_B:
		str = "SSLv3 read certificate verify B";
		break;

	/* DTLS */
	case DTLS1_ST_CR_HELLO_VERIFY_REQUEST_A:
		str = "DTLS1 read hello verify request A";
		break;
	case DTLS1_ST_CR_HELLO_VERIFY_REQUEST_B:
		str = "DTLS1 read hello verify request B";
		break;
	case DTLS1_ST_SW_HELLO_VERIFY_REQUEST_A:
		str = "DTLS1 write hello verify request A";
		break;
	case DTLS1_ST_SW_HELLO_VERIFY_REQUEST_B:
		str = "DTLS1 write hello verify request B";
		break;

	default:
		str = "unknown state";
		break;
	}
	return (str);
}

const char *
SSL_rstate_string_long(const SSL *s)
{
	const char *str;

	switch (s->internal->rstate) {
	case SSL_ST_READ_HEADER:
		str = "read header";
		break;
	case SSL_ST_READ_BODY:
		str = "read body";
		break;
	case SSL_ST_READ_DONE:
		str = "read done";
		break;
	default:
		str = "unknown";
		break;
	}
	return (str);
}

const char *
SSL_state_string(const SSL *s)
{
	const char *str;

	switch (S3I(s)->hs.state) {
	case SSL_ST_BEFORE:
		str = "PINIT ";
		break;
	case SSL_ST_ACCEPT:
		str = "AINIT ";
		break;
	case SSL_ST_CONNECT:
		str = "CINIT ";
		break;
	case SSL_ST_OK:
		str = "SSLOK ";
		break;

	/* SSLv3 additions */
	case SSL3_ST_SW_FLUSH:
	case SSL3_ST_CW_FLUSH:
		str = "3FLUSH";
		break;
	case SSL3_ST_CW_CLNT_HELLO_A:
		str = "3WCH_A";
		break;
	case SSL3_ST_CW_CLNT_HELLO_B:
		str = "3WCH_B";
		break;
	case SSL3_ST_CR_SRVR_HELLO_A:
		str = "3RSH_A";
		break;
	case SSL3_ST_CR_SRVR_HELLO_B:
		str = "3RSH_B";
		break;
	case SSL3_ST_CR_CERT_A:
		str = "3RSC_A";
		break;
	case SSL3_ST_CR_CERT_B:
		str = "3RSC_B";
		break;
	case SSL3_ST_CR_KEY_EXCH_A:
		str = "3RSKEA";
		break;
	case SSL3_ST_CR_KEY_EXCH_B:
		str = "3RSKEB";
		break;
	case SSL3_ST_CR_CERT_REQ_A:
		str = "3RCR_A";
		break;
	case SSL3_ST_CR_CERT_REQ_B:
		str = "3RCR_B";
		break;
	case SSL3_ST_CR_SRVR_DONE_A:
		str = "3RSD_A";
		break;
	case SSL3_ST_CR_SRVR_DONE_B:
		str = "3RSD_B";
		break;
	case SSL3_ST_CW_CERT_A:
		str = "3WCC_A";
		break;
	case SSL3_ST_CW_CERT_B:
		str = "3WCC_B";
		break;
	case SSL3_ST_CW_CERT_C:
		str = "3WCC_C";
		break;
	case SSL3_ST_CW_CERT_D:
		str = "3WCC_D";
		break;
	case SSL3_ST_CW_KEY_EXCH_A:
		str = "3WCKEA";
		break;
	case SSL3_ST_CW_KEY_EXCH_B:
		str = "3WCKEB";
		break;
	case SSL3_ST_CW_CERT_VRFY_A:
		str = "3WCV_A";
		break;
	case SSL3_ST_CW_CERT_VRFY_B:
		str = "3WCV_B";
		break;

	case SSL3_ST_SW_CHANGE_A:
	case SSL3_ST_CW_CHANGE_A:
		str = "3WCCSA";
		break;
	case SSL3_ST_SW_CHANGE_B:
	case SSL3_ST_CW_CHANGE_B:
		str = "3WCCSB";
		break;
	case SSL3_ST_SW_FINISHED_A:
	case SSL3_ST_CW_FINISHED_A:
		str = "3WFINA";
		break;
	case SSL3_ST_SW_FINISHED_B:
	case SSL3_ST_CW_FINISHED_B:
		str = "3WFINB";
		break;
	case SSL3_ST_SR_CHANGE_A:
	case SSL3_ST_CR_CHANGE_A:
		str = "3RCCSA";
		break;
	case SSL3_ST_SR_CHANGE_B:
	case SSL3_ST_CR_CHANGE_B:
		str = "3RCCSB";
		break;
	case SSL3_ST_SR_FINISHED_A:
	case SSL3_ST_CR_FINISHED_A:
		str = "3RFINA";
		break;
	case SSL3_ST_SR_FINISHED_B:
	case SSL3_ST_CR_FINISHED_B:
		str = "3RFINB";
		break;

	case SSL3_ST_SW_HELLO_REQ_A:
		str = "3WHR_A";
		break;
	case SSL3_ST_SW_HELLO_REQ_B:
		str = "3WHR_B";
		break;
	case SSL3_ST_SW_HELLO_REQ_C:
		str = "3WHR_C";
		break;
	case SSL3_ST_SR_CLNT_HELLO_A:
		str = "3RCH_A";
		break;
	case SSL3_ST_SR_CLNT_HELLO_B:
		str = "3RCH_B";
		break;
	case SSL3_ST_SR_CLNT_HELLO_C:
		str = "3RCH_C";
		break;
	case SSL3_ST_SW_SRVR_HELLO_A:
		str = "3WSH_A";
		break;
	case SSL3_ST_SW_SRVR_HELLO_B:
		str = "3WSH_B";
		break;
	case SSL3_ST_SW_CERT_A:
		str = "3WSC_A";
		break;
	case SSL3_ST_SW_CERT_B:
		str = "3WSC_B";
		break;
	case SSL3_ST_SW_KEY_EXCH_A:
		str = "3WSKEA";
		break;
	case SSL3_ST_SW_KEY_EXCH_B:
		str = "3WSKEB";
		break;
	case SSL3_ST_SW_CERT_REQ_A:
		str = "3WCR_A";
		break;
	case SSL3_ST_SW_CERT_REQ_B:
		str = "3WCR_B";
		break;
	case SSL3_ST_SW_SRVR_DONE_A:
		str = "3WSD_A";
		break;
	case SSL3_ST_SW_SRVR_DONE_B:
		str = "3WSD_B";
		break;
	case SSL3_ST_SR_CERT_A:
		str = "3RCC_A";
		break;
	case SSL3_ST_SR_CERT_B:
		str = "3RCC_B";
		break;
	case SSL3_ST_SR_KEY_EXCH_A:
		str = "3RCKEA";
		break;
	case SSL3_ST_SR_KEY_EXCH_B:
		str = "3RCKEB";
		break;
	case SSL3_ST_SR_CERT_VRFY_A:
		str = "3RCV_A";
		break;
	case SSL3_ST_SR_CERT_VRFY_B:
		str = "3RCV_B";
		break;

	/* DTLS */
	case DTLS1_ST_CR_HELLO_VERIFY_REQUEST_A:
		str = "DRCHVA";
		break;
	case DTLS1_ST_CR_HELLO_VERIFY_REQUEST_B:
		str = "DRCHVB";
		break;
	case DTLS1_ST_SW_HELLO_VERIFY_REQUEST_A:
		str = "DWCHVA";
		break;
	case DTLS1_ST_SW_HELLO_VERIFY_REQUEST_B:
		str = "DWCHVB";
		break;

	default:
		str = "UNKWN ";
		break;
	}
	return (str);
}

const char *
SSL_alert_type_string_long(int value)
{
	value >>= 8;
	if (value == SSL3_AL_WARNING)
		return ("warning");
	else if (value == SSL3_AL_FATAL)
		return ("fatal");
	else
		return ("unknown");
}

const char *
SSL_alert_type_string(int value)
{
	value >>= 8;
	if (value == SSL3_AL_WARNING)
		return ("W");
	else if (value == SSL3_AL_FATAL)
		return ("F");
	else
		return ("U");
}

const char *
SSL_alert_desc_string(int value)
{
	const char *str;

	switch (value & 0xff) {
	case SSL3_AD_CLOSE_NOTIFY:
		str = "CN";
		break;
	case SSL3_AD_UNEXPECTED_MESSAGE:
		str = "UM";
		break;
	case SSL3_AD_BAD_RECORD_MAC:
		str = "BM";
		break;
	case SSL3_AD_DECOMPRESSION_FAILURE:
		str = "DF";
		break;
	case SSL3_AD_HANDSHAKE_FAILURE:
		str = "HF";
		break;
	case SSL3_AD_NO_CERTIFICATE:
		str = "NC";
		break;
	case SSL3_AD_BAD_CERTIFICATE:
		str = "BC";
		break;
	case SSL3_AD_UNSUPPORTED_CERTIFICATE:
		str = "UC";
		break;
	case SSL3_AD_CERTIFICATE_REVOKED:
		str = "CR";
		break;
	case SSL3_AD_CERTIFICATE_EXPIRED:
		str = "CE";
		break;
	case SSL3_AD_CERTIFICATE_UNKNOWN:
		str = "CU";
		break;
	case SSL3_AD_ILLEGAL_PARAMETER:
		str = "IP";
		break;
	case TLS1_AD_DECRYPTION_FAILED:
		str = "DC";
		break;
	case TLS1_AD_RECORD_OVERFLOW:
		str = "RO";
		break;
	case TLS1_AD_UNKNOWN_CA:
		str = "CA";
		break;
	case TLS1_AD_ACCESS_DENIED:
		str = "AD";
		break;
	case TLS1_AD_DECODE_ERROR:
		str = "DE";
		break;
	case TLS1_AD_DECRYPT_ERROR:
		str = "CY";
		break;
	case TLS1_AD_EXPORT_RESTRICTION:
		str = "ER";
		break;
	case TLS1_AD_PROTOCOL_VERSION:
		str = "PV";
		break;
	case TLS1_AD_INSUFFICIENT_SECURITY:
		str = "IS";
		break;
	case TLS1_AD_INTERNAL_ERROR:
		str = "IE";
		break;
	case TLS1_AD_USER_CANCELLED:
		str = "US";
		break;
	case TLS1_AD_NO_RENEGOTIATION:
		str = "NR";
		break;
	case TLS1_AD_UNSUPPORTED_EXTENSION:
		str = "UE";
		break;
	case TLS1_AD_CERTIFICATE_UNOBTAINABLE:
		str = "CO";
		break;
	case TLS1_AD_UNRECOGNIZED_NAME:
		str = "UN";
		break;
	case TLS1_AD_BAD_CERTIFICATE_STATUS_RESPONSE:
		str = "BR";
		break;
	case TLS1_AD_BAD_CERTIFICATE_HASH_VALUE:
		str = "BH";
		break;
	case TLS1_AD_UNKNOWN_PSK_IDENTITY:
		str = "UP";
		break;
	default:
		str = "UK";
		break;
	}
	return (str);
}

const char *
SSL_alert_desc_string_long(int value)
{
	const char *str;

	switch (value & 0xff) {
	case SSL3_AD_CLOSE_NOTIFY:
		str = "close notify";
		break;
	case SSL3_AD_UNEXPECTED_MESSAGE:
		str = "unexpected_message";
		break;
	case SSL3_AD_BAD_RECORD_MAC:
		str = "bad record mac";
		break;
	case SSL3_AD_DECOMPRESSION_FAILURE:
		str = "decompression failure";
		break;
	case SSL3_AD_HANDSHAKE_FAILURE:
		str = "handshake failure";
		break;
	case SSL3_AD_NO_CERTIFICATE:
		str = "no certificate";
		break;
	case SSL3_AD_BAD_CERTIFICATE:
		str = "bad certificate";
		break;
	case SSL3_AD_UNSUPPORTED_CERTIFICATE:
		str = "unsupported certificate";
		break;
	case SSL3_AD_CERTIFICATE_REVOKED:
		str = "certificate revoked";
		break;
	case SSL3_AD_CERTIFICATE_EXPIRED:
		str = "certificate expired";
		break;
	case SSL3_AD_CERTIFICATE_UNKNOWN:
		str = "certificate unknown";
		break;
	case SSL3_AD_ILLEGAL_PARAMETER:
		str = "illegal parameter";
		break;
	case TLS1_AD_DECRYPTION_FAILED:
		str = "decryption failed";
		break;
	case TLS1_AD_RECORD_OVERFLOW:
		str = "record overflow";
		break;
	case TLS1_AD_UNKNOWN_CA:
		str = "unknown CA";
		break;
	case TLS1_AD_ACCESS_DENIED:
		str = "access denied";
		break;
	case TLS1_AD_DECODE_ERROR:
		str = "decode error";
		break;
	case TLS1_AD_DECRYPT_ERROR:
		str = "decrypt error";
		break;
	case TLS1_AD_EXPORT_RESTRICTION:
		str = "export restriction";
		break;
	case TLS1_AD_PROTOCOL_VERSION:
		str = "protocol version";
		break;
	case TLS1_AD_INSUFFICIENT_SECURITY:
		str = "insufficient security";
		break;
	case TLS1_AD_INTERNAL_ERROR:
		str = "internal error";
		break;
	case TLS1_AD_USER_CANCELLED:
		str = "user canceled";
		break;
	case TLS1_AD_NO_RENEGOTIATION:
		str = "no renegotiation";
		break;
	case TLS1_AD_UNSUPPORTED_EXTENSION:
		str = "unsupported extension";
		break;
	case TLS1_AD_CERTIFICATE_UNOBTAINABLE:
		str = "certificate unobtainable";
		break;
	case TLS1_AD_UNRECOGNIZED_NAME:
		str = "unrecognized name";
		break;
	case TLS1_AD_BAD_CERTIFICATE_STATUS_RESPONSE:
		str = "bad certificate status response";
		break;
	case TLS1_AD_BAD_CERTIFICATE_HASH_VALUE:
		str = "bad certificate hash value";
		break;
	case TLS1_AD_UNKNOWN_PSK_IDENTITY:
		str = "unknown PSK identity";
		break;
	default:
		str = "unknown";
		break;
	}
	return (str);
}

const char *
SSL_rstate_string(const SSL *s)
{
	const char *str;

	switch (s->internal->rstate) {
	case SSL_ST_READ_HEADER:
		str = "RH";
		break;
	case SSL_ST_READ_BODY:
		str = "RB";
		break;
	case SSL_ST_READ_DONE:
		str = "RD";
		break;
	default:
		str = "unknown";
		break;
	}
	return (str);
}
