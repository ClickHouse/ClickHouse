/* $OpenBSD: d1_both.c,v 1.51 2017/05/07 04:22:24 beck Exp $ */
/*
 * DTLS implementation written by Nagendra Modadugu
 * (nagendra@cs.stanford.edu) for the OpenSSL project 2005.
 */
/* ====================================================================
 * Copyright (c) 1998-2005 The OpenSSL Project.  All rights reserved.
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

#include <limits.h>
#include <stdio.h>
#include <string.h>

#include "ssl_locl.h"

#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <openssl/objects.h>
#include <openssl/x509.h>

#include "pqueue.h"
#include "bytestring.h"

#define RSMBLY_BITMASK_SIZE(msg_len) (((msg_len) + 7) / 8)

#define RSMBLY_BITMASK_MARK(bitmask, start, end) { \
			if ((end) - (start) <= 8) { \
				long ii; \
				for (ii = (start); ii < (end); ii++) bitmask[((ii) >> 3)] |= (1 << ((ii) & 7)); \
			} else { \
				long ii; \
				bitmask[((start) >> 3)] |= bitmask_start_values[((start) & 7)]; \
				for (ii = (((start) >> 3) + 1); ii < ((((end) - 1)) >> 3); ii++) bitmask[ii] = 0xff; \
				bitmask[(((end) - 1) >> 3)] |= bitmask_end_values[((end) & 7)]; \
			} }

#define RSMBLY_BITMASK_IS_COMPLETE(bitmask, msg_len, is_complete) { \
			long ii; \
			OPENSSL_assert((msg_len) > 0); \
			is_complete = 1; \
			if (bitmask[(((msg_len) - 1) >> 3)] != bitmask_end_values[((msg_len) & 7)]) is_complete = 0; \
			if (is_complete) for (ii = (((msg_len) - 1) >> 3) - 1; ii >= 0 ; ii--) \
				if (bitmask[ii] != 0xff) { is_complete = 0; break; } }

static unsigned char bitmask_start_values[] = {
	0xff, 0xfe, 0xfc, 0xf8, 0xf0, 0xe0, 0xc0, 0x80
};
static unsigned char bitmask_end_values[] = {
	0xff, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f
};

/* XDTLS:  figure out the right values */
static unsigned int g_probable_mtu[] = {1500 - 28, 512 - 28, 256 - 28};

static unsigned int dtls1_guess_mtu(unsigned int curr_mtu);
static void dtls1_fix_message_header(SSL *s, unsigned long frag_off,
    unsigned long frag_len);
static unsigned char *dtls1_write_message_header(SSL *s, unsigned char *p);
static void dtls1_set_message_header_int(SSL *s, unsigned char mt,
    unsigned long len, unsigned short seq_num, unsigned long frag_off,
    unsigned long frag_len);
static long dtls1_get_message_fragment(SSL *s, int st1, int stn, long max,
    int *ok);

static hm_fragment *
dtls1_hm_fragment_new(unsigned long frag_len, int reassembly)
{
	hm_fragment *frag = NULL;
	unsigned char *buf = NULL;
	unsigned char *bitmask = NULL;

	frag = malloc(sizeof(hm_fragment));
	if (frag == NULL)
		return NULL;

	if (frag_len) {
		buf = malloc(frag_len);
		if (buf == NULL) {
			free(frag);
			return NULL;
		}
	}

	/* zero length fragment gets zero frag->fragment */
	frag->fragment = buf;

	/* Initialize reassembly bitmask if necessary */
	if (reassembly) {
		bitmask = malloc(RSMBLY_BITMASK_SIZE(frag_len));
		if (bitmask == NULL) {
			free(buf);
			free(frag);
			return NULL;
		}
		memset(bitmask, 0, RSMBLY_BITMASK_SIZE(frag_len));
	}

	frag->reassembly = bitmask;

	return frag;
}

static void
dtls1_hm_fragment_free(hm_fragment *frag)
{
	if (frag == NULL)
		return;

	if (frag->msg_header.is_ccs) {
		EVP_CIPHER_CTX_free(
		    frag->msg_header.saved_retransmit_state.enc_write_ctx);
		EVP_MD_CTX_destroy(
		    frag->msg_header.saved_retransmit_state.write_hash);
	}
	free(frag->fragment);
	free(frag->reassembly);
	free(frag);
}

/* send s->internal->init_buf in records of type 'type' (SSL3_RT_HANDSHAKE or SSL3_RT_CHANGE_CIPHER_SPEC) */
int
dtls1_do_write(SSL *s, int type)
{
	int ret;
	int curr_mtu;
	unsigned int len, frag_off, mac_size, blocksize;

	/* AHA!  Figure out the MTU, and stick to the right size */
	if (D1I(s)->mtu < dtls1_min_mtu() &&
	    !(SSL_get_options(s) & SSL_OP_NO_QUERY_MTU)) {
		D1I(s)->mtu = BIO_ctrl(SSL_get_wbio(s),
		    BIO_CTRL_DGRAM_QUERY_MTU, 0, NULL);

		/*
		 * I've seen the kernel return bogus numbers when it
		 * doesn't know the MTU (ie., the initial write), so just
		 * make sure we have a reasonable number
		 */
		if (D1I(s)->mtu < dtls1_min_mtu()) {
			D1I(s)->mtu = 0;
			D1I(s)->mtu = dtls1_guess_mtu(D1I(s)->mtu);
			BIO_ctrl(SSL_get_wbio(s), BIO_CTRL_DGRAM_SET_MTU,
			    D1I(s)->mtu, NULL);
		}
	}

	OPENSSL_assert(D1I(s)->mtu >= dtls1_min_mtu());
	/* should have something reasonable now */

	if (s->internal->init_off == 0  && type == SSL3_RT_HANDSHAKE)
		OPENSSL_assert(s->internal->init_num ==
		    (int)D1I(s)->w_msg_hdr.msg_len + DTLS1_HM_HEADER_LENGTH);

	if (s->internal->write_hash)
		mac_size = EVP_MD_CTX_size(s->internal->write_hash);
	else
		mac_size = 0;

	if (s->internal->enc_write_ctx &&
	    (EVP_CIPHER_mode( s->internal->enc_write_ctx->cipher) & EVP_CIPH_CBC_MODE))
		blocksize = 2 * EVP_CIPHER_block_size(s->internal->enc_write_ctx->cipher);
	else
		blocksize = 0;

	frag_off = 0;
	while (s->internal->init_num) {
		curr_mtu = D1I(s)->mtu - BIO_wpending(SSL_get_wbio(s)) -
		    DTLS1_RT_HEADER_LENGTH - mac_size - blocksize;

		if (curr_mtu <= DTLS1_HM_HEADER_LENGTH) {
			/* grr.. we could get an error if MTU picked was wrong */
			ret = BIO_flush(SSL_get_wbio(s));
			if (ret <= 0)
				return ret;
			curr_mtu = D1I(s)->mtu - DTLS1_RT_HEADER_LENGTH -
			    mac_size - blocksize;
		}

		if (s->internal->init_num > curr_mtu)
			len = curr_mtu;
		else
			len = s->internal->init_num;


		/* XDTLS: this function is too long.  split out the CCS part */
		if (type == SSL3_RT_HANDSHAKE) {
			if (s->internal->init_off != 0) {
				OPENSSL_assert(s->internal->init_off > DTLS1_HM_HEADER_LENGTH);
				s->internal->init_off -= DTLS1_HM_HEADER_LENGTH;
				s->internal->init_num += DTLS1_HM_HEADER_LENGTH;

				if (s->internal->init_num > curr_mtu)
					len = curr_mtu;
				else
					len = s->internal->init_num;
			}

			dtls1_fix_message_header(s, frag_off,
			    len - DTLS1_HM_HEADER_LENGTH);

			dtls1_write_message_header(s,
			    (unsigned char *)&s->internal->init_buf->data[s->internal->init_off]);

			OPENSSL_assert(len >= DTLS1_HM_HEADER_LENGTH);
		}

		ret = dtls1_write_bytes(s, type,
		    &s->internal->init_buf->data[s->internal->init_off], len);
		if (ret < 0) {
			/*
			 * Might need to update MTU here, but we don't know
			 * which previous packet caused the failure -- so
			 * can't really retransmit anything.  continue as
			 * if everything is fine and wait for an alert to
			 * handle the retransmit
			 */
			if (BIO_ctrl(SSL_get_wbio(s),
			    BIO_CTRL_DGRAM_MTU_EXCEEDED, 0, NULL) > 0)
				D1I(s)->mtu = BIO_ctrl(SSL_get_wbio(s),
				    BIO_CTRL_DGRAM_QUERY_MTU, 0, NULL);
			else
				return (-1);
		} else {

			/*
			 * Bad if this assert fails, only part of the
			 * handshake message got sent.  but why would
			 * this happen?
			 */
			OPENSSL_assert(len == (unsigned int)ret);

			if (type == SSL3_RT_HANDSHAKE &&
			    !D1I(s)->retransmitting) {
				/*
				 * Should not be done for 'Hello Request's,
				 * but in that case we'll ignore the result
				 * anyway
				 */
				unsigned char *p = (unsigned char *)&s->internal->init_buf->data[s->internal->init_off];
				const struct hm_header_st *msg_hdr = &D1I(s)->w_msg_hdr;
				int xlen;

				if (frag_off == 0) {
					/*
					 * Reconstruct message header is if it
					 * is being sent in single fragment
					 */
					*p++ = msg_hdr->type;
					l2n3(msg_hdr->msg_len, p);
					s2n (msg_hdr->seq, p);
					l2n3(0, p);
					l2n3(msg_hdr->msg_len, p);
					p -= DTLS1_HM_HEADER_LENGTH;
					xlen = ret;
				} else {
					p += DTLS1_HM_HEADER_LENGTH;
					xlen = ret - DTLS1_HM_HEADER_LENGTH;
				}

				tls1_finish_mac(s, p, xlen);
			}

			if (ret == s->internal->init_num) {
				if (s->internal->msg_callback)
					s->internal->msg_callback(1, s->version, type,
					    s->internal->init_buf->data,
					    (size_t)(s->internal->init_off + s->internal->init_num),
					    s, s->internal->msg_callback_arg);

				s->internal->init_off = 0;
				/* done writing this message */
				s->internal->init_num = 0;

				return (1);
			}
			s->internal->init_off += ret;
			s->internal->init_num -= ret;
			frag_off += (ret -= DTLS1_HM_HEADER_LENGTH);
		}
	}
	return (0);
}


/*
 * Obtain handshake message of message type 'mt' (any if mt == -1),
 * maximum acceptable body length 'max'.
 * Read an entire handshake message.  Handshake messages arrive in
 * fragments.
 */
long
dtls1_get_message(SSL *s, int st1, int stn, int mt, long max, int *ok)
{
	int i, al;
	struct hm_header_st *msg_hdr;
	unsigned char *p;
	unsigned long msg_len;

	/*
	 * s3->internal->tmp is used to store messages that are unexpected, caused
	 * by the absence of an optional handshake message
	 */
	if (S3I(s)->tmp.reuse_message) {
		S3I(s)->tmp.reuse_message = 0;
		if ((mt >= 0) && (S3I(s)->tmp.message_type != mt)) {
			al = SSL_AD_UNEXPECTED_MESSAGE;
			SSLerror(s, SSL_R_UNEXPECTED_MESSAGE);
			goto f_err;
		}
		*ok = 1;
		s->internal->init_msg = s->internal->init_buf->data + DTLS1_HM_HEADER_LENGTH;
		s->internal->init_num = (int)S3I(s)->tmp.message_size;
		return s->internal->init_num;
	}

	msg_hdr = &D1I(s)->r_msg_hdr;
	memset(msg_hdr, 0x00, sizeof(struct hm_header_st));

again:
	i = dtls1_get_message_fragment(s, st1, stn, max, ok);
	if (i == DTLS1_HM_BAD_FRAGMENT ||
	    i == DTLS1_HM_FRAGMENT_RETRY)  /* bad fragment received */
		goto again;
	else if (i <= 0 && !*ok)
		return i;

	p = (unsigned char *)s->internal->init_buf->data;
	msg_len = msg_hdr->msg_len;

	/* reconstruct message header */
	*(p++) = msg_hdr->type;
	l2n3(msg_len, p);
	s2n (msg_hdr->seq, p);
	l2n3(0, p);
	l2n3(msg_len, p);

	p -= DTLS1_HM_HEADER_LENGTH;
	msg_len += DTLS1_HM_HEADER_LENGTH;

	tls1_finish_mac(s, p, msg_len);
	if (s->internal->msg_callback)
		s->internal->msg_callback(0, s->version, SSL3_RT_HANDSHAKE, p, msg_len,
		    s, s->internal->msg_callback_arg);

	memset(msg_hdr, 0x00, sizeof(struct hm_header_st));

	/* Don't change sequence numbers while listening */
	if (!D1I(s)->listen)
		D1I(s)->handshake_read_seq++;

	s->internal->init_msg = s->internal->init_buf->data + DTLS1_HM_HEADER_LENGTH;
	return s->internal->init_num;

f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, al);
	*ok = 0;
	return -1;
}


static int
dtls1_preprocess_fragment(SSL *s, struct hm_header_st *msg_hdr, int max)
{
	size_t frag_off, frag_len, msg_len;

	msg_len = msg_hdr->msg_len;
	frag_off = msg_hdr->frag_off;
	frag_len = msg_hdr->frag_len;

	/* sanity checking */
	if ((frag_off + frag_len) > msg_len) {
		SSLerror(s, SSL_R_EXCESSIVE_MESSAGE_SIZE);
		return SSL_AD_ILLEGAL_PARAMETER;
	}

	if ((frag_off + frag_len) > (unsigned long)max) {
		SSLerror(s, SSL_R_EXCESSIVE_MESSAGE_SIZE);
		return SSL_AD_ILLEGAL_PARAMETER;
	}

	if ( D1I(s)->r_msg_hdr.frag_off == 0) /* first fragment */
	{
		/*
		 * msg_len is limited to 2^24, but is effectively checked
		 * against max above
		 */
		if (!BUF_MEM_grow_clean(s->internal->init_buf,
		    msg_len + DTLS1_HM_HEADER_LENGTH)) {
			SSLerror(s, ERR_R_BUF_LIB);
			return SSL_AD_INTERNAL_ERROR;
		}

		S3I(s)->tmp.message_size = msg_len;
		D1I(s)->r_msg_hdr.msg_len = msg_len;
		S3I(s)->tmp.message_type = msg_hdr->type;
		D1I(s)->r_msg_hdr.type = msg_hdr->type;
		D1I(s)->r_msg_hdr.seq = msg_hdr->seq;
	} else if (msg_len != D1I(s)->r_msg_hdr.msg_len) {
		/*
		 * They must be playing with us! BTW, failure to enforce
		 * upper limit would open possibility for buffer overrun.
		 */
		SSLerror(s, SSL_R_EXCESSIVE_MESSAGE_SIZE);
		return SSL_AD_ILLEGAL_PARAMETER;
	}

	return 0; /* no error */
}

static int
dtls1_retrieve_buffered_fragment(SSL *s, long max, int *ok)
{
	/*
	 * (0) check whether the desired fragment is available
	 * if so:
	 * (1) copy over the fragment to s->internal->init_buf->data[]
	 * (2) update s->internal->init_num
	 */
	pitem *item;
	hm_fragment *frag;
	int al;

	*ok = 0;
	item = pqueue_peek(D1I(s)->buffered_messages);
	if (item == NULL)
		return 0;

	frag = (hm_fragment *)item->data;

	/* Don't return if reassembly still in progress */
	if (frag->reassembly != NULL)
		return 0;

	if (D1I(s)->handshake_read_seq == frag->msg_header.seq) {
		unsigned long frag_len = frag->msg_header.frag_len;
		pqueue_pop(D1I(s)->buffered_messages);

		al = dtls1_preprocess_fragment(s, &frag->msg_header, max);

		if (al == 0) /* no alert */
		{
			unsigned char *p = (unsigned char *)s->internal->init_buf->data + DTLS1_HM_HEADER_LENGTH;
			memcpy(&p[frag->msg_header.frag_off],
			    frag->fragment, frag->msg_header.frag_len);
		}

		dtls1_hm_fragment_free(frag);
		pitem_free(item);

		if (al == 0) {
			*ok = 1;
			return frag_len;
		}

		ssl3_send_alert(s, SSL3_AL_FATAL, al);
		s->internal->init_num = 0;
		*ok = 0;
		return -1;
	} else
		return 0;
}

/*
 * dtls1_max_handshake_message_len returns the maximum number of bytes
 * permitted in a DTLS handshake message for |s|. The minimum is 16KB,
 * but may be greater if the maximum certificate list size requires it.
 */
static unsigned long
dtls1_max_handshake_message_len(const SSL *s)
{
	unsigned long max_len;

	max_len = DTLS1_HM_HEADER_LENGTH + SSL3_RT_MAX_ENCRYPTED_LENGTH;
	if (max_len < (unsigned long)s->internal->max_cert_list)
		return s->internal->max_cert_list;
	return max_len;
}

static int
dtls1_reassemble_fragment(SSL *s, struct hm_header_st* msg_hdr, int *ok)
{
	hm_fragment *frag = NULL;
	pitem *item = NULL;
	int i = -1, is_complete;
	unsigned char seq64be[8];
	unsigned long frag_len = msg_hdr->frag_len;

	if ((msg_hdr->frag_off + frag_len) > msg_hdr->msg_len ||
	    msg_hdr->msg_len > dtls1_max_handshake_message_len(s))
		goto err;

	if (frag_len == 0) {
		i = DTLS1_HM_FRAGMENT_RETRY;
		goto err;
	}

	/* Try to find item in queue */
	memset(seq64be, 0, sizeof(seq64be));
	seq64be[6] = (unsigned char)(msg_hdr->seq >> 8);
	seq64be[7] = (unsigned char)msg_hdr->seq;
	item = pqueue_find(D1I(s)->buffered_messages, seq64be);

	if (item == NULL) {
		frag = dtls1_hm_fragment_new(msg_hdr->msg_len, 1);
		if (frag == NULL)
			goto err;
		memcpy(&(frag->msg_header), msg_hdr, sizeof(*msg_hdr));
		frag->msg_header.frag_len = frag->msg_header.msg_len;
		frag->msg_header.frag_off = 0;
	} else {
		frag = (hm_fragment*)item->data;
		if (frag->msg_header.msg_len != msg_hdr->msg_len) {
			item = NULL;
			frag = NULL;
			goto err;
		}
	}

	/*
	 * If message is already reassembled, this must be a
	 * retransmit and can be dropped.
	 */
	if (frag->reassembly == NULL) {
		unsigned char devnull [256];

		while (frag_len) {
			i = s->method->internal->ssl_read_bytes(s, SSL3_RT_HANDSHAKE,
			    devnull, frag_len > sizeof(devnull) ?
			    sizeof(devnull) : frag_len, 0);
			if (i <= 0)
				goto err;
			frag_len -= i;
		}
		i = DTLS1_HM_FRAGMENT_RETRY;
		goto err;
	}

	/* read the body of the fragment (header has already been read */
	i = s->method->internal->ssl_read_bytes(s, SSL3_RT_HANDSHAKE,
	    frag->fragment + msg_hdr->frag_off, frag_len, 0);
	if (i <= 0 || (unsigned long)i != frag_len)
		goto err;

	RSMBLY_BITMASK_MARK(frag->reassembly, (long)msg_hdr->frag_off,
	    (long)(msg_hdr->frag_off + frag_len));

	RSMBLY_BITMASK_IS_COMPLETE(frag->reassembly, (long)msg_hdr->msg_len,
	    is_complete);

	if (is_complete) {
		free(frag->reassembly);
		frag->reassembly = NULL;
	}

	if (item == NULL) {
		memset(seq64be, 0, sizeof(seq64be));
		seq64be[6] = (unsigned char)(msg_hdr->seq >> 8);
		seq64be[7] = (unsigned char)(msg_hdr->seq);

		item = pitem_new(seq64be, frag);
		if (item == NULL) {
			i = -1;
			goto err;
		}

		pqueue_insert(D1I(s)->buffered_messages, item);
	}

	return DTLS1_HM_FRAGMENT_RETRY;

err:
	if (item == NULL && frag != NULL)
		dtls1_hm_fragment_free(frag);
	*ok = 0;
	return i;
}


static int
dtls1_process_out_of_seq_message(SSL *s, struct hm_header_st* msg_hdr, int *ok)
{
	int i = -1;
	hm_fragment *frag = NULL;
	pitem *item = NULL;
	unsigned char seq64be[8];
	unsigned long frag_len = msg_hdr->frag_len;

	if ((msg_hdr->frag_off + frag_len) > msg_hdr->msg_len)
		goto err;

	/* Try to find item in queue, to prevent duplicate entries */
	memset(seq64be, 0, sizeof(seq64be));
	seq64be[6] = (unsigned char) (msg_hdr->seq >> 8);
	seq64be[7] = (unsigned char) msg_hdr->seq;
	item = pqueue_find(D1I(s)->buffered_messages, seq64be);

	/*
	 * If we already have an entry and this one is a fragment,
	 * don't discard it and rather try to reassemble it.
	 */
	if (item != NULL && frag_len < msg_hdr->msg_len)
		item = NULL;

	/*
	 * Discard the message if sequence number was already there, is
	 * too far in the future, already in the queue or if we received
	 * a FINISHED before the SERVER_HELLO, which then must be a stale
	 * retransmit.
	 */
	if (msg_hdr->seq <= D1I(s)->handshake_read_seq ||
	    msg_hdr->seq > D1I(s)->handshake_read_seq + 10 || item != NULL ||
	    (D1I(s)->handshake_read_seq == 0 &&
	    msg_hdr->type == SSL3_MT_FINISHED)) {
		unsigned char devnull [256];

		while (frag_len) {
			i = s->method->internal->ssl_read_bytes(s, SSL3_RT_HANDSHAKE,
			    devnull, frag_len > sizeof(devnull) ?
			    sizeof(devnull) : frag_len, 0);
			if (i <= 0)
				goto err;
			frag_len -= i;
		}
	} else {
		if (frag_len < msg_hdr->msg_len)
			return dtls1_reassemble_fragment(s, msg_hdr, ok);

		if (frag_len > dtls1_max_handshake_message_len(s))
			goto err;

		frag = dtls1_hm_fragment_new(frag_len, 0);
		if (frag == NULL)
			goto err;

		memcpy(&(frag->msg_header), msg_hdr, sizeof(*msg_hdr));

		if (frag_len) {
			/* read the body of the fragment (header has already been read */
			i = s->method->internal->ssl_read_bytes(s, SSL3_RT_HANDSHAKE,
			    frag->fragment, frag_len, 0);
			if (i <= 0 || (unsigned long)i != frag_len)
				goto err;
		}

		memset(seq64be, 0, sizeof(seq64be));
		seq64be[6] = (unsigned char)(msg_hdr->seq >> 8);
		seq64be[7] = (unsigned char)(msg_hdr->seq);

		item = pitem_new(seq64be, frag);
		if (item == NULL)
			goto err;

		pqueue_insert(D1I(s)->buffered_messages, item);
	}

	return DTLS1_HM_FRAGMENT_RETRY;

err:
	if (item == NULL && frag != NULL)
		dtls1_hm_fragment_free(frag);
	*ok = 0;
	return i;
}


static long
dtls1_get_message_fragment(SSL *s, int st1, int stn, long max, int *ok)
{
	unsigned char wire[DTLS1_HM_HEADER_LENGTH];
	unsigned long len, frag_off, frag_len;
	int i, al;
	struct hm_header_st msg_hdr;

again:
	/* see if we have the required fragment already */
	if ((frag_len = dtls1_retrieve_buffered_fragment(s, max, ok)) || *ok) {
		if (*ok)
			s->internal->init_num = frag_len;
		return frag_len;
	}

	/* read handshake message header */
	i = s->method->internal->ssl_read_bytes(s, SSL3_RT_HANDSHAKE, wire,
	    DTLS1_HM_HEADER_LENGTH, 0);
	if (i <= 0) 	/* nbio, or an error */
	{
		s->internal->rwstate = SSL_READING;
		*ok = 0;
		return i;
	}
	/* Handshake fails if message header is incomplete */
	if (i != DTLS1_HM_HEADER_LENGTH ||
	    /* parse the message fragment header */
	    dtls1_get_message_header(wire, &msg_hdr) == 0) {
		al = SSL_AD_UNEXPECTED_MESSAGE;
		SSLerror(s, SSL_R_UNEXPECTED_MESSAGE);
		goto f_err;
	}

	/*
	 * if this is a future (or stale) message it gets buffered
	 * (or dropped)--no further processing at this time
	 * While listening, we accept seq 1 (ClientHello with cookie)
	 * although we're still expecting seq 0 (ClientHello)
	 */
	if (msg_hdr.seq != D1I(s)->handshake_read_seq &&
	    !(D1I(s)->listen && msg_hdr.seq == 1))
		return dtls1_process_out_of_seq_message(s, &msg_hdr, ok);

	len = msg_hdr.msg_len;
	frag_off = msg_hdr.frag_off;
	frag_len = msg_hdr.frag_len;

	if (frag_len && frag_len < len)
		return dtls1_reassemble_fragment(s, &msg_hdr, ok);

	if (!s->server && D1I(s)->r_msg_hdr.frag_off == 0 &&
	    wire[0] == SSL3_MT_HELLO_REQUEST) {
		/*
		 * The server may always send 'Hello Request' messages --
		 * we are doing a handshake anyway now, so ignore them
		 * if their format is correct. Does not count for
		 * 'Finished' MAC.
		 */
		if (wire[1] == 0 && wire[2] == 0 && wire[3] == 0) {
			if (s->internal->msg_callback)
				s->internal->msg_callback(0, s->version,
				    SSL3_RT_HANDSHAKE, wire,
				    DTLS1_HM_HEADER_LENGTH, s,
				    s->internal->msg_callback_arg);

			s->internal->init_num = 0;
			goto again;
		}
		else /* Incorrectly formated Hello request */
		{
			al = SSL_AD_UNEXPECTED_MESSAGE;
			SSLerror(s, SSL_R_UNEXPECTED_MESSAGE);
			goto f_err;
		}
	}

	if ((al = dtls1_preprocess_fragment(s, &msg_hdr, max)))
		goto f_err;

	/* XDTLS:  ressurect this when restart is in place */
	S3I(s)->hs.state = stn;

	if (frag_len > 0) {
		unsigned char *p = (unsigned char *)s->internal->init_buf->data + DTLS1_HM_HEADER_LENGTH;

		i = s->method->internal->ssl_read_bytes(s, SSL3_RT_HANDSHAKE,
		    &p[frag_off], frag_len, 0);
		/* XDTLS:  fix this--message fragments cannot span multiple packets */
		if (i <= 0) {
			s->internal->rwstate = SSL_READING;
			*ok = 0;
			return i;
		}
	} else
		i = 0;

	/*
	 * XDTLS:  an incorrectly formatted fragment should cause the
	 * handshake to fail
	 */
	if (i != (int)frag_len) {
		al = SSL3_AD_ILLEGAL_PARAMETER;
		SSLerror(s, SSL3_AD_ILLEGAL_PARAMETER);
		goto f_err;
	}

	*ok = 1;

	/*
	 * Note that s->internal->init_num is *not* used as current offset in
	 * s->internal->init_buf->data, but as a counter summing up fragments'
	 * lengths: as soon as they sum up to handshake packet
	 * length, we assume we have got all the fragments.
	 */
	s->internal->init_num = frag_len;
	return frag_len;

f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, al);
	s->internal->init_num = 0;

	*ok = 0;
	return (-1);
}

/*
 * for these 2 messages, we need to
 * ssl->enc_read_ctx			re-init
 * ssl->s3->internal->read_sequence		zero
 * ssl->s3->internal->read_mac_secret		re-init
 * ssl->session->read_sym_enc		assign
 * ssl->session->read_hash		assign
 */
int
dtls1_send_change_cipher_spec(SSL *s, int a, int b)
{
	unsigned char *p;

	if (S3I(s)->hs.state == a) {
		p = (unsigned char *)s->internal->init_buf->data;
		*p++=SSL3_MT_CCS;
		D1I(s)->handshake_write_seq = D1I(s)->next_handshake_write_seq;
		s->internal->init_num = DTLS1_CCS_HEADER_LENGTH;

		s->internal->init_off = 0;

		dtls1_set_message_header_int(s, SSL3_MT_CCS, 0,
		    D1I(s)->handshake_write_seq, 0, 0);

		/* buffer the message to handle re-xmits */
		dtls1_buffer_message(s, 1);

		S3I(s)->hs.state = b;
	}

	/* SSL3_ST_CW_CHANGE_B */
	return (dtls1_do_write(s, SSL3_RT_CHANGE_CIPHER_SPEC));
}

int
dtls1_read_failed(SSL *s, int code)
{
	if (code > 0) {
#ifdef DEBUG
		fprintf(stderr, "invalid state reached %s:%d",
		    __FILE__, __LINE__);
#endif
		return 1;
	}

	if (!dtls1_is_timer_expired(s)) {
		/*
		 * not a timeout, none of our business, let higher layers
		 * handle this.  in fact it's probably an error
		 */
		return code;
	}

	if (!SSL_in_init(s))  /* done, no need to send a retransmit */
	{
		BIO_set_flags(SSL_get_rbio(s), BIO_FLAGS_READ);
		return code;
	}

	return dtls1_handle_timeout(s);
}

int
dtls1_get_queue_priority(unsigned short seq, int is_ccs)
{
	/*
	 * The index of the retransmission queue actually is the message
	 * sequence number, since the queue only contains messages of a
	 * single handshake. However, the ChangeCipherSpec has no message
	 * sequence number and so using only the sequence will result in
	 * the CCS and Finished having the same index. To prevent this, the
	 * sequence number is multiplied by 2. In case of a CCS 1 is
	 * subtracted.  This does not only differ CSS and Finished, it also
	 * maintains the order of the index (important for priority queues)
	 * and fits in the unsigned short variable.
	 */
	return seq * 2 - is_ccs;
}

int
dtls1_retransmit_buffered_messages(SSL *s)
{
	pqueue sent = s->d1->sent_messages;
	piterator iter;
	pitem *item;
	hm_fragment *frag;
	int found = 0;

	iter = pqueue_iterator(sent);

	for (item = pqueue_next(&iter); item != NULL;
	    item = pqueue_next(&iter)) {
		frag = (hm_fragment *)item->data;
		if (dtls1_retransmit_message(s,
		    (unsigned short)dtls1_get_queue_priority(
		    frag->msg_header.seq, frag->msg_header.is_ccs), 0,
		    &found) <= 0 && found) {
#ifdef DEBUG
			fprintf(stderr, "dtls1_retransmit_message() failed\n");
#endif
			return -1;
		}
	}

	return 1;
}

int
dtls1_buffer_message(SSL *s, int is_ccs)
{
	pitem *item;
	hm_fragment *frag;
	unsigned char seq64be[8];

	/* Buffer the messsage in order to handle DTLS retransmissions. */

	/*
	 * This function is called immediately after a message has
	 * been serialized
	 */
	OPENSSL_assert(s->internal->init_off == 0);

	frag = dtls1_hm_fragment_new(s->internal->init_num, 0);
	if (frag == NULL)
		return 0;

	memcpy(frag->fragment, s->internal->init_buf->data, s->internal->init_num);

	if (is_ccs) {
		OPENSSL_assert(D1I(s)->w_msg_hdr.msg_len +
		    ((s->version == DTLS1_VERSION) ?
		    DTLS1_CCS_HEADER_LENGTH : 3) == (unsigned int)s->internal->init_num);
	} else {
		OPENSSL_assert(D1I(s)->w_msg_hdr.msg_len +
		    DTLS1_HM_HEADER_LENGTH == (unsigned int)s->internal->init_num);
	}

	frag->msg_header.msg_len = D1I(s)->w_msg_hdr.msg_len;
	frag->msg_header.seq = D1I(s)->w_msg_hdr.seq;
	frag->msg_header.type = D1I(s)->w_msg_hdr.type;
	frag->msg_header.frag_off = 0;
	frag->msg_header.frag_len = D1I(s)->w_msg_hdr.msg_len;
	frag->msg_header.is_ccs = is_ccs;

	/* save current state*/
	frag->msg_header.saved_retransmit_state.enc_write_ctx = s->internal->enc_write_ctx;
	frag->msg_header.saved_retransmit_state.write_hash = s->internal->write_hash;
	frag->msg_header.saved_retransmit_state.session = s->session;
	frag->msg_header.saved_retransmit_state.epoch = D1I(s)->w_epoch;

	memset(seq64be, 0, sizeof(seq64be));
	seq64be[6] = (unsigned char)(dtls1_get_queue_priority(
	    frag->msg_header.seq, frag->msg_header.is_ccs) >> 8);
	seq64be[7] = (unsigned char)(dtls1_get_queue_priority(
	    frag->msg_header.seq, frag->msg_header.is_ccs));

	item = pitem_new(seq64be, frag);
	if (item == NULL) {
		dtls1_hm_fragment_free(frag);
		return 0;
	}

	pqueue_insert(s->d1->sent_messages, item);
	return 1;
}

int
dtls1_retransmit_message(SSL *s, unsigned short seq, unsigned long frag_off,
    int *found)
{
	int ret;
	/* XDTLS: for now assuming that read/writes are blocking */
	pitem *item;
	hm_fragment *frag;
	unsigned long header_length;
	unsigned char seq64be[8];
	struct dtls1_retransmit_state saved_state;
	unsigned char save_write_sequence[8];

	/*
	  OPENSSL_assert(s->internal->init_num == 0);
	  OPENSSL_assert(s->internal->init_off == 0);
	 */

	/* XDTLS:  the requested message ought to be found, otherwise error */
	memset(seq64be, 0, sizeof(seq64be));
	seq64be[6] = (unsigned char)(seq >> 8);
	seq64be[7] = (unsigned char)seq;

	item = pqueue_find(s->d1->sent_messages, seq64be);
	if (item == NULL) {
#ifdef DEBUG
		fprintf(stderr, "retransmit:  message %d non-existant\n", seq);
#endif
		*found = 0;
		return 0;
	}

	*found = 1;
	frag = (hm_fragment *)item->data;

	if (frag->msg_header.is_ccs)
		header_length = DTLS1_CCS_HEADER_LENGTH;
	else
		header_length = DTLS1_HM_HEADER_LENGTH;

	memcpy(s->internal->init_buf->data, frag->fragment,
	    frag->msg_header.msg_len + header_length);
	s->internal->init_num = frag->msg_header.msg_len + header_length;

	dtls1_set_message_header_int(s, frag->msg_header.type,
	    frag->msg_header.msg_len, frag->msg_header.seq, 0,
	    frag->msg_header.frag_len);

	/* save current state */
	saved_state.enc_write_ctx = s->internal->enc_write_ctx;
	saved_state.write_hash = s->internal->write_hash;
	saved_state.session = s->session;
	saved_state.epoch = D1I(s)->w_epoch;

	D1I(s)->retransmitting = 1;

	/* restore state in which the message was originally sent */
	s->internal->enc_write_ctx = frag->msg_header.saved_retransmit_state.enc_write_ctx;
	s->internal->write_hash = frag->msg_header.saved_retransmit_state.write_hash;
	s->session = frag->msg_header.saved_retransmit_state.session;
	D1I(s)->w_epoch = frag->msg_header.saved_retransmit_state.epoch;

	if (frag->msg_header.saved_retransmit_state.epoch ==
	    saved_state.epoch - 1) {
		memcpy(save_write_sequence, S3I(s)->write_sequence,
		    sizeof(S3I(s)->write_sequence));
		memcpy(S3I(s)->write_sequence, D1I(s)->last_write_sequence,
		    sizeof(S3I(s)->write_sequence));
	}

	ret = dtls1_do_write(s, frag->msg_header.is_ccs ?
	    SSL3_RT_CHANGE_CIPHER_SPEC : SSL3_RT_HANDSHAKE);

	/* restore current state */
	s->internal->enc_write_ctx = saved_state.enc_write_ctx;
	s->internal->write_hash = saved_state.write_hash;
	s->session = saved_state.session;
	D1I(s)->w_epoch = saved_state.epoch;

	if (frag->msg_header.saved_retransmit_state.epoch ==
	    saved_state.epoch - 1) {
		memcpy(D1I(s)->last_write_sequence, S3I(s)->write_sequence,
		    sizeof(S3I(s)->write_sequence));
		memcpy(S3I(s)->write_sequence, save_write_sequence,
		    sizeof(S3I(s)->write_sequence));
	}

	D1I(s)->retransmitting = 0;

	(void)BIO_flush(SSL_get_wbio(s));
	return ret;
}

/* call this function when the buffered messages are no longer needed */
void
dtls1_clear_record_buffer(SSL *s)
{
	pitem *item;

	for(item = pqueue_pop(s->d1->sent_messages); item != NULL;
	    item = pqueue_pop(s->d1->sent_messages)) {
		dtls1_hm_fragment_free((hm_fragment *)item->data);
		pitem_free(item);
	}
}

void
dtls1_set_message_header(SSL *s, unsigned char mt, unsigned long len,
    unsigned long frag_off, unsigned long frag_len)
{
	/* Don't change sequence numbers while listening */
	if (frag_off == 0 && !D1I(s)->listen) {
		D1I(s)->handshake_write_seq = D1I(s)->next_handshake_write_seq;
		D1I(s)->next_handshake_write_seq++;
	}

	dtls1_set_message_header_int(s, mt, len, D1I(s)->handshake_write_seq,
	    frag_off, frag_len);
}

/* don't actually do the writing, wait till the MTU has been retrieved */
static void
dtls1_set_message_header_int(SSL *s, unsigned char mt, unsigned long len,
    unsigned short seq_num, unsigned long frag_off, unsigned long frag_len)
{
	struct hm_header_st *msg_hdr = &D1I(s)->w_msg_hdr;

	msg_hdr->type = mt;
	msg_hdr->msg_len = len;
	msg_hdr->seq = seq_num;
	msg_hdr->frag_off = frag_off;
	msg_hdr->frag_len = frag_len;
}

static void
dtls1_fix_message_header(SSL *s, unsigned long frag_off, unsigned long frag_len)
{
	struct hm_header_st *msg_hdr = &D1I(s)->w_msg_hdr;

	msg_hdr->frag_off = frag_off;
	msg_hdr->frag_len = frag_len;
}

static unsigned char *
dtls1_write_message_header(SSL *s, unsigned char *p)
{
	struct hm_header_st *msg_hdr = &D1I(s)->w_msg_hdr;

	*p++ = msg_hdr->type;
	l2n3(msg_hdr->msg_len, p);

	s2n(msg_hdr->seq, p);
	l2n3(msg_hdr->frag_off, p);
	l2n3(msg_hdr->frag_len, p);

	return p;
}

unsigned int
dtls1_min_mtu(void)
{
	return (g_probable_mtu[(sizeof(g_probable_mtu) /
	    sizeof(g_probable_mtu[0])) - 1]);
}

static unsigned int
dtls1_guess_mtu(unsigned int curr_mtu)
{
	unsigned int i;

	if (curr_mtu == 0)
		return g_probable_mtu[0];

	for (i = 0; i < sizeof(g_probable_mtu) / sizeof(g_probable_mtu[0]); i++)
		if (curr_mtu > g_probable_mtu[i])
			return g_probable_mtu[i];

	return curr_mtu;
}

int
dtls1_get_message_header(unsigned char *data, struct hm_header_st *msg_hdr)
{
	CBS header;
	uint32_t msg_len, frag_off, frag_len;
	uint16_t seq;
	uint8_t type;

	CBS_init(&header, data, sizeof(*msg_hdr));

	memset(msg_hdr, 0, sizeof(*msg_hdr));

	if (!CBS_get_u8(&header, &type))
		return 0;
	if (!CBS_get_u24(&header, &msg_len))
		return 0;
	if (!CBS_get_u16(&header, &seq))
		return 0;
	if (!CBS_get_u24(&header, &frag_off))
		return 0;
	if (!CBS_get_u24(&header, &frag_len))
		return 0;

	msg_hdr->type = type;
	msg_hdr->msg_len = msg_len;
	msg_hdr->seq = seq;
	msg_hdr->frag_off = frag_off;
	msg_hdr->frag_len = frag_len;

	return 1;
}

void
dtls1_get_ccs_header(unsigned char *data, struct ccs_header_st *ccs_hdr)
{
	memset(ccs_hdr, 0x00, sizeof(struct ccs_header_st));

	ccs_hdr->type = *(data++);
}

int
dtls1_shutdown(SSL *s)
{
	int ret;

	ret = ssl3_shutdown(s);
	return ret;
}
