/* $OpenBSD: ssl_pkt.c,v 1.12 2017/05/07 04:22:24 beck Exp $ */
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
 * Copyright (c) 1998-2002 The OpenSSL Project.  All rights reserved.
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

#include <errno.h>
#include <stdio.h>

#include "ssl_locl.h"

#include <openssl/buffer.h>
#include <openssl/evp.h>

#include "bytestring.h"

static int do_ssl3_write(SSL *s, int type, const unsigned char *buf,
    unsigned int len, int create_empty_fragment);
static int ssl3_get_record(SSL *s);

/*
 * Force a WANT_READ return for certain error conditions where
 * we don't want to spin internally.
 */
static void
ssl_force_want_read(SSL *s)
{
	BIO * bio;

	bio = SSL_get_rbio(s);
	BIO_clear_retry_flags(bio);
	BIO_set_retry_read(bio);
	s->internal->rwstate = SSL_READING;
}

/*
 * If extend == 0, obtain new n-byte packet; if extend == 1, increase
 * packet by another n bytes.
 * The packet will be in the sub-array of s->s3->rbuf.buf specified
 * by s->internal->packet and s->internal->packet_length.
 * (If s->internal->read_ahead is set, 'max' bytes may be stored in rbuf
 * [plus s->internal->packet_length bytes if extend == 1].)
 */
static int
ssl3_read_n(SSL *s, int n, int max, int extend)
{
	int i, len, left;
	size_t align;
	unsigned char *pkt;
	SSL3_BUFFER *rb;

	if (n <= 0)
		return n;

	rb = &(s->s3->rbuf);
	if (rb->buf == NULL)
		if (!ssl3_setup_read_buffer(s))
			return -1;

	left = rb->left;
	align = (size_t)rb->buf + SSL3_RT_HEADER_LENGTH;
	align = (-align) & (SSL3_ALIGN_PAYLOAD - 1);

	if (!extend) {
		/* start with empty packet ... */
		if (left == 0)
			rb->offset = align;
		else if (align != 0 && left >= SSL3_RT_HEADER_LENGTH) {
			/* check if next packet length is large
			 * enough to justify payload alignment... */
			pkt = rb->buf + rb->offset;
			if (pkt[0] == SSL3_RT_APPLICATION_DATA &&
			    (pkt[3]<<8|pkt[4]) >= 128) {
				/* Note that even if packet is corrupted
				 * and its length field is insane, we can
				 * only be led to wrong decision about
				 * whether memmove will occur or not.
				 * Header values has no effect on memmove
				 * arguments and therefore no buffer
				 * overrun can be triggered. */
				memmove(rb->buf + align, pkt, left);
				rb->offset = align;
			}
		}
		s->internal->packet = rb->buf + rb->offset;
		s->internal->packet_length = 0;
		/* ... now we can act as if 'extend' was set */
	}

	/* For DTLS/UDP reads should not span multiple packets
	 * because the read operation returns the whole packet
	 * at once (as long as it fits into the buffer). */
	if (SSL_IS_DTLS(s)) {
		if (left > 0 && n > left)
			n = left;
	}

	/* if there is enough in the buffer from a previous read, take some */
	if (left >= n) {
		s->internal->packet_length += n;
		rb->left = left - n;
		rb->offset += n;
		return (n);
	}

	/* else we need to read more data */

	len = s->internal->packet_length;
	pkt = rb->buf + align;
	/* Move any available bytes to front of buffer:
	 * 'len' bytes already pointed to by 'packet',
	 * 'left' extra ones at the end */
	if (s->internal->packet != pkt)  {
		/* len > 0 */
		memmove(pkt, s->internal->packet, len + left);
		s->internal->packet = pkt;
		rb->offset = len + align;
	}

	if (n > (int)(rb->len - rb->offset)) {
		/* does not happen */
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		return -1;
	}

	if (!s->internal->read_ahead) {
		/* ignore max parameter */
		max = n;
	} else {
		if (max < n)
			max = n;
		if (max > (int)(rb->len - rb->offset))
			max = rb->len - rb->offset;
	}

	while (left < n) {
		/* Now we have len+left bytes at the front of s->s3->rbuf.buf
		 * and need to read in more until we have len+n (up to
		 * len+max if possible) */

		errno = 0;
		if (s->rbio != NULL) {
			s->internal->rwstate = SSL_READING;
			i = BIO_read(s->rbio, pkt + len + left, max - left);
		} else {
			SSLerror(s, SSL_R_READ_BIO_NOT_SET);
			i = -1;
		}

		if (i <= 0) {
			rb->left = left;
			if (s->internal->mode & SSL_MODE_RELEASE_BUFFERS &&
			    !SSL_IS_DTLS(s)) {
				if (len + left == 0)
					ssl3_release_read_buffer(s);
			}
			return (i);
		}
		left += i;

		/*
		 * reads should *never* span multiple packets for DTLS because
		 * the underlying transport protocol is message oriented as
		 * opposed to byte oriented as in the TLS case.
		 */
		if (SSL_IS_DTLS(s)) {
			if (n > left)
				n = left; /* makes the while condition false */
		}
	}

	/* done reading, now the book-keeping */
	rb->offset += n;
	rb->left = left - n;
	s->internal->packet_length += n;
	s->internal->rwstate = SSL_NOTHING;

	return (n);
}

int
ssl3_packet_read(SSL *s, int plen)
{
	int n;

	n = ssl3_read_n(s, plen, s->s3->rbuf.len, 0);
	if (n <= 0)
		return n;
	if (s->internal->packet_length < plen)
		return s->internal->packet_length;

	return plen;
}

int
ssl3_packet_extend(SSL *s, int plen)
{
	int rlen, n;

	if (s->internal->packet_length >= plen)
		return plen;
	rlen = plen - s->internal->packet_length;

	n = ssl3_read_n(s, rlen, rlen, 1);
	if (n <= 0)
		return n;
	if (s->internal->packet_length < plen)
		return s->internal->packet_length;

	return plen;
}

/* Call this to get a new input record.
 * It will return <= 0 if more data is needed, normally due to an error
 * or non-blocking IO.
 * When it finishes, one packet has been decoded and can be found in
 * ssl->s3->internal->rrec.type    - is the type of record
 * ssl->s3->internal->rrec.data, 	 - data
 * ssl->s3->internal->rrec.length, - number of bytes
 */
/* used only by ssl3_read_bytes */
static int
ssl3_get_record(SSL *s)
{
	int al;
	int enc_err, n, i, ret = -1;
	SSL3_RECORD *rr;
	SSL_SESSION *sess;
	unsigned char md[EVP_MAX_MD_SIZE];
	unsigned mac_size, orig_len;

	rr = &(S3I(s)->rrec);
	sess = s->session;

 again:
	/* check if we have the header */
	if ((s->internal->rstate != SSL_ST_READ_BODY) ||
	    (s->internal->packet_length < SSL3_RT_HEADER_LENGTH)) {
		CBS header;
		uint16_t len, ssl_version;
		uint8_t type;

		n = ssl3_packet_read(s, SSL3_RT_HEADER_LENGTH);
		if (n <= 0)
			return (n);

		s->internal->mac_packet = 1;
		s->internal->rstate = SSL_ST_READ_BODY;

		if (s->server && s->internal->first_packet) {
			if ((ret = ssl_server_legacy_first_packet(s)) != 1)
				return (ret);
			ret = -1;
		}

		CBS_init(&header, s->internal->packet, SSL3_RT_HEADER_LENGTH);

		/* Pull apart the header into the SSL3_RECORD */
		if (!CBS_get_u8(&header, &type) ||
		    !CBS_get_u16(&header, &ssl_version) ||
		    !CBS_get_u16(&header, &len)) {
			SSLerror(s, SSL_R_BAD_PACKET_LENGTH);
			goto err;
		}

		rr->type = type;
		rr->length = len;

		/* Lets check version */
		if (!s->internal->first_packet && ssl_version != s->version) {
			SSLerror(s, SSL_R_WRONG_VERSION_NUMBER);
			if ((s->version & 0xFF00) == (ssl_version & 0xFF00) &&
			    !s->internal->enc_write_ctx && !s->internal->write_hash)
				/* Send back error using their minor version number :-) */
				s->version = ssl_version;
			al = SSL_AD_PROTOCOL_VERSION;
			goto f_err;
		}

		if ((ssl_version >> 8) != SSL3_VERSION_MAJOR) {
			SSLerror(s, SSL_R_WRONG_VERSION_NUMBER);
			goto err;
		}

		if (rr->length > s->s3->rbuf.len - SSL3_RT_HEADER_LENGTH) {
			al = SSL_AD_RECORD_OVERFLOW;
			SSLerror(s, SSL_R_PACKET_LENGTH_TOO_LONG);
			goto f_err;
		}

		/* now s->internal->rstate == SSL_ST_READ_BODY */
	}

	/* s->internal->rstate == SSL_ST_READ_BODY, get and decode the data */

	n = ssl3_packet_extend(s, SSL3_RT_HEADER_LENGTH + rr->length);
	if (n <= 0)
		return (n);
	if (n != SSL3_RT_HEADER_LENGTH + rr->length)
		return (n);

	s->internal->rstate = SSL_ST_READ_HEADER; /* set state for later operations */

	/* At this point, s->internal->packet_length == SSL3_RT_HEADER_LNGTH + rr->length,
	 * and we have that many bytes in s->internal->packet
	 */
	rr->input = &(s->internal->packet[SSL3_RT_HEADER_LENGTH]);

	/* ok, we can now read from 's->internal->packet' data into 'rr'
	 * rr->input points at rr->length bytes, which
	 * need to be copied into rr->data by either
	 * the decryption or by the decompression
	 * When the data is 'copied' into the rr->data buffer,
	 * rr->input will be pointed at the new buffer */

	/* We now have - encrypted [ MAC [ compressed [ plain ] ] ]
	 * rr->length bytes of encrypted compressed stuff. */

	/* check is not needed I believe */
	if (rr->length > SSL3_RT_MAX_ENCRYPTED_LENGTH) {
		al = SSL_AD_RECORD_OVERFLOW;
		SSLerror(s, SSL_R_ENCRYPTED_LENGTH_TOO_LONG);
		goto f_err;
	}

	/* decrypt in place in 'rr->input' */
	rr->data = rr->input;

	enc_err = s->method->internal->ssl3_enc->enc(s, 0);
	/* enc_err is:
	 *    0: (in non-constant time) if the record is publically invalid.
	 *    1: if the padding is valid
	 *    -1: if the padding is invalid */
	if (enc_err == 0) {
		al = SSL_AD_DECRYPTION_FAILED;
		SSLerror(s, SSL_R_BLOCK_CIPHER_PAD_IS_WRONG);
		goto f_err;
	}


	/* r->length is now the compressed data plus mac */
	if ((sess != NULL) && (s->enc_read_ctx != NULL) &&
	    (EVP_MD_CTX_md(s->read_hash) != NULL)) {
		/* s->read_hash != NULL => mac_size != -1 */
		unsigned char *mac = NULL;
		unsigned char mac_tmp[EVP_MAX_MD_SIZE];

		mac_size = EVP_MD_CTX_size(s->read_hash);
		OPENSSL_assert(mac_size <= EVP_MAX_MD_SIZE);

		/* kludge: *_cbc_remove_padding passes padding length in rr->type */
		orig_len = rr->length + ((unsigned int)rr->type >> 8);

		/* orig_len is the length of the record before any padding was
		 * removed. This is public information, as is the MAC in use,
		 * therefore we can safely process the record in a different
		 * amount of time if it's too short to possibly contain a MAC.
		 */
		if (orig_len < mac_size ||
			/* CBC records must have a padding length byte too. */
		    (EVP_CIPHER_CTX_mode(s->enc_read_ctx) == EVP_CIPH_CBC_MODE &&
		    orig_len < mac_size + 1)) {
			al = SSL_AD_DECODE_ERROR;
			SSLerror(s, SSL_R_LENGTH_TOO_SHORT);
			goto f_err;
		}

		if (EVP_CIPHER_CTX_mode(s->enc_read_ctx) == EVP_CIPH_CBC_MODE) {
			/* We update the length so that the TLS header bytes
			 * can be constructed correctly but we need to extract
			 * the MAC in constant time from within the record,
			 * without leaking the contents of the padding bytes.
			 * */
			mac = mac_tmp;
			ssl3_cbc_copy_mac(mac_tmp, rr, mac_size, orig_len);
			rr->length -= mac_size;
		} else {
			/* In this case there's no padding, so |orig_len|
			 * equals |rec->length| and we checked that there's
			 * enough bytes for |mac_size| above. */
			rr->length -= mac_size;
			mac = &rr->data[rr->length];
		}

		i = tls1_mac(s,md,0 /* not send */);
		if (i < 0 || mac == NULL ||
		    timingsafe_memcmp(md, mac, (size_t)mac_size) != 0)
			enc_err = -1;
		if (rr->length >
		    SSL3_RT_MAX_COMPRESSED_LENGTH + mac_size)
			enc_err = -1;
	}

	if (enc_err < 0) {
		/*
		 * A separate 'decryption_failed' alert was introduced with
		 * TLS 1.0, SSL 3.0 only has 'bad_record_mac'. But unless a
		 * decryption failure is directly visible from the ciphertext
		 * anyway, we should not reveal which kind of error
		 * occurred -- this might become visible to an attacker
		 * (e.g. via a logfile)
		 */
		al = SSL_AD_BAD_RECORD_MAC;
		SSLerror(s, SSL_R_DECRYPTION_FAILED_OR_BAD_RECORD_MAC);
		goto f_err;
	}

	if (rr->length > SSL3_RT_MAX_PLAIN_LENGTH) {
		al = SSL_AD_RECORD_OVERFLOW;
		SSLerror(s, SSL_R_DATA_LENGTH_TOO_LONG);
		goto f_err;
	}

	rr->off = 0;
	/*
	 * So at this point the following is true
	 *
	 * ssl->s3->internal->rrec.type 	is the type of record
	 * ssl->s3->internal->rrec.length	== number of bytes in record
	 * ssl->s3->internal->rrec.off	== offset to first valid byte
	 * ssl->s3->internal->rrec.data	== where to take bytes from, increment
	 *			   after use :-).
	 */

	/* we have pulled in a full packet so zero things */
	s->internal->packet_length = 0;

	if (rr->length == 0) {
		/*
		 * CBC countermeasures for known IV weaknesses
		 * can legitimately insert a single empty record,
		 * so we allow ourselves to read once past a single
		 * empty record without forcing want_read.
		 */
		if (s->internal->empty_record_count++ > SSL_MAX_EMPTY_RECORDS) {
			SSLerror(s, SSL_R_PEER_BEHAVING_BADLY);
			return -1;
		}
		if (s->internal->empty_record_count > 1) {
			ssl_force_want_read(s);
			return -1;
		}
		goto again;
	} else {
		s->internal->empty_record_count = 0;
	}

	return (1);

f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, al);
err:
	return (ret);
}

/* Call this to write data in records of type 'type'
 * It will return <= 0 if not all data has been sent or non-blocking IO.
 */
int
ssl3_write_bytes(SSL *s, int type, const void *buf_, int len)
{
	const unsigned char *buf = buf_;
	unsigned int tot, n, nw;
	int i;

	if (len < 0) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		return -1;
	}

	s->internal->rwstate = SSL_NOTHING;
	tot = S3I(s)->wnum;
	S3I(s)->wnum = 0;

	if (SSL_in_init(s) && !s->internal->in_handshake) {
		i = s->internal->handshake_func(s);
		if (i < 0)
			return (i);
		if (i == 0) {
			SSLerror(s, SSL_R_SSL_HANDSHAKE_FAILURE);
			return -1;
		}
	}

	if (len < tot)
		len = tot;
	n = (len - tot);
	for (;;) {
		if (n > s->max_send_fragment)
			nw = s->max_send_fragment;
		else
			nw = n;

		i = do_ssl3_write(s, type, &(buf[tot]), nw, 0);
		if (i <= 0) {
			S3I(s)->wnum = tot;
			return i;
		}

		if ((i == (int)n) || (type == SSL3_RT_APPLICATION_DATA &&
		    (s->internal->mode & SSL_MODE_ENABLE_PARTIAL_WRITE))) {
			/*
			 * Next chunk of data should get another prepended
			 * empty fragment in ciphersuites with known-IV
			 * weakness.
			 */
			S3I(s)->empty_fragment_done = 0;

			return tot + i;
		}

		n -= i;
		tot += i;
	}
}

static int
do_ssl3_write(SSL *s, int type, const unsigned char *buf,
    unsigned int len, int create_empty_fragment)
{
	unsigned char *p, *plen;
	int i, mac_size, clear = 0;
	int prefix_len = 0;
	int eivlen;
	size_t align;
	SSL3_RECORD *wr;
	SSL3_BUFFER *wb = &(s->s3->wbuf);
	SSL_SESSION *sess;

	if (wb->buf == NULL)
		if (!ssl3_setup_write_buffer(s))
			return -1;

	/* first check if there is a SSL3_BUFFER still being written
	 * out.  This will happen with non blocking IO */
	if (wb->left != 0)
		return (ssl3_write_pending(s, type, buf, len));

	/* If we have an alert to send, lets send it */
	if (s->s3->alert_dispatch) {
		i = s->method->ssl_dispatch_alert(s);
		if (i <= 0)
			return (i);
		/* if it went, fall through and send more stuff */
		/* we may have released our buffer, so get it again */
		if (wb->buf == NULL)
			if (!ssl3_setup_write_buffer(s))
				return -1;
	}

	if (len == 0 && !create_empty_fragment)
		return 0;

	wr = &(S3I(s)->wrec);
	sess = s->session;

	if ((sess == NULL) || (s->internal->enc_write_ctx == NULL) ||
	    (EVP_MD_CTX_md(s->internal->write_hash) == NULL)) {
		clear = s->internal->enc_write_ctx ? 0 : 1; /* must be AEAD cipher */
		mac_size = 0;
	} else {
		mac_size = EVP_MD_CTX_size(s->internal->write_hash);
		if (mac_size < 0)
			goto err;
	}

	/*
	 * 'create_empty_fragment' is true only when this function calls
	 * itself.
	 */
	if (!clear && !create_empty_fragment && !S3I(s)->empty_fragment_done) {
		/*
		 * Countermeasure against known-IV weakness in CBC ciphersuites
		 * (see http://www.openssl.org/~bodo/tls-cbc.txt)
		 */
		if (S3I(s)->need_empty_fragments &&
		    type == SSL3_RT_APPLICATION_DATA) {
			/* recursive function call with 'create_empty_fragment' set;
			 * this prepares and buffers the data for an empty fragment
			 * (these 'prefix_len' bytes are sent out later
			 * together with the actual payload) */
			prefix_len = do_ssl3_write(s, type, buf, 0, 1);
			if (prefix_len <= 0)
				goto err;

			if (prefix_len >
				(SSL3_RT_HEADER_LENGTH + SSL3_RT_SEND_MAX_ENCRYPTED_OVERHEAD)) {
				/* insufficient space */
				SSLerror(s, ERR_R_INTERNAL_ERROR);
				goto err;
			}
		}

		S3I(s)->empty_fragment_done = 1;
	}

	if (create_empty_fragment) {
		/* extra fragment would be couple of cipher blocks,
		 * which would be multiple of SSL3_ALIGN_PAYLOAD, so
		 * if we want to align the real payload, then we can
		 * just pretent we simply have two headers. */
		align = (size_t)wb->buf + 2 * SSL3_RT_HEADER_LENGTH;
		align = (-align) & (SSL3_ALIGN_PAYLOAD - 1);

		p = wb->buf + align;
		wb->offset = align;
	} else if (prefix_len) {
		p = wb->buf + wb->offset + prefix_len;
	} else {
		align = (size_t)wb->buf + SSL3_RT_HEADER_LENGTH;
		align = (-align) & (SSL3_ALIGN_PAYLOAD - 1);

		p = wb->buf + align;
		wb->offset = align;
	}

	/* write the header */

	*(p++) = type&0xff;
	wr->type = type;

	*(p++) = (s->version >> 8);
	/* Some servers hang if iniatial client hello is larger than 256
	 * bytes and record version number > TLS 1.0
	 */
	if (S3I(s)->hs.state == SSL3_ST_CW_CLNT_HELLO_B && !s->internal->renegotiate &&
	    TLS1_get_version(s) > TLS1_VERSION)
		*(p++) = 0x1;
	else
		*(p++) = s->version&0xff;

	/* field where we are to write out packet length */
	plen = p;
	p += 2;

	/* Explicit IV length. */
	if (s->internal->enc_write_ctx && SSL_USE_EXPLICIT_IV(s)) {
		int mode = EVP_CIPHER_CTX_mode(s->internal->enc_write_ctx);
		if (mode == EVP_CIPH_CBC_MODE) {
			eivlen = EVP_CIPHER_CTX_iv_length(s->internal->enc_write_ctx);
			if (eivlen <= 1)
				eivlen = 0;
		}
		/* Need explicit part of IV for GCM mode */
		else if (mode == EVP_CIPH_GCM_MODE)
			eivlen = EVP_GCM_TLS_EXPLICIT_IV_LEN;
		else
			eivlen = 0;
	} else if (s->internal->aead_write_ctx != NULL &&
	    s->internal->aead_write_ctx->variable_nonce_in_record) {
		eivlen = s->internal->aead_write_ctx->variable_nonce_len;
	} else
		eivlen = 0;

	/* lets setup the record stuff. */
	wr->data = p + eivlen;
	wr->length = (int)len;
	wr->input = (unsigned char *)buf;

	/* we now 'read' from wr->input, wr->length bytes into wr->data */

	memcpy(wr->data, wr->input, wr->length);
	wr->input = wr->data;

	/* we should still have the output to wr->data and the input
	 * from wr->input.  Length should be wr->length.
	 * wr->data still points in the wb->buf */

	if (mac_size != 0) {
		if (tls1_mac(s,
		    &(p[wr->length + eivlen]), 1) < 0)
			goto err;
		wr->length += mac_size;
	}

	wr->input = p;
	wr->data = p;

	if (eivlen) {
		/* if (RAND_pseudo_bytes(p, eivlen) <= 0)
			goto err;
		*/
		wr->length += eivlen;
	}

	/* ssl3_enc can only have an error on read */
	s->method->internal->ssl3_enc->enc(s, 1);

	/* record length after mac and block padding */
	s2n(wr->length, plen);

	/* we should now have
	 * wr->data pointing to the encrypted data, which is
	 * wr->length long */
	wr->type=type; /* not needed but helps for debugging */
	wr->length += SSL3_RT_HEADER_LENGTH;

	if (create_empty_fragment) {
		/* we are in a recursive call;
		 * just return the length, don't write out anything here
		 */
		return wr->length;
	}

	/* now let's set up wb */
	wb->left = prefix_len + wr->length;

	/* memorize arguments so that ssl3_write_pending can detect
	 * bad write retries later */
	S3I(s)->wpend_tot = len;
	S3I(s)->wpend_buf = buf;
	S3I(s)->wpend_type = type;
	S3I(s)->wpend_ret = len;

	/* we now just need to write the buffer */
	return ssl3_write_pending(s, type, buf, len);
err:
	return -1;
}

/* if s->s3->wbuf.left != 0, we need to call this */
int
ssl3_write_pending(SSL *s, int type, const unsigned char *buf, unsigned int len)
{
	int i;
	SSL3_BUFFER *wb = &(s->s3->wbuf);

	/* XXXX */
	if ((S3I(s)->wpend_tot > (int)len) || ((S3I(s)->wpend_buf != buf) &&
	    !(s->internal->mode & SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER)) ||
	    (S3I(s)->wpend_type != type)) {
		SSLerror(s, SSL_R_BAD_WRITE_RETRY);
		return (-1);
	}

	for (;;) {
		errno = 0;
		if (s->wbio != NULL) {
			s->internal->rwstate = SSL_WRITING;
			i = BIO_write(s->wbio,
			(char *)&(wb->buf[wb->offset]),
			(unsigned int)wb->left);
		} else {
			SSLerror(s, SSL_R_BIO_NOT_SET);
			i = -1;
		}
		if (i == wb->left) {
			wb->left = 0;
			wb->offset += i;
			if (s->internal->mode & SSL_MODE_RELEASE_BUFFERS &&
			    !SSL_IS_DTLS(s))
				ssl3_release_write_buffer(s);
			s->internal->rwstate = SSL_NOTHING;
			return (S3I(s)->wpend_ret);
		} else if (i <= 0) {
			/*
			 * For DTLS, just drop it. That's kind of the
			 * whole point in using a datagram service.
			 */
			if (SSL_IS_DTLS(s))
				wb->left = 0;
			return (i);
		}
		wb->offset += i;
		wb->left -= i;
	}
}

/* Return up to 'len' payload bytes received in 'type' records.
 * 'type' is one of the following:
 *
 *   -  SSL3_RT_HANDSHAKE (when ssl3_get_message calls us)
 *   -  SSL3_RT_APPLICATION_DATA (when ssl3_read calls us)
 *   -  0 (during a shutdown, no data has to be returned)
 *
 * If we don't have stored data to work from, read a SSL/TLS record first
 * (possibly multiple records if we still don't have anything to return).
 *
 * This function must handle any surprises the peer may have for us, such as
 * Alert records (e.g. close_notify), ChangeCipherSpec records (not really
 * a surprise, but handled as if it were), or renegotiation requests.
 * Also if record payloads contain fragments too small to process, we store
 * them until there is enough for the respective protocol (the record protocol
 * may use arbitrary fragmentation and even interleaving):
 *     Change cipher spec protocol
 *             just 1 byte needed, no need for keeping anything stored
 *     Alert protocol
 *             2 bytes needed (AlertLevel, AlertDescription)
 *     Handshake protocol
 *             4 bytes needed (HandshakeType, uint24 length) -- we just have
 *             to detect unexpected Client Hello and Hello Request messages
 *             here, anything else is handled by higher layers
 *     Application data protocol
 *             none of our business
 */
int
ssl3_read_bytes(SSL *s, int type, unsigned char *buf, int len, int peek)
{
	void (*cb)(const SSL *ssl, int type2, int val) = NULL;
	int al, i, j, ret, rrcount = 0;
	unsigned int n;
	SSL3_RECORD *rr;

	if (s->s3->rbuf.buf == NULL) /* Not initialized yet */
		if (!ssl3_setup_read_buffer(s))
			return (-1);

	if (len < 0) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		return -1;
	}

	if ((type && type != SSL3_RT_APPLICATION_DATA &&
	    type != SSL3_RT_HANDSHAKE) ||
	    (peek && (type != SSL3_RT_APPLICATION_DATA))) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		return -1;
	}

	if ((type == SSL3_RT_HANDSHAKE) &&
	    (S3I(s)->handshake_fragment_len > 0)) {
		/* (partially) satisfy request from storage */
		unsigned char *src = S3I(s)->handshake_fragment;
		unsigned char *dst = buf;
		unsigned int k;

		/* peek == 0 */
		n = 0;
		while ((len > 0) && (S3I(s)->handshake_fragment_len > 0)) {
			*dst++ = *src++;
			len--;
			S3I(s)->handshake_fragment_len--;
			n++;
		}
		/* move any remaining fragment bytes: */
		for (k = 0; k < S3I(s)->handshake_fragment_len; k++)
			S3I(s)->handshake_fragment[k] = *src++;
		return n;
	}

	/*
	 * Now S3I(s)->handshake_fragment_len == 0 if
	 * type == SSL3_RT_HANDSHAKE.
	 */
	if (!s->internal->in_handshake && SSL_in_init(s)) {
		/* type == SSL3_RT_APPLICATION_DATA */
		i = s->internal->handshake_func(s);
		if (i < 0)
			return (i);
		if (i == 0) {
			SSLerror(s, SSL_R_SSL_HANDSHAKE_FAILURE);
			return (-1);
		}
	}

start:
	/*
	 * Do not process more than three consecutive records, otherwise the
	 * peer can cause us to loop indefinitely. Instead, return with an
	 * SSL_ERROR_WANT_READ so the caller can choose when to handle further
	 * processing. In the future, the total number of non-handshake and
	 * non-application data records per connection should probably also be
	 * limited...
	 */
	if (rrcount++ >= 3) {
		ssl_force_want_read(s);
		return -1;
	}

	s->internal->rwstate = SSL_NOTHING;

	/*
	 * S3I(s)->rrec.type	    - is the type of record
	 * S3I(s)->rrec.data,    - data
	 * S3I(s)->rrec.off,     - offset into 'data' for next read
	 * S3I(s)->rrec.length,  - number of bytes.
	 */
	rr = &(S3I(s)->rrec);

	/* get new packet if necessary */
	if ((rr->length == 0) || (s->internal->rstate == SSL_ST_READ_BODY)) {
		ret = ssl3_get_record(s);
		if (ret <= 0)
			return (ret);
	}

	/* we now have a packet which can be read and processed */

	if (S3I(s)->change_cipher_spec /* set when we receive ChangeCipherSpec,
	                               * reset by ssl3_get_finished */
	    && (rr->type != SSL3_RT_HANDSHAKE)) {
		al = SSL_AD_UNEXPECTED_MESSAGE;
		SSLerror(s, SSL_R_DATA_BETWEEN_CCS_AND_FINISHED);
		goto f_err;
	}

	/* If the other end has shut down, throw anything we read away
	 * (even in 'peek' mode) */
	if (s->internal->shutdown & SSL_RECEIVED_SHUTDOWN) {
		rr->length = 0;
		s->internal->rwstate = SSL_NOTHING;
		return (0);
	}


	/* SSL3_RT_APPLICATION_DATA or SSL3_RT_HANDSHAKE */
	if (type == rr->type) {
		/* make sure that we are not getting application data when we
		 * are doing a handshake for the first time */
		if (SSL_in_init(s) && (type == SSL3_RT_APPLICATION_DATA) &&
			(s->enc_read_ctx == NULL)) {
			al = SSL_AD_UNEXPECTED_MESSAGE;
			SSLerror(s, SSL_R_APP_DATA_IN_HANDSHAKE);
			goto f_err;
		}

		if (len <= 0)
			return (len);

		if ((unsigned int)len > rr->length)
			n = rr->length;
		else
			n = (unsigned int)len;

		memcpy(buf, &(rr->data[rr->off]), n);
		if (!peek) {
			memset(&(rr->data[rr->off]), 0, n);
			rr->length -= n;
			rr->off += n;
			if (rr->length == 0) {
				s->internal->rstate = SSL_ST_READ_HEADER;
				rr->off = 0;
				if (s->internal->mode & SSL_MODE_RELEASE_BUFFERS &&
				    s->s3->rbuf.left == 0)
					ssl3_release_read_buffer(s);
			}
		}
		return (n);
	}


	/* If we get here, then type != rr->type; if we have a handshake
	 * message, then it was unexpected (Hello Request or Client Hello). */

	{
		/*
		 * In case of record types for which we have 'fragment'
		 * storage, * fill that so that we can process the data
		 * at a fixed place.
		 */
		unsigned int dest_maxlen = 0;
		unsigned char *dest = NULL;
		unsigned int *dest_len = NULL;

		if (rr->type == SSL3_RT_HANDSHAKE) {
			dest_maxlen = sizeof S3I(s)->handshake_fragment;
			dest = S3I(s)->handshake_fragment;
			dest_len = &S3I(s)->handshake_fragment_len;
		} else if (rr->type == SSL3_RT_ALERT) {
			dest_maxlen = sizeof S3I(s)->alert_fragment;
			dest = S3I(s)->alert_fragment;
			dest_len = &S3I(s)->alert_fragment_len;
		}
		if (dest_maxlen > 0) {
			/* available space in 'dest' */
			n = dest_maxlen - *dest_len;
			if (rr->length < n)
				n = rr->length; /* available bytes */

			/* now move 'n' bytes: */
			while (n-- > 0) {
				dest[(*dest_len)++] = rr->data[rr->off++];
				rr->length--;
			}

			if (*dest_len < dest_maxlen)
				goto start; /* fragment was too small */
		}
	}

	/* S3I(s)->handshake_fragment_len == 4  iff  rr->type == SSL3_RT_HANDSHAKE;
	 * S3I(s)->alert_fragment_len == 2      iff  rr->type == SSL3_RT_ALERT.
	 * (Possibly rr is 'empty' now, i.e. rr->length may be 0.) */

	/* If we are a client, check for an incoming 'Hello Request': */
	if ((!s->server) && (S3I(s)->handshake_fragment_len >= 4) &&
	    (S3I(s)->handshake_fragment[0] == SSL3_MT_HELLO_REQUEST) &&
	    (s->session != NULL) && (s->session->cipher != NULL)) {
		S3I(s)->handshake_fragment_len = 0;

		if ((S3I(s)->handshake_fragment[1] != 0) ||
		    (S3I(s)->handshake_fragment[2] != 0) ||
		    (S3I(s)->handshake_fragment[3] != 0)) {
			al = SSL_AD_DECODE_ERROR;
			SSLerror(s, SSL_R_BAD_HELLO_REQUEST);
			goto f_err;
		}

		if (s->internal->msg_callback)
			s->internal->msg_callback(0, s->version, SSL3_RT_HANDSHAKE,
			    S3I(s)->handshake_fragment, 4, s,
			    s->internal->msg_callback_arg);

		if (SSL_is_init_finished(s) &&
		    !(s->s3->flags & SSL3_FLAGS_NO_RENEGOTIATE_CIPHERS) &&
		    !S3I(s)->renegotiate) {
			ssl3_renegotiate(s);
			if (ssl3_renegotiate_check(s)) {
				i = s->internal->handshake_func(s);
				if (i < 0)
					return (i);
				if (i == 0) {
					SSLerror(s, SSL_R_SSL_HANDSHAKE_FAILURE);
					return (-1);
				}

				if (!(s->internal->mode & SSL_MODE_AUTO_RETRY)) {
					if (s->s3->rbuf.left == 0) {
						/* no read-ahead left? */
			/* In the case where we try to read application data,
			 * but we trigger an SSL handshake, we return -1 with
			 * the retry option set.  Otherwise renegotiation may
			 * cause nasty problems in the blocking world */
						ssl_force_want_read(s);
						return (-1);
					}
				}
			}
		}
		/* we either finished a handshake or ignored the request,
		 * now try again to obtain the (application) data we were asked for */
		goto start;
	}
	/* Disallow client initiated renegotiation if configured. */
	if (s->server && SSL_is_init_finished(s) &&
	    S3I(s)->handshake_fragment_len >= 4 &&
	    S3I(s)->handshake_fragment[0] == SSL3_MT_CLIENT_HELLO &&
	    (s->internal->options & SSL_OP_NO_CLIENT_RENEGOTIATION)) {
		al = SSL_AD_NO_RENEGOTIATION;
		goto f_err;
	}
	/* If we are a server and get a client hello when renegotiation isn't
	 * allowed send back a no renegotiation alert and carry on.
	 * WARNING: experimental code, needs reviewing (steve)
	 */
	if (s->server &&
	    SSL_is_init_finished(s) &&
	    !S3I(s)->send_connection_binding &&
	    (S3I(s)->handshake_fragment_len >= 4) &&
	    (S3I(s)->handshake_fragment[0] == SSL3_MT_CLIENT_HELLO) &&
	    (s->session != NULL) && (s->session->cipher != NULL)) {
		/*S3I(s)->handshake_fragment_len = 0;*/
		rr->length = 0;
		ssl3_send_alert(s, SSL3_AL_WARNING, SSL_AD_NO_RENEGOTIATION);
		goto start;
	}
	if (S3I(s)->alert_fragment_len >= 2) {
		int alert_level = S3I(s)->alert_fragment[0];
		int alert_descr = S3I(s)->alert_fragment[1];

		S3I(s)->alert_fragment_len = 0;

		if (s->internal->msg_callback)
			s->internal->msg_callback(0, s->version, SSL3_RT_ALERT,
			    S3I(s)->alert_fragment, 2, s, s->internal->msg_callback_arg);

		if (s->internal->info_callback != NULL)
			cb = s->internal->info_callback;
		else if (s->ctx->internal->info_callback != NULL)
			cb = s->ctx->internal->info_callback;

		if (cb != NULL) {
			j = (alert_level << 8) | alert_descr;
			cb(s, SSL_CB_READ_ALERT, j);
		}

		if (alert_level == SSL3_AL_WARNING) {
			S3I(s)->warn_alert = alert_descr;
			if (alert_descr == SSL_AD_CLOSE_NOTIFY) {
				s->internal->shutdown |= SSL_RECEIVED_SHUTDOWN;
				return (0);
			}
			/* This is a warning but we receive it if we requested
			 * renegotiation and the peer denied it. Terminate with
			 * a fatal alert because if application tried to
			 * renegotiatie it presumably had a good reason and
			 * expects it to succeed.
			 *
			 * In future we might have a renegotiation where we
			 * don't care if the peer refused it where we carry on.
			 */
			else if (alert_descr == SSL_AD_NO_RENEGOTIATION) {
				al = SSL_AD_HANDSHAKE_FAILURE;
				SSLerror(s, SSL_R_NO_RENEGOTIATION);
				goto f_err;
			}
		} else if (alert_level == SSL3_AL_FATAL) {
			s->internal->rwstate = SSL_NOTHING;
			S3I(s)->fatal_alert = alert_descr;
			SSLerror(s, SSL_AD_REASON_OFFSET + alert_descr);
			ERR_asprintf_error_data("SSL alert number %d",
			    alert_descr);
			s->internal->shutdown |= SSL_RECEIVED_SHUTDOWN;
			SSL_CTX_remove_session(s->ctx, s->session);
			return (0);
		} else {
			al = SSL_AD_ILLEGAL_PARAMETER;
			SSLerror(s, SSL_R_UNKNOWN_ALERT_TYPE);
			goto f_err;
		}

		goto start;
	}

	if (s->internal->shutdown & SSL_SENT_SHUTDOWN) {
		/* but we have not received a shutdown */
		s->internal->rwstate = SSL_NOTHING;
		rr->length = 0;
		return (0);
	}

	if (rr->type == SSL3_RT_CHANGE_CIPHER_SPEC) {
		/* 'Change Cipher Spec' is just a single byte, so we know
		 * exactly what the record payload has to look like */
		if ((rr->length != 1) || (rr->off != 0) ||
			(rr->data[0] != SSL3_MT_CCS)) {
			al = SSL_AD_ILLEGAL_PARAMETER;
			SSLerror(s, SSL_R_BAD_CHANGE_CIPHER_SPEC);
			goto f_err;
		}

		/* Check we have a cipher to change to */
		if (S3I(s)->hs.new_cipher == NULL) {
			al = SSL_AD_UNEXPECTED_MESSAGE;
			SSLerror(s, SSL_R_CCS_RECEIVED_EARLY);
			goto f_err;
		}

		/* Check that we should be receiving a Change Cipher Spec. */
		if (!(s->s3->flags & SSL3_FLAGS_CCS_OK)) {
			al = SSL_AD_UNEXPECTED_MESSAGE;
			SSLerror(s, SSL_R_CCS_RECEIVED_EARLY);
			goto f_err;
		}
		s->s3->flags &= ~SSL3_FLAGS_CCS_OK;

		rr->length = 0;

		if (s->internal->msg_callback) {
			s->internal->msg_callback(0, s->version,
			    SSL3_RT_CHANGE_CIPHER_SPEC, rr->data, 1, s,
			    s->internal->msg_callback_arg);
		}

		S3I(s)->change_cipher_spec = 1;
		if (!ssl3_do_change_cipher_spec(s))
			goto err;
		else
			goto start;
	}

	/* Unexpected handshake message (Client Hello, or protocol violation) */
	if ((S3I(s)->handshake_fragment_len >= 4) && !s->internal->in_handshake) {
		if (((S3I(s)->hs.state&SSL_ST_MASK) == SSL_ST_OK) &&
		    !(s->s3->flags & SSL3_FLAGS_NO_RENEGOTIATE_CIPHERS)) {
			S3I(s)->hs.state = s->server ? SSL_ST_ACCEPT : SSL_ST_CONNECT;
			s->internal->renegotiate = 1;
			s->internal->new_session = 1;
		}
		i = s->internal->handshake_func(s);
		if (i < 0)
			return (i);
		if (i == 0) {
			SSLerror(s, SSL_R_SSL_HANDSHAKE_FAILURE);
			return (-1);
		}

		if (!(s->internal->mode & SSL_MODE_AUTO_RETRY)) {
			if (s->s3->rbuf.left == 0) { /* no read-ahead left? */
				/* In the case where we try to read application data,
				 * but we trigger an SSL handshake, we return -1 with
				 * the retry option set.  Otherwise renegotiation may
				 * cause nasty problems in the blocking world */
				ssl_force_want_read(s);
				return (-1);
			}
		}
		goto start;
	}

	switch (rr->type) {
	default:
		/*
		 * TLS up to v1.1 just ignores unknown message types:
		 * TLS v1.2 give an unexpected message alert.
		 */
		if (s->version >= TLS1_VERSION &&
		    s->version <= TLS1_1_VERSION) {
			rr->length = 0;
			goto start;
		}
		al = SSL_AD_UNEXPECTED_MESSAGE;
		SSLerror(s, SSL_R_UNEXPECTED_RECORD);
		goto f_err;
	case SSL3_RT_CHANGE_CIPHER_SPEC:
	case SSL3_RT_ALERT:
	case SSL3_RT_HANDSHAKE:
		/* we already handled all of these, with the possible exception
		 * of SSL3_RT_HANDSHAKE when s->internal->in_handshake is set, but that
		 * should not happen when type != rr->type */
		al = SSL_AD_UNEXPECTED_MESSAGE;
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		goto f_err;
	case SSL3_RT_APPLICATION_DATA:
		/* At this point, we were expecting handshake data,
		 * but have application data.  If the library was
		 * running inside ssl3_read() (i.e. in_read_app_data
		 * is set) and it makes sense to read application data
		 * at this point (session renegotiation not yet started),
		 * we will indulge it.
		 */
		if (S3I(s)->in_read_app_data &&
		    (S3I(s)->total_renegotiations != 0) &&
		    (((S3I(s)->hs.state & SSL_ST_CONNECT) &&
		    (S3I(s)->hs.state >= SSL3_ST_CW_CLNT_HELLO_A) &&
		    (S3I(s)->hs.state <= SSL3_ST_CR_SRVR_HELLO_A)) ||
		    ((S3I(s)->hs.state & SSL_ST_ACCEPT) &&
		    (S3I(s)->hs.state <= SSL3_ST_SW_HELLO_REQ_A) &&
		    (S3I(s)->hs.state >= SSL3_ST_SR_CLNT_HELLO_A)))) {
			S3I(s)->in_read_app_data = 2;
			return (-1);
		} else {
			al = SSL_AD_UNEXPECTED_MESSAGE;
			SSLerror(s, SSL_R_UNEXPECTED_RECORD);
			goto f_err;
		}
	}
	/* not reached */

f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, al);
err:
	return (-1);
}

int
ssl3_do_change_cipher_spec(SSL *s)
{
	int i;
	const char *sender;
	int slen;

	if (S3I(s)->hs.state & SSL_ST_ACCEPT)
		i = SSL3_CHANGE_CIPHER_SERVER_READ;
	else
		i = SSL3_CHANGE_CIPHER_CLIENT_READ;

	if (S3I(s)->hs.key_block == NULL) {
		if (s->session == NULL || s->session->master_key_length == 0) {
			/* might happen if dtls1_read_bytes() calls this */
			SSLerror(s, SSL_R_CCS_RECEIVED_EARLY);
			return (0);
		}

		s->session->cipher = S3I(s)->hs.new_cipher;
		if (!tls1_setup_key_block(s))
			return (0);
	}

	if (!tls1_change_cipher_state(s, i))
		return (0);

	/* we have to record the message digest at
	 * this point so we can get it before we read
	 * the finished message */
	if (S3I(s)->hs.state & SSL_ST_CONNECT) {
		sender = TLS_MD_SERVER_FINISH_CONST;
		slen = TLS_MD_SERVER_FINISH_CONST_SIZE;
	} else {
		sender = TLS_MD_CLIENT_FINISH_CONST;
		slen = TLS_MD_CLIENT_FINISH_CONST_SIZE;
	}

	i = tls1_final_finish_mac(s, sender, slen,
	    S3I(s)->tmp.peer_finish_md);
	if (i == 0) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		return 0;
	}
	S3I(s)->tmp.peer_finish_md_len = i;

	return (1);
}

int
ssl3_send_alert(SSL *s, int level, int desc)
{
	/* Map tls/ssl alert value to correct one */
	desc = tls1_alert_code(desc);
	if (desc < 0)
		return -1;
	/* If a fatal one, remove from cache */
	if ((level == 2) && (s->session != NULL))
		SSL_CTX_remove_session(s->ctx, s->session);

	s->s3->alert_dispatch = 1;
	s->s3->send_alert[0] = level;
	s->s3->send_alert[1] = desc;
	if (s->s3->wbuf.left == 0) /* data still being written out? */
		return s->method->ssl_dispatch_alert(s);

	/* else data is still being written out, we will get written
	 * some time in the future */
	return -1;
}

int
ssl3_dispatch_alert(SSL *s)
{
	int i, j;
	void (*cb)(const SSL *ssl, int type, int val) = NULL;

	s->s3->alert_dispatch = 0;
	i = do_ssl3_write(s, SSL3_RT_ALERT, &s->s3->send_alert[0], 2, 0);
	if (i <= 0) {
		s->s3->alert_dispatch = 1;
	} else {
		/* Alert sent to BIO.  If it is important, flush it now.
		 * If the message does not get sent due to non-blocking IO,
		 * we will not worry too much. */
		if (s->s3->send_alert[0] == SSL3_AL_FATAL)
			(void)BIO_flush(s->wbio);

		if (s->internal->msg_callback)
			s->internal->msg_callback(1, s->version, SSL3_RT_ALERT,
			    s->s3->send_alert, 2, s, s->internal->msg_callback_arg);

		if (s->internal->info_callback != NULL)
			cb = s->internal->info_callback;
		else if (s->ctx->internal->info_callback != NULL)
			cb = s->ctx->internal->info_callback;

		if (cb != NULL) {
			j = (s->s3->send_alert[0]<<8)|s->s3->send_alert[1];
			cb(s, SSL_CB_WRITE_ALERT, j);
		}
	}
	return (i);
}
