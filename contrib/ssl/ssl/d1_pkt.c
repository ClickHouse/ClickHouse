/* $OpenBSD: d1_pkt.c,v 1.63 2017/05/07 04:22:24 beck Exp $ */
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

#include <machine/endian.h>

#include <errno.h>
#include <stdio.h>

#include "ssl_locl.h"

#include <openssl/buffer.h>
#include <openssl/evp.h>

#include "pqueue.h"
#include "bytestring.h"

static int	do_dtls1_write(SSL *s, int type, const unsigned char *buf,
		    unsigned int len);


/* mod 128 saturating subtract of two 64-bit values in big-endian order */
static int
satsub64be(const unsigned char *v1, const unsigned char *v2)
{
	int ret, sat, brw, i;

	if (sizeof(long) == 8)
		do {
			long l;

			if (BYTE_ORDER == LITTLE_ENDIAN)
				break;
			/* not reached on little-endians */
			/* following test is redundant, because input is
			 * always aligned, but I take no chances... */
			if (((size_t)v1 | (size_t)v2) & 0x7)
				break;

			l  = *((long *)v1);
			l -= *((long *)v2);
			if (l > 128)
				return 128;
			else if (l<-128)
				return -128;
			else
				return (int)l;
		} while (0);

	ret = (int)v1[7] - (int)v2[7];
	sat = 0;
	brw = ret >> 8;	/* brw is either 0 or -1 */
	if (ret & 0x80) {
		for (i = 6; i >= 0; i--) {
			brw += (int)v1[i]-(int)v2[i];
			sat |= ~brw;
			brw >>= 8;
		}
	} else {
		for (i = 6; i >= 0; i--) {
			brw += (int)v1[i]-(int)v2[i];
			sat |= brw;
			brw >>= 8;
		}
	}
	brw <<= 8;	/* brw is either 0 or -256 */

	if (sat & 0xff)
		return brw | 0x80;
	else
		return brw + (ret & 0xFF);
}

static int have_handshake_fragment(SSL *s, int type, unsigned char *buf,
    int len, int peek);
static int dtls1_record_replay_check(SSL *s, DTLS1_BITMAP *bitmap);
static void dtls1_record_bitmap_update(SSL *s, DTLS1_BITMAP *bitmap);
static DTLS1_BITMAP *dtls1_get_bitmap(SSL *s, SSL3_RECORD *rr,
    unsigned int *is_next_epoch);
static int dtls1_buffer_record(SSL *s, record_pqueue *q,
    unsigned char *priority);
static int dtls1_process_record(SSL *s);

/* copy buffered record into SSL structure */
static int
dtls1_copy_record(SSL *s, pitem *item)
{
	DTLS1_RECORD_DATA *rdata;

	rdata = (DTLS1_RECORD_DATA *)item->data;

	free(s->s3->rbuf.buf);

	s->internal->packet = rdata->packet;
	s->internal->packet_length = rdata->packet_length;
	memcpy(&(s->s3->rbuf), &(rdata->rbuf), sizeof(SSL3_BUFFER));
	memcpy(&(S3I(s)->rrec), &(rdata->rrec), sizeof(SSL3_RECORD));

	/* Set proper sequence number for mac calculation */
	memcpy(&(S3I(s)->read_sequence[2]), &(rdata->packet[5]), 6);

	return (1);
}


static int
dtls1_buffer_record(SSL *s, record_pqueue *queue, unsigned char *priority)
{
	DTLS1_RECORD_DATA *rdata;
	pitem *item;

	/* Limit the size of the queue to prevent DOS attacks */
	if (pqueue_size(queue->q) >= 100)
		return 0;

	rdata = malloc(sizeof(DTLS1_RECORD_DATA));
	item = pitem_new(priority, rdata);
	if (rdata == NULL || item == NULL)
		goto init_err;

	rdata->packet = s->internal->packet;
	rdata->packet_length = s->internal->packet_length;
	memcpy(&(rdata->rbuf), &(s->s3->rbuf), sizeof(SSL3_BUFFER));
	memcpy(&(rdata->rrec), &(S3I(s)->rrec), sizeof(SSL3_RECORD));

	item->data = rdata;


	s->internal->packet = NULL;
	s->internal->packet_length = 0;
	memset(&(s->s3->rbuf), 0, sizeof(SSL3_BUFFER));
	memset(&(S3I(s)->rrec), 0, sizeof(SSL3_RECORD));

	if (!ssl3_setup_buffers(s))
		goto err;

	/* insert should not fail, since duplicates are dropped */
	if (pqueue_insert(queue->q, item) == NULL)
		goto err;

	return (1);

err:
	free(rdata->rbuf.buf);

init_err:
	SSLerror(s, ERR_R_INTERNAL_ERROR);
	free(rdata);
	pitem_free(item);
	return (-1);
}


static int
dtls1_retrieve_buffered_record(SSL *s, record_pqueue *queue)
{
	pitem *item;

	item = pqueue_pop(queue->q);
	if (item) {
		dtls1_copy_record(s, item);

		free(item->data);
		pitem_free(item);

		return (1);
	}

	return (0);
}


/* retrieve a buffered record that belongs to the new epoch, i.e., not processed
 * yet */
#define dtls1_get_unprocessed_record(s) \
                   dtls1_retrieve_buffered_record((s), \
		       &((D1I(s))->unprocessed_rcds))

/* retrieve a buffered record that belongs to the current epoch, ie, processed */
#define dtls1_get_processed_record(s) \
                   dtls1_retrieve_buffered_record((s), \
		       &((D1I(s))->processed_rcds))

static int
dtls1_process_buffered_records(SSL *s)
{
	pitem *item;

	item = pqueue_peek(D1I(s)->unprocessed_rcds.q);
	if (item) {
		/* Check if epoch is current. */
		if (D1I(s)->unprocessed_rcds.epoch != D1I(s)->r_epoch)
			return (1);
		/* Nothing to do. */

		/* Process all the records. */
		while (pqueue_peek(D1I(s)->unprocessed_rcds.q)) {
			dtls1_get_unprocessed_record(s);
			if (! dtls1_process_record(s))
				return (0);
			if (dtls1_buffer_record(s, &(D1I(s)->processed_rcds),
			    S3I(s)->rrec.seq_num) < 0)
				return (-1);
		}
	}

    /* sync epoch numbers once all the unprocessed records
     * have been processed */
	D1I(s)->processed_rcds.epoch = D1I(s)->r_epoch;
	D1I(s)->unprocessed_rcds.epoch = D1I(s)->r_epoch + 1;

	return (1);
}

static int
dtls1_process_record(SSL *s)
{
	int i, al;
	int enc_err;
	SSL_SESSION *sess;
	SSL3_RECORD *rr;
	unsigned int mac_size, orig_len;
	unsigned char md[EVP_MAX_MD_SIZE];

	rr = &(S3I(s)->rrec);
	sess = s->session;

	/* At this point, s->internal->packet_length == SSL3_RT_HEADER_LNGTH + rr->length,
	 * and we have that many bytes in s->internal->packet
	 */
	rr->input = &(s->internal->packet[DTLS1_RT_HEADER_LENGTH]);

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
		/* For DTLS we simply ignore bad packets. */
		rr->length = 0;
		s->internal->packet_length = 0;
		goto err;
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

		i = tls1_mac(s, md, 0 /* not send */);
		if (i < 0 || mac == NULL || timingsafe_memcmp(md, mac, (size_t)mac_size) != 0)
			enc_err = -1;
		if (rr->length > SSL3_RT_MAX_COMPRESSED_LENGTH + mac_size)
			enc_err = -1;
	}

	if (enc_err < 0) {
		/* decryption failed, silently discard message */
		rr->length = 0;
		s->internal->packet_length = 0;
		goto err;
	}

	if (rr->length > SSL3_RT_MAX_PLAIN_LENGTH) {
		al = SSL_AD_RECORD_OVERFLOW;
		SSLerror(s, SSL_R_DATA_LENGTH_TOO_LONG);
		goto f_err;
	}

	rr->off = 0;
	/* So at this point the following is true
	 * ssl->s3->internal->rrec.type 	is the type of record
	 * ssl->s3->internal->rrec.length	== number of bytes in record
	 * ssl->s3->internal->rrec.off	== offset to first valid byte
	 * ssl->s3->internal->rrec.data	== where to take bytes from, increment
	 *			   after use :-).
	 */

	/* we have pulled in a full packet so zero things */
	s->internal->packet_length = 0;
	return (1);

f_err:
	ssl3_send_alert(s, SSL3_AL_FATAL, al);
err:
	return (0);
}


/* Call this to get a new input record.
 * It will return <= 0 if more data is needed, normally due to an error
 * or non-blocking IO.
 * When it finishes, one packet has been decoded and can be found in
 * ssl->s3->internal->rrec.type    - is the type of record
 * ssl->s3->internal->rrec.data, 	 - data
 * ssl->s3->internal->rrec.length, - number of bytes
 */
/* used only by dtls1_read_bytes */
int
dtls1_get_record(SSL *s)
{
	SSL3_RECORD *rr;
	unsigned char *p = NULL;
	DTLS1_BITMAP *bitmap;
	unsigned int is_next_epoch;
	int n;

	rr = &(S3I(s)->rrec);

	/* The epoch may have changed.  If so, process all the
	 * pending records.  This is a non-blocking operation. */
	if (dtls1_process_buffered_records(s) < 0)
		return (-1);

	/* if we're renegotiating, then there may be buffered records */
	if (dtls1_get_processed_record(s))
		return 1;

	/* get something from the wire */
	if (0) {
again:
		/* dump this record on all retries */
		rr->length = 0;
		s->internal->packet_length = 0;
	}

	/* check if we have the header */
	if ((s->internal->rstate != SSL_ST_READ_BODY) ||
	    (s->internal->packet_length < DTLS1_RT_HEADER_LENGTH)) {
		CBS header, seq_no;
		uint16_t epoch, len, ssl_version;
		uint8_t type;

		n = ssl3_packet_read(s, DTLS1_RT_HEADER_LENGTH);
		if (n <= 0)
			return (n);

		/* If this packet contained a partial record, dump it. */
		if (n != DTLS1_RT_HEADER_LENGTH)
			goto again;

		s->internal->rstate = SSL_ST_READ_BODY;

		CBS_init(&header, s->internal->packet, s->internal->packet_length);

		/* Pull apart the header into the DTLS1_RECORD */
		if (!CBS_get_u8(&header, &type))
			goto again;
		if (!CBS_get_u16(&header, &ssl_version))
			goto again;

		/* sequence number is 64 bits, with top 2 bytes = epoch */
		if (!CBS_get_u16(&header, &epoch) ||
		    !CBS_get_bytes(&header, &seq_no, 6))
			goto again;

		if (!CBS_write_bytes(&seq_no, &(S3I(s)->read_sequence[2]),
		    sizeof(S3I(s)->read_sequence) - 2, NULL))
			goto again;
		if (!CBS_get_u16(&header, &len))
			goto again;

		rr->type = type;
		rr->epoch = epoch;
		rr->length = len;

		/* unexpected version, silently discard */
		if (!s->internal->first_packet && ssl_version != s->version)
			goto again;

		/* wrong version, silently discard record */
		if ((ssl_version & 0xff00) != (s->version & 0xff00))
			goto again;

		/* record too long, silently discard it */
		if (rr->length > SSL3_RT_MAX_ENCRYPTED_LENGTH)
			goto again;

		/* now s->internal->rstate == SSL_ST_READ_BODY */
		p = (unsigned char *)CBS_data(&header);
	}

	/* s->internal->rstate == SSL_ST_READ_BODY, get and decode the data */

	n = ssl3_packet_extend(s, DTLS1_RT_HEADER_LENGTH + rr->length);
	if (n <= 0)
		return (n);

	/* If this packet contained a partial record, dump it. */
	if (n != DTLS1_RT_HEADER_LENGTH + rr->length)
		goto again;

	s->internal->rstate = SSL_ST_READ_HEADER; /* set state for later operations */

	/* match epochs.  NULL means the packet is dropped on the floor */
	bitmap = dtls1_get_bitmap(s, rr, &is_next_epoch);
	if (bitmap == NULL)
		goto again;

	/*
	 * Check whether this is a repeat, or aged record.
	 * Don't check if we're listening and this message is
	 * a ClientHello. They can look as if they're replayed,
	 * since they arrive from different connections and
	 * would be dropped unnecessarily.
	 */
	if (!(D1I(s)->listen && rr->type == SSL3_RT_HANDSHAKE &&
	    p != NULL && *p == SSL3_MT_CLIENT_HELLO) &&
	    !dtls1_record_replay_check(s, bitmap))
		goto again;

	/* just read a 0 length packet */
	if (rr->length == 0)
		goto again;

	/* If this record is from the next epoch (either HM or ALERT),
	 * and a handshake is currently in progress, buffer it since it
	 * cannot be processed at this time. However, do not buffer
	 * anything while listening.
	 */
	if (is_next_epoch) {
		if ((SSL_in_init(s) || s->internal->in_handshake) && !D1I(s)->listen) {
			if (dtls1_buffer_record(s, &(D1I(s)->unprocessed_rcds),
			    rr->seq_num) < 0)
				return (-1);
			/* Mark receipt of record. */
			dtls1_record_bitmap_update(s, bitmap);
		}
		goto again;
	}

	if (!dtls1_process_record(s))
		goto again;

	/* Mark receipt of record. */
	dtls1_record_bitmap_update(s, bitmap);

	return (1);
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
dtls1_read_bytes(SSL *s, int type, unsigned char *buf, int len, int peek)
{
	int al, i, j, ret;
	unsigned int n;
	SSL3_RECORD *rr;
	void (*cb)(const SSL *ssl, int type2, int val) = NULL;

	if (s->s3->rbuf.buf == NULL) /* Not initialized yet */
		if (!ssl3_setup_buffers(s))
			return (-1);

	if ((type &&
	     type != SSL3_RT_APPLICATION_DATA && type != SSL3_RT_HANDSHAKE) ||
	    (peek && (type != SSL3_RT_APPLICATION_DATA))) {
		SSLerror(s, ERR_R_INTERNAL_ERROR);
		return -1;
	}

	/* check whether there's a handshake message (client hello?) waiting */
	if ((ret = have_handshake_fragment(s, type, buf, len, peek)))
		return ret;

	/* Now D1I(s)->handshake_fragment_len == 0 if type == SSL3_RT_HANDSHAKE. */

	if (!s->internal->in_handshake && SSL_in_init(s))
	{
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
	s->internal->rwstate = SSL_NOTHING;

	/* S3I(s)->rrec.type	    - is the type of record
	 * S3I(s)->rrec.data,    - data
	 * S3I(s)->rrec.off,     - offset into 'data' for next read
	 * S3I(s)->rrec.length,  - number of bytes. */
	rr = &(S3I(s)->rrec);

	/* We are not handshaking and have no data yet,
	 * so process data buffered during the last handshake
	 * in advance, if any.
	 */
	if (S3I(s)->hs.state == SSL_ST_OK && rr->length == 0) {
		pitem *item;
		item = pqueue_pop(D1I(s)->buffered_app_data.q);
		if (item) {

			dtls1_copy_record(s, item);

			free(item->data);
			pitem_free(item);
		}
	}

	/* Check for timeout */
	if (dtls1_handle_timeout(s) > 0)
		goto start;

	/* get new packet if necessary */
	if ((rr->length == 0) || (s->internal->rstate == SSL_ST_READ_BODY)) {
		ret = dtls1_get_record(s);
		if (ret <= 0) {
			ret = dtls1_read_failed(s, ret);
			/* anything other than a timeout is an error */
			if (ret <= 0)
				return (ret);
			else
				goto start;
		}
	}

	if (D1I(s)->listen && rr->type != SSL3_RT_HANDSHAKE) {
		rr->length = 0;
		goto start;
	}

	/* we now have a packet which can be read and processed */

	if (S3I(s)->change_cipher_spec /* set when we receive ChangeCipherSpec,
	                               * reset by ssl3_get_finished */
	    && (rr->type != SSL3_RT_HANDSHAKE)) {
		/* We now have application data between CCS and Finished.
		 * Most likely the packets were reordered on their way, so
		 * buffer the application data for later processing rather
		 * than dropping the connection.
		 */
		if (dtls1_buffer_record(s, &(D1I(s)->buffered_app_data),
		    rr->seq_num) < 0) {
			SSLerror(s, ERR_R_INTERNAL_ERROR);
			return (-1);
		}
		rr->length = 0;
		goto start;
	}

	/* If the other end has shut down, throw anything we read away
	 * (even in 'peek' mode) */
	if (s->internal->shutdown & SSL_RECEIVED_SHUTDOWN) {
		rr->length = 0;
		s->internal->rwstate = SSL_NOTHING;
		return (0);
	}


	if (type == rr->type) /* SSL3_RT_APPLICATION_DATA or SSL3_RT_HANDSHAKE */
	{
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
			rr->length -= n;
			rr->off += n;
			if (rr->length == 0) {
				s->internal->rstate = SSL_ST_READ_HEADER;
				rr->off = 0;
			}
		}

		return (n);
	}


	/* If we get here, then type != rr->type; if we have a handshake
	 * message, then it was unexpected (Hello Request or Client Hello). */

	/* In case of record types for which we have 'fragment' storage,
	 * fill that so that we can process the data at a fixed place.
	 */
	{
		unsigned int k, dest_maxlen = 0;
		unsigned char *dest = NULL;
		unsigned int *dest_len = NULL;

		if (rr->type == SSL3_RT_HANDSHAKE) {
			dest_maxlen = sizeof D1I(s)->handshake_fragment;
			dest = D1I(s)->handshake_fragment;
			dest_len = &D1I(s)->handshake_fragment_len;
		} else if (rr->type == SSL3_RT_ALERT) {
			dest_maxlen = sizeof(D1I(s)->alert_fragment);
			dest = D1I(s)->alert_fragment;
			dest_len = &D1I(s)->alert_fragment_len;
		}
		/* else it's a CCS message, or application data or wrong */
		else if (rr->type != SSL3_RT_CHANGE_CIPHER_SPEC) {
			/* Application data while renegotiating
			 * is allowed. Try again reading.
			 */
			if (rr->type == SSL3_RT_APPLICATION_DATA) {
				BIO *bio;
				S3I(s)->in_read_app_data = 2;
				bio = SSL_get_rbio(s);
				s->internal->rwstate = SSL_READING;
				BIO_clear_retry_flags(bio);
				BIO_set_retry_read(bio);
				return (-1);
			}

			/* Not certain if this is the right error handling */
			al = SSL_AD_UNEXPECTED_MESSAGE;
			SSLerror(s, SSL_R_UNEXPECTED_RECORD);
			goto f_err;
		}

		if (dest_maxlen > 0) {
            /* XDTLS:  In a pathalogical case, the Client Hello
             *  may be fragmented--don't always expect dest_maxlen bytes */
			if (rr->length < dest_maxlen) {
#ifdef DTLS1_AD_MISSING_HANDSHAKE_MESSAGE
				/*
				 * for normal alerts rr->length is 2, while
				 * dest_maxlen is 7 if we were to handle this
				 * non-existing alert...
				 */
				FIX ME
#endif
				s->internal->rstate = SSL_ST_READ_HEADER;
				rr->length = 0;
				goto start;
			}

			/* now move 'n' bytes: */
			for ( k = 0; k < dest_maxlen; k++) {
				dest[k] = rr->data[rr->off++];
				rr->length--;
			}
			*dest_len = dest_maxlen;
		}
	}

	/* D1I(s)->handshake_fragment_len == 12  iff  rr->type == SSL3_RT_HANDSHAKE;
	 * D1I(s)->alert_fragment_len == 7      iff  rr->type == SSL3_RT_ALERT.
	 * (Possibly rr is 'empty' now, i.e. rr->length may be 0.) */

	/* If we are a client, check for an incoming 'Hello Request': */
	if ((!s->server) &&
	    (D1I(s)->handshake_fragment_len >= DTLS1_HM_HEADER_LENGTH) &&
	    (D1I(s)->handshake_fragment[0] == SSL3_MT_HELLO_REQUEST) &&
	    (s->session != NULL) && (s->session->cipher != NULL)) {
		D1I(s)->handshake_fragment_len = 0;

		if ((D1I(s)->handshake_fragment[1] != 0) ||
		    (D1I(s)->handshake_fragment[2] != 0) ||
		    (D1I(s)->handshake_fragment[3] != 0)) {
			al = SSL_AD_DECODE_ERROR;
			SSLerror(s, SSL_R_BAD_HELLO_REQUEST);
			goto err;
		}

		/* no need to check sequence number on HELLO REQUEST messages */

		if (s->internal->msg_callback)
			s->internal->msg_callback(0, s->version, SSL3_RT_HANDSHAKE,
		D1I(s)->handshake_fragment, 4, s, s->internal->msg_callback_arg);

		if (SSL_is_init_finished(s) &&
		    !(s->s3->flags & SSL3_FLAGS_NO_RENEGOTIATE_CIPHERS) &&
		    !S3I(s)->renegotiate) {
			D1I(s)->handshake_read_seq++;
			s->internal->new_session = 1;
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
					if (s->s3->rbuf.left == 0) /* no read-ahead left? */
					{
						BIO *bio;
						/* In the case where we try to read application data,
						 * but we trigger an SSL handshake, we return -1 with
						 * the retry option set.  Otherwise renegotiation may
						 * cause nasty problems in the blocking world */
						s->internal->rwstate = SSL_READING;
						bio = SSL_get_rbio(s);
						BIO_clear_retry_flags(bio);
						BIO_set_retry_read(bio);
						return (-1);
					}
				}
			}
		}
		/* we either finished a handshake or ignored the request,
		 * now try again to obtain the (application) data we were asked for */
		goto start;
	}

	if (D1I(s)->alert_fragment_len >= DTLS1_AL_HEADER_LENGTH) {
		int alert_level = D1I(s)->alert_fragment[0];
		int alert_descr = D1I(s)->alert_fragment[1];

		D1I(s)->alert_fragment_len = 0;

		if (s->internal->msg_callback)
			s->internal->msg_callback(0, s->version, SSL3_RT_ALERT,
		D1I(s)->alert_fragment, 2, s, s->internal->msg_callback_arg);

		if (s->internal->info_callback != NULL)
			cb = s->internal->info_callback;
		else if (s->ctx->internal->info_callback != NULL)
			cb = s->ctx->internal->info_callback;

		if (cb != NULL) {
			j = (alert_level << 8) | alert_descr;
			cb(s, SSL_CB_READ_ALERT, j);
		}

		if (alert_level == 1) /* warning */
		{
			S3I(s)->warn_alert = alert_descr;
			if (alert_descr == SSL_AD_CLOSE_NOTIFY) {
				s->internal->shutdown |= SSL_RECEIVED_SHUTDOWN;
				return (0);
			}
		} else if (alert_level == 2) /* fatal */
		{
			s->internal->rwstate = SSL_NOTHING;
			S3I(s)->fatal_alert = alert_descr;
			SSLerror(s, SSL_AD_REASON_OFFSET + alert_descr);
			ERR_asprintf_error_data("SSL alert number %d",
			    alert_descr);
			s->internal->shutdown|=SSL_RECEIVED_SHUTDOWN;
			SSL_CTX_remove_session(s->ctx, s->session);
			return (0);
		} else {
			al = SSL_AD_ILLEGAL_PARAMETER;
			SSLerror(s, SSL_R_UNKNOWN_ALERT_TYPE);
			goto f_err;
		}

		goto start;
	}

	if (s->internal->shutdown & SSL_SENT_SHUTDOWN) /* but we have not received a shutdown */
	{
		s->internal->rwstate = SSL_NOTHING;
		rr->length = 0;
		return (0);
	}

	if (rr->type == SSL3_RT_CHANGE_CIPHER_SPEC) {
		struct ccs_header_st ccs_hdr;
		unsigned int ccs_hdr_len = DTLS1_CCS_HEADER_LENGTH;

		dtls1_get_ccs_header(rr->data, &ccs_hdr);

		/* 'Change Cipher Spec' is just a single byte, so we know
		 * exactly what the record payload has to look like */
		/* XDTLS: check that epoch is consistent */
		if ((rr->length != ccs_hdr_len) ||
		    (rr->off != 0) || (rr->data[0] != SSL3_MT_CCS)) {
			i = SSL_AD_ILLEGAL_PARAMETER;
			SSLerror(s, SSL_R_BAD_CHANGE_CIPHER_SPEC);
			goto err;
		}

		rr->length = 0;

		if (s->internal->msg_callback)
			s->internal->msg_callback(0, s->version, SSL3_RT_CHANGE_CIPHER_SPEC,
		rr->data, 1, s, s->internal->msg_callback_arg);

		/* We can't process a CCS now, because previous handshake
		 * messages are still missing, so just drop it.
		 */
		if (!D1I(s)->change_cipher_spec_ok) {
			goto start;
		}

		D1I(s)->change_cipher_spec_ok = 0;

		S3I(s)->change_cipher_spec = 1;
		if (!ssl3_do_change_cipher_spec(s))
			goto err;

		/* do this whenever CCS is processed */
		dtls1_reset_seq_numbers(s, SSL3_CC_READ);

		goto start;
	}

	/* Unexpected handshake message (Client Hello, or protocol violation) */
	if ((D1I(s)->handshake_fragment_len >= DTLS1_HM_HEADER_LENGTH) &&
	    !s->internal->in_handshake) {
		struct hm_header_st msg_hdr;

		/* this may just be a stale retransmit */
		if (!dtls1_get_message_header(rr->data, &msg_hdr))
			return -1;
		if (rr->epoch != D1I(s)->r_epoch) {
			rr->length = 0;
			goto start;
		}

		/* If we are server, we may have a repeated FINISHED of the
		 * client here, then retransmit our CCS and FINISHED.
		 */
		if (msg_hdr.type == SSL3_MT_FINISHED) {
			if (dtls1_check_timeout_num(s) < 0)
				return -1;

			dtls1_retransmit_buffered_messages(s);
			rr->length = 0;
			goto start;
		}

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
			if (s->s3->rbuf.left == 0) /* no read-ahead left? */
			{
				BIO *bio;
				/* In the case where we try to read application data,
				 * but we trigger an SSL handshake, we return -1 with
				 * the retry option set.  Otherwise renegotiation may
				 * cause nasty problems in the blocking world */
				s->internal->rwstate = SSL_READING;
				bio = SSL_get_rbio(s);
				BIO_clear_retry_flags(bio);
				BIO_set_retry_read(bio);
				return (-1);
			}
		}
		goto start;
	}

	switch (rr->type) {
	default:
		/* TLS just ignores unknown message types */
		if (s->version == TLS1_VERSION) {
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
		    (S3I(s)->hs.state <= SSL3_ST_CR_SRVR_HELLO_A)) || (
		    (S3I(s)->hs.state & SSL_ST_ACCEPT) &&
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
dtls1_write_app_data_bytes(SSL *s, int type, const void *buf_, int len)
{
	int i;

	if (SSL_in_init(s) && !s->internal->in_handshake)
	{
		i = s->internal->handshake_func(s);
		if (i < 0)
			return (i);
		if (i == 0) {
			SSLerror(s, SSL_R_SSL_HANDSHAKE_FAILURE);
			return -1;
		}
	}

	if (len > SSL3_RT_MAX_PLAIN_LENGTH) {
		SSLerror(s, SSL_R_DTLS_MESSAGE_TOO_BIG);
		return -1;
	}

	i = dtls1_write_bytes(s, type, buf_, len);
	return i;
}


	/* this only happens when a client hello is received and a handshake
	 * is started. */
static int
have_handshake_fragment(SSL *s, int type, unsigned char *buf,
    int len, int peek)
{

	if ((type == SSL3_RT_HANDSHAKE) && (D1I(s)->handshake_fragment_len > 0))
		/* (partially) satisfy request from storage */
	{
		unsigned char *src = D1I(s)->handshake_fragment;
		unsigned char *dst = buf;
		unsigned int k, n;

		/* peek == 0 */
		n = 0;
		while ((len > 0) && (D1I(s)->handshake_fragment_len > 0)) {
			*dst++ = *src++;
			len--;
			D1I(s)->handshake_fragment_len--;
			n++;
		}
		/* move any remaining fragment bytes: */
		for (k = 0; k < D1I(s)->handshake_fragment_len; k++)
			D1I(s)->handshake_fragment[k] = *src++;
		return n;
	}

	return 0;
}


/* Call this to write data in records of type 'type'
 * It will return <= 0 if not all data has been sent or non-blocking IO.
 */
int
dtls1_write_bytes(SSL *s, int type, const void *buf, int len)
{
	int i;

	OPENSSL_assert(len <= SSL3_RT_MAX_PLAIN_LENGTH);
	s->internal->rwstate = SSL_NOTHING;
	i = do_dtls1_write(s, type, buf, len);
	return i;
}

int
do_dtls1_write(SSL *s, int type, const unsigned char *buf, unsigned int len)
{
	unsigned char *p, *pseq;
	int i, mac_size, clear = 0;
	int prefix_len = 0;
	SSL3_RECORD *wr;
	SSL3_BUFFER *wb;
	SSL_SESSION *sess;
	int bs;

	/* first check if there is a SSL3_BUFFER still being written
	 * out.  This will happen with non blocking IO */
	if (s->s3->wbuf.left != 0) {
		OPENSSL_assert(0); /* XDTLS:  want to see if we ever get here */
		return (ssl3_write_pending(s, type, buf, len));
	}

	/* If we have an alert to send, lets send it */
	if (s->s3->alert_dispatch) {
		i = s->method->ssl_dispatch_alert(s);
		if (i <= 0)
			return (i);
		/* if it went, fall through and send more stuff */
	}

	if (len == 0)
		return 0;

	wr = &(S3I(s)->wrec);
	wb = &(s->s3->wbuf);
	sess = s->session;

	if ((sess == NULL) || (s->internal->enc_write_ctx == NULL) ||
	    (EVP_MD_CTX_md(s->internal->write_hash) == NULL))
		clear = 1;

	if (clear)
		mac_size = 0;
	else {
		mac_size = EVP_MD_CTX_size(s->internal->write_hash);
		if (mac_size < 0)
			goto err;
	}

	/* DTLS implements explicit IV, so no need for empty fragments. */

	p = wb->buf + prefix_len;

	/* write the header */

	*(p++) = type&0xff;
	wr->type = type;

	*(p++) = (s->version >> 8);
	*(p++) = s->version&0xff;

	/* field where we are to write out packet epoch, seq num and len */
	pseq = p;

	p += 10;

	/* lets setup the record stuff. */

	/* Make space for the explicit IV in case of CBC.
	 * (this is a bit of a boundary violation, but what the heck).
	 */
	if (s->internal->enc_write_ctx &&
	    (EVP_CIPHER_mode(s->internal->enc_write_ctx->cipher) & EVP_CIPH_CBC_MODE))
		bs = EVP_CIPHER_block_size(s->internal->enc_write_ctx->cipher);
	else
		bs = 0;

	wr->data = p + bs;
	/* make room for IV in case of CBC */
	wr->length = (int)len;
	wr->input = (unsigned char *)buf;

	/* we now 'read' from wr->input, wr->length bytes into
	 * wr->data */

	memcpy(wr->data, wr->input, wr->length);
	wr->input = wr->data;

	/* we should still have the output to wr->data and the input
	 * from wr->input.  Length should be wr->length.
	 * wr->data still points in the wb->buf */

	if (mac_size != 0) {
		if (tls1_mac(s, &(p[wr->length + bs]), 1) < 0)
			goto err;
		wr->length += mac_size;
	}

	/* this is true regardless of mac size */
	wr->input = p;
	wr->data = p;


	/* ssl3_enc can only have an error on read */
	if (bs)	/* bs != 0 in case of CBC */
	{
		arc4random_buf(p, bs);
		/* master IV and last CBC residue stand for
		 * the rest of randomness */
		wr->length += bs;
	}

	s->method->internal->ssl3_enc->enc(s, 1);

	/* record length after mac and block padding */
/*	if (type == SSL3_RT_APPLICATION_DATA ||
	(type == SSL3_RT_ALERT && ! SSL_in_init(s))) */

	/* there's only one epoch between handshake and app data */

	s2n(D1I(s)->w_epoch, pseq);

	/* XDTLS: ?? */
/*	else
	s2n(D1I(s)->handshake_epoch, pseq);
*/

	memcpy(pseq, &(S3I(s)->write_sequence[2]), 6);
	pseq += 6;
	s2n(wr->length, pseq);

	/* we should now have
	 * wr->data pointing to the encrypted data, which is
	 * wr->length long */
	wr->type=type; /* not needed but helps for debugging */
	wr->length += DTLS1_RT_HEADER_LENGTH;

	tls1_record_sequence_increment(S3I(s)->write_sequence);

	/* now let's set up wb */
	wb->left = prefix_len + wr->length;
	wb->offset = 0;

	/* memorize arguments so that ssl3_write_pending can detect bad write retries later */
	S3I(s)->wpend_tot = len;
	S3I(s)->wpend_buf = buf;
	S3I(s)->wpend_type = type;
	S3I(s)->wpend_ret = len;

	/* we now just need to write the buffer */
	return ssl3_write_pending(s, type, buf, len);
err:
	return -1;
}



static int
dtls1_record_replay_check(SSL *s, DTLS1_BITMAP *bitmap)
{
	int cmp;
	unsigned int shift;
	const unsigned char *seq = S3I(s)->read_sequence;

	cmp = satsub64be(seq, bitmap->max_seq_num);
	if (cmp > 0) {
		memcpy (S3I(s)->rrec.seq_num, seq, 8);
		return 1; /* this record in new */
	}
	shift = -cmp;
	if (shift >= sizeof(bitmap->map)*8)
		return 0; /* stale, outside the window */
	else if (bitmap->map & (1UL << shift))
		return 0; /* record previously received */

	memcpy(S3I(s)->rrec.seq_num, seq, 8);
	return 1;
}


static void
dtls1_record_bitmap_update(SSL *s, DTLS1_BITMAP *bitmap)
{
	int cmp;
	unsigned int shift;
	const unsigned char *seq = S3I(s)->read_sequence;

	cmp = satsub64be(seq, bitmap->max_seq_num);
	if (cmp > 0) {
		shift = cmp;
		if (shift < sizeof(bitmap->map)*8)
			bitmap->map <<= shift, bitmap->map |= 1UL;
		else
			bitmap->map = 1UL;
		memcpy(bitmap->max_seq_num, seq, 8);
	} else {
		shift = -cmp;
		if (shift < sizeof(bitmap->map) * 8)
			bitmap->map |= 1UL << shift;
	}
}


int
dtls1_dispatch_alert(SSL *s)
{
	int i, j;
	void (*cb)(const SSL *ssl, int type, int val) = NULL;
	unsigned char buf[DTLS1_AL_HEADER_LENGTH];
	unsigned char *ptr = &buf[0];

	s->s3->alert_dispatch = 0;

	memset(buf, 0x00, sizeof(buf));
	*ptr++ = s->s3->send_alert[0];
	*ptr++ = s->s3->send_alert[1];

#ifdef DTLS1_AD_MISSING_HANDSHAKE_MESSAGE
	if (s->s3->send_alert[1] == DTLS1_AD_MISSING_HANDSHAKE_MESSAGE) {
		s2n(D1I(s)->handshake_read_seq, ptr);
		l2n3(D1I(s)->r_msg_hdr.frag_off, ptr);
	}
#endif

	i = do_dtls1_write(s, SSL3_RT_ALERT, &buf[0], sizeof(buf));
	if (i <= 0) {
		s->s3->alert_dispatch = 1;
		/* fprintf( stderr, "not done with alert\n" ); */
	} else {
		if (s->s3->send_alert[0] == SSL3_AL_FATAL
#ifdef DTLS1_AD_MISSING_HANDSHAKE_MESSAGE
		|| s->s3->send_alert[1] == DTLS1_AD_MISSING_HANDSHAKE_MESSAGE
#endif
		)
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


static DTLS1_BITMAP *
dtls1_get_bitmap(SSL *s, SSL3_RECORD *rr, unsigned int *is_next_epoch)
{

	*is_next_epoch = 0;

	/* In current epoch, accept HM, CCS, DATA, & ALERT */
	if (rr->epoch == D1I(s)->r_epoch)
		return &D1I(s)->bitmap;

	/* Only HM and ALERT messages can be from the next epoch */
	else if (rr->epoch == (unsigned long)(D1I(s)->r_epoch + 1) &&
		(rr->type == SSL3_RT_HANDSHAKE || rr->type == SSL3_RT_ALERT)) {
		*is_next_epoch = 1;
		return &D1I(s)->next_bitmap;
	}

	return NULL;
}

void
dtls1_reset_seq_numbers(SSL *s, int rw)
{
	unsigned char *seq;
	unsigned int seq_bytes = sizeof(S3I(s)->read_sequence);

	if (rw & SSL3_CC_READ) {
		seq = S3I(s)->read_sequence;
		D1I(s)->r_epoch++;
		memcpy(&(D1I(s)->bitmap), &(D1I(s)->next_bitmap), sizeof(DTLS1_BITMAP));
		memset(&(D1I(s)->next_bitmap), 0x00, sizeof(DTLS1_BITMAP));
	} else {
		seq = S3I(s)->write_sequence;
		memcpy(D1I(s)->last_write_sequence, seq, sizeof(S3I(s)->write_sequence));
		D1I(s)->w_epoch++;
	}

	memset(seq, 0x00, seq_bytes);
}
