/****************************************************************************
  Copyright (C) 2012 Monty Program AB
  2016 MariaDB Corporation AB

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Library General Public
  License as published by the Free Software Foundation; either
  version 2 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Library General Public License for more details.

  You should have received a copy of the GNU Library General Public
  License along with this library; if not see <http://www.gnu.org/licenses>
  or write to the Free Software Foundation, Inc., 
  51 Franklin St., Fifth Floor, Boston, MA 02110, USA
 *****************************************************************************/

/* This code came from the PHP project, initially written by
   Stefan Esser */


#include "ma_global.h"
#include "string.h"

/* This code is heavily based on the PHP md5 implementation */ 

#include "ma_sha1.h"


static void ma_SHA1Transform(uint32[5], const unsigned char[64]);
static void ma_SHA1Encode(unsigned char *, uint32 *, unsigned int);
static void ma_SHA1Decode(uint32 *, const unsigned char *, unsigned int);

static unsigned char PADDING[64] =
{
  0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

/* F, G, H and I are basic SHA1 functions.
*/
#define F(x, y, z) ((z) ^ ((x) & ((y) ^ (z))))
#define G(x, y, z) ((x) ^ (y) ^ (z))
#define H(x, y, z) (((x) & (y)) | ((z) & ((x) | (y))))
#define I(x, y, z) ((x) ^ (y) ^ (z))

/* ROTATE_LEFT rotates x left n bits.
*/
#define ROTATE_LEFT(x, n) (((x) << (n)) | ((x) >> (32-(n))))

/* W[i]
*/
#define W(i) ( tmp=x[(i-3)&15]^x[(i-8)&15]^x[(i-14)&15]^x[i&15], \
    (x[i&15]=ROTATE_LEFT(tmp, 1)) )  

/* FF, GG, HH, and II transformations for rounds 1, 2, 3, and 4.
*/
#define FF(a, b, c, d, e, w) { \
  (e) += F ((b), (c), (d)) + (w) + (uint32)(0x5A827999); \
  (e) += ROTATE_LEFT ((a), 5); \
  (b) = ROTATE_LEFT((b), 30); \
}
#define GG(a, b, c, d, e, w) { \
  (e) += G ((b), (c), (d)) + (w) + (uint32)(0x6ED9EBA1); \
  (e) += ROTATE_LEFT ((a), 5); \
  (b) = ROTATE_LEFT((b), 30); \
}
#define HH(a, b, c, d, e, w) { \
  (e) += H ((b), (c), (d)) + (w) + (uint32)(0x8F1BBCDC); \
  (e) += ROTATE_LEFT ((a), 5); \
  (b) = ROTATE_LEFT((b), 30); \
}
#define II(a, b, c, d, e, w) { \
  (e) += I ((b), (c), (d)) + (w) + (uint32)(0xCA62C1D6); \
  (e) += ROTATE_LEFT ((a), 5); \
  (b) = ROTATE_LEFT((b), 30); \
}


/* {{{ ma_SHA1Init
 * SHA1 initialization. Begins an SHA1 operation, writing a new context.
 */
void ma_SHA1Init(_MA_SHA1_CTX * context)
{
  context->count[0] = context->count[1] = 0;
  /* Load magic initialization constants.
  */
  context->state[0] = 0x67452301;
  context->state[1] = 0xefcdab89;
  context->state[2] = 0x98badcfe;
  context->state[3] = 0x10325476;
  context->state[4] = 0xc3d2e1f0;
}
/* }}} */

/* {{{ ma_SHA1Update
   SHA1 block update operation. Continues an SHA1 message-digest
   operation, processing another message block, and updating the
   context.
   */
void ma_SHA1Update(_MA_SHA1_CTX * context, const unsigned char *input,
    size_t inputLen)
{
  unsigned int i, index, partLen;

  /* Compute number of bytes mod 64 */
  index = (unsigned int) ((context->count[0] >> 3) & 0x3F);

  /* Update number of bits */
  if ((context->count[0] += ((uint32) inputLen << 3))
      < ((uint32) inputLen << 3))
    context->count[1]++;
  context->count[1] += ((uint32) inputLen >> 29);

  partLen = 64 - index;

  /* Transform as many times as possible.
  */
  if (inputLen >= partLen) {
    memcpy
      ((unsigned char*) & context->buffer[index], (unsigned char*) input, partLen);
    ma_SHA1Transform(context->state, context->buffer);

    for (i = partLen; i + 63 < inputLen; i += 64)
      ma_SHA1Transform(context->state, &input[i]);

    index = 0;
  } else
    i = 0;

  /* Buffer remaining input */
  memcpy
    ((unsigned char*) & context->buffer[index], (unsigned char*) & input[i],
     inputLen - i);
}
/* }}} */

/* {{{ ma_SHA1Final
   SHA1 finalization. Ends an SHA1 message-digest operation, writing the
   the message digest and zeroizing the context.
   */
void ma_SHA1Final(unsigned char digest[20], _MA_SHA1_CTX * context)
{
  unsigned char bits[8];
  unsigned int index, padLen;

  /* Save number of bits */
  bits[7] = context->count[0] & 0xFF;
  bits[6] = (context->count[0] >> 8) & 0xFF;
  bits[5] = (context->count[0] >> 16) & 0xFF;
  bits[4] = (context->count[0] >> 24) & 0xFF;
  bits[3] = context->count[1] & 0xFF;
  bits[2] = (context->count[1] >> 8) & 0xFF;
  bits[1] = (context->count[1] >> 16) & 0xFF;
  bits[0] = (context->count[1] >> 24) & 0xFF;

  /* Pad out to 56 mod 64.
  */
  index = (unsigned int) ((context->count[0] >> 3) & 0x3f);
  padLen = (index < 56) ? (56 - index) : (120 - index);
  ma_SHA1Update(context, PADDING, padLen);

  /* Append length (before padding) */
  ma_SHA1Update(context, bits, 8);

  /* Store state in digest */
  ma_SHA1Encode(digest, context->state, 20);

  /* Zeroize sensitive information.
  */
  memset((unsigned char*) context, 0, sizeof(*context));
}
/* }}} */

/* {{{ ma_SHA1Transform
 * SHA1 basic transformation. Transforms state based on block.
 */
static void ma_SHA1Transform(uint32 state[5], const unsigned char block[64])
{
  uint32 a = state[0], b = state[1], c = state[2];
  uint32 d = state[3], e = state[4], x[16], tmp;

  ma_SHA1Decode(x, block, 64);

  /* Round 1 */
  FF(a, b, c, d, e, x[0]);   /* 1 */
  FF(e, a, b, c, d, x[1]);   /* 2 */
  FF(d, e, a, b, c, x[2]);   /* 3 */
  FF(c, d, e, a, b, x[3]);   /* 4 */
  FF(b, c, d, e, a, x[4]);   /* 5 */
  FF(a, b, c, d, e, x[5]);   /* 6 */
  FF(e, a, b, c, d, x[6]);   /* 7 */
  FF(d, e, a, b, c, x[7]);   /* 8 */
  FF(c, d, e, a, b, x[8]);   /* 9 */
  FF(b, c, d, e, a, x[9]);   /* 10 */
  FF(a, b, c, d, e, x[10]);  /* 11 */
  FF(e, a, b, c, d, x[11]);  /* 12 */
  FF(d, e, a, b, c, x[12]);  /* 13 */
  FF(c, d, e, a, b, x[13]);  /* 14 */
  FF(b, c, d, e, a, x[14]);  /* 15 */
  FF(a, b, c, d, e, x[15]);  /* 16 */
  FF(e, a, b, c, d, W(16));  /* 17 */
  FF(d, e, a, b, c, W(17));  /* 18 */
  FF(c, d, e, a, b, W(18));  /* 19 */
  FF(b, c, d, e, a, W(19));  /* 20 */

  /* Round 2 */
  GG(a, b, c, d, e, W(20));  /* 21 */
  GG(e, a, b, c, d, W(21));  /* 22 */
  GG(d, e, a, b, c, W(22));  /* 23 */
  GG(c, d, e, a, b, W(23));  /* 24 */
  GG(b, c, d, e, a, W(24));  /* 25 */
  GG(a, b, c, d, e, W(25));  /* 26 */
  GG(e, a, b, c, d, W(26));  /* 27 */
  GG(d, e, a, b, c, W(27));  /* 28 */
  GG(c, d, e, a, b, W(28));  /* 29 */
  GG(b, c, d, e, a, W(29));  /* 30 */
  GG(a, b, c, d, e, W(30));  /* 31 */
  GG(e, a, b, c, d, W(31));  /* 32 */
  GG(d, e, a, b, c, W(32));  /* 33 */
  GG(c, d, e, a, b, W(33));  /* 34 */
  GG(b, c, d, e, a, W(34));  /* 35 */
  GG(a, b, c, d, e, W(35));  /* 36 */
  GG(e, a, b, c, d, W(36));  /* 37 */
  GG(d, e, a, b, c, W(37));  /* 38 */
  GG(c, d, e, a, b, W(38));  /* 39 */
  GG(b, c, d, e, a, W(39));  /* 40 */

  /* Round 3 */
  HH(a, b, c, d, e, W(40));  /* 41 */
  HH(e, a, b, c, d, W(41));  /* 42 */
  HH(d, e, a, b, c, W(42));  /* 43 */
  HH(c, d, e, a, b, W(43));  /* 44 */
  HH(b, c, d, e, a, W(44));  /* 45 */
  HH(a, b, c, d, e, W(45));  /* 46 */
  HH(e, a, b, c, d, W(46));  /* 47 */
  HH(d, e, a, b, c, W(47));  /* 48 */
  HH(c, d, e, a, b, W(48));  /* 49 */
  HH(b, c, d, e, a, W(49));  /* 50 */
  HH(a, b, c, d, e, W(50));  /* 51 */
  HH(e, a, b, c, d, W(51));  /* 52 */
  HH(d, e, a, b, c, W(52));  /* 53 */
  HH(c, d, e, a, b, W(53));  /* 54 */
  HH(b, c, d, e, a, W(54));  /* 55 */
  HH(a, b, c, d, e, W(55));  /* 56 */
  HH(e, a, b, c, d, W(56));  /* 57 */
  HH(d, e, a, b, c, W(57));  /* 58 */
  HH(c, d, e, a, b, W(58));  /* 59 */
  HH(b, c, d, e, a, W(59));  /* 60 */

  /* Round 4 */
  II(a, b, c, d, e, W(60));  /* 61 */
  II(e, a, b, c, d, W(61));  /* 62 */
  II(d, e, a, b, c, W(62));  /* 63 */
  II(c, d, e, a, b, W(63));  /* 64 */
  II(b, c, d, e, a, W(64));  /* 65 */
  II(a, b, c, d, e, W(65));  /* 66 */
  II(e, a, b, c, d, W(66));  /* 67 */
  II(d, e, a, b, c, W(67));  /* 68 */
  II(c, d, e, a, b, W(68));  /* 69 */
  II(b, c, d, e, a, W(69));  /* 70 */
  II(a, b, c, d, e, W(70));  /* 71 */
  II(e, a, b, c, d, W(71));  /* 72 */
  II(d, e, a, b, c, W(72));  /* 73 */
  II(c, d, e, a, b, W(73));  /* 74 */
  II(b, c, d, e, a, W(74));  /* 75 */
  II(a, b, c, d, e, W(75));  /* 76 */
  II(e, a, b, c, d, W(76));  /* 77 */
  II(d, e, a, b, c, W(77));  /* 78 */
  II(c, d, e, a, b, W(78));  /* 79 */
  II(b, c, d, e, a, W(79));  /* 80 */

  state[0] += a;
  state[1] += b;
  state[2] += c;
  state[3] += d;
  state[4] += e;

  /* Zeroize sensitive information. */
  memset((unsigned char*) x, 0, sizeof(x));
}
/* }}} */

/* {{{ ma_SHA1Encode
   Encodes input (uint32) into output (unsigned char). Assumes len is
   a multiple of 4.
   */
static void ma_SHA1Encode(unsigned char *output, uint32 *input, unsigned int len)
{
  unsigned int i, j;

  for (i = 0, j = 0; j < len; i++, j += 4) {
    output[j] = (unsigned char) ((input[i] >> 24) & 0xff);
    output[j + 1] = (unsigned char) ((input[i] >> 16) & 0xff);
    output[j + 2] = (unsigned char) ((input[i] >> 8) & 0xff);
    output[j + 3] = (unsigned char) (input[i] & 0xff);
  }
}
/* }}} */

/* {{{ ma_SHA1Decode
   Decodes input (unsigned char) into output (uint32). Assumes len is
   a multiple of 4.
   */
static void ma_SHA1Decode(uint32 *output, const unsigned char * input, unsigned int len)
{
  unsigned int i, j;

  for (i = 0, j = 0; j < len; i++, j += 4)
    output[i] = ((uint32) input[j + 3]) | (((uint32) input[j + 2]) << 8) |
      (((uint32) input[j + 1]) << 16) | (((uint32) input[j]) << 24);
}
/* }}} */

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: sw=4 ts=4 fdm=marker
 * vim<600: sw=4 ts=4
 */
