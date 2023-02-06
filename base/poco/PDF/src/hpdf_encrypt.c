/*
 * << Haru Free PDF Library >> -- hpdf_encryor.c
 *
 * URL: http://libharu.org
 *
 * Copyright (c) 1999-2006 Takeshi Kanno <takeshi_kanno@est.hi-ho.ne.jp>
 * Copyright (c) 2007-2009 Antony Dovgal <tony@daylessday.org>
 *
 * Permission to use, copy, modify, distribute and sell this software
 * and its documentation for any purpose is hereby granted without fee,
 * provided that the above copyright notice appear in all copies and
 * that both that copyright notice and this permission notice appear
 * in supporting documentation.
 * It is provided "as is" without express or implied warranty.
 *
 *------------------------------------------------------------------------------
 *
 * The code implements MD5 message-digest algorithm is based on the code
 * written by Colin Plumb.
 * The copyright of it is as follows.
 *
 * This code implements the MD5 message-digest algorithm.
 * The algorithm is due to Ron Rivest.  This code was
 * written by Colin Plumb in 1993, no copyright is claimed.
 * This code is in the public domain; do with it what you wish.
 *
 * Equivalent code is available from RSA Data Security, Inc.
 * This code has been tested against that, and is equivalent,
 * except that you don't need to include two pages of legalese
 * with every copy.
 *
 * To compute the message digest of a chunk of bytes, declare an
 * MD5Context structure, pass it to MD5Init, call MD5Update as
 * needed on buffers full of bytes, and then call MD5Final, which
 * will fill a supplied 16-byte array with the digest.
 *
 *---------------------------------------------------------------------------*/

#include "hpdf_conf.h"
#include "hpdf_consts.h"
#include "hpdf_utils.h"
#include "hpdf_encrypt.h"

static const HPDF_BYTE HPDF_PADDING_STRING[] = {
    0x28, 0xBF, 0x4E, 0x5E, 0x4E, 0x75, 0x8A, 0x41,
    0x64, 0x00, 0x4E, 0x56, 0xFF, 0xFA, 0x01, 0x08,
    0x2E, 0x2E, 0x00, 0xB6, 0xD0, 0x68, 0x3E, 0x80,
    0x2F, 0x0C, 0xA9, 0xFE, 0x64, 0x53, 0x69, 0x7A
};


/*---------------------------------------------------------------------------*/
/*------ MD5 message-digest algorithm ---------------------------------------*/

static void
MD5Transform  (HPDF_UINT32       buf[4],
               const HPDF_UINT32 in[16]);


static void
MD5ByteReverse  (HPDF_BYTE    *buf,
                 HPDF_UINT32  longs);


void
HPDF_MD5Init  (struct HPDF_MD5Context  *ctx)
{
    ctx->buf[0] = 0x67452301;
    ctx->buf[1] = 0xefcdab89;
    ctx->buf[2] = 0x98badcfe;
    ctx->buf[3] = 0x10325476;

    ctx->bits[0] = 0;
    ctx->bits[1] = 0;
}


void
HPDF_MD5Update  (struct HPDF_MD5Context *ctx,
                 const HPDF_BYTE        *buf,
                 HPDF_UINT32            len)
{
    HPDF_UINT32 t;

    /* Update bitcount */

    t = ctx->bits[0];
    if ((ctx->bits[0] = t + ((HPDF_UINT32) len << 3)) < t)
        ctx->bits[1]++;     /* Carry from low to high */
    ctx->bits[1] += len >> 29;

    t = (t >> 3) & 0x3f; /* Bytes already in shsInfo->data */

    /* Handle any leading odd-sized chunks */

    if (t) {
        HPDF_BYTE *p = (HPDF_BYTE *) ctx->in + t;

        t = 64 - t;
        if (len < t)
        {
            HPDF_MemCpy (p, buf, len);
            return;
        }
        HPDF_MemCpy (p, buf, t);
        MD5ByteReverse (ctx->in, 16);
        MD5Transform (ctx->buf, (HPDF_UINT32 *) ctx->in);
        buf += t;
        len -= t;
    }
    /* Process data in 64-byte chunks */

    while (len >= 64) {
        HPDF_MemCpy (ctx->in, buf, 64);
        MD5ByteReverse (ctx->in, 16);
        MD5Transform (ctx->buf, (HPDF_UINT32 *) ctx->in);
        buf += 64;
        len -= 64;
    }

    /* Handle any remaining bytes of data. */

    HPDF_MemCpy (ctx->in, buf, len);
}


/*
 * Final wrapup - pad to 64-byte boundary with the bit pattern
 * 1 0* (64-bit count of bits processed, MSB-first)
 */
void
HPDF_MD5Final  (HPDF_BYTE              digest[16],
                struct HPDF_MD5Context *ctx)
{
    HPDF_UINT32 count;
    HPDF_BYTE *p;

    /* Compute number of bytes mod 64 */
    count = (ctx->bits[0] >> 3) & 0x3F;

    /* Set the first char of padding to 0x80.  This is safe since there is
       always at least one byte free */
    p = ctx->in + count;
    *p++ = 0x80;

    /* Bytes of padding needed to make 64 bytes */
    count = 64 - 1 - count;

    /* Pad out to 56 mod 64 */
    if (count < 8) {
        /* Two lots of padding:  Pad the first block to 64 bytes */
        HPDF_MemSet (p, 0, count);
        MD5ByteReverse (ctx->in, 16);
        MD5Transform (ctx->buf, (HPDF_UINT32 *) ctx->in);

        /* Now fill the next block with 56 bytes */
        HPDF_MemSet (ctx->in, 0, 56);
    } else {
        /* Pad block to 56 bytes */
        HPDF_MemSet (p, 0, count - 8);
    }
    MD5ByteReverse (ctx->in, 14);

    /* Append length in bits and transform */
    ((HPDF_UINT32 *) ctx->in)[14] = ctx->bits[0];
    ((HPDF_UINT32 *) ctx->in)[15] = ctx->bits[1];

    MD5Transform (ctx->buf, (HPDF_UINT32 *) ctx->in);
    MD5ByteReverse ((HPDF_BYTE *) ctx->buf, 4);
    HPDF_MemCpy ((HPDF_BYTE *)digest, (HPDF_BYTE *)ctx->buf, 16);
    HPDF_MemSet ((HPDF_BYTE *)ctx, 0, sizeof (ctx));   /* In case it's sensitive */
}

/* The four core functions - F1 is optimized somewhat */

/* #define F1(x, y, z) (x & y | ~x & z) */
#define F1(x, y, z) (z ^ (x & (y ^ z)))
#define F2(x, y, z) F1(z, x, y)
#define F3(x, y, z) (x ^ y ^ z)
#define F4(x, y, z) (y ^ (x | ~z))

/* This is the central step in the HPDF_MD5 algorithm. */
#define HPDF_MD5STEP(f, w, x, y, z, data, s) \
 ( w += f(x, y, z) + data,  w = w<<s | w>>(32-s),  w += x )


/*
 * The core of the MD5 algorithm, this alters an existing MD5 hash to
 * reflect the addition of 16 longwords of new data.  MD5Update blocks
 * the data and converts bytes into longwords for this routine.
 */
static void
MD5Transform  (HPDF_UINT32       buf[4],
               const HPDF_UINT32 in[16])
{
    register HPDF_UINT32 a, b, c, d;

    a = buf[0];
    b = buf[1];
    c = buf[2];
    d = buf[3];

    HPDF_MD5STEP (F1, a, b, c, d, in[0] + 0xd76aa478, 7);
    HPDF_MD5STEP (F1, d, a, b, c, in[1] + 0xe8c7b756, 12);
    HPDF_MD5STEP (F1, c, d, a, b, in[2] + 0x242070db, 17);
    HPDF_MD5STEP (F1, b, c, d, a, in[3] + 0xc1bdceee, 22);
    HPDF_MD5STEP (F1, a, b, c, d, in[4] + 0xf57c0faf, 7);
    HPDF_MD5STEP (F1, d, a, b, c, in[5] + 0x4787c62a, 12);
    HPDF_MD5STEP (F1, c, d, a, b, in[6] + 0xa8304613, 17);
    HPDF_MD5STEP (F1, b, c, d, a, in[7] + 0xfd469501, 22);
    HPDF_MD5STEP (F1, a, b, c, d, in[8] + 0x698098d8, 7);
    HPDF_MD5STEP (F1, d, a, b, c, in[9] + 0x8b44f7af, 12);
    HPDF_MD5STEP (F1, c, d, a, b, in[10] + 0xffff5bb1, 17);
    HPDF_MD5STEP (F1, b, c, d, a, in[11] + 0x895cd7be, 22);
    HPDF_MD5STEP (F1, a, b, c, d, in[12] + 0x6b901122, 7);
    HPDF_MD5STEP (F1, d, a, b, c, in[13] + 0xfd987193, 12);
    HPDF_MD5STEP (F1, c, d, a, b, in[14] + 0xa679438e, 17);
    HPDF_MD5STEP (F1, b, c, d, a, in[15] + 0x49b40821, 22);

    HPDF_MD5STEP (F2, a, b, c, d, in[1] + 0xf61e2562, 5);
    HPDF_MD5STEP (F2, d, a, b, c, in[6] + 0xc040b340, 9);
    HPDF_MD5STEP (F2, c, d, a, b, in[11] + 0x265e5a51, 14);
    HPDF_MD5STEP (F2, b, c, d, a, in[0] + 0xe9b6c7aa, 20);
    HPDF_MD5STEP (F2, a, b, c, d, in[5] + 0xd62f105d, 5);
    HPDF_MD5STEP (F2, d, a, b, c, in[10] + 0x02441453, 9);
    HPDF_MD5STEP (F2, c, d, a, b, in[15] + 0xd8a1e681, 14);
    HPDF_MD5STEP (F2, b, c, d, a, in[4] + 0xe7d3fbc8, 20);
    HPDF_MD5STEP (F2, a, b, c, d, in[9] + 0x21e1cde6, 5);
    HPDF_MD5STEP (F2, d, a, b, c, in[14] + 0xc33707d6, 9);
    HPDF_MD5STEP (F2, c, d, a, b, in[3] + 0xf4d50d87, 14);
    HPDF_MD5STEP (F2, b, c, d, a, in[8] + 0x455a14ed, 20);
    HPDF_MD5STEP (F2, a, b, c, d, in[13] + 0xa9e3e905, 5);
    HPDF_MD5STEP (F2, d, a, b, c, in[2] + 0xfcefa3f8, 9);
    HPDF_MD5STEP (F2, c, d, a, b, in[7] + 0x676f02d9, 14);
    HPDF_MD5STEP (F2, b, c, d, a, in[12] + 0x8d2a4c8a, 20);

    HPDF_MD5STEP (F3, a, b, c, d, in[5] + 0xfffa3942, 4);
    HPDF_MD5STEP (F3, d, a, b, c, in[8] + 0x8771f681, 11);
    HPDF_MD5STEP (F3, c, d, a, b, in[11] + 0x6d9d6122, 16);
    HPDF_MD5STEP (F3, b, c, d, a, in[14] + 0xfde5380c, 23);
    HPDF_MD5STEP (F3, a, b, c, d, in[1] + 0xa4beea44, 4);
    HPDF_MD5STEP (F3, d, a, b, c, in[4] + 0x4bdecfa9, 11);
    HPDF_MD5STEP (F3, c, d, a, b, in[7] + 0xf6bb4b60, 16);
    HPDF_MD5STEP (F3, b, c, d, a, in[10] + 0xbebfbc70, 23);
    HPDF_MD5STEP (F3, a, b, c, d, in[13] + 0x289b7ec6, 4);
    HPDF_MD5STEP (F3, d, a, b, c, in[0] + 0xeaa127fa, 11);
    HPDF_MD5STEP (F3, c, d, a, b, in[3] + 0xd4ef3085, 16);
    HPDF_MD5STEP (F3, b, c, d, a, in[6] + 0x04881d05, 23);
    HPDF_MD5STEP (F3, a, b, c, d, in[9] + 0xd9d4d039, 4);
    HPDF_MD5STEP (F3, d, a, b, c, in[12] + 0xe6db99e5, 11);
    HPDF_MD5STEP (F3, c, d, a, b, in[15] + 0x1fa27cf8, 16);
    HPDF_MD5STEP (F3, b, c, d, a, in[2] + 0xc4ac5665, 23);

    HPDF_MD5STEP (F4, a, b, c, d, in[0] + 0xf4292244, 6);
    HPDF_MD5STEP (F4, d, a, b, c, in[7] + 0x432aff97, 10);
    HPDF_MD5STEP (F4, c, d, a, b, in[14] + 0xab9423a7, 15);
    HPDF_MD5STEP (F4, b, c, d, a, in[5] + 0xfc93a039, 21);
    HPDF_MD5STEP (F4, a, b, c, d, in[12] + 0x655b59c3, 6);
    HPDF_MD5STEP (F4, d, a, b, c, in[3] + 0x8f0ccc92, 10);
    HPDF_MD5STEP (F4, c, d, a, b, in[10] + 0xffeff47d, 15);
    HPDF_MD5STEP (F4, b, c, d, a, in[1] + 0x85845dd1, 21);
    HPDF_MD5STEP (F4, a, b, c, d, in[8] + 0x6fa87e4f, 6);
    HPDF_MD5STEP (F4, d, a, b, c, in[15] + 0xfe2ce6e0, 10);
    HPDF_MD5STEP (F4, c, d, a, b, in[6] + 0xa3014314, 15);
    HPDF_MD5STEP (F4, b, c, d, a, in[13] + 0x4e0811a1, 21);
    HPDF_MD5STEP (F4, a, b, c, d, in[4] + 0xf7537e82, 6);
    HPDF_MD5STEP (F4, d, a, b, c, in[11] + 0xbd3af235, 10);
    HPDF_MD5STEP (F4, c, d, a, b, in[2] + 0x2ad7d2bb, 15);
    HPDF_MD5STEP (F4, b, c, d, a, in[9] + 0xeb86d391, 21);

    buf[0] += a;
    buf[1] += b;
    buf[2] += c;
    buf[3] += d;
}


static void
MD5ByteReverse  (HPDF_BYTE    *buf,
                 HPDF_UINT32  longs)
{
    HPDF_UINT32 t;
    do
    {
        t = (HPDF_UINT32) ((HPDF_UINT32) buf[3] << 8 | buf[2]) << 16 |
        ((HPDF_UINT32) buf[1] << 8 | buf[0]);
        *(HPDF_UINT32 *) buf = t;
        buf += 4;
    }
    while (--longs);
}

/*----- encrypt-obj ---------------------------------------------------------*/

static void
ARC4Init  (HPDF_ARC4_Ctx_Rec  *ctx,
           const HPDF_BYTE    *key,
           HPDF_UINT           key_len);


static void
ARC4CryptBuf (HPDF_ARC4_Ctx_Rec   *ctx,
              const HPDF_BYTE     *in,
              HPDF_BYTE           *out,
              HPDF_UINT            len);


/*---------------------------------------------------------------------------*/

void
HPDF_PadOrTrancatePasswd  (const char  *pwd,
                           HPDF_BYTE        *new_pwd)
{
    HPDF_UINT len = HPDF_StrLen (pwd, HPDF_PASSWD_LEN + 1);

    HPDF_PTRACE((" HPDF_PadOrTrancatePasswd\n"));

    HPDF_MemSet (new_pwd, 0x00, HPDF_PASSWD_LEN);

    if (len >= HPDF_PASSWD_LEN) {
        HPDF_MemCpy (new_pwd, (HPDF_BYTE *)pwd, HPDF_PASSWD_LEN);
    } else {
        if (len > 0) {
            HPDF_MemCpy (new_pwd, (HPDF_BYTE *)pwd, len);
        }
        HPDF_MemCpy (new_pwd + len, HPDF_PADDING_STRING, HPDF_PASSWD_LEN - len);
    }
}


void
HPDF_Encrypt_Init  (HPDF_Encrypt  attr)
{
    HPDF_MemSet (attr, 0, sizeof(HPDF_Encrypt_Rec));
    attr->mode = HPDF_ENCRYPT_R2;
    attr->key_len = 5;
    HPDF_MemCpy (attr->owner_passwd, HPDF_PADDING_STRING, HPDF_PASSWD_LEN);
    HPDF_MemCpy (attr->user_passwd, HPDF_PADDING_STRING, HPDF_PASSWD_LEN);
    attr->permission = HPDF_ENABLE_PRINT | HPDF_ENABLE_EDIT_ALL |
        HPDF_ENABLE_COPY | HPDF_ENABLE_EDIT | HPDF_PERMISSION_PAD;
}


void
HPDF_Encrypt_CreateOwnerKey  (HPDF_Encrypt  attr)
{
    HPDF_ARC4_Ctx_Rec rc4_ctx;
    HPDF_MD5_CTX md5_ctx;
    HPDF_BYTE digest[HPDF_MD5_KEY_LEN];
    HPDF_BYTE tmppwd[HPDF_PASSWD_LEN];

    HPDF_PTRACE((" HPDF_Encrypt_CreateOwnerKey\n"));

    /* create md5-digest using the value of owner_passwd */

    /* Algorithm 3.3 step 2 */
    HPDF_MD5Init(&md5_ctx);
    HPDF_MD5Update(&md5_ctx, attr->owner_passwd, HPDF_PASSWD_LEN);

    HPDF_PTRACE(("@ Algorithm 3.3 step 2\n"));

    HPDF_MD5Final(digest, &md5_ctx);

    /* Algorithm 3.3 step 3 (Revision 3 only) */
    if (attr->mode == HPDF_ENCRYPT_R3) {
        HPDF_UINT i;

        for (i = 0; i < 50; i++) {
            HPDF_MD5Init(&md5_ctx);

            /* HPDF_MD5Update (&md5_ctx, digest, HPDF_MD5_KEY_LEN); */
            HPDF_MD5Update (&md5_ctx, digest, attr->key_len);
            HPDF_MD5Final(digest, &md5_ctx);

            HPDF_PTRACE(("@ Algorithm 3.3 step 3 loop %u\n", i));
        }
    }

    /* Algorithm 3.3 step 4 */
    HPDF_PTRACE(("@ Algorithm 3.3 step 7 loop 0\n"));

    ARC4Init (&rc4_ctx, digest, attr->key_len);

    HPDF_PTRACE(("@ Algorithm 3.3 step 5\n"));

    /* Algorithm 3.3 step 6 */
    HPDF_PTRACE(("@ Algorithm 3.3 step 6\n"));
    ARC4CryptBuf (&rc4_ctx, attr->user_passwd, tmppwd, HPDF_PASSWD_LEN);

    /* Algorithm 3.3 step 7 */
    HPDF_PTRACE(("@ Algorithm 3.3 step 7\n"));
    if (attr->mode == HPDF_ENCRYPT_R3) {
        HPDF_BYTE tmppwd2[HPDF_PASSWD_LEN];
        HPDF_UINT i;

        for (i = 1; i <= 19; i++) {
            HPDF_UINT j;
            HPDF_BYTE new_key[HPDF_MD5_KEY_LEN];

            for (j = 0; j < attr->key_len; j++)
                new_key[j] = (HPDF_BYTE)(digest[j] ^ i);

            HPDF_PTRACE(("@ Algorithm 3.3 step 7 loop %u\n", i));

            HPDF_MemCpy (tmppwd2, tmppwd, HPDF_PASSWD_LEN);
            ARC4Init(&rc4_ctx, new_key, attr->key_len);
            ARC4CryptBuf(&rc4_ctx, tmppwd2, tmppwd, HPDF_PASSWD_LEN);
        }
    }

    /* Algorithm 3.3 step 8 */
    HPDF_PTRACE(("@ Algorithm 3.3 step 8\n"));
    HPDF_MemCpy (attr->owner_key, tmppwd, HPDF_PASSWD_LEN);
}


void
HPDF_Encrypt_CreateEncryptionKey  (HPDF_Encrypt  attr)
{
    HPDF_MD5_CTX md5_ctx;
    HPDF_BYTE tmp_flg[4];

    HPDF_PTRACE((" HPDF_Encrypt_CreateEncryptionKey\n"));

    /* Algorithm3.2 step2 */
    HPDF_MD5Init(&md5_ctx);
    HPDF_MD5Update(&md5_ctx, attr->user_passwd, HPDF_PASSWD_LEN);

    /* Algorithm3.2 step3 */
    HPDF_MD5Update(&md5_ctx, attr->owner_key, HPDF_PASSWD_LEN);


    /* Algorithm3.2 step4 */
    HPDF_PTRACE(("@@@ permission =%d\n", attr->permission));
    tmp_flg[0] = (HPDF_BYTE)(attr->permission);
    tmp_flg[1] = (HPDF_BYTE)(attr->permission >> 8);
    tmp_flg[2] = (HPDF_BYTE)(attr->permission >> 16);
    tmp_flg[3] = (HPDF_BYTE)(attr->permission >> 24);

    HPDF_MD5Update(&md5_ctx, tmp_flg, 4);

    /* Algorithm3.2 step5 */
    HPDF_PTRACE(("@ Algorithm 3.2 step 5\n"));

    HPDF_MD5Update(&md5_ctx, attr->encrypt_id, HPDF_ID_LEN);
    HPDF_MD5Final(attr->encryption_key, &md5_ctx);

    /* Algorithm 3.2 step6 (Revision 3 only) */
    if (attr->mode == HPDF_ENCRYPT_R3) {
        HPDF_UINT i;

        for (i = 0; i < 50; i++) {
            HPDF_PTRACE(("@ Algorithm 3.3 step 6 loop %u\n", i));
            HPDF_MD5Init(&md5_ctx);
            HPDF_MD5Update (&md5_ctx, attr->encryption_key, attr->key_len);
            HPDF_MD5Final(attr->encryption_key, &md5_ctx);
        }
    }
}


void
HPDF_Encrypt_CreateUserKey  (HPDF_Encrypt  attr)
{
    HPDF_ARC4_Ctx_Rec ctx;

    HPDF_PTRACE((" HPDF_Encrypt_CreateUserKey\n"));

    /* Algorithm 3.4/5 step1 */

    /* Algorithm 3.4 step2 */
    ARC4Init(&ctx, attr->encryption_key, attr->key_len);
    ARC4CryptBuf(&ctx, HPDF_PADDING_STRING, attr->user_key, HPDF_PASSWD_LEN);

    if (attr->mode == HPDF_ENCRYPT_R3) {
        HPDF_MD5_CTX md5_ctx;
        HPDF_BYTE digest[HPDF_MD5_KEY_LEN];
        HPDF_BYTE digest2[HPDF_MD5_KEY_LEN];
        HPDF_UINT i;

        /* Algorithm 3.5 step2 (same as Algorithm3.2 step2) */
        HPDF_MD5Init(&md5_ctx);
        HPDF_MD5Update(&md5_ctx, HPDF_PADDING_STRING, HPDF_PASSWD_LEN);

        /* Algorithm 3.5 step3 */
        HPDF_MD5Update(&md5_ctx, attr->encrypt_id, HPDF_ID_LEN);
        HPDF_MD5Final(digest, &md5_ctx);

        HPDF_PTRACE(("@ Algorithm 3.5 step 3\n"));

        /* Algorithm 3.5 step4 */
        ARC4Init(&ctx, attr->encryption_key, attr->key_len);
        ARC4CryptBuf(&ctx, digest, digest2, HPDF_MD5_KEY_LEN);

        HPDF_PTRACE(("@ Algorithm 3.5 step 4\n"));

        /* Algorithm 3.5 step5 */
        for (i = 1; i <= 19; i++) {
            HPDF_UINT j;
            HPDF_BYTE new_key[HPDF_MD5_KEY_LEN];

            HPDF_PTRACE(("@ Algorithm 3.5 step 5 loop %u\n", i));

            for (j = 0; j < attr->key_len; j++)
                new_key[j] = (HPDF_BYTE)(attr->encryption_key[j] ^ i);

            HPDF_MemCpy (digest, digest2, HPDF_MD5_KEY_LEN);

            ARC4Init(&ctx, new_key, attr->key_len);
            ARC4CryptBuf(&ctx, digest, digest2, HPDF_MD5_KEY_LEN);
        }

        /* use the result of Algorithm 3.4 as 'arbitrary padding' */
        HPDF_MemSet (attr->user_key, 0, HPDF_PASSWD_LEN);
        HPDF_MemCpy (attr->user_key, digest2, HPDF_MD5_KEY_LEN);
    }
}


void
ARC4Init  (HPDF_ARC4_Ctx_Rec  *ctx,
                        const HPDF_BYTE    *key,
                        HPDF_UINT          key_len)
{
    HPDF_BYTE tmp_array[HPDF_ARC4_BUF_SIZE];
    HPDF_UINT i;
    HPDF_UINT j = 0;

    HPDF_PTRACE((" ARC4Init\n"));

    for (i = 0; i < HPDF_ARC4_BUF_SIZE; i++)
        ctx->state[i] = (HPDF_BYTE)i;

    for (i = 0; i < HPDF_ARC4_BUF_SIZE; i++)
        tmp_array[i] = key[i % key_len];

    for (i = 0; i < HPDF_ARC4_BUF_SIZE; i++) {
        HPDF_BYTE tmp;

        j = (j + ctx->state[i] + tmp_array[i]) % HPDF_ARC4_BUF_SIZE;

        tmp = ctx->state[i];
        ctx->state[i] = ctx->state[j];
        ctx->state[j] = tmp;
    }

    ctx->idx1 = 0;
    ctx->idx2 = 0;
}


void
ARC4CryptBuf (HPDF_ARC4_Ctx_Rec  *ctx,
                           const HPDF_BYTE    *in,
                           HPDF_BYTE          *out,
                           HPDF_UINT          len)
{
    HPDF_UINT i;
    HPDF_UINT t;
    HPDF_BYTE K;

    HPDF_PTRACE((" ARC4CryptBuf\n"));

    for (i = 0; i < len; i++) {
        HPDF_BYTE tmp;

        ctx->idx1 = (HPDF_BYTE)((ctx->idx1 + 1) % 256);
        ctx->idx2 = (HPDF_BYTE)((ctx->idx2 +  ctx->state[ctx->idx1]) % 256);

        tmp = ctx->state[ctx->idx1];
        ctx->state[ctx->idx1] = ctx->state[ctx->idx2];
        ctx->state[ctx->idx2] = tmp;

        t = (ctx->state[ctx->idx1] + ctx->state[ctx->idx2]) % 256;
        K = ctx->state[t];

        out[i] = (HPDF_BYTE)(in[i] ^ K);
    }
}


void
HPDF_Encrypt_InitKey  (HPDF_Encrypt  attr,
                       HPDF_UINT32       object_id,
                       HPDF_UINT16       gen_no)
{
    HPDF_MD5_CTX ctx;
    HPDF_UINT key_len;

    HPDF_PTRACE((" HPDF_Encrypt_Init\n"));

    attr->encryption_key[attr->key_len] = (HPDF_BYTE)object_id;
    attr->encryption_key[attr->key_len + 1] = (HPDF_BYTE)(object_id >> 8);
    attr->encryption_key[attr->key_len + 2] = (HPDF_BYTE)(object_id >> 16);
    attr->encryption_key[attr->key_len + 3] = (HPDF_BYTE)gen_no;
    attr->encryption_key[attr->key_len + 4] = (HPDF_BYTE)(gen_no >> 8);

    HPDF_PTRACE(("@@@ OID=%u, gen_no=%u\n", (HPDF_INT)object_id, gen_no));

    HPDF_MD5Init(&ctx);
    HPDF_MD5Update(&ctx, attr->encryption_key, attr->key_len + 5);
    HPDF_MD5Final(attr->md5_encryption_key, &ctx);

    key_len = (attr->key_len + 5 > HPDF_ENCRYPT_KEY_MAX) ?
                    HPDF_ENCRYPT_KEY_MAX : attr->key_len + 5;

    ARC4Init(&attr->arc4ctx, attr->md5_encryption_key, key_len);
}


void
HPDF_Encrypt_Reset  (HPDF_Encrypt  attr)
{
    HPDF_UINT key_len = (attr->key_len + 5 > HPDF_ENCRYPT_KEY_MAX) ?
                    HPDF_ENCRYPT_KEY_MAX : attr->key_len + 5;

    HPDF_PTRACE((" HPDF_Encrypt_Reset\n"));

    ARC4Init(&attr->arc4ctx, attr->md5_encryption_key, key_len);
}


void
HPDF_Encrypt_CryptBuf  (HPDF_Encrypt  attr,
                        const HPDF_BYTE   *src,
                        HPDF_BYTE         *dst,
                        HPDF_UINT         len)
{
    ARC4CryptBuf(&attr->arc4ctx, src, dst, len);
}


/*--------------------------------------------------------------------------*/
/*--------------------------------------------------------------------------*/

