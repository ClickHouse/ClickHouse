/*
 * << Haru Free PDF Library >> -- hpdf_encrypt.h
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

#ifndef HPDF_ENCRYPT_H
#define HPDF_ENCRYPT_H

#include "hpdf_mmgr.h"

#ifdef __cplusplus
extern "C" {
#endif

/*----------------------------------------------------------------------------*/
/*----- encrypt-dict ---------------------------------------------------------*/

#define HPDF_ID_LEN              16
#define HPDF_PASSWD_LEN          32
#define HPDF_ENCRYPT_KEY_MAX     16
#define HPDF_MD5_KEY_LEN         16
#define HPDF_PERMISSION_PAD      0xFFFFFFC0
#define HPDF_ARC4_BUF_SIZE       256


typedef struct HPDF_MD5Context
{
    HPDF_UINT32 buf[4];
    HPDF_UINT32 bits[2];
    HPDF_BYTE in[64];
} HPDF_MD5_CTX;


typedef struct _HPDF_ARC4_Ctx_Rec {
    HPDF_BYTE    idx1;
    HPDF_BYTE    idx2;
    HPDF_BYTE    state[HPDF_ARC4_BUF_SIZE];
} HPDF_ARC4_Ctx_Rec;


typedef struct _HPDF_Encrypt_Rec  *HPDF_Encrypt;

typedef struct _HPDF_Encrypt_Rec {
    HPDF_EncryptMode   mode;

    /* key_len must be a multiple of 8, and between 40 to 128 */
    HPDF_UINT          key_len;

    /* owner-password (not encrypted) */
    HPDF_BYTE          owner_passwd[HPDF_PASSWD_LEN];

    /* user-password (not encrypted) */
    HPDF_BYTE          user_passwd[HPDF_PASSWD_LEN];

    /* owner-password (encrypted) */
    HPDF_BYTE          owner_key[HPDF_PASSWD_LEN];

    /* user-password (encrypted) */
    HPDF_BYTE          user_key[HPDF_PASSWD_LEN];

    HPDF_INT           permission;
    HPDF_BYTE          encrypt_id[HPDF_ID_LEN];
    HPDF_BYTE          encryption_key[HPDF_MD5_KEY_LEN + 5];
    HPDF_BYTE          md5_encryption_key[HPDF_MD5_KEY_LEN];
    HPDF_ARC4_Ctx_Rec  arc4ctx;
} HPDF_Encrypt_Rec;


void
HPDF_MD5Init  (struct HPDF_MD5Context  *ctx);


void
HPDF_MD5Update  (struct HPDF_MD5Context *ctx,
                 const HPDF_BYTE        *buf,
                 HPDF_UINT32            len);


void
HPDF_MD5Final  (HPDF_BYTE              digest[16],
                struct HPDF_MD5Context *ctx);

void
HPDF_PadOrTrancatePasswd  (const char  *pwd,
                           HPDF_BYTE        *new_pwd);


void
HPDF_Encrypt_Init  (HPDF_Encrypt  attr);


void
HPDF_Encrypt_CreateUserKey  (HPDF_Encrypt  attr);


void
HPDF_Encrypt_CreateOwnerKey  (HPDF_Encrypt  attr);


void
HPDF_Encrypt_CreateEncryptionKey  (HPDF_Encrypt  attr);


void
HPDF_Encrypt_InitKey  (HPDF_Encrypt  attr,
                       HPDF_UINT32       object_id,
                       HPDF_UINT16       gen_no);


void
HPDF_Encrypt_Reset  (HPDF_Encrypt  attr);


void
HPDF_Encrypt_CryptBuf  (HPDF_Encrypt  attr,
                        const HPDF_BYTE   *src,
                        HPDF_BYTE         *dst,
                        HPDF_UINT         len);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _HPDF_ENCRYPT_H */


