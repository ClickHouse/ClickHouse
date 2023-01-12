/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/* lib/crypto/openssl/enc_provider/aes.c */
/*
 * Copyright (C) 2003, 2007, 2008, 2009 by the Massachusetts Institute of Technology.
 * All rights reserved.
 *
 * Export of this software from the United States of America may
 *   require a specific license from the United States Government.
 *   It is the responsibility of any person or organization contemplating
 *   export to obtain such a license before exporting.
 *
 * WITHIN THAT CONSTRAINT, permission to use, copy, modify, and
 * distribute this software and its documentation for any purpose and
 * without fee is hereby granted, provided that the above copyright
 * notice appear in all copies and that both that copyright notice and
 * this permission notice appear in supporting documentation, and that
 * the name of M.I.T. not be used in advertising or publicity pertaining
 * to distribution of the software without specific, written prior
 * permission.  Furthermore if you modify this software you must label
 * your software as modified software and not distribute it in such a
 * fashion that it might be confused with the original M.I.T. software.
 * M.I.T. makes no representations about the suitability of
 * this software for any purpose.  It is provided "as is" without express
 * or implied warranty.
 */

#include "crypto_int.h"
#include <openssl/evp.h>
#include <openssl/aes.h>

/* proto's */
static krb5_error_code
cbc_enc(krb5_key key, const krb5_data *ivec, krb5_crypto_iov *data,
        size_t num_data);
static krb5_error_code
cbc_decr(krb5_key key, const krb5_data *ivec, krb5_crypto_iov *data,
         size_t num_data);
static krb5_error_code
cts_encr(krb5_key key, const krb5_data *ivec, krb5_crypto_iov *data,
         size_t num_data, size_t dlen);
static krb5_error_code
cts_decr(krb5_key key, const krb5_data *ivec, krb5_crypto_iov *data,
         size_t num_data, size_t dlen);

#define BLOCK_SIZE 16
#define NUM_BITS 8
#define IV_CTS_BUF_SIZE 16 /* 16 - hardcoded in CRYPTO_cts128_en/decrypt */

static const EVP_CIPHER *
map_mode(unsigned int len)
{
    if (len==16)
        return EVP_aes_128_cbc();
    if (len==32)
        return EVP_aes_256_cbc();
    else
        return NULL;
}

/* Encrypt one block using CBC. */
static krb5_error_code
cbc_enc(krb5_key key, const krb5_data *ivec, krb5_crypto_iov *data,
        size_t num_data)
{
    int             ret, olen = BLOCK_SIZE;
    unsigned char   iblock[BLOCK_SIZE], oblock[BLOCK_SIZE];
    EVP_CIPHER_CTX  *ctx;
    struct iov_cursor cursor;

    ctx = EVP_CIPHER_CTX_new();
    if (ctx == NULL)
        return ENOMEM;

    ret = EVP_EncryptInit_ex(ctx, map_mode(key->keyblock.length),
                             NULL, key->keyblock.contents, (ivec) ? (unsigned char*)ivec->data : NULL);
    if (ret == 0) {
        EVP_CIPHER_CTX_free(ctx);
        return KRB5_CRYPTO_INTERNAL;
    }

    k5_iov_cursor_init(&cursor, data, num_data, BLOCK_SIZE, FALSE);
    k5_iov_cursor_get(&cursor, iblock);
    EVP_CIPHER_CTX_set_padding(ctx,0);
    ret = EVP_EncryptUpdate(ctx, oblock, &olen, iblock, BLOCK_SIZE);
    if (ret == 1)
        k5_iov_cursor_put(&cursor, oblock);
    EVP_CIPHER_CTX_free(ctx);

    zap(iblock, BLOCK_SIZE);
    zap(oblock, BLOCK_SIZE);
    return (ret == 1) ? 0 : KRB5_CRYPTO_INTERNAL;
}

/* Decrypt one block using CBC. */
static krb5_error_code
cbc_decr(krb5_key key, const krb5_data *ivec, krb5_crypto_iov *data,
         size_t num_data)
{
    int              ret = 0, olen = BLOCK_SIZE;
    unsigned char    iblock[BLOCK_SIZE], oblock[BLOCK_SIZE];
    EVP_CIPHER_CTX   *ctx;
    struct iov_cursor cursor;

    ctx = EVP_CIPHER_CTX_new();
    if (ctx == NULL)
        return ENOMEM;

    ret = EVP_DecryptInit_ex(ctx, map_mode(key->keyblock.length),
                             NULL, key->keyblock.contents, (ivec) ? (unsigned char*)ivec->data : NULL);
    if (ret == 0) {
        EVP_CIPHER_CTX_free(ctx);
        return KRB5_CRYPTO_INTERNAL;
    }

    k5_iov_cursor_init(&cursor, data, num_data, BLOCK_SIZE, FALSE);
    k5_iov_cursor_get(&cursor, iblock);
    EVP_CIPHER_CTX_set_padding(ctx,0);
    ret = EVP_DecryptUpdate(ctx, oblock, &olen, iblock, BLOCK_SIZE);
    if (ret == 1)
        k5_iov_cursor_put(&cursor, oblock);
    EVP_CIPHER_CTX_free(ctx);

    zap(iblock, BLOCK_SIZE);
    zap(oblock, BLOCK_SIZE);
    return (ret == 1) ? 0 : KRB5_CRYPTO_INTERNAL;
}

static krb5_error_code
cts_encr(krb5_key key, const krb5_data *ivec, krb5_crypto_iov *data,
         size_t num_data, size_t dlen)
{
    int                    ret = 0;
    size_t                 size = 0;
    unsigned char         *oblock = NULL, *dbuf = NULL;
    unsigned char          iv_cts[IV_CTS_BUF_SIZE];
    struct iov_cursor      cursor;
    AES_KEY                enck;

    memset(iv_cts,0,sizeof(iv_cts));
    if (ivec && ivec->data){
        if (ivec->length != sizeof(iv_cts))
            return KRB5_CRYPTO_INTERNAL;
        memcpy(iv_cts, ivec->data,ivec->length);
    }

    oblock = OPENSSL_malloc(dlen);
    if (!oblock){
        return ENOMEM;
    }
    dbuf = OPENSSL_malloc(dlen);
    if (!dbuf){
        OPENSSL_free(oblock);
        return ENOMEM;
    }

    k5_iov_cursor_init(&cursor, data, num_data, dlen, FALSE);
    k5_iov_cursor_get(&cursor, dbuf);

    AES_set_encrypt_key(key->keyblock.contents,
                        NUM_BITS * key->keyblock.length, &enck);

    size = CRYPTO_cts128_encrypt((unsigned char *)dbuf, oblock, dlen, &enck,
                                 iv_cts, AES_cbc_encrypt);
    if (size <= 0)
        ret = KRB5_CRYPTO_INTERNAL;
    else
        k5_iov_cursor_put(&cursor, oblock);

    if (!ret && ivec && ivec->data)
        memcpy(ivec->data, iv_cts, sizeof(iv_cts));

    zap(oblock, dlen);
    zap(dbuf, dlen);
    OPENSSL_free(oblock);
    OPENSSL_free(dbuf);

    return ret;
}

static krb5_error_code
cts_decr(krb5_key key, const krb5_data *ivec, krb5_crypto_iov *data,
         size_t num_data, size_t dlen)
{
    int                    ret = 0;
    size_t                 size = 0;
    unsigned char         *oblock = NULL;
    unsigned char         *dbuf = NULL;
    unsigned char          iv_cts[IV_CTS_BUF_SIZE];
    struct iov_cursor      cursor;
    AES_KEY                deck;

    memset(iv_cts,0,sizeof(iv_cts));
    if (ivec && ivec->data){
        if (ivec->length != sizeof(iv_cts))
            return KRB5_CRYPTO_INTERNAL;
        memcpy(iv_cts, ivec->data,ivec->length);
    }

    oblock = OPENSSL_malloc(dlen);
    if (!oblock)
        return ENOMEM;
    dbuf = OPENSSL_malloc(dlen);
    if (!dbuf){
        OPENSSL_free(oblock);
        return ENOMEM;
    }

    AES_set_decrypt_key(key->keyblock.contents,
                        NUM_BITS * key->keyblock.length, &deck);

    k5_iov_cursor_init(&cursor, data, num_data, dlen, FALSE);
    k5_iov_cursor_get(&cursor, dbuf);

    size = CRYPTO_cts128_decrypt((unsigned char *)dbuf, oblock,
                                 dlen, &deck,
                                 iv_cts, AES_cbc_encrypt);
    if (size <= 0)
        ret = KRB5_CRYPTO_INTERNAL;
    else
        k5_iov_cursor_put(&cursor, oblock);

    if (!ret && ivec && ivec->data)
        memcpy(ivec->data, iv_cts, sizeof(iv_cts));

    zap(oblock, dlen);
    zap(dbuf, dlen);
    OPENSSL_free(oblock);
    OPENSSL_free(dbuf);

    return ret;
}

krb5_error_code
krb5int_aes_encrypt(krb5_key key, const krb5_data *ivec,
                    krb5_crypto_iov *data, size_t num_data)
{
    int    ret = 0;
    size_t input_length, nblocks;

    input_length = iov_total_length(data, num_data, FALSE);
    nblocks = (input_length + BLOCK_SIZE - 1) / BLOCK_SIZE;
    if (nblocks == 1) {
        if (input_length != BLOCK_SIZE)
            return KRB5_BAD_MSIZE;
        ret = cbc_enc(key, ivec, data, num_data);
    } else if (nblocks > 1) {
        ret = cts_encr(key, ivec, data, num_data, input_length);
    }

    return ret;
}

krb5_error_code
krb5int_aes_decrypt(krb5_key key, const krb5_data *ivec,
                    krb5_crypto_iov *data, size_t num_data)
{
    int    ret = 0;
    size_t input_length, nblocks;

    input_length = iov_total_length(data, num_data, FALSE);
    nblocks = (input_length + BLOCK_SIZE - 1) / BLOCK_SIZE;
    if (nblocks == 1) {
        if (input_length != BLOCK_SIZE)
            return KRB5_BAD_MSIZE;
        ret = cbc_decr(key, ivec, data, num_data);
    } else if (nblocks > 1) {
        ret = cts_decr(key, ivec, data, num_data, input_length);
    }

    return ret;
}

static krb5_error_code
krb5int_aes_init_state (const krb5_keyblock *key, krb5_keyusage usage,
                        krb5_data *state)
{
    state->length = 16;
    state->data = (void *) malloc(16);
    if (state->data == NULL)
        return ENOMEM;
    memset(state->data, 0, state->length);
    return 0;
}
const struct krb5_enc_provider krb5int_enc_aes128 = {
    16,
    16, 16,
    krb5int_aes_encrypt,
    krb5int_aes_decrypt,
    NULL,
    krb5int_aes_init_state,
    krb5int_default_free_state
};

const struct krb5_enc_provider krb5int_enc_aes256 = {
    16,
    32, 32,
    krb5int_aes_encrypt,
    krb5int_aes_decrypt,
    NULL,
    krb5int_aes_init_state,
    krb5int_default_free_state
};
