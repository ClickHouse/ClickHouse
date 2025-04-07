#pragma once

#include "config.h"

#include <Common/Exception.h>

#if USE_SSL
#include <Common/OpenSSLHelpers.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#endif

#include <memory>


namespace DB
{

namespace ErrorCodes
{
    extern const int OPENSSL_ERROR;
}

#if USE_SSL

class KeyPair
{
public:

    explicit KeyPair(EVP_PKEY *key_) : key(key_) {}
    explicit operator EVP_PKEY *() const { return key; }

    KeyPair(const KeyPair&) = delete;
    KeyPair& operator=(const KeyPair&) = delete;

    KeyPair(KeyPair&& other) noexcept : key(other.key)
    {
        other.key = nullptr;
    }

    KeyPair& operator=(KeyPair&& other) noexcept
    {
        if (this == &other)
            return *this;

        if (key)
            EVP_PKEY_free(key);

        key = other.key;
        other.key = nullptr;

        return *this;
    }

    static KeyPair fromFile(const std::string & path, const std::string & password = "")
    {
        EVP_PKEY* key = nullptr;

        /// Try to load a private key.
        {
            BIO *file = BIO_new_file(path.c_str(), "r");

            if (!file)
                throw Exception(ErrorCodes::OPENSSL_ERROR, "BIO_new_file failed: {}", getOpenSSLErrors());

            key = PEM_read_bio_PrivateKey(
                file,
                nullptr,
                nullptr,
                password.empty() ? nullptr : const_cast<char *>(password.c_str())
            );

            BIO_free(file);
        }

        /// Maybe it is a public key.
        if (!key)
        {
            BIO *file = BIO_new_file(path.c_str(), "r");

            if (!file)
                throw Exception(ErrorCodes::OPENSSL_ERROR, "BIO_new_file failed: {}", getOpenSSLErrors());

            key = PEM_read_bio_PUBKEY(file, nullptr, nullptr, nullptr);

            BIO_free(file);
        }

        if (!key)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Failed to load key from a file: {}", getOpenSSLErrors());

        return KeyPair(key);
    }

    static KeyPair generateRSA(uint32_t bits = 2048, uint32_t exponent = RSA_F4)
    {
        EVP_PKEY *key = nullptr;

        using EVP_PKEY_CTX_ptr = std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)>;
        EVP_PKEY_CTX_ptr ctx(EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr), EVP_PKEY_CTX_free);

        if (!ctx)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_PKEY_CTX_new_id failed: {}", getOpenSSLErrors());

        if (EVP_PKEY_keygen_init(ctx.get()) <= 0)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_PKEY_keygen_init failed: {}", getOpenSSLErrors());

        BIGNUM* bn = BN_new();
        if (!bn || !BN_set_word(bn, exponent))
        {
            BN_free(bn);
            throw Exception(ErrorCodes::OPENSSL_ERROR, "BN_set_word failed: {}", getOpenSSLErrors());
        }

        size_t exp_len = BN_num_bytes(bn);
        std::vector<unsigned char> exp_buf(exp_len);
        BN_bn2bin(bn, exp_buf.data());
        BN_free(bn);

        OSSL_PARAM params[] = {
            OSSL_PARAM_int("bits", &bits),
            OSSL_PARAM_BN("pubexp", exp_buf.data(), exp_buf.size()),
            OSSL_PARAM_END
        };

        if (EVP_PKEY_CTX_set_params(ctx.get(), params) <= 0)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_PKEY_CTX_set_params failed: {}", getOpenSSLErrors());

        if (EVP_PKEY_keygen(ctx.get(), &key) <= 0)
        {
            if (key)
                EVP_PKEY_free(key);
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_PKEY_keygen failed: {}", getOpenSSLErrors());
        }

        return KeyPair(key);
    }

    ~KeyPair()
    {
        if (key)
            EVP_PKEY_free(key);
    }

    std::string publicKey() const
    {
        using BIO_ptr = std::unique_ptr<BIO, decltype(&BIO_free)>;
        BIO_ptr bio(BIO_new(BIO_s_mem()), BIO_free);

        if (!bio)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "BIO_new failed: {}", getOpenSSLErrors());

        if (!PEM_write_bio_PUBKEY(bio.get(), key))
        {
            throw Exception(ErrorCodes::OPENSSL_ERROR, "PEM_write_bio_PUBKEY failed: {}", getOpenSSLErrors());
        }

        char * data;
        uint64_t len = BIO_get_mem_data(bio.get(), &data);
        std::string result(data, len);

        return result;
    }

private:
    EVP_PKEY *key;
};

#endif
}
