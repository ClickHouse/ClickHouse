#pragma once

#include "config.h"

#include <Common/Exception.h>

#if USE_SSL
#    include <openssl/evp.h>
#    include <openssl/pem.h>
#    include <openssl/rsa.h>
#    include <Common/OpenSSLHelpers.h>
#endif

#include <memory>


namespace DB
{

#if USE_SSL

class KeyPair
{
    using BIO_ptr = std::unique_ptr<BIO, decltype(&BIO_free)>;

public:
    explicit KeyPair(EVP_PKEY * key_);
    explicit operator EVP_PKEY *() const;

    KeyPair(const KeyPair &) = delete;
    KeyPair & operator=(const KeyPair &) = delete;

    KeyPair(KeyPair && other) noexcept;
    KeyPair & operator=(KeyPair && other) noexcept;

    ~KeyPair();

    static KeyPair fromFile(const std::string & path, const std::string & password = "");
    static KeyPair generateRSA(uint32_t bits = 2048, uint32_t exponent = RSA_F4);

    std::string publicKey() const;

private:
    EVP_PKEY * key;
};

#endif
}
