#pragma once

#include "config.h"

#include <Common/Exception.h>

#if USE_SSL
#include <base/types.h>

#include <openssl/evp.h>
#include <openssl/kdf.h>
#include <openssl/stack.h>
#include <openssl/x509.h>


namespace DB
{

struct X509ExtensionStackDeleter
{
    void operator()(X509_EXTENSIONS * ptr) const
    {
        OPENSSL_sk_pop_free(
            reinterpret_cast<OPENSSL_STACK *>(ptr),
            &X509ExtensionStackDeleter::freeX509Extension
        );
    }

    static void freeX509Extension(void * ptr)
    {
        X509_EXTENSION_free(static_cast<X509_EXTENSION *>(ptr));
    }
};

using BIO_ptr = std::unique_ptr<BIO, decltype(&BIO_free)>;
using EVP_KDF_CTX_ptr = std::unique_ptr<EVP_KDF_CTX, decltype(&EVP_KDF_CTX_free)>;
using EVP_KDF_ptr = std::unique_ptr<EVP_KDF, decltype(&EVP_KDF_free)>;
using EVP_MAC_CTX_ptr = std::unique_ptr<EVP_MAC_CTX, decltype(&EVP_MAC_CTX_free)>;
using EVP_MAC_ptr = std::unique_ptr<EVP_MAC, decltype(&EVP_MAC_free)>;
using X509_EXTENSIONS_ptr = std::unique_ptr<X509_EXTENSIONS, X509ExtensionStackDeleter>;
using X509_REQ_ptr = std::unique_ptr<X509_REQ, decltype(&X509_REQ_free)>;


/// Encodes `text` and returns it.
std::string encodeSHA256(std::string_view text);
std::string encodeSHA256(const void * text, size_t size);
std::vector<uint8_t> encodeSHA256(const std::vector<uint8_t> & data);

/// `out` must be at least 32 bytes long.
void encodeSHA256(std::string_view text, unsigned char * out);
void encodeSHA256(const void * text, size_t size, unsigned char * out);

std::vector<uint8_t> hmacSHA256(const std::vector<uint8_t> & key, const std::string & data);
std::vector<uint8_t> pbkdf2SHA256(std::string_view password, const std::vector<uint8_t>& salt, int iterations);

/// FIXME
std::string calculateHMACwithSHA256(std::string to_sign, EVP_PKEY * pkey);

/// Generate Certificate Signing Request with given `subject(s)` and private key.
std::string generateCSR(std::vector<std::string>, EVP_PKEY * pkey);

/// Returns concatenation of error strings for all errors that OpenSSL has recorded, emptying the error queue.
String getOpenSSLErrors();

}
#endif
