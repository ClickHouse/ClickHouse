#include "config.h"

#if USE_SSL
#include "OpenSSLHelpers.h"
#include <base/scope_guard.h>
#include <openssl/err.h>
#include <openssl/sha.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Poco/Crypto/RSAKey.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int OPENSSL_ERROR;
}

std::string encodeSHA256(std::string_view text)
{
    return encodeSHA256(text.data(), text.size());
}
std::string encodeSHA256(const void * text, size_t size)
{
    std::string out;
    out.resize(32);
    encodeSHA256(text, size, reinterpret_cast<unsigned char *>(out.data()));
    return out;
}
void encodeSHA256(std::string_view text, unsigned char * out)
{
    encodeSHA256(text.data(), text.size(), out);
}
void encodeSHA256(const void * text, size_t size, unsigned char * out)
{
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, reinterpret_cast<const UInt8 *>(text), size);
    SHA256_Final(out, &ctx);
}

std::string calculateHMACwithSHA256(std::string to_sign, const Poco::Crypto::RSAKey & key)
{
    EVP_PKEY * pkey = EVP_PKEY_new();

    if (EVP_PKEY_assign_RSA(pkey, key.impl()->getRSA()) != 1)
    {
        EVP_PKEY_free(pkey);
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Error converting RSA key to an EVP_PKEY structure: {}", getOpenSSLErrors());
    }

    size_t signature_length = 0;

    EVP_MD_CTX * context(EVP_MD_CTX_create());
    const EVP_MD * digest = EVP_get_digestbyname("SHA256");
    if (!digest || EVP_DigestInit_ex(context, digest, nullptr) != 1
        || EVP_DigestSignInit(context, nullptr, digest, nullptr, pkey) != 1
        || EVP_DigestSignUpdate(context, to_sign.c_str(), to_sign.size()) != 1
        || EVP_DigestSignFinal(context, nullptr, &signature_length) != 1)
    {
        EVP_MD_CTX_destroy(context);
        EVP_PKEY_free(pkey);
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Error forming SHA256 digest: {}", getOpenSSLErrors());
    }

    std::vector<unsigned char> signature_bytes(signature_length);
    if (EVP_DigestSignFinal(context, &signature_bytes.front(), &signature_length) != 1)
    {
        EVP_MD_CTX_destroy(context);
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Error finalizing SHA256 digest: {}", getOpenSSLErrors());
    }

    EVP_MD_CTX_destroy(context);
    // EVP_PKEY_free(pkey);  /// FIXME this segfaults due to cleaning RSAKey memory.

    std::string signature = { signature_bytes.begin(), signature_bytes.end() };
    return base64Encode(signature, /*url_encoding*/ true, /*no_padding*/ true);
}

String getOpenSSLErrors()
{
    String res;
    ERR_print_errors_cb([](const char * str, size_t len, void * ctx)
    {
        String & out = *reinterpret_cast<String*>(ctx);
        if (!out.empty())
            out += ", ";
        out.append(str, len);
        return 1;
    }, &res);
    return res;
}

}
#endif
