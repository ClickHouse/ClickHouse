#include "config.h"

#if USE_SSL
#include <Common/OpenSSLHelpers.h>
#include <base/scope_guard.h>

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <openssl/kdf.h>
#include <openssl/core_names.h>


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
    out.resize(SHA256_DIGEST_LENGTH);
    encodeSHA256(text, size, reinterpret_cast<unsigned char *>(out.data()));
    return out;
}

std::vector<uint8_t> encodeSHA256(const std::vector<uint8_t> & data)
{
    std::vector<uint8_t> hash(SHA256_DIGEST_LENGTH);
    encodeSHA256(data.data(), data.size(), hash.data());
    return hash;
}

void encodeSHA256(std::string_view text, unsigned char * out)
{
    encodeSHA256(text.data(), text.size(), out);
}

void encodeSHA256(const void * text, size_t size, unsigned char * out)
{
    auto ctx = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>(EVP_MD_CTX_new(), EVP_MD_CTX_free);

    if (!EVP_DigestInit(ctx.get(), EVP_sha256()))
        throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_DigestInit failed: {}", getOpenSSLErrors());

    if (!EVP_DigestUpdate(ctx.get(), text, size))
        throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_DigestUpdate failed: {}", getOpenSSLErrors());

    if (!EVP_DigestFinal(ctx.get(), out, nullptr))
        throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_DigestFinal failed: {}", getOpenSSLErrors());
}

std::vector<uint8_t> hmacSHA256(const std::vector<uint8_t> & key, const std::string & data)
{
    std::vector<uint8_t> result(EVP_MAX_MD_SIZE);
    size_t out_len = 0;

    using MacPtr = std::unique_ptr<EVP_MAC, decltype(&EVP_MAC_free)>;
    MacPtr mac(EVP_MAC_fetch(nullptr, "HMAC", nullptr), &EVP_MAC_free);
    if (!mac)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_MAC_fetch failed: {}", getOpenSSLErrors());

    using CtxPtr = std::unique_ptr<EVP_MAC_CTX, decltype(&EVP_MAC_CTX_free)>;
    CtxPtr ctx(EVP_MAC_CTX_new(mac.get()), &EVP_MAC_CTX_free);
    if (!ctx)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_MAC_CTX_new failed: {}", getOpenSSLErrors());

    OSSL_PARAM params[] = {
        OSSL_PARAM_utf8_string(OSSL_MAC_PARAM_DIGEST, const_cast<char*>("SHA256"), 0),
        OSSL_PARAM_END
    };

    if (!EVP_MAC_init(ctx.get(), key.data(), key.size(), params))
        throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_MAC_init failed: {}", getOpenSSLErrors());

    if (!EVP_MAC_update(ctx.get(), reinterpret_cast<const unsigned char*>(data.data()), data.size()))
        throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_MAC_update failed: {}", getOpenSSLErrors());

    if (!EVP_MAC_final(ctx.get(), result.data(), &out_len, result.size()))
        throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_MAC_final failed: {}", getOpenSSLErrors());

    result.resize(out_len);
    return result;
}

std::vector<uint8_t> pbkdf2SHA256(std::string_view password, const std::vector<uint8_t>& salt, int iterations)
{
    using EVP_KDF_ptr = std::unique_ptr<EVP_KDF, decltype(&EVP_KDF_free)>;
    using EVP_KDF_CTX_ptr = std::unique_ptr<EVP_KDF_CTX, decltype(&EVP_KDF_CTX_free)>;

    EVP_KDF_ptr kdf(EVP_KDF_fetch(nullptr, "PBKDF2", nullptr), &EVP_KDF_free);
    if (!kdf)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_KDF_fetch failed");

    EVP_KDF_CTX_ptr ctx(EVP_KDF_CTX_new(kdf.get()), &EVP_KDF_CTX_free);
    if (!ctx)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_KDF_CTX_new failed");

    std::vector<uint8_t> derived_key(SHA256_DIGEST_LENGTH);

    OSSL_PARAM params[] = {
        OSSL_PARAM_construct_utf8_string("digest", const_cast<char*>("SHA256"), 0),
        OSSL_PARAM_construct_octet_string("salt", const_cast<uint8_t*>(salt.data()), salt.size()),
        OSSL_PARAM_construct_int("iter", &iterations),
        OSSL_PARAM_construct_octet_string("pass", const_cast<char*>(password.data()), password.size()),
        OSSL_PARAM_construct_end()
    };

    if (EVP_KDF_derive(ctx.get(), derived_key.data(), derived_key.size(), params) != 1)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_KDF_derive failed");

    return derived_key;
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
