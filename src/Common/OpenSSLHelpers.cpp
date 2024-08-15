#include "config.h"

#if USE_SSL
#include "OpenSSLHelpers.h"
#include <base/scope_guard.h>
#include <openssl/err.h>
#include <openssl/sha.h>

namespace DB
{

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

/// perfect place for this method
// std::string hmacSha256(std::string to_sign, Poco::Crypto::RSAKey & key)
// {
//     EVP_PKEY * pkey = EVP_PKEY_new();
//     auto ret = EVP_PKEY_assign_RSA(pkey, key.impl()->getRSA());
//     if (ret != 1)
//     {
//         throw std::runtime_error("Error assigning RSA key to EVP_PKEY");
//     }
//
//     size_t signature_length = 0;
//
//     EVP_MD_CTX * context(EVP_MD_CTX_create());
//     const EVP_MD * sha256 = EVP_get_digestbyname("SHA256");
//     if (!sha256 || EVP_DigestInit_ex(context, sha256, nullptr) != 1 || EVP_DigestSignInit(context, nullptr, sha256, nullptr, pkey) != 1
//         || EVP_DigestSignUpdate(context, to_sign.c_str(), to_sign.size()) != 1
//         || EVP_DigestSignFinal(context, nullptr, &signature_length) != 1)
//     {
//         throw std::runtime_error("Error creating SHA256 digest");
//     }
//
//     std::vector<unsigned char> signature(signature_length);
//     if (EVP_DigestSignFinal(context, &signature.front(), &signature_length) != 1)
//     {
//         throw std::runtime_error("Error creating SHA256 digest in final signature");
//     }
//
//     std::string signature_str = std::string(signature.begin(), signature.end());
//
//     return base64Encode(signature_str, /*url_encoding*/ true, /*no_padding*/ true);
// }

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
