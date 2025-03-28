#include "config.h"

#if USE_SSL
#include "OpenSSLHelpers.h"
#include <base/scope_guard.h>
#include <openssl/err.h>
#include <openssl/evp.h>

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
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    SCOPE_EXIT(EVP_MD_CTX_free(ctx));  // Or use RAII

    if (!EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) ||
        !EVP_DigestUpdate(ctx, text, size) ||
        !EVP_DigestFinal_ex(ctx, out, nullptr))
    {
        throw Exception(ErrorCodes::OPENSSL_ERROR, "SHA256 EVP hashing failed: {}", getOpenSSLErrors());
    }
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
