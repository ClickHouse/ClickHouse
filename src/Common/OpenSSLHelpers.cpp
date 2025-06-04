#include "config.h"

#if USE_SSL
#include "OpenSSLHelpers.h"
#include <base/scope_guard.h>

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <openssl/kdf.h>
#include <openssl/core_names.h>

#include <openssl/pem.h>
#include <openssl/x509v3.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/Crypto/KeyPair.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int OPENSSL_ERROR;
    extern const int BAD_ARGUMENTS;
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

std::string calculateHMACwithSHA256(std::string to_sign, EVP_PKEY * pkey)
{
    size_t signature_length = 0;

    EVP_MD_CTX * context(EVP_MD_CTX_create());
    const EVP_MD * digest = EVP_get_digestbyname("SHA256");
    if (!digest || EVP_DigestInit_ex(context, digest, nullptr) != 1
        || EVP_DigestSignInit(context, nullptr, digest, nullptr, pkey) != 1
        || EVP_DigestSignUpdate(context, to_sign.c_str(), to_sign.size()) != 1
        || EVP_DigestSignFinal(context, nullptr, &signature_length) != 1)
    {
        EVP_MD_CTX_destroy(context);
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Error forming SHA256 digest: {}", getOpenSSLErrors());
    }

    std::vector<unsigned char> signature_bytes(signature_length);
    if (EVP_DigestSignFinal(context, &signature_bytes.front(), &signature_length) != 1)
    {
        EVP_MD_CTX_destroy(context);
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Error finalizing SHA256 digest: {}", getOpenSSLErrors());
    }

    EVP_MD_CTX_destroy(context);

    std::string signature = { signature_bytes.begin(), signature_bytes.end() };
    return base64Encode(signature, /*url_encoding*/ true, /*no_padding*/ true);
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

std::string generateCSR(std::vector<std::string> domain_names, std::string pkey)
{
    if (domain_names.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No domain names provided");

    auto name = domain_names.front();

    /// Convert private key to EVP_PKEY
    EVP_PKEY * key = EVP_PKEY_new();
    BIO * pkey_bio(BIO_new_mem_buf(pkey.c_str(), -1));
    if (PEM_read_bio_PrivateKey(pkey_bio, &key, nullptr, nullptr) == nullptr)
    {
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Error reading private key: {}", getOpenSSLErrors());
    }

    /// Create X509 object and set subject name
    X509_REQ * req(X509_REQ_new());
    X509_NAME * cn = X509_REQ_get_subject_name(req);
    if (!X509_NAME_add_entry_by_txt(cn, "CN", MBSTRING_ASC, reinterpret_cast<const unsigned char *>(name.c_str()), -1, -1, 0))
    {
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Error adding CN to X509_NAME: {}", getOpenSSLErrors());
    }

    /// Add Subject Alternative Names
    if (domain_names.size() > 1)
    {
        X509_EXTENSIONS * extensions(sk_X509_EXTENSION_new_null());

        std::string other_domain_names;
        for (auto it = domain_names.begin() + 1; it != domain_names.end(); ++it)
        {
            if (it->empty())
                continue;
            other_domain_names += fmt::format("DNS:{}, ", *it);
        }

        /// Add Subject Alternative Name extension
        auto * nid = X509V3_EXT_conf_nid(nullptr, nullptr, NID_subject_alt_name, other_domain_names.c_str());

        int ret = OPENSSL_sk_push(reinterpret_cast<OPENSSL_STACK *>(extensions), static_cast<const void *>(nid));
        if (!ret)
        {
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Unable to add Subject Alternative Name to extensions {}", getOpenSSLErrors());
        }

        // And tie it to the X509
        if (X509_REQ_add_extensions(req, extensions) != 1)
        {
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Unable to add Subject Alternative Names to CSR {}", getOpenSSLErrors());
        }
    }

    if (!X509_REQ_set_pubkey(req, key))
    {
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Failure in X509_REQ_set_pubkey");
    }

    if (!X509_REQ_sign(req, key, EVP_sha256()))
    {
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Failure in X509_REQ_sign");
    }

    /// And our CSR is ready
    BIO * req_bio(BIO_new(BIO_s_mem()));
    if (i2d_X509_REQ_bio(req_bio, req) < 0)
    {
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Failure in i2d_X509_REQ_bio");
    }

    /// Convert CSR to string
    std::string csr;
    char buffer[1024];
    int bytes_read;
    while ((bytes_read = BIO_read(req_bio, buffer, sizeof(buffer))) > 0)
    {
        csr.append(buffer, bytes_read);
    }
    csr = base64Encode(csr, /*url_encoding*/ true, /*no_padding*/ true);

    return csr;
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
