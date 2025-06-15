#include "config.h"

#if USE_SSL

#include "OpenSSLHelpers.h"

#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/Crypto/KeyPair.h>

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <openssl/kdf.h>
#include <openssl/core_names.h>
#include <openssl/obj_mac.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>

#include <base/scope_guard.h>


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

    EVP_MAC_ptr mac(EVP_MAC_fetch(nullptr, "HMAC", nullptr), &EVP_MAC_free);
    if (!mac)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_MAC_fetch failed: {}", getOpenSSLErrors());

    EVP_MAC_CTX_ptr ctx(EVP_MAC_CTX_new(mac.get()), &EVP_MAC_CTX_free);
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

std::string generateCSR(const std::vector<std::string> domain_names, EVP_PKEY * key)
{
    if (domain_names.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No domain names provided");

    if (!key)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No private key provided");

    /// Create X509 object and set subject name
    X509_REQ_ptr req(X509_REQ_new(), X509_REQ_free);
    if (!req)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Error creating X509_REQ: {}", getOpenSSLErrors());

    /// Owned by X509_REQ, does not need to be freed.
    X509_NAME * subject_name = X509_REQ_get_subject_name(req.get());
    if (!subject_name)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Error getting subject name from X509_REQ: {}", getOpenSSLErrors());

    if (!X509_NAME_add_entry_by_NID(
        /*name=*/ subject_name,
        /*nid=*/ NID_commonName,
        /*type=*/ MBSTRING_ASC,
        /*bytes=*/ reinterpret_cast<const unsigned char *>(domain_names.front().c_str()),
        /*len=*/ -1,
        /*loc=*/ -1,
        /*set=*/ 0
    ))
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Error adding CN to X509_NAME: {}", getOpenSSLErrors());

    /// Add Subject Alternative Names
    if (domain_names.size() > 1)
    {
        std::string san_entries;
        for (size_t i = 1; i < domain_names.size(); ++i)
        {
            if (!domain_names[i].empty())
                san_entries += "DNS:" + domain_names[i] + ",";
        }
        if (!san_entries.empty() && san_entries.back() == ',')
            san_entries.pop_back();

        /// Add Subject Alternative Name extension.
        /// Everything in this stack and the stack itself will be freed by smart pointer.
        X509_EXTENSIONS_ptr extensions(sk_X509_EXTENSION_new_null());
        if (!extensions)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Unable to create extensions stack for Subject Alternative Name {}", getOpenSSLErrors());

        auto * san_extension = X509V3_EXT_conf_nid(nullptr, nullptr, NID_subject_alt_name, san_entries.c_str());
        if (!san_extension)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Unable to create Subject Alternative Name extension {}", getOpenSSLErrors());

        if (!OPENSSL_sk_push(
            reinterpret_cast<OPENSSL_STACK *>(extensions.get()),
            reinterpret_cast<void *>(san_extension)
        ))
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Unable to add Subject Alternative Name to extensions {}", getOpenSSLErrors());

        // And tie it to the X509
        if (!X509_REQ_add_extensions(req.get(), extensions.get()))
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Unable to add Subject Alternative Names to CSR {}", getOpenSSLErrors());
    }

    if (!X509_REQ_set_pubkey(req.get(), key))
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Failure in X509_REQ_set_pubkey: {}", getOpenSSLErrors());

    if (!X509_REQ_sign(req.get(), key, EVP_sha256()))
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Failure in X509_REQ_sign: {}", getOpenSSLErrors());

    /// And our CSR is ready
    BIO_ptr req_bio(BIO_new(BIO_s_mem()), BIO_free);
    if (i2d_X509_REQ_bio(req_bio.get(), req.get()) < 0)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Failure in i2d_X509_REQ_bio: {}", getOpenSSLErrors());

    /// Convert CSR to string
    int csr_len = BIO_ctrl_pending(req_bio.get());
    if (csr_len <= 0)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Failure in BIO_ctrl_pending: {}", getOpenSSLErrors());

    std::string csr(csr_len, '\0');
    if (BIO_read(req_bio.get(), csr.data(), csr_len) != csr_len)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Failure in BIO_read: {}", getOpenSSLErrors());

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
