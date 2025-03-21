#include "config.h"

#if USE_SSL
#include "OpenSSLHelpers.h"
#include <base/scope_guard.h>
#include <openssl/err.h>
#include <openssl/rsa.h>
#include <openssl/sha.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Poco/Crypto/RSAKey.h>

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

    auto * rsa = key.impl()->getRSA();
    /// No need to free pkey, it is owned by key.impl()
    if (EVP_PKEY_assign_RSA(pkey, rsa) != 1)
    {
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
