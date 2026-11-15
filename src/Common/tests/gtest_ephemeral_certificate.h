#pragma once

#include <Poco/Net/Context.h>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <stdexcept>
#include <string>

#include <unistd.h>

/// Generate a self-signed certificate and private key in memory,
/// write them to temporary files for Poco::Net::Context.
struct EphemeralCert
{
    std::string cert_path;
    std::string key_path;

    EphemeralCert()
    {
        EVP_PKEY * pkey = EVP_RSA_gen(2048);
        if (!pkey)
            throw std::runtime_error("EVP_RSA_gen failed");

        X509 * x509 = X509_new();
        if (!x509)
        {
            EVP_PKEY_free(pkey);
            throw std::runtime_error("X509_new failed");
        }

        ASN1_INTEGER_set(X509_get_serialNumber(x509), 1);
        X509_gmtime_adj(X509_getm_notBefore(x509), 0);
        X509_gmtime_adj(X509_getm_notAfter(x509), 3600);
        X509_set_pubkey(x509, pkey);

        X509_NAME * name = X509_get_subject_name(x509);
        X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC, reinterpret_cast<const unsigned char *>("localhost"), -1, -1, 0);
        X509_set_issuer_name(x509, name);
        X509_sign(x509, pkey, EVP_sha256());

        cert_path = writeToTempFile(
            [&](BIO * bio) { PEM_write_bio_X509(bio, x509); }, "cert");
        key_path = writeToTempFile(
            [&](BIO * bio) { PEM_write_bio_PrivateKey(bio, pkey, nullptr, nullptr, 0, nullptr, nullptr); }, "key");

        X509_free(x509);
        EVP_PKEY_free(pkey);
    }

    ~EphemeralCert()
    {
        (void)unlink(cert_path.c_str());
        (void)unlink(key_path.c_str());
    }

    Poco::Net::Context::Ptr makeContext(Poco::Net::Context::Usage usage) const
    {
        Poco::Net::Context::Params params;
        params.privateKeyFile = key_path;
        params.certificateFile = cert_path;
        params.verificationMode = Poco::Net::Context::VERIFY_NONE;
        return new Poco::Net::Context(usage, params);
    }

private:
    template <typename Fn>
    static std::string writeToTempFile(Fn writer, const char * suffix)
    {
        char path[256];
        (void)snprintf(path, sizeof(path), "/tmp/gtest_ssl_%s_XXXXXX", suffix);
        int fd = mkstemp(path);
        if (fd < 0)
            throw std::runtime_error("mkstemp failed");

        BIO * bio = BIO_new_fd(fd, BIO_CLOSE);
        writer(bio);
        BIO_free(bio);
        return path;
    }
};
