#pragma once

#include <Common/Exception.h>

#include <boost/container/flat_set.hpp>

#include "config.h"

#if USE_SSL
#    include <openssl/bio.h>
#    include <openssl/crypto.h>
#    include <openssl/err.h>
#    include <openssl/pem.h>
#    include <openssl/x509.h>
#    include <openssl/x509v3.h>
#    include <Common/OpenSSLHelpers.h>
#endif

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int OPENSSL_ERROR;
    extern const int BAD_ARGUMENTS;
}


#if USE_SSL
class X509Certificate
{
public:
    using List = std::vector<X509Certificate>;
    constexpr static size_t NAME_BUFFER_SIZE = 256;

    explicit X509Certificate(X509 * cert_)
        : certificate(cert_)
    {
    }
    explicit operator X509 *() const { return certificate; }

    X509Certificate & operator=(X509Certificate && other) noexcept
    {
        if (this == &other)
            return *this;

        if (certificate)
            X509_free(certificate);

        certificate = other.certificate;
        other.certificate = nullptr;

        return *this;
    }

    X509Certificate(const X509Certificate &) = delete;
    X509Certificate & operator=(const X509Certificate &) = delete;

    X509Certificate(X509Certificate && other) noexcept
        : certificate(other.certificate)
    {
        other.certificate = nullptr;
    }

    explicit X509Certificate(const std::string & path)
    {
        using BIO_ptr = std::unique_ptr<BIO, decltype(&BIO_free)>;
        BIO_ptr bio(BIO_new(BIO_s_mem()), BIO_free);

        if (!bio)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "BIO_new failed: {}", getOpenSSLErrors());

        BIO * file = BIO_new_file(path.c_str(), "r");

        if (!file)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "BIO_new_file failed: {}", getOpenSSLErrors());

        certificate = PEM_read_bio_X509(file, nullptr, nullptr, nullptr);
        if (!certificate)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "PEM_read_bio_X509 failed for file {}: {}", path, getOpenSSLErrors());
    }

    static X509Certificate::List fromFile(const std::string & path)
    {
        using BIO_ptr = std::unique_ptr<BIO, decltype(&BIO_free)>;

        BIO_ptr bio(BIO_new_file(path.c_str(), "r"), BIO_free);
        if (!bio)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "BIO_new_file failed: {}", getOpenSSLErrors());

        X509Certificate::List certs;

        while (true)
        {
            X509 * cert = PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr);

            if (!cert)
            {
                auto err = ERR_peek_last_error();
                if (err == 0)
                    break;

                /// We read at least one cert, and can't find
                /// the beginning of a next one.
                /// This most likely means we reached the end of the file.
                if (!certs.empty()
                    /// Manually unwrap ERR_GET_REASON(err) due to ossl_unused
                    /// https://github.com/openssl/openssl/issues/16776
                    ///
                    /// To be fixed in OpenSSL 3.4+
                    && ((err & ERR_SYSTEM_FLAG) == 0 && (err & ERR_REASON_MASK) == PEM_R_NO_START_LINE))
                    /// Means we reached the end of the file.
                    break;

                throw Exception(ErrorCodes::OPENSSL_ERROR, "PEM_read_bio_X509 failed: c:{}, f:{}, e:{}", certs.size(), path, getOpenSSLErrors());
            }

            certs.emplace_back(cert);
        }

        if (certs.empty())
            throw Exception(ErrorCodes::OPENSSL_ERROR, "No certificates found in file: {}", path);

        return certs;
    }

    ~X509Certificate()
    {
        if (certificate)
            X509_free(certificate);
    }

    uint64_t version() const
    {
        // This is defined by standards (X.509 et al) to be
        // one less than the certificate version.
        // So, eg. a version 3 certificate will return 2.
        return X509_get_version(certificate) + 1;
    }

    std::string serialNumber() const
    {
        ASN1_INTEGER * serial = X509_get_serialNumber(certificate);
        BIGNUM * bn = ASN1_INTEGER_to_BN(serial, nullptr);
        char * hex = BN_bn2hex(bn);
        std::string result(hex);
        OPENSSL_free(hex);
        BN_free(bn);
        return result;
    }

    std::string signatureAlgorithm() const
    {
        const X509_ALGOR * sig_alg = X509_get0_tbs_sigalg(certificate);
        char buffer[NAME_BUFFER_SIZE];
        OBJ_obj2txt(buffer, sizeof(buffer), sig_alg->algorithm, 0);
        return buffer;
    }

    std::string issuerName() const
    {
        char buffer[NAME_BUFFER_SIZE];
        X509_NAME_oneline(X509_get_issuer_name(certificate), buffer, sizeof(buffer));
        return buffer;
    }

    std::string subjectName() const
    {
        char buffer[NAME_BUFFER_SIZE];
        X509_NAME_oneline(X509_get_subject_name(certificate), buffer, sizeof(buffer));
        return buffer;
    }

    std::string issuerName(uint nid) const
    {
        if (X509_NAME * issuer = X509_get_issuer_name(certificate))
        {
            char buffer[NAME_BUFFER_SIZE];
            if (X509_NAME_get_text_by_NID(issuer, nid, buffer, sizeof(buffer)) >= 0)
                return std::string(buffer);
        }
        return std::string();
    }

    std::string subjectName(uint nid) const
    {
        if (X509_NAME * subj = X509_get_subject_name(certificate))
        {
            char buffer[NAME_BUFFER_SIZE];
            if (X509_NAME_get_text_by_NID(subj, nid, buffer, sizeof(buffer)) >= 0)
                return std::string(buffer);
        }
        return std::string();
    }

    std::string commonName() const { return subjectName(NID_commonName); }

    std::string publicKeyAlgorithm() const
    {
        EVP_PKEY * pkey = X509_get_pubkey(certificate);
        if (!pkey)
            return {};

        int nid = EVP_PKEY_id(pkey);
        EVP_PKEY_free(pkey);

        if (nid == NID_undef)
            return {};

        char buf[128] = {0};
        OBJ_obj2txt(buf, sizeof(buf), OBJ_nid2obj(nid), 0);
        return std::string(buf);
    }

    std::string validFrom() const
    {
        ASN1_TIME * valid_from = X509_get_notBefore(certificate);
        return reinterpret_cast<char *>(valid_from->data);
    }

    std::string expiresOn() const
    {
        ASN1_TIME * not_before = X509_get_notBefore(certificate);
        return reinterpret_cast<char *>(not_before->data);
    }

    class Subjects
    {
    public:
        using container = boost::container::flat_set<String>;
        enum class Type
        {
            CN,
            SAN
        };

    private:
        std::array<container, size_t(Type::SAN) + 1> subjects;

    public:
        inline const container & at(Type type_) const { return subjects[static_cast<size_t>(type_)]; }
        inline bool empty()
        {
            for (auto & subject_list : subjects)
            {
                if (!subject_list.empty())
                    return false;
            }
            return true;
        }
        void insert(const String & subject_type_, String && subject) { insert(parseSubjectType(subject_type_), std::move(subject)); }
        void insert(Type type_, String && subject) { subjects[static_cast<size_t>(type_)].insert(std::move(subject)); }

        static X509Certificate::Subjects::Type parseSubjectType(const String & type_)
        {
            if (type_ == "CN")
                return X509Certificate::Subjects::Type::CN;
            if (type_ == "SAN")
                return X509Certificate::Subjects::Type::SAN;

            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown X509 Certificate Subject Type: {}", type_);
        }

        String toString(X509Certificate::Subjects::Type type_)
        {
            switch (type_)
            {
                case X509Certificate::Subjects::Type::CN:
                    return "CN";
                case X509Certificate::Subjects::Type::SAN:
                    return "SAN";
            }
        }

        bool operator==(const X509Certificate::Subjects & rhs) const
        {
            for (X509Certificate::Subjects::Type type : {X509Certificate::Subjects::Type::CN, X509Certificate::Subjects::Type::SAN})
            {
                if (this->at(type) != rhs.at(type))
                    return false;
            }

            return true;
        }
    };

    Subjects extractAllSubjects()
    {
        Subjects subjects;

        if (!commonName().empty())
            subjects.insert(Subjects::Type::CN, commonName());

        auto general_names_deleter = [](STACK_OF(GENERAL_NAME) * names) { GENERAL_NAMES_free(names); };

        using GeneralNamesPtr = std::unique_ptr<STACK_OF(GENERAL_NAME), decltype(general_names_deleter)>;
        GeneralNamesPtr cert_names(
            static_cast<STACK_OF(GENERAL_NAME) *>(X509_get_ext_d2i(certificate, NID_subject_alt_name, nullptr, nullptr)),
            general_names_deleter);

        if (!cert_names)
            return subjects;

        const auto * names = reinterpret_cast<const STACK_OF(GENERAL_NAME) *>(cert_names.get());
        uint8_t count = OPENSSL_sk_num(reinterpret_cast<const _STACK *>(names));
        for (uint8_t i = 0; i < count; ++i)
        {
            const GENERAL_NAME * name = static_cast<const GENERAL_NAME *>(OPENSSL_sk_value(reinterpret_cast<const _STACK *>(names), i));

            if (name->type == GEN_DNS || name->type == GEN_URI)
            {
                const ASN1_IA5STRING * ia5 = name->d.ia5;
                const char * data = reinterpret_cast<const char *>(ASN1_STRING_get0_data(ia5));
                std::size_t len = ASN1_STRING_length(ia5);
                if (data && len > 0)
                {
                    std::string prefix = (name->type == GEN_DNS) ? "DNS:" : "URI:";
                    subjects.insert(Subjects::Type::SAN, prefix + std::string(data, len));
                }
            }
        }

        return subjects;
    }

private:
    X509 * certificate;
};
#endif

}
