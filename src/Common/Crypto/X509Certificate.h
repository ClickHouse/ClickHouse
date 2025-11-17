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


#if USE_SSL
class X509Certificate
{
    using BIO_ptr = std::unique_ptr<BIO, decltype(&BIO_free)>;

public:
    using List = std::vector<X509Certificate>;
    constexpr static size_t NAME_BUFFER_SIZE = 256;

    explicit X509Certificate(X509 * cert_);
    explicit operator X509 *() const;

    X509Certificate & operator=(X509Certificate && other) noexcept;
    X509Certificate(X509Certificate && other) noexcept;

    X509Certificate(const X509Certificate &) = delete;
    X509Certificate & operator=(const X509Certificate &) = delete;

    ~X509Certificate();

    explicit X509Certificate(const std::string & path);

    static X509Certificate::List fromFile(const std::string & path);
    static X509Certificate::List fromBuffer(const std::string & buffer);

    uint64_t version() const;
    std::string serialNumber() const;
    std::string signatureAlgorithm() const;
    std::string issuerName() const;
    std::string subjectName() const;
    std::string issuerName(uint nid) const;
    std::string subjectName(uint nid) const;
    std::string commonName() const;
    std::string publicKeyAlgorithm() const;
    std::string validFrom() const;
    std::string expiresOn() const;

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
        const container & at(Type type_) const;
        bool empty();

        void insert(const String & subject_type_, String && subject);
        void insert(Type type_, String && subject);

        static X509Certificate::Subjects::Type parseSubjectType(const String & type_);
        String toString(X509Certificate::Subjects::Type type_);

        bool operator==(const X509Certificate::Subjects & rhs) const;
    };

    Subjects extractAllSubjects();

private:
    X509 * certificate;
};
#endif

}
