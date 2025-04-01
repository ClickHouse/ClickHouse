#include <Access/Common/SSLCertificateSubjects.h>
#include <Common/Exception.h>


#if USE_SSL
#include <Poco/Net/X509Certificate.h>
#include <openssl/x509v3.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

#if USE_SSL
SSLCertificateSubjects extractSSLCertificateSubjects(const Poco::Net::X509Certificate & certificate)
{
    SSLCertificateSubjects subjects;
    if (!certificate.commonName().empty())
        subjects.insert(SSLCertificateSubjects::Type::CN, certificate.commonName());

    auto general_names_deleter = [](STACK_OF(GENERAL_NAME)* names)
    {
        GENERAL_NAMES_free(names);
    };

    using GeneralNamesPtr = std::unique_ptr<STACK_OF(GENERAL_NAME), decltype(general_names_deleter)>;
    GeneralNamesPtr cert_names(
        static_cast<STACK_OF(GENERAL_NAME)*>(
            X509_get_ext_d2i(const_cast<X509*>(certificate.certificate()), NID_subject_alt_name, nullptr, nullptr)
        ),
        general_names_deleter
    );

    if (!cert_names)
        return subjects;

    const auto * names = reinterpret_cast<const STACK_OF(GENERAL_NAME)*>(cert_names.get());
    uint8_t count = OPENSSL_sk_num(reinterpret_cast<const _STACK*>(names));
    for (uint8_t i = 0; i < count; ++i)
    {
        const GENERAL_NAME* name = static_cast<const GENERAL_NAME*>(OPENSSL_sk_value(reinterpret_cast<const _STACK*>(names), i));

        if (name->type == GEN_DNS || name->type == GEN_URI)
        {
            const ASN1_IA5STRING* ia5 = name->d.ia5;
            const char* data = reinterpret_cast<const char*>(ASN1_STRING_get0_data(ia5));
            std::size_t len = ASN1_STRING_length(ia5);
            if (data && len > 0)
            {
                std::string prefix = (name->type == GEN_DNS) ? "DNS:" : "URI:";
                subjects.insert(SSLCertificateSubjects::Type::SAN, prefix + std::string(data, len));
            }
        }
    }

    return subjects;
}
#endif


void SSLCertificateSubjects::insert(const String & subject_type_, String && subject)
{
    insert(parseSSLCertificateSubjectType(subject_type_), std::move(subject));
}

void SSLCertificateSubjects::insert(Type subject_type_, String && subject)
{
    subjects[static_cast<size_t>(subject_type_)].insert(std::move(subject));
}

SSLCertificateSubjects::Type parseSSLCertificateSubjectType(const String & type_)
{
    if (type_ == "CN")
        return SSLCertificateSubjects::Type::CN;
    if (type_ == "SAN")
        return SSLCertificateSubjects::Type::SAN;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown SSL Certificate Subject Type: {}", type_);
}

String toString(SSLCertificateSubjects::Type type_)
{
    switch (type_)
    {
        case SSLCertificateSubjects::Type::CN:
            return "CN";
        case SSLCertificateSubjects::Type::SAN:
            return "SAN";
    }
}

bool operator==(const SSLCertificateSubjects & lhs, const SSLCertificateSubjects & rhs)
{
    for (SSLCertificateSubjects::Type type : {SSLCertificateSubjects::Type::CN, SSLCertificateSubjects::Type::SAN})
    {
        if (lhs.at(type) != rhs.at(type))
            return false;
    }
    return true;
}

}

