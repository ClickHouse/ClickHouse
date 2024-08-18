#include <Access/Common/SSLCertificateSubjects.h>
#include <Common/Exception.h>

#if USE_SSL
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
    {
        subjects.insert(SSLCertificateSubjects::Type::CN, certificate.commonName());
    }

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wused-but-marked-unused"
    auto stackof_general_name_deleter = [](void * ptr) { GENERAL_NAMES_free(static_cast<STACK_OF(GENERAL_NAME) *>(ptr)); };
    std::unique_ptr<void, decltype(stackof_general_name_deleter)> cert_names(
        X509_get_ext_d2i(const_cast<X509 *>(certificate.certificate()), NID_subject_alt_name, nullptr, nullptr),
        stackof_general_name_deleter);

    if (STACK_OF(GENERAL_NAME) * names = static_cast<STACK_OF(GENERAL_NAME) *>(cert_names.get()))
    {
        for (int i = 0; i < sk_GENERAL_NAME_num(names); ++i)
        {
            const GENERAL_NAME * name = sk_GENERAL_NAME_value(names, i);
            if (name->type == GEN_DNS || name->type == GEN_URI)
            {
                const char * data = reinterpret_cast<const char *>(ASN1_STRING_get0_data(name->d.ia5));
                std::size_t len = ASN1_STRING_length(name->d.ia5);
                std::string subject = (name->type == GEN_DNS ? "DNS:" : "URI:") + std::string(data, len);
                subjects.insert(SSLCertificateSubjects::Type::SAN, std::move(subject));
            }
        }
    }

#pragma clang diagnostic pop
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

