#include <Access/Credentials.h>
#include <Common/Exception.h>
#include <Poco/Net/HTTPRequest.h>
#include <Common/Crypto/X509Certificate.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

Credentials::Credentials(const String & user_name_)
    : user_name(user_name_)
{
}

const String & Credentials::getUserName() const
{
    if (!isReady())
        throwNotReady();
    return user_name;
}

bool Credentials::isReady() const
{
    return is_ready;
}

void Credentials::throwNotReady()
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Credentials are not ready");
}

AlwaysAllowCredentials::AlwaysAllowCredentials()
{
    is_ready = true;
}

AlwaysAllowCredentials::AlwaysAllowCredentials(const String & user_name_)
    : Credentials(user_name_)
{
    is_ready = true;
}

void AlwaysAllowCredentials::setUserName(const String & user_name_)
{
    user_name = user_name_;
}

#if USE_SSL
SSLCertificateCredentials::SSLCertificateCredentials(const String & user_name_, X509Certificate::Subjects && subjects_)
    : Credentials(user_name_)
    , certificate_subjects(subjects_)
{
    is_ready = true;
}

const X509Certificate::Subjects & SSLCertificateCredentials::getSSLCertificateSubjects() const
{
    if (!isReady())
        throwNotReady();
    return certificate_subjects;
}
#endif

BasicCredentials::BasicCredentials()
{
    is_ready = true;
}

BasicCredentials::BasicCredentials(const String & user_name_)
    : Credentials(user_name_)
{
    is_ready = true;
}

BasicCredentials::BasicCredentials(const String & user_name_, const String & password_)
    : Credentials(user_name_)
    , password(password_)
{
    is_ready = true;
}

void BasicCredentials::setUserName(const String & user_name_)
{
    user_name = user_name_;
}

void BasicCredentials::setPassword(const String & password_)
{
    password = password_;
}

const String & BasicCredentials::getPassword() const
{
    if (!isReady())
        throwNotReady();
    return password;
}

}
