#include <Access/Common/AuthenticationData.h>
#include <Common/Exception.h>
#include <Common/OpenSSLHelpers.h>
#include <Poco/SHA1Engine.h>
#include <base/types.h>
#include <boost/algorithm/hex.hpp>
#include <boost/algorithm/string/case_conv.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}


const AuthenticationTypeInfo & AuthenticationTypeInfo::get(AuthenticationType type_)
{
    static constexpr auto make_info = [](const char * raw_name_)
    {
        String init_name = raw_name_;
        boost::to_lower(init_name);
        return AuthenticationTypeInfo{raw_name_, std::move(init_name)};
    };

    switch (type_)
    {
        case AuthenticationType::NO_PASSWORD:
        {
            static const auto info = make_info("NO_PASSWORD");
            return info;
        }
        case AuthenticationType::PLAINTEXT_PASSWORD:
        {
            static const auto info = make_info("PLAINTEXT_PASSWORD");
            return info;
        }
        case AuthenticationType::SHA256_PASSWORD:
        {
            static const auto info = make_info("SHA256_PASSWORD");
            return info;
        }
        case AuthenticationType::DOUBLE_SHA1_PASSWORD:
        {
            static const auto info = make_info("DOUBLE_SHA1_PASSWORD");
            return info;
        }
        case AuthenticationType::LDAP:
        {
            static const auto info = make_info("LDAP");
            return info;
        }
        case AuthenticationType::KERBEROS:
        {
            static const auto info = make_info("KERBEROS");
            return info;
        }
        case AuthenticationType::SSL_CERTIFICATE:
        {
            static const auto info = make_info("SSL_CERTIFICATE");
            return info;
        }
        case AuthenticationType::MAX:
            break;
    }
    throw Exception("Unknown authentication type: " + std::to_string(static_cast<int>(type_)), ErrorCodes::LOGICAL_ERROR);
}


AuthenticationData::Digest AuthenticationData::Util::encodeSHA256(std::string_view text [[maybe_unused]])
{
#if USE_SSL
    Digest hash;
    hash.resize(32);
    ::DB::encodeSHA256(text, hash.data());
    return hash;
#else
    throw DB::Exception(
        "SHA256 passwords support is disabled, because ClickHouse was built without SSL library",
        DB::ErrorCodes::SUPPORT_IS_DISABLED);
#endif
}


AuthenticationData::Digest AuthenticationData::Util::encodeSHA1(std::string_view text)
{
    Poco::SHA1Engine engine;
    engine.update(text.data(), text.size());
    return engine.digest();
}


bool operator ==(const AuthenticationData & lhs, const AuthenticationData & rhs)
{
    return (lhs.type == rhs.type) && (lhs.password_hash == rhs.password_hash)
        && (lhs.ldap_server_name == rhs.ldap_server_name) && (lhs.kerberos_realm == rhs.kerberos_realm)
        && (lhs.ssl_certificate_common_names == rhs.ssl_certificate_common_names);
}


void AuthenticationData::setPassword(const String & password_)
{
    switch (type)
    {
        case AuthenticationType::PLAINTEXT_PASSWORD:
            return setPasswordHashBinary(Util::stringToDigest(password_));

        case AuthenticationType::SHA256_PASSWORD:
            return setPasswordHashBinary(Util::encodeSHA256(password_));

        case AuthenticationType::DOUBLE_SHA1_PASSWORD:
            return setPasswordHashBinary(Util::encodeDoubleSHA1(password_));

        case AuthenticationType::NO_PASSWORD:
        case AuthenticationType::LDAP:
        case AuthenticationType::KERBEROS:
        case AuthenticationType::SSL_CERTIFICATE:
            throw Exception("Cannot specify password for authentication type " + toString(type), ErrorCodes::LOGICAL_ERROR);

        case AuthenticationType::MAX:
            break;
    }
    throw Exception("setPassword(): authentication type " + toString(type) + " not supported", ErrorCodes::NOT_IMPLEMENTED);
}


String AuthenticationData::getPassword() const
{
    if (type != AuthenticationType::PLAINTEXT_PASSWORD)
        throw Exception("Cannot decode the password", ErrorCodes::LOGICAL_ERROR);
    return String(password_hash.data(), password_hash.data() + password_hash.size());
}


void AuthenticationData::setPasswordHashHex(const String & hash)
{
    Digest digest;
    digest.resize(hash.size() / 2);

    try
    {
        boost::algorithm::unhex(hash.begin(), hash.end(), digest.data());
    }
    catch (const std::exception &)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot read password hash in hex, check for valid characters [0-9a-fA-F] and length");
    }

    setPasswordHashBinary(digest);
}


String AuthenticationData::getPasswordHashHex() const
{
    if (type == AuthenticationType::LDAP || type == AuthenticationType::KERBEROS || type == AuthenticationType::SSL_CERTIFICATE)
        throw Exception("Cannot get password hex hash for authentication type " + toString(type), ErrorCodes::LOGICAL_ERROR);

    String hex;
    hex.resize(password_hash.size() * 2);
    boost::algorithm::hex(password_hash.begin(), password_hash.end(), hex.data());
    return hex;
}


void AuthenticationData::setPasswordHashBinary(const Digest & hash)
{
    switch (type)
    {
        case AuthenticationType::PLAINTEXT_PASSWORD:
        {
            password_hash = hash;
            return;
        }

        case AuthenticationType::SHA256_PASSWORD:
        {
            if (hash.size() != 32)
                throw Exception(
                    "Password hash for the 'SHA256_PASSWORD' authentication type has length " + std::to_string(hash.size())
                        + " but must be exactly 32 bytes.",
                    ErrorCodes::BAD_ARGUMENTS);
            password_hash = hash;
            return;
        }

        case AuthenticationType::DOUBLE_SHA1_PASSWORD:
        {
            if (hash.size() != 20)
                throw Exception(
                    "Password hash for the 'DOUBLE_SHA1_PASSWORD' authentication type has length " + std::to_string(hash.size())
                        + " but must be exactly 20 bytes.",
                    ErrorCodes::BAD_ARGUMENTS);
            password_hash = hash;
            return;
        }

        case AuthenticationType::NO_PASSWORD:
        case AuthenticationType::LDAP:
        case AuthenticationType::KERBEROS:
        case AuthenticationType::SSL_CERTIFICATE:
            throw Exception("Cannot specify password binary hash for authentication type " + toString(type), ErrorCodes::LOGICAL_ERROR);

        case AuthenticationType::MAX:
            break;
    }
    throw Exception("setPasswordHashBinary(): authentication type " + toString(type) + " not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void AuthenticationData::setSalt(String salt_)
{
    if (type != AuthenticationType::SHA256_PASSWORD)
        throw Exception("setSalt(): authentication type " + toString(type) + " not supported", ErrorCodes::NOT_IMPLEMENTED);
    salt = std::move(salt_);
}

String AuthenticationData::getSalt() const
{
    return salt;
}

void AuthenticationData::setSSLCertificateCommonNames(boost::container::flat_set<String> common_names_)
{
    if (common_names_.empty())
        throw Exception("The 'SSL CERTIFICATE' authentication type requires a non-empty list of common names.", ErrorCodes::BAD_ARGUMENTS);
    ssl_certificate_common_names = std::move(common_names_);
}

}
