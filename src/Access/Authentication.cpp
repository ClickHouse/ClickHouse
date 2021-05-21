#include <Access/Authentication.h>
#include <Access/Credentials.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/LDAPClient.h>
#include <Access/GSSAcceptor.h>
#include <Common/Exception.h>
#include <Poco/SHA1Engine.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}


const Authentication::TypeInfo & Authentication::TypeInfo::get(Type type_)
{
    static constexpr auto make_info = [](const char * raw_name_)
    {
        String init_name = raw_name_;
        boost::to_lower(init_name);
        return TypeInfo{raw_name_, std::move(init_name)};
    };

    switch (type_)
    {
        case NO_PASSWORD:
        {
            static const auto info = make_info("NO_PASSWORD");
            return info;
        }
        case PLAINTEXT_PASSWORD:
        {
            static const auto info = make_info("PLAINTEXT_PASSWORD");
            return info;
        }
        case SHA256_PASSWORD:
        {
            static const auto info = make_info("SHA256_PASSWORD");
            return info;
        }
        case DOUBLE_SHA1_PASSWORD:
        {
            static const auto info = make_info("DOUBLE_SHA1_PASSWORD");
            return info;
        }
        case LDAP:
        {
            static const auto info = make_info("LDAP");
            return info;
        }
        case KERBEROS:
        {
            static const auto info = make_info("KERBEROS");
            return info;
        }
        case MAX_TYPE:
            break;
    }
    throw Exception("Unknown authentication type: " + std::to_string(static_cast<int>(type_)), ErrorCodes::LOGICAL_ERROR);
}

String toString(Authentication::Type type_)
{
    return Authentication::TypeInfo::get(type_).raw_name;
}


Authentication::Digest Authentication::encodeSHA256(const std::string_view & text [[maybe_unused]])
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

Authentication::Digest Authentication::encodeSHA1(const std::string_view & text)
{
    Poco::SHA1Engine engine;
    engine.update(text.data(), text.size());
    return engine.digest();
}


void Authentication::setPassword(const String & password_)
{
    switch (type)
    {
        case PLAINTEXT_PASSWORD:
            return setPasswordHashBinary(encodePlainText(password_));

        case SHA256_PASSWORD:
            return setPasswordHashBinary(encodeSHA256(password_));

        case DOUBLE_SHA1_PASSWORD:
            return setPasswordHashBinary(encodeDoubleSHA1(password_));

        case NO_PASSWORD:
        case LDAP:
        case KERBEROS:
            throw Exception("Cannot specify password for authentication type " + toString(type), ErrorCodes::LOGICAL_ERROR);

        case MAX_TYPE:
            break;
    }
    throw Exception("setPassword(): authentication type " + toString(type) + " not supported", ErrorCodes::NOT_IMPLEMENTED);
}


String Authentication::getPassword() const
{
    if (type != PLAINTEXT_PASSWORD)
        throw Exception("Cannot decode the password", ErrorCodes::LOGICAL_ERROR);
    return String(password_hash.data(), password_hash.data() + password_hash.size());
}


void Authentication::setPasswordHashHex(const String & hash)
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

String Authentication::getPasswordHashHex() const
{
    if (type == LDAP || type == KERBEROS)
        throw Exception("Cannot get password hex hash for authentication type " + toString(type), ErrorCodes::LOGICAL_ERROR);

    String hex;
    hex.resize(password_hash.size() * 2);
    boost::algorithm::hex(password_hash.begin(), password_hash.end(), hex.data());
    return hex;
}


void Authentication::setPasswordHashBinary(const Digest & hash)
{
    switch (type)
    {
        case PLAINTEXT_PASSWORD:
        {
            password_hash = hash;
            return;
        }

        case SHA256_PASSWORD:
        {
            if (hash.size() != 32)
                throw Exception(
                    "Password hash for the 'SHA256_PASSWORD' authentication type has length " + std::to_string(hash.size())
                        + " but must be exactly 32 bytes.",
                    ErrorCodes::BAD_ARGUMENTS);
            password_hash = hash;
            return;
        }

        case DOUBLE_SHA1_PASSWORD:
        {
            if (hash.size() != 20)
                throw Exception(
                    "Password hash for the 'DOUBLE_SHA1_PASSWORD' authentication type has length " + std::to_string(hash.size())
                        + " but must be exactly 20 bytes.",
                    ErrorCodes::BAD_ARGUMENTS);
            password_hash = hash;
            return;
        }

        case NO_PASSWORD:
        case LDAP:
        case KERBEROS:
            throw Exception("Cannot specify password binary hash for authentication type " + toString(type), ErrorCodes::LOGICAL_ERROR);

        case MAX_TYPE:
            break;
    }
    throw Exception("setPasswordHashBinary(): authentication type " + toString(type) + " not supported", ErrorCodes::NOT_IMPLEMENTED);
}

const String & Authentication::getLDAPServerName() const
{
    return ldap_server_name;
}

void Authentication::setLDAPServerName(const String & name)
{
    ldap_server_name = name;
}

const String & Authentication::getKerberosRealm() const
{
    return kerberos_realm;
}

void Authentication::setKerberosRealm(const String & realm)
{
    kerberos_realm = realm;
}


Authentication::Digest Authentication::getPasswordDoubleSHA1() const
{
    switch (type)
    {
        case NO_PASSWORD:
        {
            Poco::SHA1Engine engine;
            return engine.digest();
        }

        case PLAINTEXT_PASSWORD:
        {
            Poco::SHA1Engine engine;
            engine.update(getPassword());
            const Digest & first_sha1 = engine.digest();
            engine.update(first_sha1.data(), first_sha1.size());
            return engine.digest();
        }

        case DOUBLE_SHA1_PASSWORD:
            return password_hash;

        case SHA256_PASSWORD:
        case LDAP:
        case KERBEROS:
            throw Exception("Cannot get password double SHA1 hash for authentication type " + toString(type), ErrorCodes::LOGICAL_ERROR);

        case MAX_TYPE:
            break;
    }
    throw Exception("getPasswordDoubleSHA1(): authentication type " + toString(type) + " not supported", ErrorCodes::NOT_IMPLEMENTED);
}


bool Authentication::areCredentialsValid(const Credentials & credentials, const ExternalAuthenticators & external_authenticators) const
{
    if (!credentials.isReady())
        return false;

    if (const auto * gss_acceptor_context = dynamic_cast<const GSSAcceptorContext *>(&credentials))
    {
        switch (type)
        {
            case NO_PASSWORD:
            case PLAINTEXT_PASSWORD:
            case SHA256_PASSWORD:
            case DOUBLE_SHA1_PASSWORD:
            case LDAP:
                throw Require<BasicCredentials>("ClickHouse Basic Authentication");

            case KERBEROS:
                return external_authenticators.checkKerberosCredentials(kerberos_realm, *gss_acceptor_context);

            case MAX_TYPE:
                break;
        }
    }

    if (const auto * basic_credentials = dynamic_cast<const BasicCredentials *>(&credentials))
    {
        switch (type)
        {
            case NO_PASSWORD:
                return true; // N.B. even if the password is not empty!

            case PLAINTEXT_PASSWORD:
            {
                if (basic_credentials->getPassword() == std::string_view{reinterpret_cast<const char *>(password_hash.data()), password_hash.size()})
                    return true;

                // For compatibility with MySQL clients which support only native authentication plugin, SHA1 can be passed instead of password.
                const auto password_sha1 = encodeSHA1(password_hash);
                return basic_credentials->getPassword() == std::string_view{reinterpret_cast<const char *>(password_sha1.data()), password_sha1.size()};
            }

            case SHA256_PASSWORD:
                return encodeSHA256(basic_credentials->getPassword()) == password_hash;

            case DOUBLE_SHA1_PASSWORD:
            {
                const auto first_sha1 = encodeSHA1(basic_credentials->getPassword());

                /// If it was MySQL compatibility server, then first_sha1 already contains double SHA1.
                if (first_sha1 == password_hash)
                    return true;

                return encodeSHA1(first_sha1) == password_hash;
            }

            case LDAP:
                return external_authenticators.checkLDAPCredentials(ldap_server_name, *basic_credentials);

            case KERBEROS:
                throw Require<GSSAcceptorContext>(kerberos_realm);

            case MAX_TYPE:
                break;
        }
    }

    if ([[maybe_unused]] const auto * always_allow_credentials = dynamic_cast<const AlwaysAllowCredentials *>(&credentials))
        return true;

    throw Exception("areCredentialsValid(): authentication type " + toString(type) + " not supported", ErrorCodes::NOT_IMPLEMENTED);
}

}
