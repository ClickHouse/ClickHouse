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
