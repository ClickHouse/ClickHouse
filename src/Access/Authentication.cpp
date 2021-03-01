#include <Access/Authentication.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/LDAPClient.h>
#include <Common/Exception.h>
#include <Poco/SHA1Engine.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
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

        case SHA256_PASSWORD:
            throw Exception("Cannot get password double SHA1 for user with 'SHA256_PASSWORD' authentication.", ErrorCodes::BAD_ARGUMENTS);

        case DOUBLE_SHA1_PASSWORD:
            return password_hash;

        case LDAP_SERVER:
            throw Exception("Cannot get password double SHA1 for user with 'LDAP_SERVER' authentication.", ErrorCodes::BAD_ARGUMENTS);

        case MAX_TYPE:
            break;
    }
    throw Exception("getPasswordDoubleSHA1(): authentication type " + toString(type) + " not supported", ErrorCodes::NOT_IMPLEMENTED);
}


bool Authentication::isCorrectPassword(const String & password_, const String & user_, const ExternalAuthenticators & external_authenticators) const
{
    switch (type)
    {
        case NO_PASSWORD:
            return true;

        case PLAINTEXT_PASSWORD:
        {
            if (password_ == std::string_view{reinterpret_cast<const char *>(password_hash.data()), password_hash.size()})
                return true;

            // For compatibility with MySQL clients which support only native authentication plugin, SHA1 can be passed instead of password.
            auto password_sha1 = encodeSHA1(password_hash);
            return password_ == std::string_view{reinterpret_cast<const char *>(password_sha1.data()), password_sha1.size()};
        }

        case SHA256_PASSWORD:
            return encodeSHA256(password_) == password_hash;

        case DOUBLE_SHA1_PASSWORD:
        {
            auto first_sha1 = encodeSHA1(password_);

            /// If it was MySQL compatibility server, then first_sha1 already contains double SHA1.
            if (first_sha1 == password_hash)
                return true;

            return encodeSHA1(first_sha1) == password_hash;
        }

        case LDAP_SERVER:
        {
            auto ldap_server_params = external_authenticators.getLDAPServerParams(server_name);
            ldap_server_params.user = user_;
            ldap_server_params.password = password_;

            LDAPSimpleAuthClient ldap_client(ldap_server_params);
            return ldap_client.check();
        }

        case MAX_TYPE:
            break;
    }
    throw Exception("Cannot check if the password is correct for authentication type " + toString(type), ErrorCodes::NOT_IMPLEMENTED);
}

}
