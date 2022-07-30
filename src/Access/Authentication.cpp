#include <Access/Authentication.h>
#include <Access/Common/AuthenticationData.h>
#include <Access/Credentials.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/LDAPClient.h>
#include <Access/GSSAcceptor.h>
#include <Common/Exception.h>
#include <Poco/SHA1Engine.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    using Digest = AuthenticationData::Digest;
    using Util = AuthenticationData::Util;

    bool checkPasswordPlainText(const String & password, const Digest & password_plaintext)
    {
        return (Util::stringToDigest(password) == password_plaintext);
    }

    bool checkPasswordDoubleSHA1(std::string_view password, const Digest & password_double_sha1)
    {
        return (Util::encodeDoubleSHA1(password) == password_double_sha1);
    }

    bool checkPasswordSHA256(std::string_view password, const Digest & password_sha256, const String & salt)
    {
        return Util::encodeSHA256(String(password).append(salt)) == password_sha256;
    }

    bool checkPasswordDoubleSHA1MySQL(std::string_view scramble, std::string_view scrambled_password, const Digest & password_double_sha1)
    {
        /// scrambled_password = SHA1(password) XOR SHA1(scramble <concat> SHA1(SHA1(password)))

        constexpr size_t scramble_length = 20;
        constexpr size_t sha1_size = Poco::SHA1Engine::DIGEST_SIZE;

        if ((scramble.size() < scramble_length) || (scramble.size() > scramble_length + 1)
            || ((scramble.size() == scramble_length + 1) && (scramble[scramble_length] != 0))
            || (scrambled_password.size() != sha1_size) || (password_double_sha1.size() != sha1_size))
            return false;

        Poco::SHA1Engine engine;
        engine.update(scramble.data(), scramble_length);
        engine.update(password_double_sha1.data(), sha1_size);
        const Poco::SHA1Engine::Digest & digest = engine.digest();

        Poco::SHA1Engine::Digest calculated_password_sha1(sha1_size);
        for (size_t i = 0; i < sha1_size; ++i)
            calculated_password_sha1[i] = scrambled_password[i] ^ digest[i];

        auto calculated_password_double_sha1 = Util::encodeSHA1(calculated_password_sha1);
        return calculated_password_double_sha1 == password_double_sha1;
    }

    bool checkPasswordPlainTextMySQL(std::string_view scramble, std::string_view scrambled_password, const Digest & password_plaintext)
    {
        return checkPasswordDoubleSHA1MySQL(scramble, scrambled_password, Util::encodeDoubleSHA1(password_plaintext));
    }
}


bool Authentication::areCredentialsValid(const Credentials & credentials, const AuthenticationData & auth_data, const ExternalAuthenticators & external_authenticators)
{
    if (!credentials.isReady())
        return false;

    if (const auto * gss_acceptor_context = typeid_cast<const GSSAcceptorContext *>(&credentials))
    {
        switch (auth_data.getType())
        {
            case AuthenticationType::NO_PASSWORD:
            case AuthenticationType::PLAINTEXT_PASSWORD:
            case AuthenticationType::SHA256_PASSWORD:
            case AuthenticationType::DOUBLE_SHA1_PASSWORD:
            case AuthenticationType::LDAP:
                throw Authentication::Require<BasicCredentials>("ClickHouse Basic Authentication");

            case AuthenticationType::KERBEROS:
                return external_authenticators.checkKerberosCredentials(auth_data.getKerberosRealm(), *gss_acceptor_context);

            case AuthenticationType::SSL_CERTIFICATE:
                throw Authentication::Require<BasicCredentials>("ClickHouse X.509 Authentication");

            case AuthenticationType::MAX:
                break;
        }
    }

    if (const auto * mysql_credentials = typeid_cast<const MySQLNative41Credentials *>(&credentials))
    {
        switch (auth_data.getType())
        {
            case AuthenticationType::NO_PASSWORD:
                return true; // N.B. even if the password is not empty!

            case AuthenticationType::PLAINTEXT_PASSWORD:
                return checkPasswordPlainTextMySQL(mysql_credentials->getScramble(), mysql_credentials->getScrambledPassword(), auth_data.getPasswordHashBinary());

            case AuthenticationType::DOUBLE_SHA1_PASSWORD:
                return checkPasswordDoubleSHA1MySQL(mysql_credentials->getScramble(), mysql_credentials->getScrambledPassword(), auth_data.getPasswordHashBinary());

            case AuthenticationType::SHA256_PASSWORD:
            case AuthenticationType::LDAP:
            case AuthenticationType::KERBEROS:
                throw Authentication::Require<BasicCredentials>("ClickHouse Basic Authentication");

            case AuthenticationType::SSL_CERTIFICATE:
                throw Authentication::Require<BasicCredentials>("ClickHouse X.509 Authentication");

            case AuthenticationType::MAX:
                break;
        }
    }

    if (const auto * basic_credentials = typeid_cast<const BasicCredentials *>(&credentials))
    {
        switch (auth_data.getType())
        {
            case AuthenticationType::NO_PASSWORD:
                return true; // N.B. even if the password is not empty!

            case AuthenticationType::PLAINTEXT_PASSWORD:
                return checkPasswordPlainText(basic_credentials->getPassword(), auth_data.getPasswordHashBinary());

            case AuthenticationType::SHA256_PASSWORD:
                return checkPasswordSHA256(basic_credentials->getPassword(), auth_data.getPasswordHashBinary(), auth_data.getSalt());

            case AuthenticationType::DOUBLE_SHA1_PASSWORD:
                return checkPasswordDoubleSHA1(basic_credentials->getPassword(), auth_data.getPasswordHashBinary());

            case AuthenticationType::LDAP:
                return external_authenticators.checkLDAPCredentials(auth_data.getLDAPServerName(), *basic_credentials);

            case AuthenticationType::KERBEROS:
                throw Authentication::Require<GSSAcceptorContext>(auth_data.getKerberosRealm());

            case AuthenticationType::SSL_CERTIFICATE:
                throw Authentication::Require<BasicCredentials>("ClickHouse X.509 Authentication");

            case AuthenticationType::MAX:
                break;
        }
    }

    if (const auto * ssl_certificate_credentials = typeid_cast<const SSLCertificateCredentials *>(&credentials))
    {
        switch (auth_data.getType())
        {
            case AuthenticationType::NO_PASSWORD:
            case AuthenticationType::PLAINTEXT_PASSWORD:
            case AuthenticationType::SHA256_PASSWORD:
            case AuthenticationType::DOUBLE_SHA1_PASSWORD:
            case AuthenticationType::LDAP:
                throw Authentication::Require<BasicCredentials>("ClickHouse Basic Authentication");

            case AuthenticationType::KERBEROS:
                throw Authentication::Require<GSSAcceptorContext>(auth_data.getKerberosRealm());

            case AuthenticationType::SSL_CERTIFICATE:
                return auth_data.getSSLCertificateCommonNames().contains(ssl_certificate_credentials->getCommonName());

            case AuthenticationType::MAX:
                break;
        }
    }

    if ([[maybe_unused]] const auto * always_allow_credentials = typeid_cast<const AlwaysAllowCredentials *>(&credentials))
        return true;

    throw Exception("areCredentialsValid(): authentication type " + toString(auth_data.getType()) + " not supported", ErrorCodes::NOT_IMPLEMENTED);
}

}
