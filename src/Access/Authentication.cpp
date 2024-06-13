#include <Access/Authentication.h>
#include <Access/AuthenticationData.h>
#include <Access/Credentials.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/LDAPClient.h>
#include <Access/GSSAcceptor.h>
#include <Poco/SHA1Engine.h>
#include <Common/Exception.h>
#include <Common/SSHWrapper.h>
#include <Common/typeid_cast.h>

#include "config.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
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

    bool checkPasswordBcrypt(std::string_view password, const Digest & password_bcrypt)
    {
        return Util::checkPasswordBcrypt(password, password_bcrypt);
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

#if USE_SSH
    bool checkSshSignature(const std::vector<SSHKey> & keys, std::string_view signature, std::string_view original)
    {
        for (const auto & key: keys)
            if (key.isPublic() && key.verifySignature(signature, original))
                return true;
        return false;
    }
#endif
}

static std::vector<AuthenticationData> getAuthenticationMethodsOfType(
    const std::vector<AuthenticationData> & authentication_methods,
    const std::unordered_set<AuthenticationType> & types)
{
    std::vector<AuthenticationData> authentication_methods_of_type;

    for (const auto & authentication_method : authentication_methods)
    {
        if (types.contains(authentication_method.getType()))
        {
            authentication_methods_of_type.push_back(authentication_method);
        }
    }

    return authentication_methods_of_type;
}

bool Authentication::areCredentialsValid(
    const Credentials & credentials,
    const std::vector<AuthenticationData> & authentication_methods,
    const ExternalAuthenticators & external_authenticators,
    SettingsChanges & settings)
{
    if (!credentials.isReady())
        return false;

    if (const auto * gss_acceptor_context = typeid_cast<const GSSAcceptorContext *>(&credentials))
    {
        auto kerberos_authentication_methods = getAuthenticationMethodsOfType(authentication_methods, {AuthenticationType::KERBEROS});

        for (const auto & kerberos_authentication : kerberos_authentication_methods)
        {
            if (external_authenticators.checkKerberosCredentials(kerberos_authentication.getKerberosRealm(), *gss_acceptor_context))
            {
                return true;
            }
        }

        return false;
    }

    if (const auto * mysql_credentials = typeid_cast<const MySQLNative41Credentials *>(&credentials))
    {
        auto mysql_authentication_methods = getAuthenticationMethodsOfType(
            authentication_methods,
            {AuthenticationType::PLAINTEXT_PASSWORD, AuthenticationType::DOUBLE_SHA1_PASSWORD});

        for (const auto & mysql_authentication_method : mysql_authentication_methods)
        {
            switch (mysql_authentication_method.getType())
            {
                case AuthenticationType::PLAINTEXT_PASSWORD:
                    if (checkPasswordPlainTextMySQL(
                        mysql_credentials->getScramble(), mysql_credentials->getScrambledPassword(), mysql_authentication_method.getPasswordHashBinary()))
                    {
                        return true;
                    }
                    break;

                case AuthenticationType::DOUBLE_SHA1_PASSWORD:
                    if (checkPasswordDoubleSHA1MySQL(
                            mysql_credentials->getScramble(),
                            mysql_credentials->getScrambledPassword(),
                            mysql_authentication_method.getPasswordHashBinary()))
                    {
                        return true;
                    }
                    break;
                default:
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "something bad happened");
            }
        }

        return false;
    }

    if (const auto * basic_credentials = typeid_cast<const BasicCredentials *>(&credentials))
    {
        auto basic_credentials_authentication_methods = getAuthenticationMethodsOfType(
            authentication_methods,
            {AuthenticationType::PLAINTEXT_PASSWORD, AuthenticationType::SHA256_PASSWORD,
             AuthenticationType::DOUBLE_SHA1_PASSWORD, AuthenticationType::LDAP, AuthenticationType::BCRYPT_PASSWORD,
             AuthenticationType::HTTP});

        for (const auto & auth_method : basic_credentials_authentication_methods)
        {
            switch (auth_method.getType())
            {
                case AuthenticationType::PLAINTEXT_PASSWORD:
                    if (checkPasswordPlainText(basic_credentials->getPassword(), auth_method.getPasswordHashBinary()))
                    {
                        return true;
                    }
                    break;

                case AuthenticationType::SHA256_PASSWORD:
                    if (checkPasswordSHA256(basic_credentials->getPassword(), auth_method.getPasswordHashBinary(), auth_method.getSalt()))
                    {
                        return true;
                    }
                    break;
                case AuthenticationType::DOUBLE_SHA1_PASSWORD:
                    if (checkPasswordDoubleSHA1(basic_credentials->getPassword(), auth_method.getPasswordHashBinary()))
                    {
                        return true;
                    }
                    break;
                case AuthenticationType::LDAP:
                    if (external_authenticators.checkLDAPCredentials(auth_method.getLDAPServerName(), *basic_credentials))
                    {
                        return true;
                    }
                    break;
                case AuthenticationType::BCRYPT_PASSWORD:
                    if (checkPasswordBcrypt(basic_credentials->getPassword(), auth_method.getPasswordHashBinary()))
                    {
                        return true;
                    }
                    break;
                case AuthenticationType::HTTP:
                    if (auth_method.getHTTPAuthenticationScheme() == HTTPAuthenticationScheme::BASIC)
                    {
                        return external_authenticators.checkHTTPBasicCredentials(
                            auth_method.getHTTPAuthenticationServerName(), *basic_credentials, settings);
                    }
                    break;
                default:
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "something bad happened");
            }
        }

        return false;
    }

    if (const auto * ssl_certificate_credentials = typeid_cast<const SSLCertificateCredentials *>(&credentials))
    {
        const auto ssl_certificate_authentication_methods = getAuthenticationMethodsOfType(authentication_methods, {AuthenticationType::SSL_CERTIFICATE});

        for (const auto & auth_method : ssl_certificate_authentication_methods)
        {
            if (auth_method.getSSLCertificateCommonNames().contains(ssl_certificate_credentials->getCommonName()))
            {
                return true;
            }
        }

        return false;
    }

#if USE_SSH
    if (const auto * ssh_credentials = typeid_cast<const SshCredentials *>(&credentials))
    {
        const auto ssh_authentication_methods = getAuthenticationMethodsOfType(authentication_methods, {AuthenticationType::SSL_CERTIFICATE});

        for (const auto & auth_method : ssh_authentication_methods)
        {
            if (checkSshSignature(auth_method.getSSHKeys(), ssh_credentials->getSignature(), ssh_credentials->getOriginal()))
            {
                return true;
            }
        }

        return false;
    }
#endif

    if ([[maybe_unused]] const auto * always_allow_credentials = typeid_cast<const AlwaysAllowCredentials *>(&credentials))
        return true;

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TODO arthur, list possible types");
//    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "areCredentialsValid(): authentication type {} not supported", toString(auth_data.getType()));
}

}
