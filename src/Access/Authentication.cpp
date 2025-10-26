#include <Access/Authentication.h>
#include <Access/AuthenticationData.h>
#include <Access/Credentials.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/LDAPClient.h>
#include <Access/GSSAcceptor.h>
#include <Common/Base64.h>
#include <Common/Crypto/X509Certificate.h>
#include <Common/Exception.h>
#include <Common/SSHWrapper.h>
#include <Common/typeid_cast.h>
#include <Poco/SHA1Engine.h>

#include <base/types.h>
#include "config.h"

#if USE_SSL
#    include <Common/OpenSSLHelpers.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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

    bool checkPasswordScramSHA256(std::string_view password, const Digest & password_scram_sha256, const String & salt)
    {
        auto digest = Util::encodeScramSHA256(password, salt);
        return digest == password_scram_sha256;
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

    bool hasPublicKey(const std::vector<SSHKey> & keys, const SSHKey & key)
    {
        return std::ranges::find_if(keys, [&](const auto & x) { return key.isEqual(x); }) != keys.end();
    }
#endif

    bool checkKerberosAuthentication(
        const GSSAcceptorContext * gss_acceptor_context,
        const AuthenticationData & authentication_method,
        const ExternalAuthenticators & external_authenticators)
    {
        return authentication_method.getType() == AuthenticationType::KERBEROS
            && external_authenticators.checkKerberosCredentials(authentication_method.getKerberosRealm(), *gss_acceptor_context);
    }

    std::string computeScramSHA256ClientProof(const std::vector<uint8_t> & salted_password [[maybe_unused]], const std::string& auth_message [[maybe_unused]])
    {
#if USE_SSL
        auto client_key = hmacSHA256(salted_password, "Client Key");
        auto stored_key = encodeSHA256(client_key);
        auto client_signature = hmacSHA256(stored_key, auth_message);

        String client_proof(client_key.size(), 0);
        for (size_t i = 0; i < client_key.size(); ++i)
            client_proof[i] = client_key[i] ^ client_signature[i];

        return base64Encode(client_proof);
#else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Client proof can be computed only with USE_SSL compile flag.");
#endif
    }

    bool checkScramSHA256Authentication(
        const ScramSHA256Credentials * scram_sha256_credentials,
        const AuthenticationData & authentication_method)
    {
        const auto & client_proof = scram_sha256_credentials->getClientProof();
        const auto & auth_message = scram_sha256_credentials->getAuthMessage();
        const auto & salt = authentication_method.getSalt();
        const auto & password = authentication_method.getPasswordHashBinary();
        auto computed_client_proof = computeScramSHA256ClientProof(password, auth_message);

        if (computed_client_proof.size() != client_proof.size())
            return false;

        for (size_t i = 0; i < computed_client_proof.size(); ++i)
        {
            if (static_cast<UInt8>(computed_client_proof[i]) != static_cast<UInt8>(client_proof[i]))
                return false;
        }
        return true;
    }

    bool checkMySQLAuthentication(
        const MySQLNative41Credentials * mysql_credentials,
        const AuthenticationData & authentication_method)
    {
        switch (authentication_method.getType())
        {
            case AuthenticationType::PLAINTEXT_PASSWORD:
                return checkPasswordPlainTextMySQL(
                    mysql_credentials->getScramble(),
                    mysql_credentials->getScrambledPassword(),
                    authentication_method.getPasswordHashBinary());
            case AuthenticationType::DOUBLE_SHA1_PASSWORD:
                return checkPasswordDoubleSHA1MySQL(
                    mysql_credentials->getScramble(),
                    mysql_credentials->getScrambledPassword(),
                    authentication_method.getPasswordHashBinary());
            default:
                return false;
        }
    }

    bool checkBasicAuthentication(
        const BasicCredentials * basic_credentials,
        const AuthenticationData & authentication_method,
        const ExternalAuthenticators & external_authenticators,
        const ClientInfo & client_info,
        SettingsChanges & settings)
    {
        switch (authentication_method.getType())
        {
            case AuthenticationType::NO_PASSWORD:
            {
                return true; // N.B. even if the password is not empty!
            }
            case AuthenticationType::PLAINTEXT_PASSWORD:
            {
                return checkPasswordPlainText(basic_credentials->getPassword(), authentication_method.getPasswordHashBinary());
            }
            case AuthenticationType::SHA256_PASSWORD:
            {
                return checkPasswordSHA256(
                    basic_credentials->getPassword(), authentication_method.getPasswordHashBinary(), authentication_method.getSalt());
            }
            case AuthenticationType::SCRAM_SHA256_PASSWORD:
            {
                return checkPasswordScramSHA256(
                    basic_credentials->getPassword(), authentication_method.getPasswordHashBinary(), authentication_method.getSalt());
            }
            case AuthenticationType::DOUBLE_SHA1_PASSWORD:
            {
                return checkPasswordDoubleSHA1(basic_credentials->getPassword(), authentication_method.getPasswordHashBinary());
            }
            case AuthenticationType::LDAP:
            {
                return external_authenticators.checkLDAPCredentials(authentication_method.getLDAPServerName(), *basic_credentials);
            }
            case AuthenticationType::BCRYPT_PASSWORD:
            {
                return checkPasswordBcrypt(basic_credentials->getPassword(), authentication_method.getPasswordHashBinary());
            }
            case AuthenticationType::HTTP:
            {
                if (authentication_method.getHTTPAuthenticationScheme() == HTTPAuthenticationScheme::BASIC)
                {
                    return external_authenticators.checkHTTPBasicCredentials(
                        authentication_method.getHTTPAuthenticationServerName(), *basic_credentials, client_info, settings);
                }
                break;
            }
            default:
                break;
        }

        return false;
    }

#if USE_SSL
    bool checkSSLCertificateAuthentication(
        const SSLCertificateCredentials * ssl_certificate_credentials,
        const AuthenticationData & authentication_method)
    {
        if (AuthenticationType::SSL_CERTIFICATE != authentication_method.getType())
        {
            return false;
        }

        for (X509Certificate::Subjects::Type type : {X509Certificate::Subjects::Type::CN, X509Certificate::Subjects::Type::SAN})
        {
            for (const auto & subject : authentication_method.getSSLCertificateSubjects().at(type))
            {
                if (ssl_certificate_credentials->getSSLCertificateSubjects().at(type).contains(subject))
                    return true;

                // Wildcard support (1 only)
                if (subject.contains('*'))
                {
                    auto prefix = std::string_view(subject).substr(0, subject.find('*'));
                    auto suffix = std::string_view(subject).substr(subject.find('*') + 1);
                    auto slashes = std::count(subject.begin(), subject.end(), '/');

                    for (const auto & certificate_subject : ssl_certificate_credentials->getSSLCertificateSubjects().at(type))
                    {
                        bool matches_wildcard = certificate_subject.starts_with(prefix) && certificate_subject.ends_with(suffix);

                        // '*' must not represent a '/' in URI, so check if the number of '/' are equal
                        bool matches_slashes = slashes == count(certificate_subject.begin(), certificate_subject.end(), '/');

                        if (matches_wildcard && matches_slashes)
                            return true;
                    }
                }
            }
        }

        return false;
    }
#endif

#if USE_SSH
    bool checkSshAuthentication(
        const SshCredentials * ssh_credentials,
        const AuthenticationData & authentication_method)
    {
        return AuthenticationType::SSH_KEY == authentication_method.getType()
            && checkSshSignature(authentication_method.getSSHKeys(), ssh_credentials->getSignature(), ssh_credentials->getOriginal());
    }

    /**
     * The idea behind this simple check is that the most of the work and verification is done by libssh.
     * What we need to do is to compare the public key extracted from the user's private key and compare it
     * to our database of keys associated with the user. Similar to how it is done with ~/.ssh/authorized_keys
     */
    bool checkSSHLoginAuthentication(
        const SSHPTYCredentials * ssh_login_credentials,
        const AuthenticationData & authentication_method)
    {
        return AuthenticationType::SSH_KEY == authentication_method.getType()
            && hasPublicKey(authentication_method.getSSHKeys(), ssh_login_credentials->getKey());
    }
#endif
}

bool Authentication::areCredentialsValid(
    const Credentials & credentials,
    const AuthenticationData & authentication_method,
    const ExternalAuthenticators & external_authenticators,
    const ClientInfo & client_info,
    SettingsChanges & settings)
{
    if (!credentials.isReady())
        return false;

    if (const auto * gss_acceptor_context = typeid_cast<const GSSAcceptorContext *>(&credentials))
    {
        return checkKerberosAuthentication(gss_acceptor_context, authentication_method, external_authenticators);
    }

    if (const auto * mysql_credentials = typeid_cast<const MySQLNative41Credentials *>(&credentials))
    {
        return checkMySQLAuthentication(mysql_credentials, authentication_method);
    }

    if (const auto * basic_credentials = typeid_cast<const BasicCredentials *>(&credentials))
    {
        return checkBasicAuthentication(basic_credentials, authentication_method, external_authenticators, client_info, settings);
    }

    if (const auto * scram_shh256_credentials = typeid_cast<const ScramSHA256Credentials *>(&credentials))
    {
        return checkScramSHA256Authentication(scram_shh256_credentials, authentication_method);
    }

#if USE_SSL
    if (const auto * ssl_certificate_credentials = typeid_cast<const SSLCertificateCredentials *>(&credentials))
    {
        return checkSSLCertificateAuthentication(ssl_certificate_credentials, authentication_method);
    }
#endif

#if USE_SSH
    if (const auto * ssh_credentials = typeid_cast<const SshCredentials *>(&credentials))
    {
        return checkSshAuthentication(ssh_credentials, authentication_method);
    }

    if (const auto * ssh_login_credentials = typeid_cast<const SSHPTYCredentials *>(&credentials))
    {
        return checkSSHLoginAuthentication(ssh_login_credentials, authentication_method);
    }
#endif

    if ([[maybe_unused]] const auto * always_allow_credentials = typeid_cast<const AlwaysAllowCredentials *>(&credentials))
        return true;

    return false;
}

}
