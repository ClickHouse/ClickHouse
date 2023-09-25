#include <Access/Authentication.h>
#include <Access/AuthenticationData.h>
#include <Access/Credentials.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/GSSAcceptor.h>
#include <Access/LDAPClient.h>
#include <Poco/SHA1Engine.h>
#include <Common/Exception.h>
#include <Common/PolymorhicVariantFactory.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    using Digest = IPasswordAuthData::Digest;
    using Util = IPasswordAuthData::Util;

    // Helper type list with all possible credentials types
    using CredentialsTypeList = TypeList<
        GSSAcceptorContext,
        AlwaysAllowCredentials,
        SSLCertificateCredentials,
        BasicCredentials,
        MySQLNative41Credentials>;
    // Helper type list with all possible authentication data types
    using AuthenticationDataTypeList = TypeList<
        NoPasswordAuthData,
        PlainTextPasswordAuthData,
        SHA256PasswordAuthData,
        DoubleSHA1PasswordAuthData,
        BcryptPasswordAuthData,
        LDAPAuthData,
        KerberosAuthData,
        SSLCertificateAuthData>;

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
}


bool Authentication::areCredentialsValid(
    const Credentials & credentials,
    const IAuthenticationData & authentication_data,
    const ExternalAuthenticators & external_authenticators)
{
    if (!credentials.isReady())
        return false;

    auto credentials_variant = PolymorhicVariantFactory<CredentialsTypeList>::AsVariant(credentials);
    auto authentication_data_variant = PolymorhicVariantFactory<AuthenticationDataTypeList>::AsVariant(authentication_data);

    return std::visit(Overloaded{
        [](const AlwaysAllowCredentials *, auto)
        {
            return true;
        },
        // MySQL credentials
        [&](const MySQLNative41Credentials *, const NoPasswordAuthData *)
        {
            return true;  // N.B. even if the password is not empty!
        },
        [&](const MySQLNative41Credentials * mysql_credentials, const PlainTextPasswordAuthData * auth_data)
        {
            return checkPasswordPlainTextMySQL(mysql_credentials->getScramble(), mysql_credentials->getScrambledPassword(), auth_data->getPasswordHashBinary());
        },
        [&](const MySQLNative41Credentials * mysql_credentials, const DoubleSHA1PasswordAuthData * auth_data)
        {
            return checkPasswordDoubleSHA1MySQL(mysql_credentials->getScramble(), mysql_credentials->getScrambledPassword(), auth_data->getPasswordHashBinary());
        },
        // No password
        [&](const BasicCredentials *, const NoPasswordAuthData *)
        {
            return true;  // N.B. even if the password is not empty!
        },
        [](const Credentials *, const NoPasswordAuthData *) -> bool
        {
            throw Authentication::Require<BasicCredentials>("ClickHouse Basic Authentication");
        },
        // Passwords
        [&](const BasicCredentials * basic_credentials, const PlainTextPasswordAuthData * auth_data)
        {
            return checkPasswordPlainText(basic_credentials->getPassword(), auth_data->getPasswordHashBinary());
        },
        [&](const BasicCredentials * basic_credentials, const SHA256PasswordAuthData * auth_data)
        {
            return checkPasswordSHA256(basic_credentials->getPassword(), auth_data->getPasswordHashBinary(), auth_data->getSalt());
        },
        [&](const BasicCredentials * basic_credentials, const DoubleSHA1PasswordAuthData * auth_data)
        {
            return checkPasswordDoubleSHA1(basic_credentials->getPassword(), auth_data->getPasswordHashBinary());
        },
        [&](const BasicCredentials * basic_credentials, const BcryptPasswordAuthData * auth_data)
        {
            return checkPasswordBcrypt(basic_credentials->getPassword(), auth_data->getPasswordHashBinary());
        },
        [](const Credentials *, const IPasswordAuthData *) -> bool
        {
            throw Authentication::Require<BasicCredentials>("ClickHouse Basic Authentication");
        },
        // Kerberos
        [&](const GSSAcceptorContext * gss_acceptor_context, const KerberosAuthData * auth_data)
        {
            return external_authenticators.checkKerberosCredentials(auth_data->getKerberosRealm(), *gss_acceptor_context);
        },
        [](const Credentials *, const KerberosAuthData * auth_data) -> bool
        {
            throw Authentication::Require<GSSAcceptorContext>(auth_data->getKerberosRealm());
        },
        // LDAP
        [&](const BasicCredentials * basic_credentials, const LDAPAuthData * auth_data)
        {
            return external_authenticators.checkLDAPCredentials(auth_data->getLDAPServerName(), *basic_credentials);
        },
        [](const Credentials *, const LDAPAuthData *) -> bool
        {
            throw Authentication::Require<BasicCredentials>("ClickHouse Basic Authentication");
        },
        // SSL certificate
        [&](const SSLCertificateCredentials * ssl_certificate_credentials, const SSLCertificateAuthData * auth_data)
        {
            return auth_data->getSSLCertificateCommonNames().contains(ssl_certificate_credentials->getCommonName());
        },
        [](const Credentials *, const SSLCertificateAuthData *) -> bool
        {
            throw Authentication::Require<BasicCredentials>("ClickHouse X.509 Authentication");
        },
        //Fallback
        [](const Credentials *, const IAuthenticationData * auth_data) -> bool
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "areCredentialsValid(): authentication type {} not supported", toString(auth_data->getType()));
        },
        }, credentials_variant, authentication_data_variant);
}
}
