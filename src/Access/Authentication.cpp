#include <Access/Authentication.h>
#include <Access/AuthenticationData.h>
#include <Access/Credentials.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/GSSAcceptor.h>
#include <Access/LDAPClient.h>
#include <Poco/Base32Decoder.h>
#include <Poco/HMACEngine.h>
#include <Poco/SHA1Engine.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include "base/types.h"

#include <iomanip>


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

    constexpr UInt32 TotpDefaultTimeInterval = 30;

    class TotpGenerator
    {
    public:
        explicit TotpGenerator(const std::string & secret, UInt32 interval = TotpDefaultTimeInterval)
            : m_secret(secret)
            , m_interval(interval)
        {
        }

        std::string generateOTP()
        {
            std::stringstream base32_secret(m_secret);
            Poco::Base32Decoder decoder(base32_secret);

            std::stringstream decoded_secret;
            decoded_secret << decoder.rdbuf();

            std::array<char, 20> buffer;
            decoded_secret.read(buffer.data(), buffer.size());
            Poco::HMACEngine<Poco::SHA1Engine> hmac(buffer.data(), buffer.size());

            std::array<UInt8, 8> time_point_bytes;
            auto time_point = getLastTimePoint();
            for (size_t i = 0; i < time_point_bytes.size(); ++i)
            {
                time_point_bytes[7 - i] = (time_point >> (i * 8)) & 0xff;
            }
            hmac.update(time_point_bytes.data(), time_point_bytes.size());
            const Poco::DigestEngine::Digest & digest = hmac.digest();

            return getOTP(digest);
        }

    private:
        std::time_t getLastTimePoint() const
        {
            const auto epoch = std::chrono::system_clock::now().time_since_epoch();
            const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(epoch);
            return static_cast<size_t>(seconds.count()) / m_interval;
        }

        std::string getOTP(const Poco::DigestEngine::Digest & bytes)
        {
            int offset = bytes[19] & 0xf;
            int code = (bytes[offset] & 0x7f) << 24;
            code |= (bytes[offset + 1] & 0xff) << 16;
            code |= (bytes[offset + 2] & 0xff) << 8;
            code |= (bytes[offset + 3] & 0xff);
            code &= 0x7fffffff;
            code %= 1000000;
            std::stringstream ss;
            ss << std::setfill('0') << std::setw(6) << code;
            return ss.str();
        }

        std::string m_secret;
        UInt32 m_interval{TotpDefaultTimeInterval};
    };

    bool checkPasswordPlainText(const String & password, const Digest & password_plaintext)
    {
        return (Util::stringToDigest(password) == password_plaintext);
    }

    bool checkOneTimePassword(const std::string_view & password, const String & secret)
    {
        return (password == TotpGenerator(secret).generateOTP());
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
            case AuthenticationType::ONE_TIME_PASSWORD:
            case AuthenticationType::BCRYPT_PASSWORD:
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

            case AuthenticationType::ONE_TIME_PASSWORD:
            case AuthenticationType::SHA256_PASSWORD:
            case AuthenticationType::BCRYPT_PASSWORD:
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

            case AuthenticationType::ONE_TIME_PASSWORD:
                return checkOneTimePassword(basic_credentials->getPassword(), auth_data.getPassword());

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

            case AuthenticationType::BCRYPT_PASSWORD:
                return checkPasswordBcrypt(basic_credentials->getPassword(), auth_data.getPasswordHashBinary());

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
            case AuthenticationType::ONE_TIME_PASSWORD:
            case AuthenticationType::SHA256_PASSWORD:
            case AuthenticationType::DOUBLE_SHA1_PASSWORD:
            case AuthenticationType::BCRYPT_PASSWORD:
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

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "areCredentialsValid(): authentication type {} not supported", toString(auth_data.getType()));
}

}
