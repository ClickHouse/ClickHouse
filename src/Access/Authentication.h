#pragma once

#include <common/types.h>
#include <Common/Exception.h>
#include <Common/OpenSSLHelpers.h>
#include <Poco/SHA1Engine.h>
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

class Credentials;
class ExternalAuthenticators;

/// Authentication type and encrypted password for checking when a user logins.
class Authentication
{
public:
    enum Type
    {
        /// User doesn't have to enter password.
        NO_PASSWORD,

        /// Password is stored as is.
        PLAINTEXT_PASSWORD,

        /// Password is encrypted in SHA256 hash.
        SHA256_PASSWORD,

        /// SHA1(SHA1(password)).
        /// This kind of hash is used by the `mysql_native_password` authentication plugin.
        DOUBLE_SHA1_PASSWORD,

        /// Password is checked by a [remote] LDAP server. Connection will be made at each authentication attempt.
        LDAP,

        /// Kerberos authentication performed through GSS-API negotiation loop.
        KERBEROS,

        MAX_TYPE,
    };

    struct TypeInfo
    {
        const char * const raw_name;
        const String name; /// Lowercased with underscores, e.g. "sha256_password".
        static const TypeInfo & get(Type type_);
    };

    // A signaling class used to communicate requirements for credentials.
    template <typename CredentialsType>
    class Require : public Exception
    {
    public:
        explicit Require(const String & realm_);
        const String & getRealm() const;

    private:
        const String realm;
    };

    using Digest = std::vector<uint8_t>;

    Authentication(Authentication::Type type_ = NO_PASSWORD) : type(type_) {}
    Authentication(const Authentication & src) = default;
    Authentication & operator =(const Authentication & src) = default;
    Authentication(Authentication && src) = default;
    Authentication & operator =(Authentication && src) = default;

    Type getType() const { return type; }

    /// Sets the password and encrypt it using the authentication type set in the constructor.
    void setPassword(const String & password_);

    /// Returns the password. Allowed to use only for Type::PLAINTEXT_PASSWORD.
    String getPassword() const;

    /// Sets the password as a string of hexadecimal digits.
    void setPasswordHashHex(const String & hash);

    String getPasswordHashHex() const;

    /// Sets the password in binary form.
    void setPasswordHashBinary(const Digest & hash);

    const Digest & getPasswordHashBinary() const { return password_hash; }

    /// Returns SHA1(SHA1(password)) used by MySQL compatibility server for authentication.
    /// Allowed to use for Type::NO_PASSWORD, Type::PLAINTEXT_PASSWORD, Type::DOUBLE_SHA1_PASSWORD.
    Digest getPasswordDoubleSHA1() const;

    /// Sets the server name for authentication type LDAP.
    const String & getLDAPServerName() const;
    void setLDAPServerName(const String & name);

    /// Sets the realm name for authentication type KERBEROS.
    const String & getKerberosRealm() const;
    void setKerberosRealm(const String & realm);

    /// Checks the credentials (passwords, readiness, etc.)
    bool areCredentialsValid(const Credentials & credentials, const ExternalAuthenticators & external_authenticators) const;

    friend bool operator ==(const Authentication & lhs, const Authentication & rhs) { return (lhs.type == rhs.type) && (lhs.password_hash == rhs.password_hash); }
    friend bool operator !=(const Authentication & lhs, const Authentication & rhs) { return !(lhs == rhs); }

private:
    static Digest encodePlainText(const std::string_view & text) { return Digest(text.data(), text.data() + text.size()); }
    static Digest encodeSHA256(const std::string_view & text);
    static Digest encodeSHA1(const std::string_view & text);
    static Digest encodeSHA1(const Digest & text) { return encodeSHA1(std::string_view{reinterpret_cast<const char *>(text.data()), text.size()}); }
    static Digest encodeDoubleSHA1(const std::string_view & text) { return encodeSHA1(encodeSHA1(text)); }

    Type type = Type::NO_PASSWORD;
    Digest password_hash;
    String ldap_server_name;
    String kerberos_realm;
};

String toString(Authentication::Type type_);

template <typename CredentialsType>
Authentication::Require<CredentialsType>::Require(const String & realm_)
    : Exception("Credentials required", ErrorCodes::BAD_ARGUMENTS)
    , realm(realm_)
{
}

template <typename CredentialsType>
const String & Authentication::Require<CredentialsType>::getRealm() const
{
    return realm;
}

}
