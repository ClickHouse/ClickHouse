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


inline const Authentication::TypeInfo & Authentication::TypeInfo::get(Type type_)
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

inline String toString(Authentication::Type type_)
{
    return Authentication::TypeInfo::get(type_).raw_name;
}


inline Authentication::Digest Authentication::encodeSHA256(const std::string_view & text [[maybe_unused]])
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

inline Authentication::Digest Authentication::encodeSHA1(const std::string_view & text)
{
    Poco::SHA1Engine engine;
    engine.update(text.data(), text.size());
    return engine.digest();
}


inline void Authentication::setPassword(const String & password_)
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


inline String Authentication::getPassword() const
{
    if (type != PLAINTEXT_PASSWORD)
        throw Exception("Cannot decode the password", ErrorCodes::LOGICAL_ERROR);
    return String(password_hash.data(), password_hash.data() + password_hash.size());
}


inline void Authentication::setPasswordHashHex(const String & hash)
{
    Digest digest;
    digest.resize(hash.size() / 2);
    boost::algorithm::unhex(hash.begin(), hash.end(), digest.data());
    setPasswordHashBinary(digest);
}

inline String Authentication::getPasswordHashHex() const
{
    if (type == LDAP || type == KERBEROS)
        throw Exception("Cannot get password hex hash for authentication type " + toString(type), ErrorCodes::LOGICAL_ERROR);

    String hex;
    hex.resize(password_hash.size() * 2);
    boost::algorithm::hex(password_hash.begin(), password_hash.end(), hex.data());
    return hex;
}


inline void Authentication::setPasswordHashBinary(const Digest & hash)
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

inline const String & Authentication::getLDAPServerName() const
{
    return ldap_server_name;
}

inline void Authentication::setLDAPServerName(const String & name)
{
    ldap_server_name = name;
}

inline const String & Authentication::getKerberosRealm() const
{
    return kerberos_realm;
}

inline void Authentication::setKerberosRealm(const String & realm)
{
    kerberos_realm = realm;
}

}
