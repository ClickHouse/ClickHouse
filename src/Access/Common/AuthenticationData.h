#pragma once

#include <base/types.h>
#include <boost/container/flat_set.hpp>
#include <vector>

namespace DB
{

enum class AuthenticationType
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

    /// Authentication is done in SSL by checking user certificate.
    /// Certificates may only be trusted if 'strict' SSL mode is enabled.
    SSL_CERTIFICATE,

    MAX,
};

struct AuthenticationTypeInfo
{
    const char * const raw_name;
    const String name; /// Lowercased with underscores, e.g. "sha256_password".
    static const AuthenticationTypeInfo & get(AuthenticationType type_);
};

inline String toString(AuthenticationType type_)
{
    return AuthenticationTypeInfo::get(type_).raw_name;
}


/// Stores data for checking password when a user logins.
class AuthenticationData
{
public:
    using Digest = std::vector<uint8_t>;

    explicit AuthenticationData(AuthenticationType type_ = AuthenticationType::NO_PASSWORD) : type(type_) {}
    AuthenticationData(const AuthenticationData & src) = default;
    AuthenticationData & operator =(const AuthenticationData & src) = default;
    AuthenticationData(AuthenticationData && src) = default;
    AuthenticationData & operator =(AuthenticationData && src) = default;

    AuthenticationType getType() const { return type; }

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

    /// Sets the server name for authentication type LDAP.
    const String & getLDAPServerName() const { return ldap_server_name; }
    void setLDAPServerName(const String & name) { ldap_server_name = name; }

    /// Sets the realm name for authentication type KERBEROS.
    const String & getKerberosRealm() const { return kerberos_realm; }
    void setKerberosRealm(const String & realm) { kerberos_realm = realm; }

    const boost::container::flat_set<String> & getSSLCertificateCommonNames() const { return ssl_certificate_common_names; }
    void setSSLCertificateCommonNames(boost::container::flat_set<String> common_names_);

    friend bool operator ==(const AuthenticationData & lhs, const AuthenticationData & rhs);
    friend bool operator !=(const AuthenticationData & lhs, const AuthenticationData & rhs) { return !(lhs == rhs); }

    struct Util
    {
        static Digest stringToDigest(const std::string_view & text) { return Digest(text.data(), text.data() + text.size()); }
        static Digest encodeSHA256(const std::string_view & text);
        static Digest encodeSHA1(const std::string_view & text);
        static Digest encodeSHA1(const Digest & text) { return encodeSHA1(std::string_view{reinterpret_cast<const char *>(text.data()), text.size()}); }
        static Digest encodeDoubleSHA1(const std::string_view & text) { return encodeSHA1(encodeSHA1(text)); }
        static Digest encodeDoubleSHA1(const Digest & text) { return encodeSHA1(encodeSHA1(text)); }
    };

private:
    AuthenticationType type = AuthenticationType::NO_PASSWORD;
    Digest password_hash;
    String ldap_server_name;
    String kerberos_realm;
    boost::container::flat_set<String> ssl_certificate_common_names;
};

}
