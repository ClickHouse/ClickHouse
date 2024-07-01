#pragma once

#include <Access/Common/AuthenticationType.h>
#include <Access/Common/HTTPAuthenticationScheme.h>
#include <Common/SSHWrapper.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/Access/ASTAuthenticationData.h>

#include <vector>
#include <base/types.h>
#include <boost/container/flat_set.hpp>
#include "config.h"
#if USE_SSL
    #include <Access/SSH/SSHPublicKey.h>
#endif
#include <vector>

#include "config.h"

namespace DB
{

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

    /// Sets the salt in String form.
    void setSalt(String salt);
    String getSalt() const;

    /// Sets the password using bcrypt hash with specified workfactor
    void setPasswordBcrypt(const String & password_, int workfactor_);

    /// Sets the server name for authentication type LDAP.
    const String & getLDAPServerName() const { return ldap_server_name; }
    void setLDAPServerName(const String & name) { ldap_server_name = name; }

    /// Sets the realm name for authentication type KERBEROS.
    const String & getKerberosRealm() const { return kerberos_realm; }
    void setKerberosRealm(const String & realm) { kerberos_realm = realm; }

    const boost::container::flat_set<String> & getSSLCertificateCommonNames() const { return ssl_certificate_common_names; }
    void setSSLCertificateCommonNames(boost::container::flat_set<String> common_names_);

#if USE_SSH
    const std::vector<SSHKey> & getSSHKeys() const { return ssh_keys; }
    void setSSHKeys(std::vector<SSHKey> && ssh_keys_) { ssh_keys = std::forward<std::vector<SSHKey>>(ssh_keys_); }
#endif

    HTTPAuthenticationScheme getHTTPAuthenticationScheme() const { return http_auth_scheme; }
    void setHTTPAuthenticationScheme(HTTPAuthenticationScheme scheme) { http_auth_scheme = scheme; }

    const String & getHTTPAuthenticationServerName() const { return http_auth_server_name; }
    void setHTTPAuthenticationServerName(const String & name) { http_auth_server_name = name; }

    friend bool operator ==(const AuthenticationData & lhs, const AuthenticationData & rhs);
    friend bool operator !=(const AuthenticationData & lhs, const AuthenticationData & rhs) { return !(lhs == rhs); }

    static AuthenticationData fromAST(const ASTAuthenticationData & query, ContextPtr context, bool check_password_rules);
    std::shared_ptr<ASTAuthenticationData> toAST() const;

    struct Util
    {
        static String digestToString(const Digest & text) { return String(text.data(), text.data() + text.size()); }
        static Digest stringToDigest(std::string_view text) { return Digest(text.data(), text.data() + text.size()); }
        static Digest encodeSHA256(std::string_view text);
        static Digest encodeSHA1(std::string_view text);
        static Digest encodeSHA1(const Digest & text) { return encodeSHA1(std::string_view{reinterpret_cast<const char *>(text.data()), text.size()}); }
        static Digest encodeDoubleSHA1(std::string_view text) { return encodeSHA1(encodeSHA1(text)); }
        static Digest encodeDoubleSHA1(const Digest & text) { return encodeSHA1(encodeSHA1(text)); }
        static Digest encodeBcrypt(std::string_view text, int workfactor);
        static bool checkPasswordBcrypt(std::string_view password, const Digest & password_bcrypt);
    };

private:
    AuthenticationType type = AuthenticationType::NO_PASSWORD;
    Digest password_hash;
    String ldap_server_name;
    String kerberos_realm;
    boost::container::flat_set<String> ssl_certificate_common_names;
    String salt;
#if USE_SSH
    std::vector<SSHKey> ssh_keys;
#endif
    /// HTTP authentication properties
    String http_auth_server_name;
    HTTPAuthenticationScheme http_auth_scheme = HTTPAuthenticationScheme::BASIC;
};

}
