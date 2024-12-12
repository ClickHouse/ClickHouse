#pragma once

#include <base/types.h>
#include <Parsers/CommonParsers.h>

namespace DB
{

enum class AuthenticationType : uint8_t
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

    /// Password is encrypted in bcrypt hash.
    BCRYPT_PASSWORD,

    /// Server sends a random string named `challenge` to the client. The client encrypts it with its SSH private key.
    /// The server decrypts the result using the SSH public key registered for the user and compares with the original string.
    SSH_KEY,

    /// Authentication through HTTP protocol
    HTTP,

    /// JSON Web Token
    JWT,

    MAX,
};

struct AuthenticationTypeInfo
{
    Keyword keyword; // Keyword used in parser
    const String name; /// Lowercased with underscores, e.g. "sha256_password".
    bool is_password;
    static const AuthenticationTypeInfo & get(AuthenticationType type_);
};

inline String toString(AuthenticationType type_)
{
    return String(toStringView(AuthenticationTypeInfo::get(type_).keyword));
}

}
