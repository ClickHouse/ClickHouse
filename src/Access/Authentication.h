#pragma once

#include <Access/AuthenticationData.h>
#include <Common/Exception.h>
#include <Interpreters/ClientInfo.h>
#include <base/types.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class Credentials;
class ExternalAuthenticators;
class SettingsChanges;

/// TODO: Try to move this checking to Credentials.
struct Authentication
{

    enum class CredentialsCheckResult : UInt8
    {
        Fail,
        NeedSecondFactor,
        Success
    };

    /// Checks the credentials (passwords, readiness, etc.)
    /// If necessary, makes a request to external authenticators and fills in the session settings if they were
    /// returned by the authentication server
    static CredentialsCheckResult areCredentialsValid(
        const Credentials & credentials,
        const AuthenticationData & authentication_method,
        const ExternalAuthenticators & external_authenticators,
        const ClientInfo & client_info,
        SettingsChanges & settings);

    /// Marks the one-time password contained in the credentials as used, enforcing single use
    /// of the codes (RFC 6238, Section 5.2). Returns false if the code has already been used before.
    /// Must be called exactly once after the whole authentication succeeds, so that a failed attempt
    /// (e.g. a wrong password combined with a valid code) cannot consume the code.
    static bool consumeOneTimePassword(const Credentials & credentials, const AuthenticationData & authentication_method);

    /** The `mysql_native_password` auth response (`MySQLNative41Credentials`) is a hash of the password alone, so it can be
      * verified only against `plaintext_password` and `double_sha1_password` methods, and it cannot carry a one-time password.
      * Accepting it for a user having any method for which this returns true would be wrong in two ways:
      *  - a method with a second factor would be accepted without the one-time password, bypassing it;
      *  - `IAccessStorage::authenticateImpl` fails close for ambiguous credentials: when the same password is accepted by
      *    several methods, the session is limited to the intersection of their `GRANTS` and expires at the earliest of
      *    their `VALID UNTIL`. A `sha256_password`, `scram_sha256_password` or `bcrypt_password` method cannot be matched
      *    by the Native41 response, so it would silently drop out of that combination, and the login would regain
      *    the broader rights or lifetime of the other method.
      * Methods verified against an external system never take part in that combination and are not password hashes
      * that a plaintext password could check better, so they do not count.
      */
    static bool mysqlNative41CannotVerify(const AuthenticationData & authentication_method);

    // A signaling class used to communicate requirements for credentials.
    template <typename CredentialsType>
    class Require : public Exception
    {
    public:
        explicit Require(const String & realm_);
        const String & getRealm() const;

        Require * clone() const override { return new Require(*this); }
        void rethrow() const override { throw *this; } /// NOLINT(bugprone-exception-copy-constructor-throws,cert-err60-cpp)

    private:
        const String realm;
    };
};


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
