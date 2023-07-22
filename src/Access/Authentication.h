#pragma once

#include <Access/Common/AuthenticationData.h>
#include <Common/Exception.h>
#include <base/types.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class Credentials;
class ExternalAuthenticators;

/// TODO: Try to move this checking to Credentials.
struct Authentication
{
    /// Checks the credentials (passwords, readiness, etc.)
    static bool areCredentialsValid(const Credentials & credentials, const AuthenticationData & auth_data, const ExternalAuthenticators & external_authenticators);

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
