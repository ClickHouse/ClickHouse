#pragma once

#include <string>
#include <string_view>
#include <base/types.h>
#include <Core/Types.h>
#include <Common/RemoteHostFilter.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/GCPOAuth.h>
#include <IO/HTTPCommon.h>

namespace DB
{

/// Google refuses a longer lifetime than one hour unless the organization policy
/// `constraints/iam.allowServiceAccountCredentialLifetimeExtension` raises the ceiling to 12 hours, so nothing
/// above this can ever be granted.
inline constexpr Int64 MAX_GCP_IMPERSONATION_LIFETIME_SECONDS = 43200;

/// Read/write access to Cloud Storage is all an impersonated token is ever used for here, so this is the scope
/// granted when `impersonation_scopes` is not set.
inline constexpr std::string_view DEFAULT_GCP_IMPERSONATION_SCOPE = "https://www.googleapis.com/auth/devstorage.read_write";

struct GCPImpersonationParams
{
    std::string target_service_account;

    Strings delegates;  // optional

    /// OAuth2 scopes the impersonated token is granted. Empty means read/write access to Cloud Storage.
    Strings scopes;

    /// Requested lifetime of the impersonated token, from the `impersonation_lifetime_seconds`.
    Int64 lifetime_seconds = 0;

    /// Override the IAM Credentials API endpoint (for test purposes).
    std::string endpoint;
};

/// Exchange `source_access_token` for an access token that acts as `params.target_service_account`:
/// https://cloud.google.com/iam/docs/reference/credentials/rest/v1/projects.serviceAccounts/generateAccessToken
/// This is generally similar to the AWS STS `AssumeRole` call. Unlike `AssumeRole`, which returns a key pair
/// plus a session token to sign with, this returns a bearer token.
GCPOAuthToken fetchImpersonatedGCPAccessToken(
    const std::string & source_access_token,
    const GCPImpersonationParams & params,
    const ConnectionTimeouts & timeouts,
    const RemoteHostFilter & remote_host_filter,
    HTTPConnectionGroupType group = HTTPConnectionGroupType::HTTP);

Strings parseGCPCommaSeparatedList(const std::string & value);

void validateGCPImpersonationParams(const GCPImpersonationParams & params);

}
