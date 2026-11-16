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

/// Requires the organization policy `constraints/iam.allowServiceAccountCredentialLifetimeExtension`
/// to exceed the default one-hour ceiling.
inline constexpr Int64 MAX_GCP_IMPERSONATION_LIFETIME_SECONDS = 43200;

inline constexpr std::string_view DEFAULT_GCP_IMPERSONATION_SCOPE = "https://www.googleapis.com/auth/devstorage.read_write";

struct GCPImpersonationParams
{
    std::string target_service_account;

    Strings delegates;

    /// OAuth2 scopes the impersonated token is granted. Empty means read/write access to Cloud Storage.
    Strings scopes;

    Int64 lifetime_seconds = 0;

    std::string endpoint;
};

/// Exchange the source token for a bearer token acting as the target service account.
/// https://cloud.google.com/iam/docs/reference/credentials/rest/v1/projects.serviceAccounts/generateAccessToken
GCPOAuthToken fetchImpersonatedGCPAccessToken(
    const std::string & source_access_token,
    const GCPImpersonationParams & params,
    const ConnectionTimeouts & timeouts,
    const RemoteHostFilter & remote_host_filter,
    HTTPConnectionGroupType group = HTTPConnectionGroupType::HTTP);

Strings parseGCPCommaSeparatedList(const std::string & value);

void validateGCPImpersonationParams(const GCPImpersonationParams & params);

}
