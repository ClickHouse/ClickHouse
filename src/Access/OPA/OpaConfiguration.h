#pragma once

#include <IO/ConnectionTimeouts.h>
#include <base/types.h>

#include <Poco/URI.h>

#include <memory>
#include <optional>
#include <string_view>
#include <unordered_set>


namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

/** Parsed contents of the `<open_policy_agent>` section of the main configuration file.
  *
  * An instance is immutable once parsed and is published as a `shared_ptr<const OpaConfiguration>`,
  * so a running query keeps a consistent snapshot of the settings even while `SYSTEM RELOAD CONFIG`
  * installs a new one.
  */
struct OpaConfiguration
{
    /// Endpoint returning a single boolean decision. Required.
    Poco::URI uri;

    /// Endpoints enabling the optional features. An absent endpoint means the feature is off.
    std::optional<Poco::URI> batch_uri;
    std::optional<Poco::URI> row_filters_uri;
    std::optional<Poco::URI> column_masking_uri;

    /// Sent as `Authorization: Bearer <token>` when not empty.
    String token;

    /// The `system` database is out of scope by default: clients poll it constantly for metadata,
    /// and routing that traffic to OPA would cost a request per poll.
    bool check_system_database = false;

    bool log_requests = false;
    bool log_responses = false;

    ConnectionTimeouts timeouts;
    size_t max_tries = 3;
    size_t retry_initial_backoff_ms = 50;
    size_t retry_max_backoff_ms = 1000;

    /// Upper bound on the number of resources in one batched request; a larger batch is split.
    size_t max_batch_size = 1000;

    /// Users whose queries bypass OPA entirely, for the administrative and technical accounts that
    /// have to keep working while a policy is broken.
    std::unordered_set<String> exempt_users;

    /// Users a policy may name in the `identity` field of a row filter or a column mask. Empty
    /// means any existing user.
    std::unordered_set<String> allowed_expression_identities;

    /// True when the configuration contains an `<open_policy_agent>` section.
    static bool isConfigured(const Poco::Util::AbstractConfiguration & config);

    /// Parses the section. Throws on a malformed section instead of disabling itself: a
    /// configuration mistake must not silently turn authorization off.
    static OpaConfiguration parse(const Poco::Util::AbstractConfiguration & config);

    bool hasBatch() const { return batch_uri.has_value(); }
    bool hasRowFilters() const { return row_filters_uri.has_value(); }
    bool hasColumnMasking() const { return column_masking_uri.has_value(); }

    /// An object of a database that is out of scope is authorized by native grants alone.
    bool isDatabaseInScope(std::string_view database) const;
    bool isUserExempt(const String & user_name) const;
    bool isIdentityAllowed(const String & user_name) const;
};

using OpaConfigurationPtr = std::shared_ptr<const OpaConfiguration>;

}
