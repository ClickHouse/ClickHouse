#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/BigQuery/BigQueryConfiguration.h>
#include <Common/logger_useful.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/URI.h>

#include <chrono>
#include <memory>
#include <mutex>

namespace DB
{

/// Produces OAuth 2.0 bearer tokens for the configured credential method, with caching until expiry.
/// The context is supplied per call (not stored) so that a single provider can be shared across
/// queries - each query fetches or refreshes the token using its own context.
class BigQueryTokenProvider
{
public:
    explicit BigQueryTokenProvider(BigQueryConfiguration configuration_);

    String getToken(const ContextPtr & context, bool force_refresh);
    /// Whether requesting a fresh token can produce a different one (false for a static access token).
    bool canRefresh() const { return configuration.credentials_kind != BigQueryConfiguration::CredentialsKind::AccessToken; }

private:
    std::pair<String, Int64> fetchTokenWithExpiration(const ContextPtr & context) const;

    const BigQueryConfiguration configuration;

    std::mutex mutex;
    String cached_token;
    std::chrono::system_clock::time_point expires_at{};
};

/// A thin client for the BigQuery v2 REST API (https://cloud.google.com/bigquery/docs/reference/rest).
class BigQueryClient
{
public:
    /// Creates a client with its own short-lived token provider (for a one-off request).
    BigQueryClient(const BigQueryConfiguration & configuration_, ContextPtr context_);
    /// Creates a client that reuses a longer-lived token provider, so an access token minted by one
    /// query survives for the next one instead of being re-requested from the token endpoint.
    BigQueryClient(const BigQueryConfiguration & configuration_, ContextPtr context_, std::shared_ptr<BigQueryTokenProvider> token_provider_);

    /// tables.get: the full table resource (schema, numRows, type).
    Poco::JSON::Object::Ptr getTable() const;

    struct TableDataPage
    {
        Poco::JSON::Array::Ptr rows;    /// null for an empty table
        String next_page_token;         /// empty when this is the last page
        UInt64 total_rows = 0;
    };

    /// tabledata.list. Timestamps are requested as int64 microseconds (formatOptions.useInt64Timestamp).
    /// `selected_fields` is a comma-separated list of columns, empty means all columns.
    TableDataPage listTableData(const String & page_token, const String & selected_fields, UInt64 max_results) const;

    /// tabledata.insertAll (streaming insert). Throws if any row of this request is rejected.
    /// A single `INSERT` is split into several such requests, which are not atomic with respect to
    /// each other: a failure here does not roll back rows accepted by earlier requests. Each row may
    /// carry a stable `insertId` for BigQuery's best-effort deduplication of retried rows.
    void insertAll(const Poco::JSON::Array::Ptr & rows) const;

private:
    Poco::JSON::Object::Ptr requestJSON(
        const String & method,
        const String & path,
        const Poco::URI::QueryParameters & params,
        const String & request_body) const;

    String tablePath() const;

    BigQueryConfiguration configuration;
    ContextPtr context;
    std::shared_ptr<BigQueryTokenProvider> token_provider;
    Poco::Net::HTTPBasicCredentials credentials;    /// empty, required by the HTTP buffer API
    LoggerPtr log;
};

}
