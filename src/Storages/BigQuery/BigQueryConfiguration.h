#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace DB
{

struct StorageID;

/// Connection and credential settings shared by the `bigquery` table function and the `BigQuery` table engine.
struct BigQueryConfiguration
{
    /// Google Cloud project that owns the dataset (for public datasets: the project of the dataset, e.g. "bigquery-public-data").
    String project;
    String dataset;
    String table;

    /// Exactly one authentication method must be provided.
    /// 1. A ready-made OAuth 2.0 access token (e.g. from `gcloud auth print-access-token`). Expires quickly, cannot be refreshed.
    String access_token;
    /// 2. The content of a Google service account key in JSON format (with `client_email` and `private_key`).
    String service_account_key;
    /// 3. An OAuth 2.0 client with a refresh token, as in Application Default Credentials
    ///    (`~/.config/gcloud/application_default_credentials.json` after `gcloud auth application-default login`).
    String client_id;
    String client_secret;
    String refresh_token;

    /// Optional project to attribute quota and billing to (sent as the `X-Goog-User-Project` header).
    String billing_project;
    /// REST API endpoint, can be overridden for tests and emulators.
    String base_url = "https://bigquery.googleapis.com";
    /// OAuth token endpoint override for tests and emulators.
    /// When empty, the `token_uri` of the service account key or the Google OAuth endpoint is used.
    String token_url;

    enum class CredentialsKind
    {
        AccessToken,
        ServiceAccountKey,
        RefreshToken,
    };

    CredentialsKind credentials_kind = CredentialsKind::AccessToken;

    /// The name of the named collection the configuration was created from (empty when the arguments
    /// were positional or in the `key = value` form). The table function reports it through
    /// `ITableFunction::getUsedNamedCollectionName` so that a permanent `CREATE TABLE ... AS bigquery(...)`
    /// table is registered as a dependency of the collection.
    String named_collection_name;

    /// Parses arguments of the table function or the table engine:
    ///   bigquery('project', 'dataset', 'table'[, 'access_token'][, key = value, ...])
    ///   bigquery(named_collection[, key = value, ...])
    /// When `table_id` is provided (the persistent table engine), the table is registered as a dependency
    /// of the named collection so that `DROP NAMED COLLECTION` is blocked while the table exists.
    static BigQueryConfiguration fromArguments(ASTs & args, ContextPtr context, const StorageID * table_id = nullptr);
};

}
