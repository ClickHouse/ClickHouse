#pragma once

#include "config.h"

#if USE_LIBPQXX
#include <Interpreters/Context_fwd.h>
#include <Storages/StorageWithCommonVirtualColumns.h>
#include <Storages/TableNameOrQuery.h>

#include <functional>
#include <string_view>

namespace Poco
{
class Logger;
}

namespace postgres
{
class PoolWithFailover;
using PoolWithFailoverPtr = std::shared_ptr<PoolWithFailover>;
}

namespace DB
{
class NamedCollection;
struct StorageID;
struct PostgreSQLSettings;

class StoragePostgreSQL final : public StorageWithCommonVirtualColumns
{
public:
    StoragePostgreSQL(
        const StorageID & table_id_,
        postgres::PoolWithFailoverPtr pool_,
        const TableNameOrQuery & remote_table_or_query_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        const String & remote_table_schema_ = "",
        const String & on_conflict = "");

    String getName() const override { return "PostgreSQL"; }

    bool isExternalDatabase() const override { return true; }

    static VirtualColumnsDescription createVirtuals();

    void readImpl(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool async_insert) override;

    struct Configuration
    {
        String host;
        UInt16 port = 0;
        String username = "default";
        String password;
        String database;
        TableNameOrQuery table_or_query;
        String schema;
        String on_conflict;

        /// TLS/SSL parameters forwarded to libpq. Empty values keep libpq's defaults.
        String ssl_mode;       /// libpq `sslmode`: disable, allow, prefer, require, verify-ca or verify-full.
        String ssl_root_cert;  /// libpq `sslrootcert`: path to the CA certificate (or the special value `system`).
        String ssl_cert;       /// libpq `sslcert`: path to the client certificate.
        String ssl_key;        /// libpq `sslkey`: path to the client private key.

        std::vector<std::pair<String, UInt16>> addresses; /// Failover replicas.
        String addresses_expr;
    };

    /// How `getConfiguration` / `processNamedCollectionResult` treat TLS/SSL certificate and key
    /// paths found in the arguments (see `validateSSLCertificatePaths`); only the caller can tell
    /// a metadata replay from fresh DDL.
    enum class SSLCertificatePathValidation
    {
        /// Fresh DDL: every SQL-provided path must reside inside `user_files`.
        Enforce,
        /// A replay of previously persisted metadata: values that are part of the persisted
        /// definition (query overrides of a named collection) are exempt from the boundary check,
        /// so a stored definition keeps loading even if `user_files_path` changed since it was
        /// created. The exemption never covers values taken from the named collection store:
        /// those are re-read on every replay and `ALTER NAMED COLLECTION` can change them after
        /// the object was created.
        ReplayExemptPersisted,
        /// The caller merges further settings over the returned configuration (the
        /// `MaterializedPostgreSQL` engines apply the `materialized_postgresql_ssl_*` settings on
        /// top of a named collection) and must validate the merged result itself: validating the
        /// raw named-collection values here would reject a definition whose unsafe collection
        /// value is overridden by a safe persisted setting.
        DeferToCaller,
    };

    /// `storage_settings` may be nullptr for callers that do not honor the `PostgreSQLSettings`
    /// (e.g. the `MaterializedPostgreSQL` engines): the setting names are then rejected in named
    /// collections instead of being accepted and silently ignored.
    static Configuration getConfiguration(ASTs engine_args, ContextPtr context, PostgreSQLSettings * storage_settings, const StorageID * table_id = nullptr, SSLCertificatePathValidation ssl_path_validation = SSLCertificatePathValidation::Enforce);

    static Configuration processNamedCollectionResult(const NamedCollection & named_collection, PostgreSQLSettings * storage_settings, ContextPtr context_, bool require_table = true, SSLCertificatePathValidation ssl_path_validation = SSLCertificatePathValidation::Enforce);

    /// TLS/SSL certificate and key paths accepted from SQL (table functions, engines, DDL-created
    /// dictionaries) must reside inside `user_files_path`: the files are opened by the server process
    /// with its own privileges, so an unrestricted path would let any user who can define a PostgreSQL
    /// source make the server open arbitrary local certificate and key files. Resolves relative paths
    /// against `user_files_path` (in place) and throws `PATH_ACCESS_DENIED` for paths outside of it.
    /// `enforce_user_files_boundary` disables only the latter, for callers replaying persisted
    /// metadata: relative paths are still resolved against `user_files_path`, so a stored definition
    /// keeps the meaning it had at CREATE time. Not applied to dictionaries defined in server
    /// configuration files, which are trusted, and in clickhouse-local, which runs with the
    /// privileges of the user who started it.
    static void validateSSLCertificatePaths(Configuration & configuration, const ContextPtr & context, bool enforce_user_files_boundary = true);

    /// Same, but the boundary is decided per option (`sslrootcert`, `sslcert`, `sslkey`). A metadata
    /// replay can only be exempted from the boundary check for values that are part of the persisted
    /// definition; a value read from a named collection is re-read on every replay and
    /// `ALTER NAMED COLLECTION` can change it in the meantime, so it must be checked again.
    static void validateSSLCertificatePaths(
        Configuration & configuration, const ContextPtr & context, const std::function<bool(std::string_view)> & enforce_user_files_boundary_for);

    static ColumnsDescription getTableStructureFromData(
        const postgres::PoolWithFailoverPtr & pool_,
        const TableNameOrQuery & table_or_query,
        const String & schema,
        const ContextPtr & context_);

private:
    TableNameOrQuery remote_table_or_query;
    String remote_table_schema;
    String on_conflict;
    postgres::PoolWithFailoverPtr pool;

    LoggerPtr log;
};

}

#endif
