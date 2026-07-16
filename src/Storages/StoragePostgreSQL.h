#pragma once

#include "config.h"

#if USE_LIBPQXX
#include <Interpreters/Context_fwd.h>
#include <Storages/StorageWithCommonVirtualColumns.h>
#include <Storages/TableNameOrQuery.h>

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

    static Configuration getConfiguration(ASTs engine_args, ContextPtr context, const StorageID * table_id = nullptr);

    static Configuration processNamedCollectionResult(const NamedCollection & named_collection, ContextPtr context_, bool require_table = true);

    /// TLS/SSL certificate and key paths accepted from SQL (table functions, engines, DDL-created
    /// dictionaries) must reside inside `user_files_path`: the files are opened by the server process
    /// with its own privileges, so an unrestricted path would let any user who can define a PostgreSQL
    /// source make the server open arbitrary local certificate and key files. Resolves relative paths
    /// against `user_files_path` (in place) and throws `PATH_ACCESS_DENIED` for paths outside of it.
    /// Not applied to dictionaries defined in server configuration files, which are trusted, and in
    /// clickhouse-local, which runs with the privileges of the user who started it.
    static void validateSSLCertificatePaths(Configuration & configuration, const ContextPtr & context);

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
