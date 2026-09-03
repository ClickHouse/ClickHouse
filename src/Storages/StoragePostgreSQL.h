#pragma once

#include "config.h"

#if USE_LIBPQXX
#include <Core/PostgreSQL/ConnectionSSLParams.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
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
        postgres::ConnectionSSLParams ssl;

        std::vector<std::pair<String, UInt16>> addresses; /// Failover replicas.
        String addresses_expr;
    };

    /// `storage_settings` may be nullptr for callers that do not honor the `PostgreSQLSettings`
    /// (e.g. the `MaterializedPostgreSQL` engines): the setting names are then rejected in named
    /// collections instead of being accepted and silently ignored.
    static Configuration getConfiguration(ASTs engine_args, ContextPtr context, PostgreSQLSettings * storage_settings, const StorageID * table_id = nullptr);

    static Configuration processNamedCollectionResult(const NamedCollection & named_collection, PostgreSQLSettings * storage_settings, ContextPtr context_, bool require_table = true);

    /// Reads the TLS/SSL parameters from a named collection: `sslmode`, the certificate and key
    /// paths (`sslrootcert` / `sslcert` / `sslkey`) and their contents forms (`sslrootcert_pem` /
    /// `sslcert_pem` / `sslkey_pem`). A path is only accepted from a named collection defined in
    /// the server configuration file and cannot be overridden in a query; the contents forms are
    /// accepted from anywhere and are masked like passwords. Throws `BAD_ARGUMENTS` otherwise.
    static postgres::ConnectionSSLParams getSSLParams(const NamedCollection & named_collection);

    /// Extracts trailing `key = value` TLS/SSL arguments (`sslmode` and the contents forms) from a
    /// positional argument list, e.g. `postgresql('host:port', 'db', 'table', 'user', 'password',
    /// sslmode = 'verify-full', sslrootcert_pem = '...')`. A certificate or key path there is
    /// rejected: it is only accepted from the server configuration file. The extracted arguments
    /// are removed from `arguments`.
    static postgres::ConnectionSSLParams extractSSLParamsFromArguments(ASTs & arguments, ContextPtr context_);

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
