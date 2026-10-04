#pragma once

#include "config.h"

#if USE_MYSQL

#include <Core/MultiEnum.h>
#include <Core/SettingsEnums.h>
#include <Processors/Sources/MySQLSource.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/StorageWithCommonVirtualColumns.h>
#include <Storages/TableNameOrQuery.h>
#include <mysqlxx/PoolWithFailover.h>

namespace Poco
{
class Logger;
}

namespace DB
{

struct MySQLSettings;
class NamedCollection;
struct StorageID;

/** Implements storage in the MySQL database.
  * Use ENGINE = mysql(host_port, database_name, table_name, user_name, password)
  */
class StorageMySQL final : public StorageWithCommonVirtualColumns, WithContext
{
public:
    StorageMySQL(
        const StorageID & table_id_,
        mysqlxx::PoolWithFailover && pool_,
        const std::string & remote_database_name_,
        const TableNameOrQuery & remote_table_or_query_,
        bool replace_query_,
        const std::string & on_duplicate_clause_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        const MySQLSettings & mysql_settings_);

    std::string getName() const override { return "MySQL"; }

    bool isExternalDatabase() const override { return true; }

    static VirtualColumnsDescription createVirtuals();

    void readImpl(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool async_insert) override;

    struct Configuration
    {
        using Addresses = std::vector<std::pair<String, UInt16>>;

        String host;
        UInt16 port = 0;
        String username = "default";
        String password;
        String database;
        TableNameOrQuery table_or_query;

        /// TLS/SSL credentials. The file paths in it may only come from the server configuration
        /// file, see `validateSSLParams`.
        mysqlxx::SSLParams ssl_params;

        bool replace_query = false;
        String on_duplicate_clause;

        Addresses addresses; /// Failover replicas.
        String addresses_expr;
    };

    static Configuration getConfiguration(ASTs engine_args, ContextPtr context_, MySQLSettings & storage_settings, const StorageID * table_id = nullptr);

    static Configuration processNamedCollectionResult(
        const NamedCollection & named_collection, MySQLSettings & storage_settings,
        ContextPtr context_, bool require_table_or_query = true);

    /// Reads the TLS/SSL credentials from a named collection.
    /// The paths `ssl_ca`, `ssl_cert` and `ssl_key` are only accepted from a collection defined in the
    /// server configuration file, and only if the query did not override them. Everywhere else the
    /// credentials have to be given as contents, in `ssl_ca_pem`, `ssl_cert_pem` and `ssl_key_pem`.
    /// The contents are the SQL-safe form of the same credential, so passing them in a query replaces
    /// the path inherited from the collection, unless the operator marked that path as not overridable.
    static mysqlxx::SSLParams getSSLParams(const NamedCollection & named_collection);

    /// Peels the trailing `ssl_ca_pem = '...'`, `ssl_cert_pem = '...'` and `ssl_key_pem = '...'`
    /// arguments off a positional argument list (the form without a named collection) and removes them
    /// from `arguments`, so that the caller sees only the positional arguments it knows about.
    /// A path (`ssl_ca`, `ssl_cert`, `ssl_key`) is rejected here: it is only accepted from a named
    /// collection defined in the server configuration file, see `getSSLParams`.
    static mysqlxx::SSLParams extractSSLParamsFromArguments(ASTs & arguments, ContextPtr context_);

    static ColumnsDescription getTableStructureFromData(
        mysqlxx::PoolWithFailover & pool_,
        const String & database,
        const TableNameOrQuery & table_or_query,
        const ContextPtr & context_,
        MultiEnum<MySQLDataTypesSupport> type_support);

private:
    friend class StorageMySQLSink;

    std::string remote_database_name;
    TableNameOrQuery remote_table_or_query;
    bool replace_query;
    std::string on_duplicate_clause;

    std::unique_ptr<MySQLSettings> mysql_settings;

    mysqlxx::PoolWithFailoverPtr pool;

    LoggerPtr log;
};

class ReadFromMySQLStep final : public ISourceStep
{
public:
    ReadFromMySQLStep(
        const Block & sample_block_,
        mysqlxx::PoolWithFailoverPtr pool_,
        const std::string & query_str_,
        const MySQLStreamSettings & mysql_input_stream_settings_
    );

    ReadFromMySQLStep(const ReadFromMySQLStep &) = default;
    ReadFromMySQLStep(ReadFromMySQLStep &&) = default;

    String getName() const override { return "ReadFromMySQL"; }

    QueryPlanStepPtr clone() const override
    {
        return std::make_unique<ReadFromMySQLStep>(*this);
    }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    mysqlxx::PoolWithFailoverPtr pool;
    String query_str;
    const MySQLStreamSettings mysql_input_stream_settings;
};

}

#endif
