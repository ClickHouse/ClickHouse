#pragma once

#include "config.h"

#if USE_MYSQL

#include <Processors/Sources/MySQLSource.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/IStorage.h>
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
class StorageMySQL final : public IStorage, WithContext
{
public:
    StorageMySQL(
        const StorageID & table_id_,
        mysqlxx::PoolWithFailover && pool_,
        const std::string & remote_database_name_,
        const std::string & remote_table_name_,
        bool replace_query_,
        const std::string & on_duplicate_clause_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        const MySQLSettings & mysql_settings_);

    std::string getName() const override { return "MySQL"; }

    bool isExternalDatabase() const override { return true; }

    void read(
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
        String table;

        String ssl_ca;
        String ssl_cert;
        String ssl_key;

        bool replace_query = false;
        String on_duplicate_clause;

        Addresses addresses; /// Failover replicas.
        String addresses_expr;
    };

    static Configuration getConfiguration(ASTs engine_args, ContextPtr context_, MySQLSettings & storage_settings, const StorageID * table_id = nullptr);

    static Configuration processNamedCollectionResult(
        const NamedCollection & named_collection, MySQLSettings & storage_settings,
        ContextPtr context_, bool require_table = true);

    static ColumnsDescription getTableStructureFromData(
        mysqlxx::PoolWithFailover & pool_,
        const String & database,
        const String & table,
        const ContextPtr & context_);

private:
    friend class StorageMySQLSink;

    std::string remote_database_name;
    std::string remote_table_name;
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
        const StreamSettings & mysql_input_stream_settings_
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
    const StreamSettings mysql_input_stream_settings;
};

}

#endif
