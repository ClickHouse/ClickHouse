#pragma once

#include "config_core.h"

#if USE_MYSQL

#include <base/shared_ptr_helper.h>

#include <Storages/IStorage.h>
#include <Storages/MySQL/MySQLSettings.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <mysqlxx/PoolWithFailover.h>

namespace Poco
{
class Logger;
}

namespace DB
{

/** Implements storage in the MySQL database.
  * Use ENGINE = mysql(host_port, database_name, table_name, user_name, password)
  * Read only.
  */
class StorageMySQL final : public shared_ptr_helper<StorageMySQL>, public IStorage, WithContext
{
    friend struct shared_ptr_helper<StorageMySQL>;
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

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    static StorageMySQLConfiguration getConfiguration(ASTs engine_args, ContextPtr context_, MySQLBaseSettings & storage_settings);

private:
    friend class StorageMySQLSink;

    std::string remote_database_name;
    std::string remote_table_name;
    bool replace_query;
    std::string on_duplicate_clause;

    MySQLSettings mysql_settings;

    mysqlxx::PoolWithFailoverPtr pool;

    Poco::Logger * log;
};

}

#endif
