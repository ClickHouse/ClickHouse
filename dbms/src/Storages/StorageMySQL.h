#pragma once

#include "config_core.h"
#if USE_MYSQL

#include <ext/shared_ptr_helper.h>

#include <Storages/IStorage.h>
#include <mysqlxx/Pool.h>


namespace DB
{

/** Implements storage in the MySQL database.
  * Use ENGINE = mysql(host_port, database_name, table_name, user_name, password)
  * Read only.
  */
class StorageMySQL : public ext::shared_ptr_helper<StorageMySQL>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageMySQL>;
public:
    StorageMySQL(
        const std::string & database_name_,
        const std::string & table_name_,
        mysqlxx::Pool && pool_,
        const std::string & remote_database_name_,
        const std::string & remote_table_name_,
        const bool replace_query_,
        const std::string & on_duplicate_clause_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const Context & context_);

    std::string getName() const override { return "MySQL"; }
    std::string getTableName() const override { return table_name; }
    std::string getDatabaseName() const override { return database_name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Context & context) override;

private:
    friend class StorageMySQLBlockOutputStream;
    std::string table_name;
    std::string database_name;

    std::string remote_database_name;
    std::string remote_table_name;
    bool replace_query;
    std::string on_duplicate_clause;

    mysqlxx::Pool pool;
    Context global_context;
};

}

#endif
