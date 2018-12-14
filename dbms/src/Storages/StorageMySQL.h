#pragma once

#include <Common/config.h>
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
public:
    StorageMySQL(
        const std::string & name,
        mysqlxx::Pool && pool,
        const std::string & remote_database_name,
        const std::string & remote_table_name,
        const bool replace_query,
        const std::string & on_duplicate_clause,
        const ColumnsDescription & columns,
        const Context & context);

    std::string getName() const override { return "MySQL"; }
    std::string getTableName() const override { return name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

private:
    friend class StorageMySQLBlockOutputStream;
    std::string name;

    std::string remote_database_name;
    std::string remote_table_name;
    bool replace_query;
    std::string on_duplicate_clause;


    mysqlxx::Pool pool;
    const Context & context;
};

}

#endif
