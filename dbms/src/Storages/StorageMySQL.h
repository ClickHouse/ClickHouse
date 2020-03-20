#pragma once

#include "config_core.h"
#if USE_MYSQL

#include <ext/shared_ptr_helper.h>

#include <Storages/IStorage.h>
#include <Interpreters/Context.h>
#include <mysqlxx/Pool.h>


namespace DB
{

/** Implements storage in the MySQL database.
  * Use ENGINE = mysql(host_port, database_name, table_name, user_name, password)
  * Read only.
  */
class StorageMySQL final : public ext::shared_ptr_helper<StorageMySQL>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageMySQL>;
public:
    StorageMySQL(
        StorageID table_id_,
        mysqlxx::Pool && pool_,
        std::string remote_database_name_,
        std::string remote_table_name_,
        const bool replace_query_,
        std::string on_duplicate_clause_,
        ColumnsDescription columns_,
        ConstraintsDescription constraints_,
        const Context & context_);

    std::string getName() const override { return "MySQL"; }

    Pipes read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Context & context) override;

private:
    friend class StorageMySQLBlockOutputStream;

    std::string remote_database_name;
    std::string remote_table_name;
    bool replace_query;
    std::string on_duplicate_clause;

    mysqlxx::Pool pool;
    Context global_context;
};

}

#endif
