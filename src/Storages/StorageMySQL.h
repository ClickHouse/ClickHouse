#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#    include <ext/shared_ptr_helper.h>

#    include <Interpreters/Context.h>
#    include <Storages/IStorage.h>
#    include <mysqlxx/Pool.h>


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
        const StorageID & table_id_,
        mysqlxx::Pool && pool_,
        const std::string & remote_database_name_,
        const std::string & remote_table_name_,
        const bool replace_query_,
        const std::string & on_duplicate_clause_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const Context & context_);

    std::string getName() const override { return "MySQL"; }

    Pipes read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, const Context & context) override;

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
