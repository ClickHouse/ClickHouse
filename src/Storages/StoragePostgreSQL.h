#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX

#include <ext/shared_ptr_helper.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

#include "pqxx/pqxx"

namespace DB
{
class StoragePostgreSQL final : public ext::shared_ptr_helper<StoragePostgreSQL>, public IStorage
{
    friend struct ext::shared_ptr_helper<StoragePostgreSQL>;
public:
    StoragePostgreSQL(
        const StorageID & table_id_,
        const std::string & remote_database_name_,
        const std::string & remote_table_name_,
        const String connection_str,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const Context & context_);

    String getName() const override { return "PostgreSQL"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    String remote_database_name;
    String remote_table_name;
    Context global_context;

    std::shared_ptr<pqxx::connection> connection;
};
}

#endif
