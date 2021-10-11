#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_SQLITE
#include <base/shared_ptr_helper.h>
#include <Storages/IStorage.h>

#include <sqlite3.h> // Y_IGNORE

namespace Poco
{
class Logger;
}

namespace DB
{

class StorageSQLite final : public shared_ptr_helper<StorageSQLite>, public IStorage, public WithContext
{
friend struct shared_ptr_helper<StorageSQLite>;

public:
    using SQLitePtr = std::shared_ptr<sqlite3>;

    StorageSQLite(
        const StorageID & table_id_,
        SQLitePtr sqlite_db_,
        const String & database_path_,
        const String & remote_table_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ContextPtr context_);

    std::string getName() const override { return "SQLite"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

private:
    String remote_table_name;
    String database_path;
    ContextPtr global_context;
    SQLitePtr sqlite_db;
    Poco::Logger * log;
};

}

#endif
