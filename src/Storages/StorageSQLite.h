#pragma once

#include <sqlite3.h>

#include <Storages/IStorage.h>

#include <ext/shared_ptr_helper.h>


namespace DB
{
class StorageSQLite final : public ext::shared_ptr_helper<StorageSQLite>, public IStorage, public WithContext
{
    friend struct ext::shared_ptr_helper<StorageSQLite>;

public:
    StorageSQLite(
        const StorageID & table_id_,
        std::shared_ptr<sqlite3> db_ptr_,
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

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

private:
    String remote_table_name;
    ContextPtr global_context;
    std::shared_ptr<sqlite3> db_ptr;
};

}
