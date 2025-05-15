#pragma once

#include "config.h"

#if USE_LANCE

#    include "LanceDB.h"
#    include "LanceTable.h"

#    include <Storages/IStorage.h>

namespace DB
{

class StorageLance final : public IStorage
{
public:
    StorageLance(
        const StorageID & table_id_,
        LanceDBPtr lance_db_,
        const String & table_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment_);

    String getName() const override { return "Lance"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*context_*/,
        QueryProcessingStage::Enum,
        size_t max_block_size,
        size_t /*num_streams*/) override;

    SinkToStoragePtr
    write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr /*context*/, bool /*async_insert*/) override;

    void alter(const AlterCommands & commands, ContextPtr local_context, AlterLockHolder & alter_lock_holder) override;
    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr /*local_context*/) const override;

    void drop() override;

private:
    LanceDBPtr lance_db;
    String table_name;
    LanceTablePtr lance_table{nullptr};
};

} // namespace DB

#endif
