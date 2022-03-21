#pragma once

#include "config_core.h"

#if USE_MYSQL

#include <Storages/StorageProxy.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class StorageMaterializedMySQL final : public shared_ptr_helper<StorageMaterializedMySQL>, public StorageProxy
{
    friend struct shared_ptr_helper<StorageMaterializedMySQL>;
public:
    String getName() const override { return "MaterializedMySQL"; }

    StorageMaterializedMySQL(const StoragePtr & nested_storage_, const IDatabase * database_);

    bool needRewriteQueryWithFinal(const Names & column_names) const override;

    Pipe read(
        const Names & column_names, const StorageSnapshotPtr & metadata_snapshot, SelectQueryInfo & query_info,
        ContextPtr context, QueryProcessingStage::Enum processed_stage, size_t max_block_size, unsigned num_streams) override;

    SinkToStoragePtr write(const ASTPtr &, const StorageMetadataPtr &, ContextPtr) override { throwNotAllowed(); }

    NamesAndTypesList getVirtuals() const override;
    ColumnSizeByName getColumnSizes() const override;

    StoragePtr getNested() const override { return nested_storage; }

    void drop() override { nested_storage->drop(); }

private:
    [[noreturn]] static void throwNotAllowed()
    {
        throw Exception("This method is not allowed for MaterializedMySQL", ErrorCodes::NOT_IMPLEMENTED);
    }

    StoragePtr nested_storage;
    const IDatabase * database;
};

}

#endif
