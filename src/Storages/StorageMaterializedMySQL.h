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

class StorageMaterializedMySQL final : public StorageProxy
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageMaterializedMySQL> create(TArgs &&... args)
    {
        return std::make_shared<StorageMaterializedMySQL>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageMaterializedMySQL(CreatePasskey, TArgs &&... args) : StorageMaterializedMySQL{std::forward<TArgs>(args)...}
    {
    }

    String getName() const override { return "MaterializedMySQL"; }

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
    StorageMaterializedMySQL(const StoragePtr & nested_storage_, const IDatabase * database_);

    [[noreturn]] static void throwNotAllowed()
    {
        throw Exception("This method is not allowed for MaterializedMySQL", ErrorCodes::NOT_IMPLEMENTED);
    }

    StoragePtr nested_storage;
    const IDatabase * database;
};

}

#endif
