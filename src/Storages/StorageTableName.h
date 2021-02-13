#pragma once

#include <Storages/StorageProxy.h>

namespace DB
{

class StorageTableName final : public ext::shared_ptr_helper<StorageTableName>, public StorageProxy
{
public:
    StorageTableName(
        const StorageID & table_id, const StorageID & nested_table_id_, ColumnsDescription columns, const Context & global_context_)
        : StorageProxy(table_id), nested_table_id(nested_table_id_), global_context(global_context_)
    {
        StorageInMemoryMetadata local_metadata;
        local_metadata.setColumns(std::move(columns));
        setInMemoryMetadata(local_metadata);
    }

    const StorageID & getNestedStorageID() const
    {
        return nested_table_id;
    }

    StoragePtr getNested() const override;

    String getName() const override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            return nested->getName();
        return StorageProxy::getName();
    }

    void startup() override {}

    void shutdown() override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            nested->shutdown();
    }

    void drop() override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            nested->drop();
    }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override
    {
        auto storage = getNested();
        auto pipe = storage->read(column_names, metadata_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
        return pipe;
    }

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, const Context & context) override
    {
        auto storage = getNested();
        return storage->write(query, metadata_snapshot, context);
    }

    void renameInMemory(const StorageID & new_table_id) override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            StorageProxy::renameInMemory(new_table_id);
        else
            IStorage::renameInMemory(new_table_id);
    }

private:
    mutable std::mutex nested_mutex;
    mutable StoragePtr nested;

    StorageID nested_table_id;
    const Context & global_context;
};

}
