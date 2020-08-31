#pragma once
#include <Storages/IStorage.h>
#include <TableFunctions/ITableFunction.h>
#include <Processors/Pipe.h>
#include <Storages/StorageProxy.h>
#include <Common/CurrentThread.h>
#include <Processors/Transforms/ConvertingTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCOMPATIBLE_COLUMNS;
}

using GetNestedStorageFunc = std::function<StoragePtr()>;

class StorageTableFunctionProxy final : public StorageProxy
{
public:
    StorageTableFunctionProxy(const StorageID & table_id_, GetNestedStorageFunc get_nested_, ColumnsDescription cached_columns)
    : StorageProxy(table_id_), get_nested(std::move(get_nested_))
    {
        StorageInMemoryMetadata cached_metadata;
        cached_metadata.setColumns(std::move(cached_columns));
        setInMemoryMetadata(cached_metadata);
    }

    StoragePtr getNested() const override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            return nested;

        auto nested_storage = get_nested();
        nested_storage->startup();
        nested = nested_storage;
        get_nested = {};
        return nested;
    }

    StoragePtr maybeGetNested() const
    {
        std::lock_guard lock{nested_mutex};
        return nested;
    }

    String getName() const override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            return nested->getName();
        return StorageProxy::getName();
    }

    void startup() override { }
    void shutdown() override
    {
        auto storage = maybeGetNested();
        if (storage)
            storage->shutdown();
    }

    Pipe read(
            const Names & column_names,
            const StorageMetadataPtr & metadata_snapshot,
            const SelectQueryInfo & query_info,
            const Context & context,
            QueryProcessingStage::Enum processed_stage,
            size_t max_block_size,
            unsigned num_streams) override
    {
        auto storage = getNested();
        auto nested_metadata = storage->getInMemoryMetadataPtr();
        auto pipe = storage->read(column_names, nested_metadata, query_info, context, processed_stage, max_block_size, num_streams);
        if (!pipe.empty())
        {
            pipe.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<ConvertingTransform>(
                       header,
                       metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID()),
                       ConvertingTransform::MatchColumnsMode::Name);
            });
        }
        return pipe;
    }

    BlockOutputStreamPtr write(
            const ASTPtr & query,
            const StorageMetadataPtr & metadata_snapshot,
            const Context & context) override
    {
        auto storage = getNested();
        auto cached_structure = metadata_snapshot->getSampleBlock();
        auto actual_structure = storage->getInMemoryMetadataPtr()->getSampleBlock();
        if (!blocksHaveEqualStructure(actual_structure, cached_structure))
        {
            throw Exception("Source storage and table function have different structure", ErrorCodes::INCOMPATIBLE_COLUMNS);
        }
        return storage->write(query, metadata_snapshot, context);
    }

    void renameInMemory(const StorageID & new_table_id) override
    {
        if (maybeGetNested())
            StorageProxy::renameInMemory(new_table_id);
        else
            IStorage::renameInMemory(new_table_id);
    }

private:
    mutable std::mutex nested_mutex;
    mutable GetNestedStorageFunc get_nested;
    mutable StoragePtr nested;
};

}
