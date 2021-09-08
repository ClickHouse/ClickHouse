#pragma once
#include <Storages/IStorage.h>
#include <TableFunctions/ITableFunction.h>
#include <Processors/Pipe.h>
#include <Storages/StorageProxy.h>
#include <Common/CurrentThread.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/getHeaderForProcessingStage.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCOMPATIBLE_COLUMNS;
}

using GetNestedStorageFunc = std::function<StoragePtr()>;

/// Lazily creates underlying storage.
/// Adds ConversionTransform in case of structure mismatch.
class StorageTableFunctionProxy final : public StorageProxy
{
public:
    StorageTableFunctionProxy(const StorageID & table_id_, GetNestedStorageFunc get_nested_,
            ColumnsDescription cached_columns, bool add_conversion_ = true)
    : StorageProxy(table_id_), get_nested(std::move(get_nested_)), add_conversion(add_conversion_)
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
        nested_storage->renameInMemory(getStorageID());
        nested = nested_storage;
        get_nested = {};
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
        std::lock_guard lock{nested_mutex};
        if (nested)
            nested->shutdown();
    }

    void flush() override
    {
        std::lock_guard lock{nested_mutex};
        if (nested)
            nested->flush();
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
            ContextPtr context,
            QueryProcessingStage::Enum processed_stage,
            size_t max_block_size,
            unsigned num_streams) override
    {
        String cnames;
        for (const auto & c : column_names)
            cnames += c + " ";
        auto storage = getNested();
        auto nested_metadata = storage->getInMemoryMetadataPtr();
        auto pipe = storage->read(column_names, nested_metadata, query_info, context,
                                  processed_stage, max_block_size, num_streams);
        if (!pipe.empty() && add_conversion)
        {
            auto to_header = getHeaderForProcessingStage(*this, column_names, metadata_snapshot,
                                                         query_info, context, processed_stage);

            auto convert_actions_dag = ActionsDAG::makeConvertingActions(
                    pipe.getHeader().getColumnsWithTypeAndName(),
                    to_header.getColumnsWithTypeAndName(),
                    ActionsDAG::MatchColumnsMode::Name);
            auto convert_actions = std::make_shared<ExpressionActions>(
                convert_actions_dag,
                ExpressionActionsSettings::fromSettings(context->getSettingsRef(), CompileExpressions::yes));

            pipe.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<ExpressionTransform>(header, convert_actions);
            });
        }
        return pipe;
    }

    BlockOutputStreamPtr write(
            const ASTPtr & query,
            const StorageMetadataPtr & metadata_snapshot,
            ContextPtr context) override
    {
        auto storage = getNested();
        auto cached_structure = metadata_snapshot->getSampleBlock();
        auto actual_structure = storage->getInMemoryMetadataPtr()->getSampleBlock();
        if (!blocksHaveEqualStructure(actual_structure, cached_structure) && add_conversion)
        {
            throw Exception("Source storage and table function have different structure", ErrorCodes::INCOMPATIBLE_COLUMNS);
        }
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

    bool isView() const override { return false; }
    void checkTableCanBeDropped() const override {}

private:
    mutable std::mutex nested_mutex;
    mutable GetNestedStorageFunc get_nested;
    mutable StoragePtr nested;
    const bool add_conversion;
};

}
