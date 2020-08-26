#pragma once
#include <Storages/IStorage.h>
#include <TableFunctions/ITableFunction.h>
#include <Processors/Pipe.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCOMPATIBLE_COLUMNS;
}

using GetStructureFunc = std::function<ColumnsDescription()>;

template<typename StorageT>
class StorageTableFunction : public StorageT
{
public:

    template<typename... StorageArgs>
    StorageTableFunction(GetStructureFunc get_structure_, StorageArgs && ... args)
    : StorageT(std::forward<StorageArgs>(args)...), get_structure(std::move(get_structure_))
    {
    }

    String getName() const { return "TableFunction" + StorageT::getName(); }

    Pipe read(
            const Names & column_names,
            const StorageMetadataPtr & metadata_snapshot,
            const SelectQueryInfo & query_info,
            const Context & context,
            QueryProcessingStage::Enum processed_stage,
            size_t max_block_size,
            unsigned num_streams)
    {
        assertSourceStructure();
        return StorageT::read(column_names, metadata_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
    }

    BlockOutputStreamPtr write(
            const ASTPtr & query,
            const StorageMetadataPtr & metadata_snapshot,
            const Context & context)
    {
        assertSourceStructure();
        return StorageT::write(query, metadata_snapshot, context);
    }

private:
    void assertSourceStructure()
    {
        if (!get_structure)
            return;

        StorageInMemoryMetadata source_metadata;
        source_metadata.setColumns(get_structure());
        actual_source_structure = source_metadata.getSampleBlock();
        if (!blocksHaveEqualStructure(StorageT::getInMemoryMetadataPtr()->getSampleBlock(), actual_source_structure))
            throw Exception("Source storage and table function have different structure", ErrorCodes::INCOMPATIBLE_COLUMNS);

        get_structure = {};
    }

    GetStructureFunc get_structure;
    Block actual_source_structure;
};

}
