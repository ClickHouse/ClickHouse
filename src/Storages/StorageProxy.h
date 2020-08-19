#pragma once
#include <Storages/IStorage.h>
#include <TableFunctions/ITableFunction.h>

namespace DB
{

template<typename StorageT>
class StorageTableFunction : public StorageT
{
public:
    using GetStructureFunc = std::function<ColumnsDescription()>;

    template<typename... StorageArgs>
    StorageTableFunction(GetStructureFunc get_structure_, StorageArgs && ... args)
    : StorageT(std::forward<StorageArgs>(args)...), get_structure(std::move(get_structure_))
    {
    }

    Pipe read(
            const Names & column_names,
            const StorageMetadataPtr & metadata_snapshot,
            const SelectQueryInfo & query_info,
            const Context & context,
            QueryProcessingStage::Enum processed_stage,
            size_t max_block_size,
            unsigned num_streams)
    {
        assertBlocksHaveEqualStructure();
        return StorageT::read(column_names, metadata_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
    }

    BlockOutputStreamPtr write(
            const ASTPtr & query,
            const StorageMetadataPtr & metadata_snapshot,
            const Context & context)
    {
        assertBlocksHaveEqualStructure();
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
        assertBlocksHaveEqualStructure(getInMemoryMetadataPtr()->getSampleBlock(), actual_source_structure);

        get_structure = {};
    }

    GetStructureFunc get_structure;
    Block actual_source_structure;
};

}
