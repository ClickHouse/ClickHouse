#pragma once
#include <DataTypes/DataTypeString.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <ext/shared_ptr_helper.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Pipe.h>

namespace DB
{

class Context;


/** Base class for system tables whose all columns have String type.
  */
template <typename Self>
class IStorageSystemOneBlock : public IStorage
{
protected:
    virtual void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const = 0;

public:
    IStorageSystemOneBlock(const String & name_) : IStorage({"system", name_})
    {
        StorageInMemoryMetadata metadata_;
        metadata_.setColumns(ColumnsDescription(Self::getNamesAndTypes()));
        setInMemoryMetadata(metadata_);
    }

    Pipes read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override
    {
        metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

        Block sample_block = metadata_snapshot->getSampleBlock();
        MutableColumns res_columns = sample_block.cloneEmptyColumns();
        fillData(res_columns, context, query_info);

        UInt64 num_rows = res_columns.at(0)->size();
        Chunk chunk(std::move(res_columns), num_rows);

        Pipes pipes;
        pipes.emplace_back(std::make_shared<SourceFromSingleChunk>(sample_block, std::move(chunk)));

        return pipes;
    }
};

}
