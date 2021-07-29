#pragma once

#include <Core/NamesAndAliases.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <common/shared_ptr_helper.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Pipe.h>

namespace DB
{

class Context;


/** IStorageSystemOneBlock is base class for system tables whose all columns can be synchronously fetched.
  *
  * Client class need to provide static method static NamesAndTypesList getNamesAndTypes() that will return list of column names and
  * their types. IStorageSystemOneBlock during read will create result columns in same order as result of getNamesAndTypes
  * and pass it with fillData method.
  *
  * Client also must override fillData and fill result columns.
  *
  * If subclass want to support virtual columns, it should override getVirtuals method of IStorage interface.
  * IStorageSystemOneBlock will add virtuals columns at the end of result columns of fillData method.
  */
template <typename Self>
class IStorageSystemOneBlock : public IStorage
{
protected:
    virtual void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const = 0;


public:
#if defined(ARCADIA_BUILD)
    IStorageSystemOneBlock(const String & name_) : IStorageSystemOneBlock(StorageID{"system", name_}) {}
#endif

    IStorageSystemOneBlock(const StorageID & table_id_) : IStorage(table_id_)
    {
        StorageInMemoryMetadata metadata_;
        metadata_.setColumns(ColumnsDescription(Self::getNamesAndTypes(), Self::getNamesAndAliases()));
        setInMemoryMetadata(metadata_);
    }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override
    {
        auto virtuals_names_and_types = getVirtuals();
        metadata_snapshot->check(column_names, virtuals_names_and_types, getStorageID());

        Block sample_block = metadata_snapshot->getSampleBlockWithVirtuals(virtuals_names_and_types);
        MutableColumns res_columns = sample_block.cloneEmptyColumns();
        fillData(res_columns, context, query_info);

        UInt64 num_rows = res_columns.at(0)->size();
        Chunk chunk(std::move(res_columns), num_rows);

        return Pipe(std::make_shared<SourceFromSingleChunk>(sample_block, std::move(chunk)));
    }

    static NamesAndAliases getNamesAndAliases() { return {}; }
};

}
