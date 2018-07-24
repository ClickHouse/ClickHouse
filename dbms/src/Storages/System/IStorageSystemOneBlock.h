#pragma once
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <ext/shared_ptr_helper.h>

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
    IStorageSystemOneBlock(const String & name_) : name(name_)
    {
        setColumns(ColumnsDescription(Self::getNamesAndTypes()));
    }

    std::string getTableName() const override
    {
        return name;
    }

    BlockInputStreams read(const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override
    {
        check(column_names);
        processed_stage = QueryProcessingStage::FetchColumns;

        Block sample_block = getSampleBlock();
        MutableColumns res_columns = sample_block.cloneEmptyColumns();
        fillData(res_columns, context, query_info);

        return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns))));
    }

private:
    const String name;
};

}
