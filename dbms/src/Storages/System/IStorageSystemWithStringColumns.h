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
class IStorageSystemWithStringColumns : public IStorage
{
protected:
    virtual void fillData(MutableColumns & res_columns) const = 0;

public:
    IStorageSystemWithStringColumns(const String & name_) : name(name_)
    {
        auto names = Self::getColumnNames();
        NamesAndTypesList name_list;
        for (const auto & name : names)
        {
            name_list.push_back(NameAndTypePair{name, std::make_shared<DataTypeString>()});
        }
        setColumns(ColumnsDescription(name_list));
    }

    std::string getTableName() const override
    {
        return name;
    }

    BlockInputStreams read(const Names & column_names,
        const SelectQueryInfo & /*query_info*/,
        const Context & /*context*/,
        QueryProcessingStage::Enum & processed_stage,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override
    {
        check(column_names);
        processed_stage = QueryProcessingStage::FetchColumns;

        Block sample_block = getSampleBlock();
        MutableColumns res_columns = sample_block.cloneEmptyColumns();
        fillData(res_columns);

        return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns))));
    }

private:
    const String name;
};

}
