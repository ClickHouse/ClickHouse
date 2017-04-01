#include <Common/Exception.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/System/StorageSystemOne.h>


namespace DB
{


StorageSystemOne::StorageSystemOne(const std::string & name_)
    : name(name_), columns{{"dummy", std::make_shared<DataTypeUInt8>()}}
{
}

StoragePtr StorageSystemOne::create(const std::string & name_)
{
    return make_shared(name_);
}


BlockInputStreams StorageSystemOne::read(
    const Names & column_names,
    ASTPtr query,
    const Context & context,
    const Settings & settings,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned threads)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    Block block;
    ColumnWithTypeAndName col;
    col.name = "dummy";
    col.type = std::make_shared<DataTypeUInt8>();
    col.column = ColumnConstUInt8(1, 0).convertToFullColumn();
    block.insert(std::move(col));

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}


}
