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


BlockInputStreams StorageSystemOne::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    Block block;
    ColumnWithTypeAndName col;
    col.name = "dummy";
    col.type = std::make_shared<DataTypeUInt8>();
    col.column = DataTypeUInt8().createConstColumn(1, UInt64(0))->convertToFullColumnIfConst();
    block.insert(std::move(col));

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}


}
