#include <Common/Exception.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/System/StorageSystemOne.h>


namespace DB
{


StorageSystemOne::StorageSystemOne(const std::string & name_)
    : name(name_)
{
    setColumns(ColumnsDescription({{"dummy", std::make_shared<DataTypeUInt8>()}}));
}


BlockInputStreams StorageSystemOne::read(
    const Names & column_names,
    const SelectQueryInfo &,
    const Context & /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    check(column_names);

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(
        Block{ColumnWithTypeAndName(
            DataTypeUInt8().createColumnConst(1, UInt64(0))->convertToFullColumnIfConst(),
            std::make_shared<DataTypeUInt8>(),
            "dummy")}));
}


}
