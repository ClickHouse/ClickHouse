#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Interpreters/PartLog.h>
#include <Common/ClickHouseRevision.h>
#include <Poco/Net/IPAddress.h>
#include <array>


namespace DB
{
Block PartLogElement::createBlock()
{
    return
    {
        {std::make_shared<ColumnUInt8>(),   std::make_shared<DataTypeUInt8>(),      "event_type"},

        {std::make_shared<ColumnUInt16>(),  std::make_shared<DataTypeDate>(),       "event_date"},
        {std::make_shared<ColumnUInt32>(),  std::make_shared<DataTypeDateTime>(),   "event_time"},

        {std::make_shared<ColumnUInt64>(),  std::make_shared<DataTypeUInt64>(),     "size_in_bytes"},
        {std::make_shared<ColumnUInt64>(),  std::make_shared<DataTypeUInt64>(),     "duration_ms"},

        {std::make_shared<ColumnString>(),  std::make_shared<DataTypeString>(),     "database"},
        {std::make_shared<ColumnString>(),  std::make_shared<DataTypeString>(),     "table"},
        {std::make_shared<ColumnString>(),  std::make_shared<DataTypeString>(),     "part_name"},
        {std::make_shared<ColumnArray>(std::make_shared<ColumnString>()),
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),    "merged_from"},
  };
}


void PartLogElement::appendToBlock(Block & block) const
{
    size_t i = 0;

    block.getByPosition(i++).column->insert(UInt64(event_type));
    block.getByPosition(i++).column->insert(UInt64(DateLUT::instance().toDayNum(event_time)));
    block.getByPosition(i++).column->insert(UInt64(event_time));

    block.getByPosition(i++).column->insert(UInt64(size_in_bytes));
    block.getByPosition(i++).column->insert(UInt64(duration_ms));

    block.getByPosition(i++).column->insert(database_name);
    block.getByPosition(i++).column->insert(table_name);
    block.getByPosition(i++).column->insert(part_name);

    Array merged_from_array;
    merged_from_array.reserve(merged_from.size());
    for (const auto & name : merged_from)
        merged_from_array.push_back(name);
    block.getByPosition(i++).column->insert(merged_from_array);
}



}
