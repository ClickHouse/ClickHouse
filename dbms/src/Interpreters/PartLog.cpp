#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/PartLog.h>


namespace DB
{

Block PartLogElement::createBlock()
{
    return
    {
        {ColumnUInt8::create(),   std::make_shared<DataTypeUInt8>(),      "event_type"},

        {ColumnUInt16::create(),  std::make_shared<DataTypeDate>(),       "event_date"},
        {ColumnUInt32::create(),  std::make_shared<DataTypeDateTime>(),   "event_time"},

        {ColumnUInt64::create(),  std::make_shared<DataTypeUInt64>(),     "size_in_bytes"},
        {ColumnUInt64::create(),  std::make_shared<DataTypeUInt64>(),     "duration_ms"},

        {ColumnString::create(),  std::make_shared<DataTypeString>(),     "database"},
        {ColumnString::create(),  std::make_shared<DataTypeString>(),     "table"},
        {ColumnString::create(),  std::make_shared<DataTypeString>(),     "part_name"},
        {ColumnArray::create(ColumnString::create()),
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),    "merged_from"},
  };
}

void PartLogElement::appendToBlock(Block & block) const
{
    MutableColumns columns = block.mutateColumns();

    size_t i = 0;

    columns[i++]->insert(UInt64(event_type));
    columns[i++]->insert(UInt64(DateLUT::instance().toDayNum(event_time)));
    columns[i++]->insert(UInt64(event_time));

    columns[i++]->insert(UInt64(size_in_bytes));
    columns[i++]->insert(UInt64(duration_ms));

    columns[i++]->insert(database_name);
    columns[i++]->insert(table_name);
    columns[i++]->insert(part_name);

    Array merged_from_array;
    merged_from_array.reserve(merged_from.size());
    for (const auto & name : merged_from)
        merged_from_array.push_back(name);

    columns[i++]->insert(merged_from_array);

    block.setColumns(std::move(columns));
}

void PartLog::addNewPart(const MergeTreeDataPart & part, double elapsed)
{
    PartLogElement elem;
    elem.event_time = time(nullptr);

    elem.event_type = PartLogElement::NEW_PART;
    elem.size_in_bytes = part.size_in_bytes;
    elem.duration_ms = elapsed / 1000000;

    elem.database_name = part.storage.getDatabaseName();
    elem.table_name = part.storage.getTableName();
    elem.part_name = part.name;

    add(elem);
}

}
