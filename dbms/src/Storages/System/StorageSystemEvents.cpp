#include <Common/ProfileEvents.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/System/StorageSystemEvents.h>


namespace DB
{


StorageSystemEvents::StorageSystemEvents(const std::string & name_)
    : name(name_)
{
    columns = NamesAndTypesList
    {
        {"event", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeUInt64>()}
    };
}


BlockInputStreams StorageSystemEvents::read(
    const Names & column_names,
    const SelectQueryInfo &,
    const Context &,
    QueryProcessingStage::Enum & processed_stage,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    MutableColumns res_columns = getSampleBlock().cloneEmptyColumns();

    for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
    {
        UInt64 value = ProfileEvents::counters[i];

        if (0 != value)
        {
            res_columns[0]->insert(String(ProfileEvents::getDescription(ProfileEvents::Event(i))));
            res_columns[1]->insert(value);
        }
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(getSampleBlock().cloneWithColumns(std::move(res_columns))));
}


}
