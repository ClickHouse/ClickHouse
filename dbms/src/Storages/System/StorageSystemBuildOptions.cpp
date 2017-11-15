#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/Settings.h>
#include <Storages/System/StorageSystemBuildOptions.h>
#include <Common/config_build.h>

namespace DB
{


StorageSystemBuildOptions::StorageSystemBuildOptions(const std::string & name_)
    : name(name_)
    , columns
    {
        { "name", std::make_shared<DataTypeString>() },
        { "value", std::make_shared<DataTypeString>() },
    }
{
}


BlockInputStreams StorageSystemBuildOptions::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    ColumnWithTypeAndName col_name{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "name"};
    ColumnWithTypeAndName col_value{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "value"};

    for (auto it = auto_config_build; *it; it += 2)
    {
        col_name.column->insert(String(it[0]));
        col_value.column->insert(String(it[1]));
    }

    Block block
    {
        col_name,
        col_value,
    };

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}


}
