#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Databases/IDatabase.h>
#include <Storages/System/StorageSystemDatabases.h>
#include <Interpreters/Context.h>


namespace DB
{


StorageSystemDatabases::StorageSystemDatabases(const std::string & name_)
    : name(name_),
    columns
    {
        {"name",     std::make_shared<DataTypeString>()},
        {"engine",     std::make_shared<DataTypeString>()},
    }
{
}


BlockInputStreams StorageSystemDatabases::read(
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

    ColumnWithTypeAndName col_name{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "name"};
    block.insert(col_name);

    ColumnWithTypeAndName col_engine{std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "engine"};
    block.insert(col_engine);

    auto databases = context.getDatabases();
    for (const auto & database : databases)
    {
        col_name.column->insert(database.first);
        col_engine.column->insert(database.second->getEngineName());
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}


}
