#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Databases/IDatabase.h>
#include <Storages/System/StorageSystemDatabases.h>
#include <Interpreters/Context.h>


namespace DB
{


StorageSystemDatabases::StorageSystemDatabases(const std::string & name_)
    : name(name_)
{
    setColumns(ColumnsDescription({
        {"name", std::make_shared<DataTypeString>()},
        {"engine", std::make_shared<DataTypeString>()},
        {"data_path", std::make_shared<DataTypeString>()},
        {"metadata_path", std::make_shared<DataTypeString>()},
    }));
}


BlockInputStreams StorageSystemDatabases::read(
    const Names & column_names,
    const SelectQueryInfo &,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    MutableColumns res_columns = getSampleBlock().cloneEmptyColumns();

    auto databases = context.getDatabases();
    for (const auto & database : databases)
    {
        res_columns[0]->insert(database.first);
        res_columns[1]->insert(database.second->getEngineName());
        res_columns[2]->insert(database.second->getDataPath());
        res_columns[3]->insert(database.second->getMetadataPath());
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(getSampleBlock().cloneWithColumns(std::move(res_columns))));
}


}
