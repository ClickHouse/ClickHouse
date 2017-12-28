#include <Storages/StorageMySQL.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Dictionaries/MySQLBlockInputStream.h>


namespace DB
{

StorageMySQL::StorageMySQL(
    const std::string & name,
    mysqlxx::Pool && pool,
    const std::string & remote_database_name,
    const std::string & remote_table_name,
    const NamesAndTypesList & columns)
    : name(name)
    , remote_database_name(remote_database_name)
    , remote_table_name(remote_table_name)
    , columns(columns)
    , pool(std::move(pool))
{
}


BlockInputStreams StorageMySQL::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    unsigned)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;
    String query = transformQueryForExternalDatabase(*query_info.query, columns, remote_database_name, remote_table_name, context);

    Block sample_block;
    for (const String & name : column_names)
    {
        auto column_data = getColumn(name);
        sample_block.insert({ column_data.type, column_data.name });
    }

    return { std::make_shared<MySQLBlockInputStream>(pool.Get(), query, sample_block, max_block_size) };
}

}
