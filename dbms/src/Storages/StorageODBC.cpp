#include <Common/PocoSessionPoolHelpers.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/StorageODBC.h>
#include <Dictionaries/ODBCBlockInputStream.h>


namespace DB
{

StorageODBC::StorageODBC(
    const std::string & name,
    const std::string & connection_string,
    const std::string & remote_database_name,
    const std::string & remote_table_name,
    const NamesAndTypesList & columns)
    : name(name)
    , connection_string(connection_string)
    , remote_database_name(remote_database_name)
    , remote_table_name(remote_table_name)
    , columns(columns_)
{
    pool = createAndCheckResizePocoSessionPool([&]
    {
        return std::make_shared<Poco::Data::SessionPool>("ODBC", connection_string);
    });
}

BlockInputStreams StorageODBC::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;
    String query = transformQueryForExternalDatabase(*query_info.query, columns, remote_database_name, remote_table_name, context);
    return { std::make_shared<ODBCBlockInputStream>(pool->get(), query, getSampleBlock(), max_block_size) };
}

}
