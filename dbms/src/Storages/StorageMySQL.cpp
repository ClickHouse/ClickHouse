#include <Storages/StorageMySQL.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Dictionaries/MySQLBlockInputStream.h>


namespace DB
{

StorageMySQL::StorageMySQL(
    const std::string & name,
    const std::string & host,
    UInt16 port,
    const std::string & remote_database_name,
    const std::string & remote_table_name,
    const std::string & user,
    const std::string & password,
    const NamesAndTypesList & columns)
    : name(name)
    , host(host), port(port)
    , remote_database_name(remote_database_name)
    , remote_table_name(remote_table_name)
    , user(user), password(password)
    , columns(columns_)
    , pool(remote_database_name, host, user, password, port)
{
}


BlockInputStreams StorageMySQL::read(const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    unsigned)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;
    String query = transformQueryForExternalDatabase(*query_info.query, columns, remote_database_name, remote_table_name, context);
    return { std::make_shared<MySQLBlockInputStream>(pool.Get(), query, getSampleBlock(), max_block_size) };
}

}
