#include <Storages/StorageMySQL.h>

#if USE_MYSQL
#include <Storages/StorageFactory.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Dictionaries/MySQLBlockInputStream.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Common/parseAddress.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


StorageMySQL::StorageMySQL(
    const std::string & name,
    mysqlxx::Pool && pool,
    const std::string & remote_database_name,
    const std::string & remote_table_name,
    const ColumnsDescription & columns_)
    : IStorage{columns_}
    , name(name)
    , remote_database_name(remote_database_name)
    , remote_table_name(remote_table_name)
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
    String query = transformQueryForExternalDatabase(*query_info.query, getColumns().ordinary, remote_database_name, remote_table_name, context);

    Block sample_block;
    for (const String & name : column_names)
    {
        auto column_data = getColumn(name);
        sample_block.insert({ column_data.type, column_data.name });
    }

    return { std::make_shared<MySQLBlockInputStream>(pool.Get(), query, sample_block, max_block_size) };
}


void registerStorageMySQL(StorageFactory & factory)
{
    factory.registerStorage("MySQL", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 5)
            throw Exception(
                "Storage MySQL requires exactly 5 parameters: MySQL('host:port', database, table, 'user', 'password').",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (size_t i = 0; i < 5; ++i)
            engine_args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[i], args.local_context);

        /// 3306 is the default MySQL port.
        auto parsed_host_port = parseAddress(static_cast<const ASTLiteral &>(*engine_args[0]).value.safeGet<String>(), 3306);

        const String & remote_database = static_cast<const ASTLiteral &>(*engine_args[1]).value.safeGet<String>();
        const String & remote_table = static_cast<const ASTLiteral &>(*engine_args[2]).value.safeGet<String>();
        const String & username = static_cast<const ASTLiteral &>(*engine_args[3]).value.safeGet<String>();
        const String & password = static_cast<const ASTLiteral &>(*engine_args[4]).value.safeGet<String>();

        mysqlxx::Pool pool(remote_database, parsed_host_port.first, username, password, parsed_host_port.second);

        return StorageMySQL::create(
            args.table_name,
            std::move(pool),
            remote_database,
            remote_table,
            args.columns);
    });
}

}

#endif
