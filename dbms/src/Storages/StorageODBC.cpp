#include <Common/PocoSessionPoolHelpers.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/StorageODBC.h>
#include <Storages/StorageFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Dictionaries/ODBCBlockInputStream.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


StorageODBC::StorageODBC(
    const std::string & name,
    const std::string & connection_string,
    const std::string & remote_database_name,
    const std::string & remote_table_name,
    const ColumnsDescription & columns_)
    : IStorage{columns_}
    , name(name)
    , remote_database_name(remote_database_name)
    , remote_table_name(remote_table_name)
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
    String query = transformQueryForExternalDatabase(
        *query_info.query, getColumns().ordinary, remote_database_name, remote_table_name, context);

    Block sample_block;
    for (const String & name : column_names)
    {
        auto column_data = getColumn(name);
        sample_block.insert({ column_data.type, column_data.name });
    }

    return { std::make_shared<ODBCBlockInputStream>(pool->get(), query, sample_block, max_block_size) };
}


void registerStorageODBC(StorageFactory & factory)
{
    factory.registerStorage("ODBC", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 3)
            throw Exception(
                "Storage ODBC requires exactly 3 parameters: ODBC('DSN', database, table).",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (size_t i = 0; i < 2; ++i)
            engine_args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[i], args.local_context);

        return StorageODBC::create(args.table_name,
            static_cast<const ASTLiteral &>(*engine_args[0]).value.safeGet<String>(),
            static_cast<const ASTLiteral &>(*engine_args[1]).value.safeGet<String>(),
            static_cast<const ASTLiteral &>(*engine_args[2]).value.safeGet<String>(),
            args.columns);
    });
}

}
