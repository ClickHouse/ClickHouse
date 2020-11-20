#include "StoragePostgreSQL.h"

#if USE_LIBPQXX

#include <Storages/StorageFactory.h>
#include <Storages/transformQueryForExternalDatabase.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>

#include <DataTypes/DataTypeString.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Core/Settings.h>
#include <Common/parseAddress.h>
#include <Parsers/ASTLiteral.h>

#include <Formats/FormatFactory.h>
#include <Formats/PostgreSQLBlockInputStream.h>

#include <IO/Operators.h>
#include <IO/WriteHelpers.h>

#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Pipe.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

StoragePostgreSQL::StoragePostgreSQL(
    const StorageID & table_id_,
    const String & remote_database_name_,
    const String & remote_table_name_,
    const String connection_str,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const Context & context_)
    : IStorage(table_id_)
    , remote_database_name(remote_database_name_)
    , remote_table_name(remote_table_name_)
    , global_context(context_)
    , connection(std::make_shared<pqxx::connection>(connection_str))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
}


Pipe StoragePostgreSQL::read(
    const Names & column_names_,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info_,
    const Context & context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size_,
    unsigned)
{
    metadata_snapshot->check(column_names_, getVirtuals(), getStorageID());

    String query = transformQueryForExternalDatabase(
        query_info_,
        metadata_snapshot->getColumns().getOrdinary(),
        IdentifierQuotingStyle::DoubleQuotes,
        remote_database_name,
        remote_table_name,
        context_);

    Block sample_block;
    for (const String & column_name : column_names_)
    {
        auto column_data = metadata_snapshot->getColumns().getPhysical(column_name);
        WhichDataType which(column_data.type);

        if (which.isEnum())
            column_data.type = std::make_shared<DataTypeString>();

        sample_block.insert({ column_data.type, column_data.name });
    }

    return Pipe(std::make_shared<SourceFromInputStream>(
            std::make_shared<PostgreSQLBlockInputStream>(connection, query, sample_block, max_block_size_)));
}


void registerStoragePostgreSQL(StorageFactory & factory)
{
    factory.registerStorage("PostgreSQL", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() < 5)
            throw Exception(
                "Storage PostgreSQL requires 5-7 parameters: PostgreSQL('host:port', database, table, 'username', 'password'.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.local_context);

        auto parsed_host_port = parseAddress(engine_args[0]->as<ASTLiteral &>().value.safeGet<String>(), 5432);
        const String & remote_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        const String & remote_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();

        String connection_str;
        if (remote_database.empty())
        {
            connection_str = fmt::format("host={} port={} user={} password={}",
                    parsed_host_port.first, std::to_string(parsed_host_port.second),
                    engine_args[3]->as<ASTLiteral &>().value.safeGet<String>(),
                    engine_args[4]->as<ASTLiteral &>().value.safeGet<String>());
        }
        else
        {
            connection_str = fmt::format("dbname={} host={} port={} user={} password={}",
                    remote_database, parsed_host_port.first, std::to_string(parsed_host_port.second),
                    engine_args[3]->as<ASTLiteral &>().value.safeGet<String>(),
                    engine_args[4]->as<ASTLiteral &>().value.safeGet<String>());
        }

        return StoragePostgreSQL::create(
            args.table_id, remote_database, remote_table, connection_str, args.columns, args.constraints, args.context);
    },
    {
        .source_access_type = AccessType::POSTGRES,
    });
}

}

#endif
