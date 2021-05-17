#include "StorageSQLite.h"

#include <DataStreams/SQLiteBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Storages/StorageFactory.h>
#include <Storages/transformQueryForExternalDatabase.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
}

StorageSQLite::StorageSQLite(
    const StorageID & table_id_,
    std::shared_ptr<sqlite3> db_ptr_,
    const String & remote_table_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_,
    const String & remote_table_schema_)
    : IStorage(table_id_)
    , remote_table_name(remote_table_name_)
    , remote_table_schema(remote_table_schema_)
    , global_context(context_)
    , db_ptr(db_ptr_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
}
Pipe StorageSQLite::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum,
    size_t max_block_size,
    unsigned int)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    /// Connection is already made to the needed database, so it should not be present in the query;
    /// remote_table_schema is empty if it is not specified, will access only table_name.
    String query = transformQueryForExternalDatabase(
        query_info,
        metadata_snapshot->getColumns().getOrdinary(),
        IdentifierQuotingStyle::DoubleQuotes,
        remote_table_schema,
        remote_table_name,
        context);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = metadata_snapshot->getColumns().getPhysical(column_name);
        sample_block.insert({column_data.type, column_data.name});
    }

    return Pipe(
        std::make_shared<SourceFromInputStream>(std::make_shared<SQLiteBlockInputStream>(db_ptr, query, sample_block, max_block_size)));
}

void registerStorageSQLite(StorageFactory & factory)
{
    factory.registerStorage(
        "SQLite",
        [](const StorageFactory::Arguments & args) -> StoragePtr {
            ASTs & engine_args = args.engine_args;

            if (engine_args.size() != 2)
                throw Exception(
                    "SQLite database requires 2 arguments: database path, table name", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            for (auto & engine_arg : engine_args)
                engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());

            const auto database_path = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
            const auto table_name = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();

            sqlite3 * tmp_db_ptr = nullptr;
            int status = sqlite3_open(database_path.c_str(), &tmp_db_ptr);
            if (status != SQLITE_OK)
            {
                throw Exception(status, sqlite3_errstr(status));
            }

            return StorageSQLite::create(
                args.table_id,
                std::shared_ptr<sqlite3>(tmp_db_ptr, sqlite3_close),
                table_name,
                args.columns,
                args.constraints,
                args.getContext(),
                "");
        },
        {
            .source_access_type = AccessType::SQLITE,
        });
}

}
