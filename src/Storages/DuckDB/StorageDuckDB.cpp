#include "StorageDuckDB.h"

#if USE_DUCKDB
#include <Common/logger_useful.h>
#include <Processors/Sources/DuckDBSource.h>
#include <Databases/DuckDB/DuckDBUtils.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/StorageFactory.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <QueryPipeline/Pipe.h>
#include <Common/filesystemHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int DUCKDB_ENGINE_ERROR;
}

StorageDuckDB::StorageDuckDB(
    const StorageID & table_id_,
    DuckDBPtr duckdb_instance_,
    const String & database_path_,
    const String & remote_table_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , remote_table_name(remote_table_name_)
    , database_path(database_path_)
    , duckdb_instance(duckdb_instance_)
    , log(&Poco::Logger::get("StorageDuckDB (" + table_id_.table_name + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
}


Pipe StorageDuckDB::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    if (!duckdb_instance)
        duckdb_instance = openDuckDB(database_path, getContext(), /* throw_on_error */true);

    storage_snapshot->check(column_names);

    String query = transformQueryForExternalDatabase(
        query_info,
        storage_snapshot->metadata->getColumns().getOrdinary(),
        IdentifierQuotingStyle::DoubleQuotes,
        "",
        remote_table_name,
        context_);
    LOG_TRACE(log, "Query: {}", query);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({column_data.type, column_data.name});
    }

    return Pipe(std::make_shared<DuckDBSource>(duckdb_instance, query, sample_block, max_block_size));
}


class DuckDBSink : public SinkToStorage
{
public:
    explicit DuckDBSink(
        const StorageDuckDB & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        StorageDuckDB::DuckDBPtr duckdb_instance_,
        const String & remote_table_name_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , storage{storage_}
        , metadata_snapshot(metadata_snapshot_)
        , duckdb_instance(duckdb_instance_)
        , remote_table_name(remote_table_name_)
    {
    }

    String getName() const override { return "DuckDBSink"; }

    void consume(Chunk chunk) override
    {
        // TODO: DuckDB Appender API

        auto block = getHeader().cloneWithColumns(chunk.getColumns());
        WriteBufferFromOwnString buf;

        buf << "INSERT INTO ";
        buf << quoteIdentifierDuckDB(remote_table_name);
        buf << " (";

        for (auto it = block.begin(); it != block.end(); ++it)
        {
            if (it != block.begin())
                buf << ", ";
            buf << quoteIdentifierDuckDB(it->name);
        }

        buf << ") VALUES ";

        auto writer = FormatFactory::instance().getOutputFormat("Values", buf, metadata_snapshot->getSampleBlock(), storage.getContext());
        writer->write(block);

        buf << ";";

        duckdb::Connection con(*duckdb_instance);
        auto result = con.Query(buf.str());

        if (result->HasError())
        {
            throw Exception(ErrorCodes::DUCKDB_ENGINE_ERROR,
                            "Failed to execute DuckDB INSERT query. Error type: {}. Message: {}",
                            result->GetErrorType(), result->GetError());
        }
    }

private:
    const StorageDuckDB & storage;
    StorageMetadataPtr metadata_snapshot;
    StorageDuckDB::DuckDBPtr duckdb_instance;
    String remote_table_name;
};


SinkToStoragePtr StorageDuckDB::write(const ASTPtr & /* query */, const StorageMetadataPtr & metadata_snapshot, ContextPtr)
{
    if (!duckdb_instance)
        duckdb_instance = openDuckDB(database_path, getContext(), /* throw_on_error */true);
    return std::make_shared<DuckDBSink>(*this, metadata_snapshot, duckdb_instance, remote_table_name);
}


void registerStorageDuckDB(StorageFactory & factory)
{
    factory.registerStorage("DuckDB", [](const StorageFactory::Arguments & args) -> StoragePtr
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "DuckDB database requires 2 arguments: database path, table name");

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());

        const auto database_path = checkAndGetLiteralArgument<String>(engine_args[0], "database_path");
        const auto table_name = checkAndGetLiteralArgument<String>(engine_args[1], "table_name");

        auto duckdb_instance = openDuckDB(database_path, args.getContext(), /* throw_on_error */!args.attach);

        return std::make_shared<StorageDuckDB>(args.table_id, duckdb_instance, database_path,
                                     table_name, args.columns, args.constraints, args.getContext());
    },
    {
        .source_access_type = AccessType::DUCKDB,
    });
}

}

#endif
