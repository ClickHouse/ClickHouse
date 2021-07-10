#include "StorageSQLite.h"

#if USE_SQLITE
#include <common/range.h>
#include <DataStreams/SQLiteBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Formats/FormatFactory.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
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
    extern const int SQLITE_ENGINE_ERROR;
}

StorageSQLite::StorageSQLite(
    const StorageID & table_id_,
    SQLitePtr sqlite_db_,
    const String & remote_table_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , remote_table_name(remote_table_name_)
    , global_context(context_)
    , sqlite_db(sqlite_db_)
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
    ContextPtr context_,
    QueryProcessingStage::Enum,
    size_t max_block_size,
    unsigned int)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    String query = transformQueryForExternalDatabase(
        query_info,
        metadata_snapshot->getColumns().getOrdinary(),
        IdentifierQuotingStyle::DoubleQuotes,
        "",
        remote_table_name,
        context_);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = metadata_snapshot->getColumns().getPhysical(column_name);
        sample_block.insert({column_data.type, column_data.name});
    }

    return Pipe(std::make_shared<SourceFromInputStream>(
                std::make_shared<SQLiteBlockInputStream>(sqlite_db, query, sample_block, max_block_size)));
}


class SQLiteBlockOutputStream : public IBlockOutputStream
{
public:
    explicit SQLiteBlockOutputStream(
        const StorageSQLite & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        StorageSQLite::SQLitePtr sqlite_db_,
        const String & remote_table_name_)
        : storage{storage_}
        , metadata_snapshot(metadata_snapshot_)
        , sqlite_db(sqlite_db_)
        , remote_table_name(remote_table_name_)
    {
    }

    Block getHeader() const override { return metadata_snapshot->getSampleBlock(); }

    void write(const Block & block) override
    {
        WriteBufferFromOwnString sqlbuf;

        sqlbuf << "INSERT INTO ";
        sqlbuf << doubleQuoteString(remote_table_name);
        sqlbuf << " (";

        for (auto it = block.begin(); it != block.end(); ++it)
        {
            if (it != block.begin())
                sqlbuf << ", ";
            sqlbuf << quoteString(it->name);
        }

        sqlbuf << ") VALUES ";

        auto writer = FormatFactory::instance().getOutputStream("Values", sqlbuf, metadata_snapshot->getSampleBlock(), storage.getContext());
        writer->write(block);

        sqlbuf << ";";

        char * err_message = nullptr;
        int status = sqlite3_exec(sqlite_db.get(), sqlbuf.str().c_str(), nullptr, nullptr, &err_message);

        if (status != SQLITE_OK)
        {
            String err_msg(err_message);
            sqlite3_free(err_message);
            throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR,
                            "Failed to execute sqlite INSERT query. Status: {}. Message: {}",
                            status, err_msg);
        }
    }

private:
    const StorageSQLite & storage;
    StorageMetadataPtr metadata_snapshot;
    StorageSQLite::SQLitePtr sqlite_db;
    String remote_table_name;
};


BlockOutputStreamPtr StorageSQLite::write(const ASTPtr & /* query */, const StorageMetadataPtr & metadata_snapshot, ContextPtr)
{
    return std::make_shared<SQLiteBlockOutputStream>(*this, metadata_snapshot, sqlite_db, remote_table_name);
}


void registerStorageSQLite(StorageFactory & factory)
{
    factory.registerStorage("SQLite", [](const StorageFactory::Arguments & args) -> StoragePtr
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 2)
            throw Exception("SQLite database requires 2 arguments: database path, table name",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());

        const auto database_path = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        const auto table_name = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();

        sqlite3 * tmp_sqlite_db = nullptr;
        int status = sqlite3_open(database_path.c_str(), &tmp_sqlite_db);
        if (status != SQLITE_OK)
            throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR,
                            "Failed to open sqlite database. Status: {}. Message: {}",
                            status, sqlite3_errstr(status));

        return StorageSQLite::create(args.table_id, std::shared_ptr<sqlite3>(tmp_sqlite_db, sqlite3_close),
                                     table_name, args.columns, args.constraints, args.getContext());
    },
    {
        .source_access_type = AccessType::SQLITE,
    });
}

}

#endif
