#include "StorageSQLite.h"

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
#include <ext/range.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StorageSQLite::StorageSQLite(
    const StorageID & table_id_,
    std::shared_ptr<sqlite3> db_ptr_,
    const String & remote_table_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , remote_table_name(remote_table_name_)
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

    return Pipe(
        std::make_shared<SourceFromInputStream>(std::make_shared<SQLiteBlockInputStream>(db_ptr, query, sample_block, max_block_size)));
}

class SQLiteBlockOutputStream : public IBlockOutputStream
{
public:
    explicit SQLiteBlockOutputStream(
        const StorageSQLite & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        std::shared_ptr<sqlite3> connection_,
        const std::string & remote_table_name_)
        : storage{storage_}
        , metadata_snapshot(metadata_snapshot_)
        , connection(connection_)
        , remote_table_name(remote_table_name_)
    {
    }

    Block getHeader() const override { return metadata_snapshot->getSampleBlock(); }


    void writePrefix() override { }

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

        auto writer
            = FormatFactory::instance().getOutputStream("Values", sqlbuf, metadata_snapshot->getSampleBlock(), storage.getContext());
        writer->write(block);

        sqlbuf << ";";

        char * err_message = nullptr;

        int status = sqlite3_exec(connection.get(), sqlbuf.str().c_str(), nullptr, nullptr, &err_message);

        if (status != SQLITE_OK)
        {
            String err_msg(err_message);
            sqlite3_free(err_message);
            throw Exception(status, "SQLITE_ERR {}: {}", status, err_msg);
        }
    }

    void writeSuffix() override { }

private:
    const StorageSQLite & storage;
    StorageMetadataPtr metadata_snapshot;
    std::shared_ptr<sqlite3> connection;
    std::string remote_table_name;
};

BlockOutputStreamPtr StorageSQLite::write(const ASTPtr & /* query */, const StorageMetadataPtr & metadata_snapshot, ContextPtr)
{
    return std::make_shared<SQLiteBlockOutputStream>(*this, metadata_snapshot, db_ptr, remote_table_name);
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
                args.getContext());
        },
        {
            .source_access_type = AccessType::SQLITE,
        });
}

}
