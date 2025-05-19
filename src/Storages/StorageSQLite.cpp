#include "StorageSQLite.h"

#if USE_SQLITE
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Core/NamesAndTypes.h>
#include <Processors/Sources/SQLiteSource.h>
#include <Databases/SQLite/SQLiteUtils.h>
#include <Databases/SQLite/fetchSQLiteTableStructure.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/StorageFactory.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <QueryPipeline/Pipe.h>
#include <Common/filesystemHelpers.h>

#include <memory>

namespace
{

using namespace DB;

ContextPtr makeSQLiteWriteContext(ContextPtr context)
{
    auto write_context = Context::createCopy(context);
    write_context->setSetting("output_format_values_escape_quote_with_quote", Field(true));
    return write_context;
}

std::shared_ptr<NamesAndTypesList> doQueryResultStructure(sqlite3 * connection, const String & query);

}


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_QUERY;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SQLITE_ENGINE_ERROR;
}

StorageSQLite::StorageSQLite(
    const StorageID & table_id_,
    SQLitePtr sqlite_db_,
    const String & database_path_,
    const TableNameOrQuery & table_or_query_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , table_or_query(table_or_query_)
    , database_path(database_path_)
    , sqlite_db(sqlite_db_)
    , log(getLogger("StorageSQLite (" + table_id_.getFullTableName() + ")"))
    , write_context(makeSQLiteWriteContext(getContext()))
{
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        auto columns = getTableStructureFromData(sqlite_db, table_or_query);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
    storage_metadata.setComment(comment);
}


ColumnsDescription StorageSQLite::getTableStructureFromData(
    const SQLitePtr & sqlite_db_,
    const TableNameOrQuery & remote_data_locator)
{
    std::shared_ptr<NamesAndTypesList> columns;
    switch (remote_data_locator.getType())
    {
    case TableNameOrQuery::Type::TABLE:
        columns = fetchSQLiteTableStructure(sqlite_db_.get(), remote_data_locator.getTableName());
        break;

    case TableNameOrQuery::Type::QUERY:
        columns = doQueryResultStructure(sqlite_db_.get(), remote_data_locator.getQuery());
        break;
    }

    if (!columns)
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "Failed to fetch table structure for {}", remote_data_locator.format());

    return ColumnsDescription{*columns};
}


Pipe StorageSQLite::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    if (!sqlite_db)
        sqlite_db = openSQLiteDB(database_path, getContext(), /* throw_on_error */true);

    storage_snapshot->check(column_names);

    String query;
    switch (table_or_query.getType())
    {
        case TableNameOrQuery::Type::TABLE:
            query = transformQueryForExternalDatabase(
                query_info,
                column_names,
                storage_snapshot->metadata->getColumns().getOrdinary(),
                IdentifierQuotingStyle::DoubleQuotes,
                LiteralEscapingStyle::Regular,
                "",
                table_or_query.getTableName(),
                context_);
            break;

        case TableNameOrQuery::Type::QUERY:
            query = "SELECT ";
            for (const auto & column : column_names)
                query += doubleQuoteString(column) + ", ";
            query = query.substr(0, query.size() - 2);
            query += " FROM (" + table_or_query.getQuery() + ") AS t";
            break;
    }

    LOG_TRACE(log, "Query: {}", query);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({column_data.type, column_data.name});
    }

    return Pipe(std::make_shared<SQLiteSource>(sqlite_db, query, sample_block, max_block_size));
}


class SQLiteSink : public SinkToStorage
{
public:
    explicit SQLiteSink(
        const StorageSQLite & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        StorageSQLite::SQLitePtr sqlite_db_,
        const String & remote_table_name_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , storage{storage_}
        , metadata_snapshot(metadata_snapshot_)
        , sqlite_db(sqlite_db_)
        , remote_table_name(remote_table_name_)
    {
    }

    String getName() const override { return "SQLiteSink"; }

    void consume(Chunk & chunk) override
    {
        auto block = getHeader().cloneWithColumns(chunk.getColumns());
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

        auto writer = FormatFactory::instance().getOutputFormat("Values", sqlbuf, metadata_snapshot->getSampleBlock(), storage.write_context);
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


SinkToStoragePtr StorageSQLite::write(const ASTPtr & /* query */, const StorageMetadataPtr & metadata_snapshot, ContextPtr /*context*/, bool /*async_insert*/)
{
    if (!sqlite_db)
        sqlite_db = openSQLiteDB(database_path, getContext(), /* throw_on_error */true);
    switch (table_or_query.getType())
    {
        case TableNameOrQuery::Type::TABLE:
            return std::make_shared<SQLiteSink>(*this, metadata_snapshot, sqlite_db, table_or_query.getTableName());

        case TableNameOrQuery::Type::QUERY:
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Trying to write into read-only table representing result of query: {}",
                table_or_query.format());
    }
}


void registerStorageSQLite(StorageFactory & factory)
{
    factory.registerStorage("SQLite", [](const StorageFactory::Arguments & args) -> StoragePtr
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Storage SQLite requires 2 arguments: database path, table name (or SELECT query)");

        std::optional<String> maybe_query;
        if (auto * subquery_ast = engine_args[1]->as<ASTSubquery>())
        {
            maybe_query = subquery_ast->children[0]->formatWithSecretsOneLine();
        }
        else if (auto * function_ast = engine_args[1]->as<ASTFunction>(); function_ast && function_ast->name == "query")
        {
            if (function_ast->arguments->children.size() != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage SQLite expects exactly one argument in query() function");

            auto evaluated_query_arg
                = evaluateConstantExpressionOrIdentifierAsLiteral(function_ast->arguments->children[0], args.getLocalContext());
            auto * query_lit = evaluated_query_arg->as<ASTLiteral>();

            if (!query_lit || query_lit->value.getType() != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage SQLite expects string literal inside query()");
            maybe_query = query_lit->value.safeGet<String>();
        }

        for (size_t i = 0; i < engine_args.size(); ++i)
        {
            auto & engine_arg = engine_args[i];
            if (i == 1 && maybe_query)
                continue;
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());
        }

        const auto database_path = checkAndGetLiteralArgument<String>(engine_args[0], "database_path");
        auto sqlite_db = openSQLiteDB(database_path, args.getContext(), /* throw_on_error */ args.mode <= LoadingStrictnessLevel::CREATE);

        TableNameOrQuery table_or_query;
        if (maybe_query)
            table_or_query = TableNameOrQuery(TableNameOrQuery::Type::QUERY, *maybe_query);
        else
            table_or_query
                = TableNameOrQuery(TableNameOrQuery::Type::TABLE, checkAndGetLiteralArgument<String>(engine_args[1], "table_name"));

        return std::make_shared<StorageSQLite>(
            args.table_id,
            sqlite_db,
            database_path,
            table_or_query,
            args.columns,
            args.constraints,
            args.comment,
            args.getContext());
    },
    {
        .supports_schema_inference = true,
        .source_access_type = AccessType::SQLITE,
    });
}

}

namespace
{

/// RAII wrapper for sqlite3_stmt
class SQLiteStatement
{
public:
    SQLiteStatement(sqlite3 * db, const String & query)
    {
        sqlite3_stmt * stmt_to_prepare = nullptr;
        if (int status = sqlite3_prepare_v2(db, query.c_str(), static_cast<int>(query.size() + 1), &stmt_to_prepare, nullptr);
            status != SQLITE_OK)
        {
            throw Exception(
                ErrorCodes::SQLITE_ENGINE_ERROR,
                "Cannot prepare sqlite statement. Status: {}. Message: {}",
                status,
                sqlite3_errstr(status));
        }
        stmt = std::unique_ptr<sqlite3_stmt, StatementDeleter>(stmt_to_prepare, StatementDeleter());
    }

    int step() { return sqlite3_step(stmt.get()); }
    size_t columnCount() const { return sqlite3_column_count(stmt.get()); }

    String columnName(size_t column_ind) const
    {
        if (column_ind >= columnCount())
            throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "Index {} out of bound when trying to read SQLite column name", column_ind);
        return sqlite3_column_name(stmt.get(), column_ind);
    }
    int valueType(size_t column_ind) const
    {
        if (column_ind >= columnCount())
            throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "Index {} out of bound when trying to get SQLite value's type", column_ind);
        return sqlite3_column_type(stmt.get(), column_ind);
    }

private:
    // TODO: extract to common place both this and SQLiteSource impls.
    struct StatementDeleter
    {
        void operator()(sqlite3_stmt * stmt_) { sqlite3_finalize(stmt_); }
    };
    std::unique_ptr<sqlite3_stmt, StatementDeleter> stmt = nullptr;
};

DataTypePtr convertSQLiteDatatype(int sqlite_type)
{
    // TODO: consider throwing on SQLITE_NULL as it is non-expected type
    if (sqlite_type == SQLITE_NULL)
        return std::make_shared<DataTypeNothing>();
    else if (sqlite_type == SQLITE_INTEGER)
        return std::make_shared<DataTypeInt64>();
    else if (sqlite_type == SQLITE_FLOAT)
        return std::make_shared<DataTypeFloat64>();
    else
        return std::make_shared<DataTypeString>();
}

/// Fetches the structure of a SQLite query result.
/// Due to dynamic nature of SQLite types and lack of strict schema, we cannot effectively infer the structure of a query result.
/// Therefore, we scan the entire result set to determine the type of each column.
std::shared_ptr<NamesAndTypesList> doQueryResultStructure(sqlite3 * connection, const String & query)
{
    SQLiteStatement compiled_stmt(connection, query);
    auto column_count = compiled_stmt.columnCount();
    if (column_count == 0)
        return nullptr;

    // We first assign sentinel NULL datatype to each column to indicate that column's type is unknown (however, it shall not be a final type).
    // Then we run over rows and handle value types:
    // - Ignore NULL values - they don't bring any useful information
    // - If type was unknown, assign type of current value
    // - On type mismatch fallback to TEXT as a universal type
    // If type is still unknown at the end of reading result, fallback to TEXT as a universal one.
    // Then cast it to an appropriate nullable version of ClickHouse Datatype.
    // Nullable is essential because of case when new, possibly NULL, values might be read in the future.
    NamesAndTypes columns(column_count);
    for (size_t i = 0; i < column_count; ++i)
        columns[i].name = compiled_stmt.columnName(i);

    std::vector<int> sqlite_column_types(column_count, SQLITE_NULL);
    while (true)
    {
        int status = compiled_stmt.step();
        if (status == SQLITE_DONE)
            break;
        else if (status != SQLITE_ROW)
            throw Exception(
                ErrorCodes::SQLITE_ENGINE_ERROR,
                "Couldn't read query result when preparing its structure, status: {}. Message: {}",
                status,
                sqlite3_errmsg(connection));

        for (size_t i = 0; i < column_count; ++i)
        {
            int cur_column_type = compiled_stmt.valueType(i);
            auto & prev_column_type = sqlite_column_types[i];
            if (cur_column_type == SQLITE_NULL)
                continue;

            if (prev_column_type == SQLITE_NULL)
                prev_column_type = cur_column_type;
            else if (prev_column_type != cur_column_type)
                prev_column_type = SQLITE_TEXT;
        }
    }
    for (size_t i = 0; i < column_count; ++i)
    {
        if (sqlite_column_types[i] == SQLITE_NULL)
            sqlite_column_types[i] = SQLITE_TEXT;
        columns[i].type = makeNullable(convertSQLiteDatatype(sqlite_column_types[i]));
    }
    return std::make_shared<NamesAndTypesList>(columns.begin(), columns.end());
}

}

#endif
