#include <Storages/StorageSQLite.h>

#if USE_SQLITE
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Processors/Sources/SQLiteSource.h>
#include <Databases/SQLite/SQLiteUtils.h>
#include <Databases/SQLite/fetchSQLiteTableStructure.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatFactory.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Formats/Impl/SQLiteCommon.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/StorageFactory.h>
#include <Storages/TableNameOrQuery.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <QueryPipeline/Pipe.h>
#include <Common/filesystemHelpers.h>
#include <base/scope_guard.h>

namespace
{

using namespace DB;

/// The `MATERIALIZED` classification of SQLite generated columns is derived from the remote schema and is
/// not preserved by the local metadata: a `MATERIALIZED` column without a default expression is formatted
/// without the `MATERIALIZED` keyword, so an explicit `CREATE TABLE ... ENGINE = SQLite(...)`, the stored
/// table definition replayed on `ATTACH`, and a `SHOW CREATE TABLE` round-trip all spell a generated column
/// as an ordinary one. Re-consult the remote schema for such an explicit column list so that a generated
/// column stays non-insertable regardless of how the metadata was obtained.
///
/// Best-effort: if the remote schema cannot be read (e.g. the database file is temporarily unavailable while
/// a persisted table is attached), the declared classification is kept rather than failing the whole
/// `CREATE`/`ATTACH` - `StorageSQLite::reclassifyGeneratedColumnsFromRemote` then re-applies the marking on
/// the first successful open in `read`/`write`.
///
/// Returns whether the remote table schema was actually observed. `false` means the schema could not be read
/// (an error, or a database that does not contain the table - e.g. an empty database created in place of a
/// missing file), so the caller must not treat the classification as finalized.
bool markRemoteGeneratedColumns(sqlite3 * sqlite_db, const String & table_name, ColumnsDescription & columns, LoggerPtr log)
{
    std::optional<ColumnsDescription> remote_columns;
    try
    {
        remote_columns = fetchSQLiteTableStructure(sqlite_db, table_name);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to read the SQLite schema of " + table_name + " while classifying generated columns");
        return false;
    }

    if (!remote_columns)
        return false;

    for (const auto & generated : remote_columns->getMaterialized())
    {
        if (!columns.has(generated.name))
            continue;

        /// Only re-classify a plain ordinary column. Respect an explicit `DEFAULT`/`MATERIALIZED`/`ALIAS`
        /// expression the user may have declared.
        const auto & existing = columns.get(generated.name);
        if (existing.default_desc.kind != ColumnDefaultKind::Default || existing.default_desc.expression)
            continue;

        columns.modify(generated.name, [](ColumnDescription & column)
        {
            column.default_desc.kind = ColumnDefaultKind::Materialized;
        });
    }

    return true;
}

}


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SQLITE_ENGINE_ERROR;
    extern const int INCORRECT_QUERY;
}

namespace
{
/// Infer the structure of a result of a user-provided query by preparing it and reading the result columns metadata.
ColumnsDescription doQueryResultStructure(sqlite3 * sqlite_db, const String & query);
}

StorageSQLite::StorageSQLite(
    const StorageID & table_id_,
    SQLitePtr sqlite_db_,
    const String & database_path_,
    const TableNameOrQuery & remote_table_or_query_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    bool generated_columns_reclassification_pending_)
    : StorageWithCommonVirtualColumns(table_id_)
    , WithContext(context_->getGlobalContext())
    , remote_table_or_query(remote_table_or_query_)
    , database_path(database_path_)
    , sqlite_db(sqlite_db_)
    , log(getLogger("StorageSQLite (" + table_id_.getFullTableName() + ")"))
{
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        auto columns = getTableStructureFromData(sqlite_db, remote_table_or_query);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);

    /// The explicit-column path (`registerStorageSQLite`) re-derives the generated-column classification from
    /// the remote schema when the schema is observable at construction time. If the database file is unavailable,
    /// or it opens successfully but the remote table/schema is still unavailable, the classification is still
    /// pending: `updateExternalDynamicMetadataIfExists` reopens/rechecks the database and repairs it before the
    /// query's metadata snapshot is taken (with `read`/`write` as a fallback), so a generated column does not stay
    /// insertable for the rest of the table's lifetime. The auto-inferred case (empty column list) gets the
    /// classification straight from `getTableStructureFromData` above, and a query-backed source is read-only, so
    /// neither needs the lazy repair.
    generated_columns_reclassification_pending = generated_columns_reclassification_pending_;
}

StorageSQLite::SQLitePtr StorageSQLite::openConnectionIfNeeded(bool throw_on_error, bool allow_create)
{
    /// Guard the one-time lazy connection bootstrap. `read`, `write`, and the
    /// `updateExternalDynamicMetadataIfExists` metadata hook all funnel through here, so the `sqlite_db`
    /// shared_ptr member is only ever written under this mutex - a plain unsynchronized
    /// `if (!sqlite_db) sqlite_db = openSQLiteDB(...)` in each of them would be a data race on the shared_ptr
    /// when two first queries run concurrently (e.g. after an `ATTACH`-while-unavailable).
    std::lock_guard lock(connection_mutex);
    if (!sqlite_db)
    {
        auto opened = openSQLiteDB(database_path, getContext(), throw_on_error, allow_create);
        if (!opened)
            return nullptr;
        sqlite_db = opened;
    }
    return sqlite_db;
}

void StorageSQLite::reclassifyGeneratedColumnsFromRemote(ContextPtr query_context)
{
    if (!generated_columns_reclassification_pending.load(std::memory_order_acquire))
        return;

    std::lock_guard lock(reclassify_mutex);
    if (!generated_columns_reclassification_pending.load(std::memory_order_relaxed))
        return;

    /// The caller has just opened `sqlite_db`, so the remote schema that was unavailable at construction time
    /// is now reachable and the pending generated-column classification can be re-derived and stored in the
    /// in-memory metadata, where subsequent reads and writes pick it up.
    auto old_metadata = getInMemoryMetadataPtr(query_context, false);
    ColumnsDescription columns = old_metadata->getColumns();

    /// Only treat the classification as repaired once the remote schema was actually observed. Opening a
    /// database that does not contain the table (e.g. an empty database that would be created in place of a
    /// still-missing file) must leave the flag set, otherwise the repair would be lost permanently once the
    /// real file becomes reachable.
    if (!markRemoteGeneratedColumns(sqlite_db.get(), remote_table_or_query.getTableName(), columns, log))
        return;

    StorageInMemoryMetadata new_metadata = *old_metadata;
    new_metadata.setColumns(std::move(columns));
    setInMemoryMetadata(new_metadata);

    generated_columns_reclassification_pending.store(false, std::memory_order_release);
}

void StorageSQLite::updateExternalDynamicMetadataIfExists(ContextPtr query_context)
{
    if (!generated_columns_reclassification_pending.load(std::memory_order_acquire))
        return;

    /// The interpreters call this hook right before they freeze the query's metadata snapshot and run the
    /// materialized-column checks (`InterpreterInsertQuery`, `InterpreterSelectQuery`, `InterpreterDescribeQuery`),
    /// so repairing the pending generated-column classification here - rather than only in `read`/`write`, which
    /// run after the snapshot is taken - lets even the first query after the database file becomes reachable
    /// (including an `INSERT`, whose column list and `SQLiteSink` omit-set both depend on the classification)
    /// see the corrected metadata.
    ///
    /// Best-effort: if the file is still unavailable, keep the classification pending and do nothing. The
    /// subsequent `read`/`write` reopen the database with `throw_on_error = true` and surface the error, so no
    /// operation runs on a stale classification silently.
    /// Non-creating probe: if the database file is still missing, keep the classification pending rather
    /// than opening a freshly created empty database (which contains no table and would otherwise mark the
    /// repair as done). The real file becoming reachable later then still repairs the classification. The
    /// guarded helper keeps this open race-free against a concurrent first `read`/`write`.
    if (!openConnectionIfNeeded(/* throw_on_error */ false, /* allow_create */ false))
        return;

    reclassifyGeneratedColumnsFromRemote(query_context);
}

VirtualColumnsDescription StorageSQLite::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}


ColumnsDescription StorageSQLite::getTableStructureFromData(
    const SQLitePtr & sqlite_db_,
    const TableNameOrQuery & table_or_query)
{
    if (table_or_query.isQuery())
        return doQueryResultStructure(sqlite_db_.get(), table_or_query.getQuery());

    const auto & table = table_or_query.getTableName();
    auto columns = fetchSQLiteTableStructure(sqlite_db_.get(), table);

    if (!columns)
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "Failed to fetch table structure for {}", table);

    return std::move(*columns);
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
    /// A read must never materialize a missing SQLite database. In particular, query-backed storages are
    /// read-only and do not have pending generated-column reclassification, so deriving `allow_create` from
    /// that flag would create an empty file on the first read after an `ATTACH` while the file is unavailable.
    openConnectionIfNeeded(/* throw_on_error */ true, /* allow_create */ false);

    /// Fallback: `updateExternalDynamicMetadataIfExists` normally repairs the pending classification before the
    /// snapshot is taken; this covers any path that reaches `read` without going through that hook. Idempotent.
    reclassifyGeneratedColumnsFromRemote(context_);

    storage_snapshot->check(column_names);

    String query;
    if (remote_table_or_query.isQuery())
    {
        /// The user-provided query is passed to SQLite as is; no outer predicate is pushed down into it, so
        /// reject any outer filter under external_table_strict_query.
        rejectOuterFilterForQueryBackedExternalSourceIfStrict(query_info, context_);
        query = buildQueryForExternalDatabaseSubquery(remote_table_or_query.getQuery(), column_names, IdentifierQuotingStyle::DoubleQuotesStandard);
    }
    else
    {
        /// Use all physical columns (ordinary + the MATERIALIZED generated columns) as the pushdown-eligible
        /// set: SQLite can filter on generated columns too, so a `WHERE` over them is still pushed down.
        ///
        /// The exception is types the sink stores as SQLite TEXT while ClickHouse compares them by value:
        /// `UInt64` (SQLite has no unsigned 64-bit integer type, so it is written as text to preserve values
        /// above the signed 64-bit range), the wider integers `Int128`/`UInt128`/`Int256`/`UInt256`,
        /// `Decimal`, dates and times, and so on (see `isPushdownSafeType`). SQLite compares such a
        /// text-stored value lexicographically and orders every text value after every numeric one, so a
        /// predicate pushed down verbatim would drop the wrong rows: `WHERE u = 5` never matches the text
        /// cell `'5'`, and `WHERE u > 2` treats `'10'` as smaller than `'2'`. Exclude those columns from the
        /// eligible set so ClickHouse applies such predicates locally instead.
        NamesAndTypesList pushdown_columns;
        for (const auto & column : storage_snapshot->metadata->getColumns().getAllPhysical())
            if (SQLiteFormatImpl::isPushdownSafeType(column.type))
                pushdown_columns.push_back(column);

        query = transformQueryForExternalDatabase(
            query_info,
            column_names,
            pushdown_columns,
            /// SQLite has no escape sequences inside quoted identifiers or string literals: an embedded quote
            /// is doubled and every other byte - a backslash or a control character such as `\n`/`\t` - stays
            /// literal. The ClickHouse-style backslash escaping of `DoubleQuotes`/`Regular` (and even the
            /// `PostgreSQL` literal style, which still backslash-escapes control characters) would make the
            /// pushed-down query look up a different identifier or match a different literal than the value
            /// actually stored, so a filter such as `WHERE s = 'a\nb'` would silently miss its rows.
            IdentifierQuotingStyle::DoubleQuotesStandard,
            LiteralEscapingStyle::StandardSQL,
            "",
            remote_table_or_query.getTableName(),
            context_);
    }
    LOG_TRACE(log, "Query: {}", query);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({column_data.type, column_data.name});
    }

    /// Each read runs on its own dedicated connection: `SQLiteSource::onCancel` aborts a running statement
    /// with `sqlite3_interrupt`, which is connection-wide in SQLite. On the shared `sqlite_db` handle - also
    /// used by every concurrent query on this table, and by all tables of a `DatabaseSQLite` - cancelling one
    /// query could interrupt an unrelated sibling statement mid-scan. `allow_create` stays false: a read must
    /// never materialize a missing database file.
    auto read_connection = openSQLiteDB(database_path, getContext(), /* throw_on_error */ true, /* allow_create */ false);
    return Pipe(std::make_shared<SQLiteSource>(read_connection, query, sample_block, max_block_size));
}


class SQLiteSink final : public SinkToStorage
{
public:
    explicit SQLiteSink(
        const StorageSQLite & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        StorageSQLite::SQLitePtr sqlite_db_,
        const String & remote_table_name_,
        const Names & explicitly_inserted_columns_)
        : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
        , metadata_snapshot(metadata_snapshot_)
        , sqlite_db(sqlite_db_)
        , remote_table_name(remote_table_name_)
        , explicitly_inserted_columns(explicitly_inserted_columns_.begin(), explicitly_inserted_columns_.end())
        , format_settings(getFormatSettings(storage_.getContext()))
    {
        /// SQLite generated columns are kept in the table structure as `MATERIALIZED` (readable but not
        /// insertable). ClickHouse still computes a placeholder for them and includes them in the block
        /// reaching the sink, so collect their names to omit automatically added placeholders from the SQLite
        /// INSERT while retaining explicitly inserted columns for SQLite to reject.
        for (const auto & column : metadata_snapshot_->getColumns().getMaterialized())
            generated_columns.insert(column.name);
    }

    String getName() const override { return "SQLiteSink"; }

    void consume(Chunk & chunk) override
    {
        auto block = getHeader().cloneWithColumns(chunk.getColumns());

        /// Drop generated columns that the insert pipeline added automatically: SQLite computes them itself,
        /// so they must not appear in the INSERT column list. A generated column explicitly named by the user
        /// is preserved, allowing SQLite to reject the unsupported write instead of silently discarding its value.
        Block insertable_block;
        for (const auto & elem : block)
            if (!generated_columns.contains(elem.name) || explicitly_inserted_columns.contains(elem.name))
                insertable_block.insert(elem);

        const size_t num_columns = insertable_block.columns();
        if (num_columns == 0)
            return;

        /// Build a parameterized statement `INSERT INTO t (c1, ...) VALUES (?, ...)` and bind each row's
        /// values instead of formatting them into the SQL text. SQLite string literals have no escape
        /// sequences, so serializing values as text would corrupt control characters and truncate on NUL;
        /// binding passes every byte to SQLite faithfully.
        WriteBufferFromOwnString sqlbuf;
        sqlbuf << "INSERT INTO " << quoteSQLiteIdentifier(remote_table_name) << " (";
        for (size_t i = 0; i < num_columns; ++i)
        {
            if (i != 0)
                sqlbuf << ", ";
            sqlbuf << quoteSQLiteIdentifier(insertable_block.getByPosition(i).name);
        }
        sqlbuf << ") VALUES (";
        for (size_t i = 0; i < num_columns; ++i)
            sqlbuf << (i == 0 ? "?" : ", ?");
        sqlbuf << ")";

        const auto query = sqlbuf.str();

        std::vector<SerializationPtr> serializations;
        serializations.reserve(num_columns);
        for (size_t i = 0; i < num_columns; ++i)
            serializations.emplace_back(insertable_block.getByPosition(i).type->getDefaultSerialization());

        /// Reusing a single-row prepared statement executes one SQLite statement per row. Without an explicit
        /// transaction, SQLite autocommits every successful row and a later constraint violation leaves a partial
        /// chunk behind. Keep the previous multi-row INSERT semantics by committing the whole chunk atomically.
        SQLiteFormatImpl::executeSQLite(sqlite_db.get(), "BEGIN");
        try
        {
            sqlite3_stmt * compiled_stmt = nullptr;
            int status = sqlite3_prepare_v2(sqlite_db.get(), query.c_str(), static_cast<int>(query.size() + 1), &compiled_stmt, nullptr);
            if (status != SQLITE_OK)
                throw Exception(
                    ErrorCodes::SQLITE_ENGINE_ERROR,
                    "Cannot prepare sqlite INSERT statement. Status: {}. Message: {}",
                    status,
                    sqlite3_errmsg(sqlite_db.get()));

            std::unique_ptr<sqlite3_stmt, StatementDeleter> statement(compiled_stmt, StatementDeleter());

            const size_t num_rows = insertable_block.rows();
            for (size_t row = 0; row < num_rows; ++row)
            {
                for (size_t i = 0; i < num_columns; ++i)
                {
                    const auto & elem = insertable_block.getByPosition(i);
                    const int sqlite_index = static_cast<int>(i + 1);
                    if (elem.column->isNullAt(row))
                        SQLiteFormatImpl::checkSQLiteStatus(
                            sqlite_db.get(), sqlite3_bind_null(statement.get(), sqlite_index), "Cannot bind NULL value");
                    else
                        /// The shared binder keeps the dispatch identical to the `SQLite` output format: `Bool` is
                        /// bound as 0/1, `UInt64` (which can exceed the SQLite INTEGER range) and NaN (which
                        /// `sqlite3_bind_double` would turn into SQLite NULL) go through the text path.
                        SQLiteFormatImpl::bindSQLiteValue(
                            sqlite_db.get(),
                            statement.get(),
                            sqlite_index,
                            *elem.column,
                            row,
                            elem.type,
                            *serializations[i],
                            format_settings);
                }

                status = sqlite3_step(statement.get());
                if (status != SQLITE_DONE)
                    throw Exception(
                        ErrorCodes::SQLITE_ENGINE_ERROR,
                        "Failed to execute sqlite INSERT query. Status: {}. Message: {}",
                        status,
                        sqlite3_errmsg(sqlite_db.get()));

                sqlite3_reset(statement.get());
                sqlite3_clear_bindings(statement.get());
            }

            statement.reset();
            SQLiteFormatImpl::executeSQLite(sqlite_db.get(), "COMMIT");
        }
        catch (...)
        {
            /// A conflict handler may have rolled the transaction back already.
            if (!sqlite3_get_autocommit(sqlite_db.get()))
            {
                try
                {
                    SQLiteFormatImpl::executeSQLite(sqlite_db.get(), "ROLLBACK");
                }
                catch (...)
                {
                    tryLogCurrentException(getLogger("SQLiteSink"), "Failed to roll back SQLite chunk transaction");
                }
            }
            throw;
        }
    }

private:
    struct StatementDeleter
    {
        void operator()(sqlite3_stmt * stmt) const { sqlite3_finalize(stmt); }
    };

    StorageMetadataPtr metadata_snapshot;
    StorageSQLite::SQLitePtr sqlite_db;
    String remote_table_name;
    std::unordered_set<String> generated_columns;
    std::unordered_set<String> explicitly_inserted_columns;
    FormatSettings format_settings;
};


SinkToStoragePtr StorageSQLite::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_, bool /*async_insert*/)
{
    if (remote_table_or_query.isQuery())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot write into a SQLite table representing the result of a query");

    openConnectionIfNeeded(
        /* throw_on_error */ true,
        /* allow_create */ !generated_columns_reclassification_pending.load(std::memory_order_acquire));

    /// Fallback: `updateExternalDynamicMetadataIfExists` normally repairs the pending classification before the
    /// insert's metadata snapshot is taken; this covers any path that reaches `write` without that hook.
    /// Idempotent - it runs at most once and is a no-op once the classification has been repaired.
    reclassifyGeneratedColumnsFromRemote(context_);

    Names explicitly_inserted_columns;
    /// The context records the explicit column list of the INSERT being executed. Adopt it when this storage
    /// is the insert's direct target: either a named table matching the recorded insertion table, or an INSERT
    /// INTO TABLE FUNCTION `sqlite`. The latter leaves the recorded insertion table unset (and `StorageID`'s
    /// comparison throws on an empty id, so check for emptiness first), but the column list is still recorded,
    /// and a generated column named in it must reach SQLite to be rejected there instead of being dropped.
    /// The direct target receives the original insert query AST here, while a materialized-view push receives
    /// the view's select query and a non-empty recorded insertion table, so it never adopts the list.
    const auto & insertion_table = context_->getInsertionTable();
    const auto * insert_query = query ? query->as<ASTInsertQuery>() : nullptr;
    bool is_direct_insert_target = insertion_table.empty()
        ? (insert_query && insert_query->table_function)
        : insertion_table == getStorageID();
    if (is_direct_insert_target)
    {
        const auto & insertion_column_names = context_->getInsertionTableColumnNames();
        if (insertion_column_names)
            explicitly_inserted_columns = *insertion_column_names;
    }

    /// A transaction is connection-wide in SQLite. Give every sink its own connection so chunk transactions from
    /// concurrent inserts cannot overlap on the shared metadata connection (which can also be shared by all tables
    /// of a `DatabaseSQLite`). The database was opened above, so this connection must not create a missing file.
    auto write_connection = openSQLiteDB(database_path, getContext(), /* throw_on_error */ true, /* allow_create */ false);

    return std::make_shared<SQLiteSink>(
        *this, metadata_snapshot, write_connection, remote_table_or_query.getTableName(), explicitly_inserted_columns);
}


void registerStorageSQLite(StorageFactory & factory);
void registerStorageSQLite(StorageFactory & factory)
{
    factory.registerStorage("SQLite", [](const StorageFactory::Arguments & args) -> StoragePtr
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "SQLite database requires 2 arguments: database path, table name (or query)");

        /// The 2nd argument is either a table name, or a query passed to SQLite as is - `(SELECT ...)` or `query('SELECT ...')`.
        auto maybe_query = tryGetExternalDatabaseQuery(
            engine_args[1], args.getLocalContext(), IdentifierQuotingStyle::DoubleQuotesStandard, LiteralEscapingStyle::StandardSQL);
        for (size_t i = 0; i < engine_args.size(); ++i)
        {
            if (i == 1 && maybe_query)
                continue;
            engine_args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[i], args.getLocalContext());
        }

        const auto database_path = checkAndGetLiteralArgument<String>(engine_args[0], "database_path");
        TableNameOrQuery table_or_query = maybe_query
            ? TableNameOrQuery(TableNameOrQuery::Type::QUERY, *maybe_query)
            : TableNameOrQuery(TableNameOrQuery::Type::TABLE, checkAndGetLiteralArgument<String>(engine_args[1], "table_name"));

        /// Only a genuine `CREATE` may materialize a missing database file. An `ATTACH` (or a server restart
        /// replaying the stored definition) must not create it as a side effect: the table has to come up with
        /// the connection left unopened, so that a later read fails closed while the file is unavailable
        /// (see `openConnectionIfNeeded`) instead of silently querying a fabricated empty database.
        const bool is_create = args.mode <= LoadingStrictnessLevel::CREATE;
        auto sqlite_db = openSQLiteDB(database_path, args.getContext(), /* throw_on_error */ is_create, /* allow_create */ is_create);

        ColumnsDescription columns = args.columns;
        /// Re-apply the generated-column classification from the remote schema for an explicitly declared
        /// column list (an explicit `CREATE`, an `ATTACH` replaying the stored definition, or a `SHOW CREATE`
        /// round-trip). The auto-inferred case (empty column list) already gets the classification straight
        /// from `fetchSQLiteTableStructure` in the storage constructor, and a query-backed source is
        /// read-only, so it needs no insertability classification.
        bool generated_columns_reclassification_pending = !columns.empty() && !table_or_query.isQuery();
        if (generated_columns_reclassification_pending && sqlite_db)
            generated_columns_reclassification_pending =
                !markRemoteGeneratedColumns(sqlite_db.get(), table_or_query.getTableName(), columns, getLogger("StorageSQLite"));

        return std::make_shared<StorageSQLite>(args.table_id, sqlite_db, database_path,
                                     table_or_query, columns, args.constraints, args.comment, args.getContext(),
                                     generated_columns_reclassification_pending);
    },
    {
        .supports_schema_inference = true,
        .source_access_type = AccessTypeObjects::Source::SQLITE,
    },
    Documentation{
        .description = R"DOCS_MD(
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# SQLite table engine

<CloudNotSupportedBadge/>

The engine allows to import and export data to SQLite and supports queries to SQLite tables directly from ClickHouse.

## Creating a table {#creating-a-table}

```sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = SQLite('db_path', 'table')
```

**Engine Parameters**

- `db_path` — Path to SQLite file with a database.
- `table` — Name of a table in the SQLite database, or a query passed to SQLite as is (see [Passing a query instead of a table name](#passing-a-query)).

## Passing a query instead of a table name {#passing-a-query}

Instead of a table name, the `table` argument can be a `SELECT` query that is passed to SQLite as is. The structure of the table is inferred from the query result. The query can be written either as a subquery, or wrapped into the `query` function:

```sql
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

Such a table is read-only: `INSERT` into it is not allowed. The same syntax is supported by the [`sqlite`](/sql-reference/table-functions/sqlite) table function.

:::note
The subquery form `(SELECT ...)` is parsed by ClickHouse and re-serialized before being sent to SQLite. It must therefore be valid ClickHouse SQL. To pass SQLite-specific syntax that ClickHouse does not parse, use the `query('...')` form, whose text is sent to SQLite verbatim.

Any outer `WHERE`, `LIMIT`, aggregation, etc. of the surrounding ClickHouse query is **not** pushed down into the passed query — it is applied in ClickHouse after the full query result is fetched. To restrict the data read from SQLite, put the filter inside the passed query. With [`external_table_strict_query = 1`](/operations/settings/settings#external_table_strict_query) an outer filter that cannot be pushed down is rejected with an exception instead of being applied locally.
:::

## Data types support {#data-types-support}

When you explicitly specify ClickHouse column types in the table definition, the following ClickHouse types can be parsed from SQLite TEXT columns:

- [Date](../../../sql-reference/data-types/date.md), [Date32](../../../sql-reference/data-types/date32.md)
- [DateTime](../../../sql-reference/data-types/datetime.md), [DateTime64](../../../sql-reference/data-types/datetime64.md)
- [UUID](../../../sql-reference/data-types/uuid.md)
- [Enum8, Enum16](../../../sql-reference/data-types/enum.md)
- [Decimal32, Decimal64, Decimal128, Decimal256](../../../sql-reference/data-types/decimal.md)
- [FixedString](../../../sql-reference/data-types/fixedstring.md)
- All integer types ([UInt8, UInt16, UInt32, UInt64, UInt128, UInt256, Int8, Int16, Int32, Int64, Int128, Int256](../../../sql-reference/data-types/int-uint.md))
- [Float32, Float64](../../../sql-reference/data-types/float.md)

See [SQLite database engine](../../../engines/database-engines/sqlite.md#data_types-support) for the default type mapping.

## Usage example {#usage-example}

Shows a query creating the SQLite table:

```sql
SHOW CREATE TABLE sqlite_db.table2;
```

```text
CREATE TABLE SQLite.table2
(
    `col1` Nullable(Int32),
    `col2` Nullable(String)
)
ENGINE = SQLite('sqlite.db','table2');
```

Returns the data from the table:

```sql
SELECT * FROM sqlite_db.table2 ORDER BY col1;
```

```text
┌─col1─┬─col2──┐
│    1 │ text1 │
│    2 │ text2 │
│    3 │ text3 │
└──────┴───────┘
```

**See Also**

- [SQLite](../../../engines/database-engines/sqlite.md) engine
- [sqlite](../../../sql-reference/table-functions/sqlite.md) table function
)DOCS_MD",
        .syntax = "ENGINE = SQLite('path_to_database_file', 'table')",
        .related = {"MySQL", "PostgreSQL"}});
}

namespace
{
ColumnsDescription doQueryResultStructure(sqlite3 * sqlite_db, const String & query)
{
    /// Wrap the query into a subquery (mirroring how the data is read) and prepare it to read the result
    /// columns metadata without executing it. SQLite is dynamically typed, so column types are inferred from
    /// the declared types of the underlying table columns; expression columns fall back to Nullable(String).
    const auto wrapped = "SELECT * FROM (" + query + ") AS __subquery";

    sqlite3_stmt * compiled_stmt = nullptr;
    int status = sqlite3_prepare_v2(sqlite_db, wrapped.c_str(), static_cast<int>(wrapped.size() + 1), &compiled_stmt, nullptr);
    if (status != SQLITE_OK)
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR,
                        "Cannot prepare SQLite query. Status: {}. Message: {}",
                        status, sqlite3_errstr(status));

    SCOPE_EXIT({ sqlite3_finalize(compiled_stmt); });

    const int column_count = sqlite3_column_count(compiled_stmt);
    if (column_count == 0)
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "SQLite query returned no columns: {}", query);

    NamesAndTypesList columns;
    for (int i = 0; i < column_count; ++i)
    {
        const char * name = sqlite3_column_name(compiled_stmt, i);
        const char * decl_type = sqlite3_column_decltype(compiled_stmt, i);

        DataTypePtr type = decl_type ? convertSQLiteDataType(decl_type) : std::make_shared<DataTypeString>();
        columns.emplace_back(String(name), std::make_shared<DataTypeNullable>(type));
    }

    return ColumnsDescription{columns};
}
}

}

#endif
