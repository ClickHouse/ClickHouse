#include <Databases/SQLite/DatabaseSQLite.h>

#if USE_SQLITE

#include <Storages/AlterCommands.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/SQLite/fetchSQLiteTableStructure.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Storages/StorageSQLite.h>
#include <Databases/SQLite/SQLiteUtils.h>
#include <Common/quoteString.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
    extern const int SQLITE_ENGINE_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int BAD_ARGUMENTS;
}

DatabaseSQLite::DatabaseSQLite(
        ContextPtr context_,
        const ASTStorage * database_engine_define_,
        bool is_attach_,
        const String & database_path_)
    : DatabaseWithAltersOnDiskBase("SQLite")
    , WithContext(context_->getGlobalContext())
    , database_engine_define(database_engine_define_->clone())
    , database_path(database_path_)
    , log(getLogger("DatabaseSQLite"))
{
    sqlite_db = openSQLiteDB(database_path_, context_, !is_attach_);
}


bool DatabaseSQLite::empty() const
{
    std::lock_guard lock(mutex);
    return fetchTablesList().empty();
}


DatabaseTablesIteratorPtr DatabaseSQLite::getTablesIterator(ContextPtr local_context, const IDatabase::FilterByNameFunction &, bool) const
{
    std::lock_guard lock(mutex);

    Tables tables;
    auto table_names = fetchTablesList();
    for (const auto & table_name : table_names)
        tables[table_name] = fetchTable(table_name, local_context, true);

    return std::make_unique<DatabaseTablesSnapshotIterator>(tables, database_name);
}


NameSet DatabaseSQLite::fetchTablesList() const
{
    if (!sqlite_db)
        sqlite_db = openSQLiteDB(database_path, getContext(), /* throw_on_error */true);

    std::unordered_set<String> tables;
    std::string query = "SELECT name FROM sqlite_master "
                        "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'";

    auto callback_get_data = [](void * res, int col_num, char ** data_by_col, char ** /* col_names */) -> int
    {
        for (int i = 0; i < col_num; ++i)
            static_cast<std::unordered_set<std::string> *>(res)->insert(data_by_col[i]);
        return 0;
    };

    char * err_message = nullptr;
    int status = sqlite3_exec(sqlite_db.get(), query.c_str(), callback_get_data, &tables, &err_message);
    if (status != SQLITE_OK)
    {
        String err_msg(err_message);
        sqlite3_free(err_message);
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR,
                        "Cannot fetch sqlite database tables. Error status: {}. Message: {}",
                        status, err_msg);
    }

    return tables;
}


bool DatabaseSQLite::checkSQLiteTable(const String & table_name) const
{
    if (!sqlite_db)
        sqlite_db = openSQLiteDB(database_path, getContext(), /* throw_on_error */true);

    const String query = "SELECT name FROM sqlite_master WHERE type = 'table' AND name = " + quoteStringSQLite(table_name) + ";";

    auto callback_get_data = [](void * res, int, char **, char **) -> int
    {
        *(static_cast<int *>(res)) += 1;
        return 0;
    };

    int count = 0;
    char * err_message = nullptr;
    int status = sqlite3_exec(sqlite_db.get(), query.c_str(), callback_get_data, &count, &err_message);
    if (status != SQLITE_OK)
    {
        String err_msg(err_message);
        sqlite3_free(err_message);
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR,
                        "Cannot check sqlite table. Error status: {}. Message: {}",
                        status, err_msg);
    }

    return (count != 0);
}


bool DatabaseSQLite::isTableExist(const String & table_name, ContextPtr) const
{
    std::lock_guard lock(mutex);
    return checkSQLiteTable(table_name);
}


StoragePtr DatabaseSQLite::tryGetTable(const String & table_name, ContextPtr local_context) const
{
    std::lock_guard lock(mutex);
    return fetchTable(table_name, local_context, false);
}


StoragePtr DatabaseSQLite::fetchTable(const String & table_name, ContextPtr local_context, bool table_checked) const
{
    if (!sqlite_db)
        sqlite_db = openSQLiteDB(database_path, getContext(), /* throw_on_error */true);

    if (!table_checked && !checkSQLiteTable(table_name))
        return StoragePtr{};

    auto columns = fetchSQLiteTableStructure(sqlite_db.get(), table_name);

    if (!columns)
        return StoragePtr{};

    auto storage = std::make_shared<StorageSQLite>(
        StorageID(database_name, table_name),
        sqlite_db,
        database_path,
        table_name,
        ColumnsDescription{*columns},
        ConstraintsDescription{},
        /* comment = */ "",
        local_context);

    return storage;
}


ASTPtr DatabaseSQLite::getCreateDatabaseQueryImpl() const
{
    auto create_query = make_intrusive<ASTCreateQuery>();
    create_query->setDatabase(database_name);
    create_query->set(create_query->storage, database_engine_define);

    if (!comment.empty())
        create_query->set(create_query->comment, make_intrusive<ASTLiteral>(comment));

    return create_query;
}

ASTPtr DatabaseSQLite::getCreateTableQueryImpl(const String & table_name, ContextPtr local_context, bool throw_on_error) const
{
    StoragePtr storage;
    {
        std::lock_guard lock(mutex);
        storage = fetchTable(table_name, local_context, false);
    }
    if (!storage)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "SQLite table {}.{} does not exist",
                            getDatabaseName(), table_name);
        return nullptr;
    }
    auto table_storage_define = database_engine_define->clone();
    ASTStorage * ast_storage = table_storage_define->as<ASTStorage>();
    ast_storage->engine->setKind(ASTFunction::Kind::TABLE_ENGINE);
    auto storage_engine_arguments = ast_storage->engine->arguments;
    auto table_id = storage->getStorageID();
    /// Add table_name to engine arguments
    storage_engine_arguments->children.insert(storage_engine_arguments->children.begin() + 1, make_intrusive<ASTLiteral>(table_id.table_name));

    const Settings & settings = getContext()->getSettingsRef();

    auto create_table_query = DB::getCreateQueryFromStorage(
        storage,
        table_storage_define,
        true,
        static_cast<uint32_t>(settings[Setting::max_parser_depth]),
        static_cast<uint32_t>(settings[Setting::max_parser_backtracks]),
        throw_on_error,
        getContext());

    return create_table_query;
}

void registerDatabaseSQLite(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        auto * engine_define = args.create_query.storage;
        const ASTFunction * engine = engine_define->engine;

        if (!engine->arguments || engine->arguments->children.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "SQLite database requires 1 argument: database path");

        const auto & arguments = engine->arguments->children;

        String database_path = safeGetLiteralValue<String>(arguments[0], "SQLite");

        return std::make_shared<DatabaseSQLite>(args.context, engine_define, args.create_query.attach, database_path);
    };
    factory.registerDatabase("SQLite", create_fn, {
        .supports_arguments = true,
        .is_external = true,
        .source_access_type = AccessTypeObjects::Source::SQLITE,
    }, Documentation{
        .description = R"DOCS_MD(
Allows to connect to [SQLite](https://www.sqlite.org/index.html) database and perform `INSERT` and `SELECT` queries to exchange data between ClickHouse and SQLite.

## Creating a database {#creating-a-database}

```sql
    CREATE DATABASE sqlite_database
    ENGINE = SQLite('db_path')
```

**Engine Parameters**

- `db_path` — Path to a file with SQLite database.

## Data types support {#data_types-support}

The table below shows the default type mapping when ClickHouse automatically infers schema from SQLite:

|  SQLite   | ClickHouse                                              |
|---------------|---------------------------------------------------------|
| INTEGER       | [Int32](../../sql-reference/data-types/int-uint.md)     |
| REAL          | [Float32](../../sql-reference/data-types/float.md)      |
| TEXT          | [String](../../sql-reference/data-types/string.md)      |
| TEXT          | [UUID](../../sql-reference/data-types/uuid.md)          |
| BLOB          | [String](../../sql-reference/data-types/string.md)      |

When you explicitly define a table with specific ClickHouse types using the [SQLite table engine](../../engines/table-engines/integrations/sqlite.md), the following ClickHouse types can be parsed from SQLite TEXT columns:

- [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md)
- [DateTime](../../sql-reference/data-types/datetime.md), [DateTime64](../../sql-reference/data-types/datetime64.md)
- [UUID](../../sql-reference/data-types/uuid.md)
- [Enum8, Enum16](../../sql-reference/data-types/enum.md)
- [Decimal32, Decimal64, Decimal128, Decimal256](../../sql-reference/data-types/decimal.md)
- [FixedString](../../sql-reference/data-types/fixedstring.md)
- All integer types ([UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../../sql-reference/data-types/int-uint.md))
- [Float32, Float64](../../sql-reference/data-types/float.md)

SQLite has dynamic typing, and its type access functions perform automatic type coercion. For example, reading a TEXT column as an integer will return 0 if the text cannot be parsed as a number. This means that if a ClickHouse table is defined with a different type than the underlying SQLite column, values may be silently coerced rather than causing an error.

## Specifics and recommendations {#specifics-and-recommendations}

SQLite stores the entire database (definitions, tables, indices, and the data itself) as a single cross-platform file on a host machine. During writing SQLite locks the entire database file, therefore write operations are performed sequentially. Read operations can be multi-tasked.
SQLite does not require service management (such as startup scripts) or access control based on `GRANT` and passwords. Access control is handled by means of file-system permissions given to the database file itself.

## Usage example {#usage-example}

Database in ClickHouse, connected to the SQLite:

```sql
CREATE DATABASE sqlite_db ENGINE = SQLite('sqlite.db');
SHOW TABLES FROM sqlite_db;
```

```text
┌──name───┐
│ table1  │
│ table2  │
└─────────┘
```

Shows the tables:

```sql
SELECT * FROM sqlite_db.table1;
```

```text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```
Inserting data into SQLite table from ClickHouse table:

```sql
CREATE TABLE clickhouse_table(`col1` String,`col2` Int16) ENGINE = MergeTree() ORDER BY col2;
INSERT INTO clickhouse_table VALUES ('text',10);
INSERT INTO sqlite_db.table1 SELECT * FROM clickhouse_table;
SELECT * FROM sqlite_db.table1;
```

```text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
│ text  │   10 │
└───────┴──────┘
```
)DOCS_MD",
        .syntax = "ENGINE = SQLite('path_to_database_file')",
        .related = {"MySQL", "PostgreSQL"}});
}
}

#endif
