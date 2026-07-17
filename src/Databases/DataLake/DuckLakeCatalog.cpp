#include <Databases/DataLake/DuckLakeCatalog.h>

#include "config.h"

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/SettingsEnums.h>
#include <Interpreters/Context.h>

#include <fmt/format.h>

#if USE_SQLITE
#include <Databases/SQLite/SQLiteUtils.h>
#include <sqlite3.h>
#endif

#if USE_LIBPQXX
#include <pqxx/pqxx>
#endif

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int SUPPORT_IS_DISABLED;
extern const int SQLITE_ENGINE_ERROR;
extern const int POSTGRESQL_CONNECTION_FAILURE;
}

namespace
{

/// Quoting helpers. Identifiers and literals embedded into catalog SQL always come from
/// either the catalog itself or from already-validated settings, but we quote anyway.
String quoteLiteral(const String & value)
{
    String result;
    result.reserve(value.size() + 2);
    result.push_back('\'');
    for (char c : value)
    {
        if (c == '\'')
            result.push_back('\'');
        result.push_back(c);
    }
    result.push_back('\'');
    return result;
}

String quoteIdentifier(const String & name)
{
    String result;
    result.reserve(name.size() + 2);
    result.push_back('"');
    for (char c : name)
    {
        if (c == '"')
            result.push_back('"');
        result.push_back(c);
    }
    result.push_back('"');
    return result;
}

bool parseBool(const std::optional<String> & value)
{
    if (!value.has_value())
        return false;
    return *value == "1" || *value == "t" || *value == "true" || *value == "yes";
}

Int64 parseInt64(const std::optional<String> & value, const String & what)
{
    if (!value.has_value())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected NULL {} in the DuckLake catalog", what);
    return std::stoll(*value);
}

}

class IDuckLakeConnection
{
public:
    virtual ~IDuckLakeConnection() = default;

    using Row = std::vector<std::optional<String>>;
    virtual std::vector<Row> exec(const String & query) = 0;
    virtual bool tableExists(const String & name) = 0;

    /// ducklake_* table reference including the catalog schema qualifier for postgres.
    virtual String qualified(const String & table) = 0;
};

#if USE_SQLITE

class DuckLakeSQLiteConnection final : public IDuckLakeConnection
{
public:
    DuckLakeSQLiteConnection(const String & database_path_, ContextPtr context_)
        : database_path(database_path_)
        , context(std::move(context_))
    {
    }

    const String & getDatabasePath() const { return database_path; }

    std::vector<Row> exec(const String & query) override
    {
        std::lock_guard lock(mutex);
        ensureOpen();

        std::vector<Row> rows;
        auto callback = [](void * res, int col_num, char ** data_by_col, char ** /* col_names */) -> int
        {
            Row row;
            row.reserve(col_num);
            for (int i = 0; i < col_num; ++i)
            {
                if (data_by_col[i])
                    row.emplace_back(String(data_by_col[i]));
                else
                    row.emplace_back(std::nullopt);
            }
            static_cast<std::vector<Row> *>(res)->push_back(std::move(row));
            return 0;
        };

        char * err_message = nullptr;
        int status = sqlite3_exec(db.get(), query.c_str(), callback, &rows, &err_message);
        if (status != SQLITE_OK)
        {
            String err_msg = err_message ? err_message : "unknown error";
            sqlite3_free(err_message);
            throw Exception(
                ErrorCodes::SQLITE_ENGINE_ERROR,
                "DuckLake catalog query failed. Error status: {}. Message: {}. Query: {}",
                status,
                err_msg,
                query);
        }
        return rows;
    }

    bool tableExists(const String & name) override
    {
        return !exec(fmt::format("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = {} LIMIT 1", quoteLiteral(name))).empty();
    }

    String qualified(const String & table) override { return quoteIdentifier(table); }

private:
    String database_path;
    ContextPtr context;
    SQLitePtr db;
    std::mutex mutex;

    void ensureOpen()
    {
        if (!db)
            db = openSQLiteDB(database_path, context, /* throw_on_error */ true);
    }
};

#endif

#if USE_LIBPQXX

class DuckLakePostgresConnection final : public IDuckLakeConnection
{
public:
    DuckLakePostgresConnection(const String & conninfo_, const String & catalog_schema_)
        : conninfo(conninfo_)
        , catalog_schema(catalog_schema_)
    {
    }

    std::vector<Row> exec(const String & query) override
    {
        std::lock_guard lock(mutex);
        try
        {
            ensureOpen();
            pqxx::nontransaction tx(*connection);
            pqxx::result res = tx.exec(query);

            std::vector<Row> rows;
            rows.reserve(res.size());
            for (const auto & prow : res)
            {
                Row row;
                row.reserve(prow.size());
                for (const auto & field : prow)
                {
                    if (field.is_null())
                        row.emplace_back(std::nullopt);
                    else
                        row.emplace_back(String(field.c_str()));
                }
                rows.push_back(std::move(row));
            }
            return rows;
        }
        catch (const pqxx::broken_connection & e)
        {
            connection.reset();
            throw Exception(ErrorCodes::POSTGRESQL_CONNECTION_FAILURE, "DuckLake catalog connection broken: {}", e.what());
        }
    }

    bool tableExists(const String & name) override
    {
        const auto rows = exec(fmt::format(
            "SELECT to_regclass({}) IS NOT NULL",
            quoteLiteral(fmt::format("{}.{}", catalog_schema, name))));
        return !rows.empty() && parseBool(rows[0][0]);
    }

    String qualified(const String & table) override
    {
        return fmt::format("{}.{}", quoteIdentifier(catalog_schema), quoteIdentifier(table));
    }

private:
    String conninfo;
    String catalog_schema;
    std::unique_ptr<pqxx::connection> connection;
    std::mutex mutex;

    void ensureOpen()
    {
        if (!connection || !connection->is_open())
            connection = std::make_unique<pqxx::connection>(conninfo);
    }
};

#endif

namespace
{

String visibilityPredicate(Int64 snapshot_id, const String & alias)
{
    return fmt::format(
        "({0} >= {1}.begin_snapshot AND ({0} < {1}.end_snapshot OR {1}.end_snapshot IS NULL))", snapshot_id, alias);
}

/// DuckLake does not vend storage credentials. For local tables no credentials are needed at
/// all, so satisfy the requiresCredentials() request with a no-op. For remote storages the
/// generic static-credentials machinery applies instead.
class NoCredentials final : public DataLake::IStorageCredentials
{
public:
    void addCredentialsToEngineArgs(DB::ASTs &) const override { }
};

}

DuckLakeCatalog::DuckLakeCatalog(
    const std::string & warehouse_,
    const std::string & backend_,
    const std::string & connection_string_,
    const std::string & catalog_schema_,
    ContextPtr context_)
    : ICatalog(warehouse_)
{
    if (backend_ == "sqlite")
    {
#if USE_SQLITE
        sqlite_database_path = connection_string_;
        connection = std::make_unique<DuckLakeSQLiteConnection>(connection_string_, context_);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "ClickHouse was compiled without SQLite support");
#endif
    }
    else if (backend_ == "postgres")
    {
#if USE_LIBPQXX
        connection = std::make_unique<DuckLakePostgresConnection>(connection_string_, catalog_schema_);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "ClickHouse was compiled without PostgreSQL support");
#endif
    }
    else
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unknown ducklake_backend '{}': expected 'postgres' or 'sqlite'",
            backend_);
    }

    /// Validate the catalog and read global metadata.
    String catalog_version;
    bool encrypted = false;
    for (const auto & row : connection->exec(fmt::format("SELECT key, value FROM {} WHERE scope IS NULL", connection->qualified("ducklake_metadata"))))
    {
        const auto & key = row.at(0);
        const auto & value = row.at(1);
        if (!key.has_value())
            continue;
        if (*key == "version")
            catalog_version = value.value_or("");
        else if (*key == "data_path")
            data_path = value.value_or("");
        else if (*key == "encrypted")
            encrypted = parseBool(value);
    }

    if (catalog_version != "1.0")
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "DuckLake catalog schema version '{}' is not supported (only '1.0')",
            catalog_version.empty() ? "<missing>" : catalog_version);
    if (encrypted)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "DuckLake catalog encryption is not supported");
    if (data_path.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DuckLake catalog has no 'data_path' in ducklake_metadata");

    if (data_path.find("://") == String::npos && !data_path.starts_with("/"))
    {
        if (sqlite_database_path.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "DuckLake catalog data_path '{}' is relative; relative data paths are only supported with the sqlite backend",
                data_path);
        /// Resolve against the catalog file location, like DuckDB does. openSQLiteDB resolves
        /// relative database paths against the user_files directory, so mirror that here.
        String base = sqlite_database_path;
        if (!base.starts_with("/"))
            base = context_->getUserFilesPath() + base;
        const auto slash = base.find_last_of('/');
        data_path = (slash == String::npos ? String() : base.substr(0, slash + 1)) + data_path;
    }
}

DuckLakeCatalog::~DuckLakeCatalog() = default;

DB::DatabaseDataLakeCatalogType DuckLakeCatalog::getCatalogType() const
{
    return DB::DatabaseDataLakeCatalogType::DUCKLAKE;
}

Int64 DuckLakeCatalog::pinSnapshot() const
{
    const auto rows = connection->exec(fmt::format("SELECT COALESCE(MAX(snapshot_id), 0) FROM {}", connection->qualified("ducklake_snapshot")));
    if (rows.empty())
        return 0;
    return parseInt64(rows[0][0], "snapshot_id");
}

bool DuckLakeCatalog::empty() const
{
    return getTables().empty();
}

DuckLakeCatalog::Namespaces DuckLakeCatalog::getNamespaces() const
{
    const Int64 snapshot = pinSnapshot();
    const auto rows = connection->exec(fmt::format(
        "SELECT schema_name FROM {} WHERE {} ORDER BY schema_name",
        connection->qualified("ducklake_schema"),
        visibilityPredicate(snapshot, "ducklake_schema")));

    Namespaces result;
    result.reserve(rows.size());
    for (const auto & row : rows)
        result.push_back(row[0].value_or(""));
    return result;
}

DataLake::CatalogTables DuckLakeCatalog::listTablesInNamespaceDirect(const std::string & namespace_name) const
{
    const Int64 snapshot = pinSnapshot();
    const auto rows = connection->exec(fmt::format(
        "SELECT t.table_name FROM {0} t "
        "JOIN {1} s ON s.schema_id = t.schema_id "
        "WHERE s.schema_name = {2} AND {3} AND {4} "
        "ORDER BY t.table_name",
        connection->qualified("ducklake_table"),
        connection->qualified("ducklake_schema"),
        quoteLiteral(namespace_name),
        visibilityPredicate(snapshot, "s"),
        visibilityPredicate(snapshot, "t")));

    DataLake::CatalogTables result;
    result.reserve(rows.size());
    for (const auto & row : rows)
        result.push_back(DataLake::CatalogTable{.name = namespace_name + "." + row[0].value_or(""), .is_readable = true});
    return result;
}

DataLake::CatalogTables DuckLakeCatalog::getTables() const
{
    DataLake::CatalogTables result;
    for (const auto & namespace_name : getNamespaces())
    {
        auto tables = listTablesInNamespaceDirect(namespace_name);
        result.insert(result.end(), std::make_move_iterator(tables.begin()), std::make_move_iterator(tables.end()));
    }
    return result;
}

std::optional<std::pair<Int64, Int64>> DuckLakeCatalog::findTable(const String & namespace_name, const String & table_name, Int64 snapshot_id) const
{
    const auto rows = connection->exec(fmt::format(
        "SELECT t.table_id, s.schema_id FROM {0} t "
        "JOIN {1} s ON s.schema_id = t.schema_id "
        "WHERE s.schema_name = {2} AND t.table_name = {3} AND {4} AND {5}",
        connection->qualified("ducklake_table"),
        connection->qualified("ducklake_schema"),
        quoteLiteral(namespace_name),
        quoteLiteral(table_name),
        visibilityPredicate(snapshot_id, "s"),
        visibilityPredicate(snapshot_id, "t")));

    if (rows.empty())
        return std::nullopt;
    return std::make_pair(parseInt64(rows[0][0], "table_id"), parseInt64(rows[0][1], "schema_id"));
}

bool DuckLakeCatalog::existsTable(const std::string & namespace_name, const std::string & table_name) const
{
    return findTable(namespace_name, table_name, pinSnapshot()).has_value();
}

namespace
{

String joinPaths(const String & base, const String & suffix)
{
    if (base.empty() || base.ends_with('/'))
        return base + suffix;
    return base + "/" + suffix;
}

}

String DuckLakeCatalog::getTableDataPath(const String & namespace_name, const String & table_name, Int64 snapshot_id) const
{
    const auto rows = connection->exec(fmt::format(
        "SELECT s.path, s.path_is_relative, t.path, t.path_is_relative FROM {0} t "
        "JOIN {1} s ON s.schema_id = t.schema_id "
        "WHERE s.schema_name = {2} AND t.table_name = {3} AND {4} AND {5}",
        connection->qualified("ducklake_table"),
        connection->qualified("ducklake_schema"),
        quoteLiteral(namespace_name),
        quoteLiteral(table_name),
        visibilityPredicate(snapshot_id, "s"),
        visibilityPredicate(snapshot_id, "t")));

    if (rows.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DuckLake table {}.{} does not exist", namespace_name, table_name);

    const auto & row = rows[0];
    const String schema_path = row[0].value_or("");
    const bool schema_path_relative = parseBool(row[1]);
    const String table_path = row[2].value_or("");
    const bool table_path_relative = parseBool(row[3]);

    String location = schema_path_relative || schema_path.empty() ? joinPaths(data_path, schema_path) : schema_path;
    location = table_path_relative || table_path.empty() ? joinPaths(location, table_path) : table_path;

    if (location.find("://") == String::npos)
        location = "file://" + location;
    return location;
}

std::vector<DuckLake::ColumnInfo> DuckLakeCatalog::getColumnRows(Int64 table_id) const
{
    const auto rows = connection->exec(fmt::format(
        "SELECT column_id, parent_column, column_order, column_name, column_type, nulls_allowed, "
        "begin_snapshot, end_snapshot FROM {} WHERE table_id = {} ORDER BY column_id",
        connection->qualified("ducklake_column"),
        table_id));

    std::vector<DuckLake::ColumnInfo> result;
    result.reserve(rows.size());
    for (const auto & row : rows)
    {
        DuckLake::ColumnInfo info;
        info.column_id = parseInt64(row[0], "column_id");
        if (row[1].has_value())
            info.parent_column = std::stoll(*row[1]);
        info.column_order = parseInt64(row[2], "column_order");
        info.name = row[3].value_or("");
        info.type = row[4].value_or("");
        info.nulls_allowed = parseBool(row[5]);
        info.begin_snapshot = parseInt64(row[6], "begin_snapshot");
        if (row[7].has_value())
            info.end_snapshot = std::stoll(*row[7]);
        result.push_back(std::move(info));
    }
    return result;
}

void DuckLakeCatalog::checkNoInlinedData(Int64 table_id, Int64 snapshot_id) const
{
    const auto inlined_tables = connection->exec(fmt::format(
        "SELECT table_name FROM {} WHERE table_id = {}",
        connection->qualified("ducklake_inlined_data_tables"),
        table_id));

    for (const auto & row : inlined_tables)
    {
        const String inlined_table = row[0].value_or("");
        if (inlined_table.empty() || !connection->tableExists(inlined_table))
            continue;
        const auto visible = connection->exec(fmt::format(
            "SELECT 1 FROM {} inlined WHERE {} LIMIT 1",
            connection->qualified(inlined_table),
            visibilityPredicate(snapshot_id, "inlined")));
        if (!visible.empty())
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "DuckLake table (id {}) has inlined data rows (data inlining) which are not supported; "
                "flush the inlined data (ducklake_flush_inlined_data) or disable inlining",
                table_id);
    }

    const String inlined_deletes = fmt::format("ducklake_inlined_delete_{}", table_id);
    if (connection->tableExists(inlined_deletes))
    {
        const auto visible = connection->exec(fmt::format(
            "SELECT 1 FROM {} WHERE begin_snapshot <= {} LIMIT 1",
            connection->qualified(inlined_deletes),
            snapshot_id));
        if (!visible.empty())
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "DuckLake table (id {}) has inlined deletions which are not supported; "
                "flush the inlined data (ducklake_flush_inlined_data) or disable inlining",
                table_id);
    }
}

DuckLakeTableSnapshotInfo DuckLakeCatalog::getTableSnapshotInfo(const String & namespace_name, const String & table_name) const
{
    const Int64 snapshot = pinSnapshot();
    const auto table = findTable(namespace_name, table_name, snapshot);
    if (!table.has_value())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DuckLake table {}.{} does not exist", namespace_name, table_name);
    const auto [table_id, schema_id] = *table;

    checkNoInlinedData(table_id, snapshot);

    const auto column_rows = getColumnRows(table_id);
    auto roots = DuckLake::buildColumnTree(column_rows, snapshot);

    return DuckLakeTableSnapshotInfo{
        .snapshot_id = snapshot,
        .table_id = table_id,
        .schema = DuckLake::getTableSchema(roots),
        .field_id_map = DuckLake::buildFieldIdMap(column_rows, snapshot),
    };
}

std::vector<DuckLakeDataFileEntry> DuckLakeCatalog::getDataFiles(Int64 table_id, Int64 snapshot_id) const
{
    const auto rows = connection->exec(fmt::format(
        "SELECT data.data_file_id, data.path, data.path_is_relative, data.record_count, data.file_size_bytes, "
        "data.encryption_key, data.mapping_id, data.file_format, "
        "del.path, del.path_is_relative, del.format, del.delete_count, del.encryption_key "
        "FROM {0} data "
        "LEFT JOIN {1} del ON del.data_file_id = data.data_file_id AND {2} "
        "WHERE data.table_id = {3} AND {4} "
        "ORDER BY data.data_file_id",
        connection->qualified("ducklake_data_file"),
        connection->qualified("ducklake_delete_file"),
        visibilityPredicate(snapshot_id, "del"),
        table_id,
        visibilityPredicate(snapshot_id, "data")));

    std::vector<DuckLakeDataFileEntry> result;
    for (const auto & row : rows)
    {
        const Int64 data_file_id = parseInt64(row[0], "data_file_id");

        if (row[5].has_value() && !row[5]->empty())
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "DuckLake data file '{}' is encrypted; Parquet modular encryption is not supported",
                row[1].value_or(""));
        if (row[6].has_value())
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "DuckLake data file '{}' uses a column name mapping (added via ducklake_add_data_files); "
                "name-mapped files are not supported",
                row[1].value_or(""));
        const String file_format = Poco::toLower(row[7].value_or("parquet"));
        if (file_format != "parquet")
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "DuckLake data file '{}' has format '{}' which is not supported (only 'parquet')",
                row[1].value_or(""),
                file_format);

        if (result.empty() || result.back().data_file_id != data_file_id)
        {
            result.push_back(DuckLakeDataFileEntry{
                .data_file_id = data_file_id,
                .path = row[1].value_or(""),
                .path_is_relative = parseBool(row[2]),
                .record_count = row[3].has_value() ? std::stoll(*row[3]) : 0,
                .file_size_bytes = row[4].has_value() ? std::stoll(*row[4]) : 0,
                .delete_files = {},
            });
        }

        if (row[8].has_value())
        {
            const String delete_format = Poco::toLower(row[10].value_or("parquet"));
            if (delete_format != "parquet")
                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED,
                    "DuckLake delete file '{}' has format '{}' (puffin deletion vectors are not supported)",
                    row[8].value_or(""),
                    delete_format);
            if (row[12].has_value() && !row[12]->empty())
                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED,
                    "DuckLake delete file '{}' is encrypted; Parquet modular encryption is not supported",
                    row[8].value_or(""));

            result.back().delete_files.push_back(DuckLakeDeleteFileEntry{
                .path = row[8].value_or(""),
                .path_is_relative = parseBool(row[9]),
                .delete_count = row[11].has_value() ? std::stoll(*row[11]) : 0,
            });
        }
    }
    return result;
}

void DuckLakeCatalog::getTableMetadata(
    const std::string & namespace_name,
    const std::string & table_name,
    DataLake::TableMetadata & result) const
{
    if (!tryGetTableMetadata(namespace_name, table_name, result))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DuckLake table {}.{} does not exist", namespace_name, table_name);
}

bool DuckLakeCatalog::tryGetTableMetadata(
    const std::string & namespace_name,
    const std::string & table_name,
    DataLake::TableMetadata & result) const
{
    const Int64 snapshot = pinSnapshot();
    if (!findTable(namespace_name, table_name, snapshot).has_value())
        return false;

    const auto info = getTableSnapshotInfo(namespace_name, table_name);
    const String location = getTableDataPath(namespace_name, table_name, snapshot);
    result.setLocation(location);
    result.setSchema(info.schema);
    result.setDataLakeSpecificProperties(DataLake::DataLakeSpecificProperties{
        .iceberg_metadata_file_location = "",
        .ducklake_schema_name = namespace_name,
        .ducklake_table_name = table_name,
    });
    if (result.requiresCredentials() && location.starts_with("file://"))
        result.setStorageCredentials(std::make_shared<NoCredentials>());
    return true;
}

}
