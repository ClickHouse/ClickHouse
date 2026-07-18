#include <Databases/DataLake/DuckLakeCatalog.h>

#include "config.h"

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/SettingsEnums.h>
#include <Interpreters/Context.h>

#include <fmt/format.h>

#include <algorithm>

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

struct DuckLakeQueryResult
{
    std::vector<String> column_names;
    using Row = std::vector<std::optional<String>>;
    std::vector<Row> rows;
};

class IDuckLakeConnection
{
public:
    virtual ~IDuckLakeConnection() = default;

    using Row = std::vector<std::optional<String>>;
    virtual DuckLakeQueryResult exec(const String & query) = 0;
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

    DuckLakeQueryResult exec(const String & query) override
    {
        std::lock_guard lock(mutex);
        ensureOpen();

        sqlite3_stmt * stmt = nullptr;
        DuckLakeQueryResult result;
        /// The query is a single statement; prepare/step it directly (sqlite3_exec would
        /// stringify BLOB values and truncate them at the first zero byte).
        int status = sqlite3_prepare_v2(db.get(), query.c_str(), static_cast<int>(query.size() + 1), &stmt, nullptr);
        if (status != SQLITE_OK)
            throw Exception(
                ErrorCodes::SQLITE_ENGINE_ERROR,
                "DuckLake catalog query failed to prepare. Error status: {}. Query: {}",
                status,
                query);

        std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> stmt_holder(stmt, &sqlite3_finalize);

        const int col_num = sqlite3_column_count(stmt);
        while ((status = sqlite3_step(stmt)) == SQLITE_ROW)
        {
            if (result.column_names.empty())
            {
                result.column_names.reserve(col_num);
                for (int i = 0; i < col_num; ++i)
                    result.column_names.emplace_back(sqlite3_column_name(stmt, i));
            }
            DuckLakeQueryResult::Row row;
            row.reserve(col_num);
            for (int i = 0; i < col_num; ++i)
            {
                if (sqlite3_column_type(stmt, i) == SQLITE_NULL)
                {
                    row.emplace_back(std::nullopt);
                    continue;
                }
                const char * data = reinterpret_cast<const char *>(sqlite3_column_text(stmt, i));
                const int bytes = sqlite3_column_bytes(stmt, i);
                row.emplace_back(String(data, bytes));
            }
            result.rows.push_back(std::move(row));
        }
        if (status != SQLITE_DONE)
            throw Exception(
                ErrorCodes::SQLITE_ENGINE_ERROR,
                "DuckLake catalog query failed. Error status: {}. Query: {}",
                status,
                query);
        return result;
    }

    bool tableExists(const String & name) override
    {
        return !exec(fmt::format("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = {} LIMIT 1", quoteLiteral(name))).rows.empty();
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

    DuckLakeQueryResult exec(const String & query) override
    {
        std::lock_guard lock(mutex);
        try
        {
            ensureOpen();
            pqxx::nontransaction tx(*connection);
            pqxx::result res = tx.exec(query);

            DuckLakeQueryResult result;
            result.column_names.reserve(res.columns());
            for (pqxx::row::size_type i = 0; i < res.columns(); ++i)
                result.column_names.emplace_back(res.column_name(i));

            result.rows.reserve(res.size());
            for (const auto & prow : res)
            {
                DuckLakeQueryResult::Row row;
                row.reserve(prow.size());
                for (const auto & field : prow)
                {
                    if (field.is_null())
                        row.emplace_back(std::nullopt);
                    else
                        row.emplace_back(String(field.c_str()));
                }
                result.rows.push_back(std::move(row));
            }
            return result;
        }
        catch (const pqxx::broken_connection & e)
        {
            connection.reset();
            throw Exception(ErrorCodes::POSTGRESQL_CONNECTION_FAILURE, "DuckLake catalog connection broken: {}", e.what());
        }
    }

    bool tableExists(const String & name) override
    {
        const auto result = exec(fmt::format(
            "SELECT to_regclass({}) IS NOT NULL",
            quoteLiteral(fmt::format("{}.{}", catalog_schema, name))));
        return !result.rows.empty() && parseBool(result.rows[0][0]);
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
    for (const auto & row : connection->exec(fmt::format("SELECT key, value FROM {} WHERE scope IS NULL", connection->qualified("ducklake_metadata"))).rows)
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

bool DuckLakeCatalog::isPostgres() const
{
    return sqlite_database_path.empty();
}

Int64 DuckLakeCatalog::pinSnapshot() const
{
    const auto result = connection->exec(fmt::format("SELECT COALESCE(MAX(snapshot_id), 0) FROM {}", connection->qualified("ducklake_snapshot")));
    if (result.rows.empty())
        return 0;
    return parseInt64(result.rows[0][0], "snapshot_id");
}

bool DuckLakeCatalog::empty() const
{
    return getTables().empty();
}

DuckLakeCatalog::Namespaces DuckLakeCatalog::getNamespaces() const
{
    const Int64 snapshot = pinSnapshot();
    const auto result = connection->exec(fmt::format(
        "SELECT schema_name FROM {} WHERE {} ORDER BY schema_name",
        connection->qualified("ducklake_schema"),
        visibilityPredicate(snapshot, "ducklake_schema")));

    Namespaces namespaces;
    namespaces.reserve(result.rows.size());
    for (const auto & row : result.rows)
        namespaces.push_back(row[0].value_or(""));
    return namespaces;
}

DataLake::CatalogTables DuckLakeCatalog::listTablesInNamespaceDirect(const std::string & namespace_name) const
{
    const Int64 snapshot = pinSnapshot();
    const auto result = connection->exec(fmt::format(
        "SELECT t.table_name FROM {0} t "
        "JOIN {1} s ON s.schema_id = t.schema_id "
        "WHERE s.schema_name = {2} AND {3} AND {4} "
        "ORDER BY t.table_name",
        connection->qualified("ducklake_table"),
        connection->qualified("ducklake_schema"),
        quoteLiteral(namespace_name),
        visibilityPredicate(snapshot, "s"),
        visibilityPredicate(snapshot, "t")));

    DataLake::CatalogTables tables;
    tables.reserve(result.rows.size());
    for (const auto & row : result.rows)
        tables.push_back(DataLake::CatalogTable{.name = namespace_name + "." + row[0].value_or(""), .is_readable = true});
    return tables;
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
    const auto result = connection->exec(fmt::format(
        "SELECT t.table_id, s.schema_id FROM {0} t "
        "JOIN {1} s ON s.schema_id = t.schema_id "
        "WHERE s.schema_name = {2} AND t.table_name = {3} AND {4} AND {5}",
        connection->qualified("ducklake_table"),
        connection->qualified("ducklake_schema"),
        quoteLiteral(namespace_name),
        quoteLiteral(table_name),
        visibilityPredicate(snapshot_id, "s"),
        visibilityPredicate(snapshot_id, "t")));

    if (result.rows.empty())
        return std::nullopt;
    return std::make_pair(parseInt64(result.rows[0][0], "table_id"), parseInt64(result.rows[0][1], "schema_id"));
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
    const auto result = connection->exec(fmt::format(
        "SELECT s.path, s.path_is_relative, t.path, t.path_is_relative FROM {0} t "
        "JOIN {1} s ON s.schema_id = t.schema_id "
        "WHERE s.schema_name = {2} AND t.table_name = {3} AND {4} AND {5}",
        connection->qualified("ducklake_table"),
        connection->qualified("ducklake_schema"),
        quoteLiteral(namespace_name),
        quoteLiteral(table_name),
        visibilityPredicate(snapshot_id, "s"),
        visibilityPredicate(snapshot_id, "t")));

    if (result.rows.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DuckLake table {}.{} does not exist", namespace_name, table_name);

    const auto & row = result.rows[0];
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
    const auto result = connection->exec(fmt::format(
        "SELECT column_id, parent_column, column_order, column_name, column_type, nulls_allowed, "
        "begin_snapshot, end_snapshot FROM {} WHERE table_id = {} ORDER BY column_id",
        connection->qualified("ducklake_column"),
        table_id));

    std::vector<DuckLake::ColumnInfo> columns;
    columns.reserve(result.rows.size());
    for (const auto & row : result.rows)
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
        columns.push_back(std::move(info));
    }
    return columns;
}

DuckLakeTableSnapshotInfo DuckLakeCatalog::getTableSnapshotInfo(const String & namespace_name, const String & table_name) const
{
    const Int64 snapshot = pinSnapshot();
    const auto table = findTable(namespace_name, table_name, snapshot);
    if (!table.has_value())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DuckLake table {}.{} does not exist", namespace_name, table_name);
    const auto [table_id, schema_id] = *table;

    const auto column_rows = getColumnRows(table_id);
    auto roots = DuckLake::buildColumnTree(column_rows, snapshot);

    std::unordered_map<Int64, NameAndTypePair> column_types;
    std::function<void(const DuckLake::ColumnNode &)> collect_types = [&](const DuckLake::ColumnNode & node)
    {
        column_types.emplace(node.info.column_id, NameAndTypePair(node.info.name, DuckLake::getColumnType(node)));
        for (const auto & child : node.children)
            collect_types(child);
    };
    for (const auto & root : roots)
        collect_types(root);

    return DuckLakeTableSnapshotInfo{
        .snapshot_id = snapshot,
        .table_id = table_id,
        .schema = DuckLake::getTableSchema(roots),
        .field_id_map = DuckLake::buildFieldIdMap(column_rows, snapshot),
        .column_types = std::move(column_types),
    };
}

DuckLakeFileListing DuckLakeCatalog::getDataFiles(Int64 table_id, Int64 snapshot_id) const
{
    DuckLakeFileListing listing;

    const auto data_files = connection->exec(fmt::format(
        "SELECT data.data_file_id, data.path, data.path_is_relative, data.record_count, data.file_size_bytes, "
        "data.encryption_key, data.mapping_id, data.file_format, data.partition_id, "
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

    for (const auto & row : data_files.rows)
    {
        const Int64 data_file_id = parseInt64(row[0], "data_file_id");

        if (row[5].has_value() && !row[5]->empty())
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "DuckLake data file '{}' is encrypted; Parquet modular encryption is not supported",
                row[1].value_or(""));
        const String file_format = Poco::toLower(row[7].value_or("parquet"));
        if (file_format != "parquet")
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "DuckLake data file '{}' has format '{}' which is not supported (only 'parquet')",
                row[1].value_or(""),
                file_format);

        if (listing.files.empty() || listing.files.back().data_file_id != data_file_id)
        {
            DuckLakeDataFileEntry entry{
                .data_file_id = data_file_id,
                .path = row[1].value_or(""),
                .path_is_relative = parseBool(row[2]),
                .record_count = row[3].has_value() ? std::stoll(*row[3]) : 0,
                .file_size_bytes = row[4].has_value() ? std::stoll(*row[4]) : 0,
                .partition_id = std::nullopt,
                .mapping_id = std::nullopt,
                .name_mapping = {},
                .delete_files = {},
                .column_stats = {},
                .partition_values = {},
                .inlined_deleted_positions = {},
            };
            if (row[8].has_value())
                entry.partition_id = std::stoll(*row[8]);
            if (row[6].has_value())
                entry.mapping_id = std::stoll(*row[6]);
            listing.files.push_back(std::move(entry));
        }

        if (row[9].has_value())
        {
            const String delete_format = Poco::toLower(row[11].value_or("parquet"));
            if (delete_format != "parquet")
                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED,
                    "DuckLake delete file '{}' has format '{}' (puffin deletion vectors are not supported)",
                    row[9].value_or(""),
                    delete_format);
            if (row[13].has_value() && !row[13]->empty())
                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED,
                    "DuckLake delete file '{}' is encrypted; Parquet modular encryption is not supported",
                    row[9].value_or(""));

            listing.files.back().delete_files.push_back(DuckLakeDeleteFileEntry{
                .path = row[9].value_or(""),
                .path_is_relative = parseBool(row[10]),
                .delete_count = row[12].has_value() ? std::stoll(*row[12]) : 0,
            });
        }
    }

    if (listing.files.empty())
        return listing;

    const auto stats = connection->exec(fmt::format(
        "SELECT data_file_id, column_id, value_count, null_count, contains_nan, min_value, max_value "
        "FROM {} WHERE table_id = {}",
        connection->qualified("ducklake_file_column_stats"),
        table_id));
    for (const auto & row : stats.rows)
    {
        const Int64 data_file_id = parseInt64(row[0], "data_file_id");
        auto it = std::lower_bound(
            listing.files.begin(), listing.files.end(), data_file_id,
            [](const DuckLakeDataFileEntry & entry, Int64 id) { return entry.data_file_id < id; });
        if (it == listing.files.end() || it->data_file_id != data_file_id)
            continue;
        it->column_stats.push_back(DuckLakeFileColumnStats{
            .column_id = parseInt64(row[1], "column_id"),
            .value_count = row[2].has_value() ? std::stoll(*row[2]) : 0,
            .null_count = row[3].has_value() ? std::stoll(*row[3]) : 0,
            .contains_nan = parseBool(row[4]),
            .min_value = row[5],
            .max_value = row[6],
        });
    }

    const auto partition_values = connection->exec(fmt::format(
        "SELECT data_file_id, partition_key_index, partition_value FROM {} WHERE table_id = {}",
        connection->qualified("ducklake_file_partition_value"),
        table_id));
    for (const auto & row : partition_values.rows)
    {
        const Int64 data_file_id = parseInt64(row[0], "data_file_id");
        const auto partition_key_index = static_cast<size_t>(parseInt64(row[1], "partition_key_index"));
        auto it = std::lower_bound(
            listing.files.begin(), listing.files.end(), data_file_id,
            [](const DuckLakeDataFileEntry & entry, Int64 id) { return entry.data_file_id < id; });
        if (it == listing.files.end() || it->data_file_id != data_file_id)
            continue;
        if (it->partition_values.size() <= partition_key_index)
            it->partition_values.resize(partition_key_index + 1);
        it->partition_values[partition_key_index] = row[2];
    }

    const auto partition_specs = connection->exec(fmt::format(
        "SELECT pc.partition_id, pc.partition_key_index, pc.column_id, pc.transform "
        "FROM {0} pc "
        "JOIN {1} pi ON pi.partition_id = pc.partition_id AND {2} "
        "WHERE pc.table_id = {3} "
        "ORDER BY pc.partition_id, pc.partition_key_index",
        connection->qualified("ducklake_partition_column"),
        connection->qualified("ducklake_partition_info"),
        visibilityPredicate(snapshot_id, "pi"),
        table_id));
    for (const auto & row : partition_specs.rows)
    {
        const Int64 partition_id = parseInt64(row[0], "partition_id");
        listing.partition_specs[partition_id].push_back(DuckLakePartitionField{
            .partition_key_index = parseInt64(row[1], "partition_key_index"),
            .column_id = parseInt64(row[2], "column_id"),
            .transform = row[3].value_or(""),
        });
    }

    /// Name mappings for files added via ducklake_add_data_files (mapping_id NOT NULL).
    /// ducklake_name_mapping rows form a tree via parent_column (mapping row id of the
    /// parent entry); flatten it to dotted source paths per mapping_id.
    {
        std::unordered_map<Int64, size_t> position_by_mapping_id;
        for (size_t i = 0; i < listing.files.size(); ++i)
        {
            if (listing.files[i].mapping_id.has_value())
                position_by_mapping_id.emplace(*listing.files[i].mapping_id, i);
        }
        if (!position_by_mapping_id.empty())
        {
            const auto column_mappings = connection->exec(fmt::format(
                "SELECT mapping_id, type FROM {} WHERE table_id = {}",
                connection->qualified("ducklake_column_mapping"),
                table_id));
            for (const auto & row : column_mappings.rows)
            {
                const String mapping_type = row[1].value_or("");
                if (mapping_type != "map_by_name")
                    throw Exception(
                        ErrorCodes::SUPPORT_IS_DISABLED,
                        "DuckLake column mapping type '{}' is not supported (only 'map_by_name')",
                        mapping_type);
            }

            const auto name_mappings = connection->exec(fmt::format(
                "SELECT nm.mapping_id, nm.column_id, nm.source_name, nm.target_field_id, "
                "nm.parent_column, nm.is_partition "
                "FROM {0} nm "
                "JOIN {1} cm ON cm.mapping_id = nm.mapping_id "
                "WHERE cm.table_id = {2}",
                connection->qualified("ducklake_name_mapping"),
                connection->qualified("ducklake_column_mapping"),
                table_id));

            struct RawNameMapRow
            {
                Int64 mapping_id;
                String source_name;
                Int64 target_field_id;
                std::optional<Int64> parent_column;
                bool is_partition;
            };
            std::unordered_map<Int64, RawNameMapRow> rows_by_column_id;
            for (const auto & row : name_mappings.rows)
            {
                const Int64 column_id = parseInt64(row[1], "column_id");
                RawNameMapRow raw{
                    .mapping_id = parseInt64(row[0], "mapping_id"),
                    .source_name = row[2].value_or(""),
                    .target_field_id = parseInt64(row[3], "target_field_id"),
                    .parent_column = std::nullopt,
                    .is_partition = parseBool(row[5]),
                };
                if (row[4].has_value())
                    raw.parent_column = std::stoll(*row[4]);
                rows_by_column_id.emplace(column_id, std::move(raw));
            }

            for (const auto & [column_id, raw] : rows_by_column_id)
            {
                const auto file_it = position_by_mapping_id.find(raw.mapping_id);
                if (file_it == position_by_mapping_id.end())
                    continue;

                /// Walk the parent chain to build the dotted source path; guard against
                /// catalog corruption (cycles) with a depth limit.
                String source_path = raw.source_name;
                std::vector<Int64> ancestor_field_ids;
                auto parent = raw.parent_column;
                for (size_t depth = 0; parent.has_value() && depth < 1000; ++depth)
                {
                    const auto parent_it = rows_by_column_id.find(*parent);
                    if (parent_it == rows_by_column_id.end())
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "DuckLake name mapping (id {}) references unknown parent column {}",
                            raw.mapping_id,
                            *parent);
                    source_path = parent_it->second.source_name + "." + source_path;
                    ancestor_field_ids.push_back(parent_it->second.target_field_id);
                    parent = parent_it->second.parent_column;
                }
                if (parent.has_value())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "DuckLake name mapping (id {}) has a cycle in parent_column links",
                        raw.mapping_id);

                listing.files[file_it->second].name_mapping.push_back(DuckLakeNameMapEntry{
                    .source_path = std::move(source_path),
                    .target_field_id = raw.target_field_id,
                    .ancestor_field_ids = std::move(ancestor_field_ids),
                    .is_partition = raw.is_partition,
                });
            }
        }
    }

    /// Inlined deletions live in ducklake_inlined_delete_N (file_id, row_id, begin_snapshot),
    /// where row_id is the file-relative position (same as the pos column of delete files).
    /// Unlike everything else the rows are physically removed by a flush, so they are only
    /// consistent with the pinned snapshot when nothing commits concurrently; the snapshot
    /// re-check at the end of this method catches that race loudly.
    const String inlined_deletes_table = fmt::format("ducklake_inlined_delete_{}", table_id);
    if (connection->tableExists(inlined_deletes_table))
    {
        std::unordered_map<Int64, size_t> position_by_file_id;
        for (size_t i = 0; i < listing.files.size(); ++i)
            position_by_file_id.emplace(listing.files[i].data_file_id, i);

        const auto inlined_deletes = connection->exec(fmt::format(
            "SELECT file_id, row_id FROM {} WHERE begin_snapshot <= {}",
            connection->qualified(inlined_deletes_table),
            snapshot_id));
        for (const auto & row : inlined_deletes.rows)
        {
            const Int64 file_id = parseInt64(row[0], "file_id");
            const Int64 row_id = parseInt64(row[1], "row_id");
            auto it = position_by_file_id.find(file_id);
            if (it == position_by_file_id.end())
                continue;
            auto & file = listing.files[it->second];
            if (row_id < 0 || row_id >= file.record_count)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Inlined deletion at position {} is out of bounds for DuckLake data file '{}' "
                    "(record_count {})",
                    row_id,
                    file.path,
                    file.record_count);
            file.inlined_deleted_positions.push_back(static_cast<UInt64>(row_id));
        }
    }

    /// The catalog is read with several independent statements; a concurrent commit (e.g. an
    /// inlining flush physically removing ducklake_inlined_delete rows) could leave the file
    /// list and the inlined deletions inconsistent. Detect it and fail loudly rather than
    /// silently resurrecting deleted rows.
    if (pinSnapshot() != snapshot_id)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DuckLake catalog changed while reading table (id {}) metadata; retry the query",
            table_id);

    return listing;
}

std::vector<DuckLakeInlinedDataTable> DuckLakeCatalog::getInlinedDataTables(Int64 table_id) const
{
    const auto result = connection->exec(fmt::format(
        "SELECT table_name, schema_version FROM {} WHERE table_id = {}",
        connection->qualified("ducklake_inlined_data_tables"),
        table_id));

    std::vector<DuckLakeInlinedDataTable> tables;
    for (const auto & row : result.rows)
    {
        const String table_name = row[0].value_or("");
        if (table_name.empty() || !connection->tableExists(table_name))
            continue;
        tables.push_back(DuckLakeInlinedDataTable{
            .table_name = table_name,
            .schema_version = parseInt64(row[1], "schema_version"),
        });
    }
    return tables;
}

std::pair<std::vector<String>, std::vector<std::vector<std::optional<String>>>>
DuckLakeCatalog::getInlinedRows(const String & inlined_table, Int64 snapshot_id) const
{
    auto result = connection->exec(fmt::format(
        "SELECT * FROM {} inlined WHERE {} ORDER BY row_id",
        connection->qualified(inlined_table),
        visibilityPredicate(snapshot_id, "inlined")));
    return {std::move(result.column_names), std::move(result.rows)};
}

std::map<Int64, Int64> DuckLakeCatalog::getSchemaVersionFirstSnapshots() const
{
    const auto result = connection->exec(fmt::format(
        "SELECT schema_version, MIN(snapshot_id) FROM {} GROUP BY schema_version",
        connection->qualified("ducklake_snapshot")));

    std::map<Int64, Int64> versions;
    for (const auto & row : result.rows)
        versions.emplace(parseInt64(row[0], "schema_version"), parseInt64(row[1], "snapshot_id"));
    return versions;
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
