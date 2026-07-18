#pragma once

#include <Databases/DataLake/ICatalog.h>
#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakeTypes.h>
#include <Interpreters/Context_fwd.h>

#include <memory>
#include <mutex>
#include <map>

namespace DB
{

class IDuckLakeConnection;

/// One row of ducklake_delete_file joined to its data file.
struct DuckLakeDeleteFileEntry
{
    String path;
    bool path_is_relative;
    Int64 delete_count;
};

/// One row of ducklake_file_column_stats for one data file.
struct DuckLakeFileColumnStats
{
    Int64 column_id;
    Int64 value_count;
    Int64 null_count;
    bool contains_nan;
    /// Min/max serialized by the DuckLake writer (plain numbers, ISO dates/timestamps,
    /// raw strings); empty when the file has no values for the column.
    std::optional<String> min_value;
    std::optional<String> max_value;
};

/// One row of ducklake_data_file (visible at a pinned snapshot) with its delete files,
/// column statistics, partition values and inlined deletions.
struct DuckLakeDataFileEntry
{
    Int64 data_file_id;
    String path;
    bool path_is_relative;
    Int64 record_count;
    Int64 file_size_bytes;
    /// Partition spec this file was written with (index into DuckLakeFileListing::partition_specs).
    std::optional<Int64> partition_id;
    std::vector<DuckLakeDeleteFileEntry> delete_files;
    std::vector<DuckLakeFileColumnStats> column_stats;
    /// Partition values serialized like stats values, indexed by partition_key_index.
    std::vector<std::optional<String>> partition_values;
    /// File-relative positions deleted via the inlined deletion table
    /// (ducklake_inlined_delete_N), already translated from global row ids.
    std::vector<UInt64> inlined_deleted_positions;
};

/// One row of ducklake_partition_column: one partition key of one partition spec.
struct DuckLakePartitionField
{
    Int64 partition_key_index;
    Int64 column_id;
    /// identity, year, month, day, hour or bucket(N).
    String transform;
};

/// Result of DuckLakeCatalog::getDataFiles: the visible files plus the partition specs
/// they reference (partition_id -> fields sorted by partition_key_index).
struct DuckLakeFileListing
{
    std::vector<DuckLakeDataFileEntry> files;
    std::unordered_map<Int64, std::vector<DuckLakePartitionField>> partition_specs;
};

/// One entry of ducklake_inlined_data_tables: an SQL table in the catalog database that
/// holds inlined insert rows for one schema version of a table.
struct DuckLakeInlinedDataTable
{
    String table_name;
    Int64 schema_version;
};

/// Everything DuckLakeMetadata needs to serve reads at one pinned snapshot.
struct DuckLakeTableSnapshotInfo
{
    Int64 snapshot_id;
    Int64 table_id;
    NamesAndTypesList schema;
    std::unordered_map<String, Int64> field_id_map;
    /// column_id -> name+type of every currently visible column (including nested children,
    /// whose names are their bare element names; only ids and types matter for them).
    std::unordered_map<Int64, NameAndTypePair> column_types;
};

/// Read-only DuckLake catalog on top of PostgreSQL or SQLite, implementing the DuckLake 1.0
/// metadata schema (ducklake_metadata_manager.cpp in the DuckLake repository).
class DuckLakeCatalog : public DataLake::ICatalog
{
public:
    DuckLakeCatalog(
        const std::string & warehouse_,
        const std::string & backend_,
        const std::string & connection_string_,
        const std::string & catalog_schema_,
        ContextPtr context_);
    ~DuckLakeCatalog() override;

    DB::DatabaseDataLakeCatalogType getCatalogType() const override;

    bool empty() const override;
    DataLake::CatalogTables getTables() const override;
    Namespaces getNamespaces() const override;
    bool existsTable(const std::string & namespace_name, const std::string & table_name) const override;
    void getTableMetadata(const std::string & namespace_name, const std::string & table_name, DataLake::TableMetadata & result) const override;
    bool tryGetTableMetadata(const std::string & namespace_name, const std::string & table_name, DataLake::TableMetadata & result) const override;
    std::optional<DataLake::StorageType> getStorageType() const override { return std::nullopt; }
    bool isTransactional() const override { return false; }

    /// Whether the catalog backend is PostgreSQL (affects how inlined values are serialized:
    /// strings/blobs are bytea hex, booleans are t/f, timestamps are DuckDB text).
    bool isPostgres() const;

    /// Pin MAX(snapshot_id) and load schema + field-id map for one table.
    /// Throws if the table does not exist.
    DuckLakeTableSnapshotInfo getTableSnapshotInfo(const String & namespace_name, const String & table_name) const;

    /// List data files (with delete files, column stats, partition values and inlined
    /// deletions) visible at `snapshot_id`, plus the partition specs they reference.
    /// Throws on unsupported per-file features (encryption, name mapping, puffin).
    DuckLakeFileListing getDataFiles(Int64 table_id, Int64 snapshot_id) const;

    /// Inlined data tables registered for `table_id` (ducklake_inlined_data_tables).
    /// Tables that do not exist in the catalog are skipped (already dropped by a flush).
    std::vector<DuckLakeInlinedDataTable> getInlinedDataTables(Int64 table_id) const;

    /// SQL column names and row values of `inlined_table` visible at `snapshot_id`
    /// (begin/end snapshot visibility), ordered by row_id.
    /// Returns empty column_names when the table does not exist.
    std::pair<std::vector<String>, std::vector<std::vector<std::optional<String>>>>
    getInlinedRows(const String & inlined_table, Int64 snapshot_id) const;

    /// Global schema_version -> first snapshot_id that has it (ducklake_snapshot is global).
    /// Inlined data tables are named with the global schema version at their creation, so
    /// this maps an inlined table's schema_version to the snapshot whose visible column
    /// names match the inlined table's SQL columns.
    std::map<Int64, Int64> getSchemaVersionFirstSnapshots() const;

    /// All ducklake_column rows (full history) of `table_id`, sorted by column_id.
    std::vector<DuckLake::ColumnInfo> getColumnRows(Int64 table_id) const;

    /// Absolute table data path (with URI scheme) as resolved from data_path + schema/table paths.
    String getTableDataPath(const String & namespace_name, const String & table_name, Int64 snapshot_id) const;

protected:
    DataLake::CatalogTables listTablesInNamespaceDirect(const std::string & namespace_name) const override;

private:
    std::unique_ptr<IDuckLakeConnection> connection;
    /// From ducklake_metadata: the catalog-wide data path prefix, exactly as stored.
    String data_path;
    /// Raw sqlite database file path (sqlite backend only), used to resolve a relative data_path.
    String sqlite_database_path;

    Int64 pinSnapshot() const;
    std::optional<std::pair<Int64, Int64>> findTable(const String & namespace_name, const String & table_name, Int64 snapshot_id) const;
};

}
