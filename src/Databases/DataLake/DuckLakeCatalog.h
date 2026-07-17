#pragma once

#include <Databases/DataLake/ICatalog.h>
#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakeTypes.h>
#include <Interpreters/Context_fwd.h>

#include <memory>
#include <mutex>

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

/// One row of ducklake_data_file (visible at a pinned snapshot) with its delete files.
struct DuckLakeDataFileEntry
{
    Int64 data_file_id;
    String path;
    bool path_is_relative;
    Int64 record_count;
    Int64 file_size_bytes;
    std::vector<DuckLakeDeleteFileEntry> delete_files;
};

/// Everything DuckLakeMetadata needs to serve reads at one pinned snapshot.
struct DuckLakeTableSnapshotInfo
{
    Int64 snapshot_id;
    Int64 table_id;
    NamesAndTypesList schema;
    std::unordered_map<String, Int64> field_id_map;
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

    /// Pin MAX(snapshot_id) and load schema + field-id map for one table, running the
    /// unsupported-feature guard checks (inlining). Throws if the table does not exist.
    DuckLakeTableSnapshotInfo getTableSnapshotInfo(const String & namespace_name, const String & table_name) const;

    /// List data files (with their positional delete files) visible at `snapshot_id`.
    /// Throws on unsupported per-file features (encryption, name mapping, puffin).
    std::vector<DuckLakeDataFileEntry> getDataFiles(Int64 table_id, Int64 snapshot_id) const;

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
    std::vector<DuckLake::ColumnInfo> getColumnRows(Int64 table_id) const;
    void checkNoInlinedData(Int64 table_id, Int64 snapshot_id) const;
};

}
