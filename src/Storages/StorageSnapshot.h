#pragma once
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class IStorage;

/// Snapshot of storage that fixes set columns that can be read in query.
/// There are 3 sources of columns: regular columns from metadata,
/// dynamic columns from object Types, virtual columns.
struct StorageSnapshot
{
    const IStorage & storage;
    const StorageMetadataPtr metadata;
    const ColumnsDescription object_columns;

    /// Additional data, on which set of columns may depend.
    /// E.g. data parts in MergeTree, list of blocks in Memory, etc.
    struct Data
    {
        virtual ~Data() = default;
    };

    using DataPtr = std::unique_ptr<const Data>;
    const DataPtr data;

    /// Projection that is used in query.
    mutable const ProjectionDescription * projection = nullptr;

    StorageSnapshot(
        const IStorage & storage_,
        const StorageMetadataPtr & metadata_)
        : storage(storage_), metadata(metadata_)
    {
        init();
    }

    StorageSnapshot(
        const IStorage & storage_,
        const StorageMetadataPtr & metadata_,
        const ColumnsDescription & object_columns_)
        : storage(storage_), metadata(metadata_), object_columns(object_columns_)
    {
        init();
    }

    StorageSnapshot(
        const IStorage & storage_,
        const StorageMetadataPtr & metadata_,
        const ColumnsDescription & object_columns_,
        DataPtr data_)
        : storage(storage_), metadata(metadata_), object_columns(object_columns_), data(std::move(data_))
    {
        init();
    }

    /// Get all available columns with types according to options.
    NamesAndTypesList getColumns(const GetColumnsOptions & options) const;

    /// Get columns with types according to options only for requested names.
    NamesAndTypesList getColumnsByNames(const GetColumnsOptions & options, const Names & names) const;

    /// Get column with type according to options for requested name.
    std::optional<NameAndTypePair> tryGetColumn(const GetColumnsOptions & options, const String & column_name) const;
    NameAndTypePair getColumn(const GetColumnsOptions & options, const String & column_name) const;

    /// Block with ordinary + materialized + aliases + virtuals + subcolumns.
    Block getSampleBlockForColumns(const Names & column_names) const;

    ColumnsDescription getDescriptionForColumns(const Names & column_names) const;

    /// Verify that all the requested names are in the table and are set correctly:
    /// list of names is not empty and the names do not repeat.
    void check(const Names & column_names) const;

    DataTypePtr getConcreteType(const String & column_name) const;

    void addProjection(const ProjectionDescription * projection_) const { projection = projection_; }

    /// If we have a projection then we should use its metadata.
    StorageMetadataPtr getMetadataForQuery() const { return projection ? projection->metadata : metadata; }

private:
    void init();

    std::unordered_map<String, DataTypePtr> virtual_columns;
};

using StorageSnapshotPtr = std::shared_ptr<const StorageSnapshot>;

}
