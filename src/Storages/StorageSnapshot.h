#pragma once
#include <Storages/StorageInMemoryMetadata.h>
// #include <sparsehash/dense_hash_map>

namespace DB
{

class IStorage;

// #if !defined(ARCADIA_BUILD)
//     using NamesAndTypesMap = google::dense_hash_map<StringRef, DataTypePtr, StringRefHash>;
// #else
//     using NamesAndTypesMap = google::sparsehash::dense_hash_map<StringRef, DataTypePtr, StringRefHash>;
// #endif

struct StorageSnapshot
{
    const IStorage & storage;
    const StorageMetadataPtr metadata;
    const ColumnsDescription object_columns;

    struct Data
    {
        virtual ~Data() = default;
    };

    using DataPtr = std::unique_ptr<const Data>;
    const DataPtr data;

    /// TODO: fix
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

    NamesAndTypesList getColumns(const GetColumnsOptions & options) const;
    NamesAndTypesList getColumnsByNames(const GetColumnsOptions & options, const Names & names) const;

    /// Block with ordinary + materialized + aliases + virtuals + subcolumns.
    Block getSampleBlockForColumns(const Names & column_names) const;

    /// Verify that all the requested names are in the table and are set correctly:
    /// list of names is not empty and the names do not repeat.
    void check(const Names & column_names) const;

    DataTypePtr getConcreteType(const String & column_name) const;

    void addProjection(const ProjectionDescription * projection_) const { projection = projection_; }

    StorageMetadataPtr getMetadataForQuery() const { return (projection ? projection->metadata : metadata); }

    bool isSubcolumnOfObject(const String & name) const;

private:
    void init();

    std::unordered_map<String, DataTypePtr> virtual_columns;
};

using StorageSnapshotPtr = std::shared_ptr<const StorageSnapshot>;

}
