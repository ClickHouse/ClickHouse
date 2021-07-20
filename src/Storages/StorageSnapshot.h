#pragma once
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class IStorage;

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using DataPartsVector = std::vector<DataPartPtr>;

struct StorageSnapshot
{
    using NameToTypeMap = std::unordered_map<String, DataTypePtr>;

    const IStorage & storage;
    const StorageMetadataPtr metadata;
    const NameToTypeMap object_types;
    const DataPartsVector parts;

    /// TODO: fix
    mutable const ProjectionDescription * projection = nullptr;

    StorageSnapshot(
        const IStorage & storage_,
        const StorageMetadataPtr & metadata_)
        : storage(storage_), metadata(metadata_)
    {
    }

    StorageSnapshot(
        const IStorage & storage_,
        const StorageMetadataPtr & metadata_,
        const NameToTypeMap & object_types_)
        : storage(storage_), metadata(metadata_), object_types(object_types_)
    {
    }

    StorageSnapshot(
        const IStorage & storage_,
        const StorageMetadataPtr & metadata_,
        const NameToTypeMap & object_types_,
        const DataPartsVector & parts_)
        : storage(storage_), metadata(metadata_), object_types(object_types_), parts(parts_)
    {
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

private:
    NamesAndTypesList addVirtualsAndObjects(const GetColumnsOptions & options, NamesAndTypesList columns_list) const;
};

using StorageSnapshotPtr = std::shared_ptr<const StorageSnapshot>;

}
