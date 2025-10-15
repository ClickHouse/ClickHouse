#pragma once
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/VirtualColumnsDescription.h>

namespace DB
{

class IStorage;
class ICompressionCodec;

using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;

/// Snapshot of storage that fixes set columns that can be read in query.
/// There are 3 sources of columns: regular columns from metadata,
/// dynamic columns from object Types, virtual columns.
struct StorageSnapshot
{
    const IStorage & storage;
    const StorageMetadataPtr metadata;
    const VirtualsDescriptionPtr virtual_columns;
    const ColumnsDescription object_columns;

    /// Additional data, on which set of columns may depend.
    /// E.g. data parts in MergeTree, list of blocks in Memory, etc.
    struct Data
    {
        virtual ~Data() = default;
    };

    using DataPtr = std::unique_ptr<Data>;
    DataPtr data;

    StorageSnapshot(
        const IStorage & storage_,
        StorageMetadataPtr metadata_);

    StorageSnapshot(
        const IStorage & storage_,
        StorageMetadataPtr metadata_,
        VirtualsDescriptionPtr virtual_columns_);

    StorageSnapshot(
        const IStorage & storage_,
        StorageMetadataPtr metadata_,
        ColumnsDescription object_columns_);

    StorageSnapshot(
        const IStorage & storage_,
        StorageMetadataPtr metadata_,
        ColumnsDescription object_columns_,
        DataPtr data_);

    std::shared_ptr<StorageSnapshot> clone(DataPtr data_) const;

    /// Get columns description
    ColumnsDescription getAllColumnsDescription() const;

    /// Get all available columns with types according to options.
    NamesAndTypesList getColumns(const GetColumnsOptions & options) const;

    /// Get columns with types according to options only for requested names.
    NamesAndTypesList getColumnsByNames(const GetColumnsOptions & options, const Names & names) const;

    /// Get column with type according to options for requested name.
    std::optional<NameAndTypePair> tryGetColumn(const GetColumnsOptions & options, const String & column_name) const;
    NameAndTypePair getColumn(const GetColumnsOptions & options, const String & column_name) const;

    CompressionCodecPtr getCodecOrDefault(const String & column_name, CompressionCodecPtr default_codec) const;
    CompressionCodecPtr getCodecOrDefault(const String & column_name) const;
    ASTPtr getCodecDescOrDefault(const String & column_name, CompressionCodecPtr default_codec) const;

    /// Block with ordinary + materialized + aliases + virtuals + subcolumns.
    Block getSampleBlockForColumns(const Names & column_names) const;

    ColumnsDescription getDescriptionForColumns(const Names & column_names) const;

    /// Verify that all the requested names are in the table and are set correctly:
    /// list of names is not empty and the names do not repeat.
    void check(const Names & column_names) const;

    DataTypePtr getConcreteType(const String & column_name) const;
};

using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

}
