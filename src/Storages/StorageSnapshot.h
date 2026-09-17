#pragma once
#include <Storages/VirtualColumnsDescription.h>

namespace DB
{

namespace UDT
{
class AuthorityStorageReadContinuationEvidence;
}

class IStorage;
class ICompressionCodec;

using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Snapshot of storage that fixes set columns that can be read in query.
/// There are 2 sources of columns: regular columns from metadata and virtual columns.
struct StorageSnapshot
{
    const IStorage & storage;
    const StorageMetadataPtr metadata;

    /// Additional data, on which set of columns may depend.
    /// E.g. data parts in MergeTree, list of blocks in Memory, etc.
    struct Data
    {
        virtual ~Data() = default;
    };
    using DataPtr = std::shared_ptr<const Data>;
    const DataPtr data;

    /// Present only for a mapped Atomic UDT object. Clones retain the same
    /// compact exact-root proof rather than reopening the current authority.
    const std::shared_ptr<const UDT::AuthorityStorageReadContinuationEvidence> udt_read_continuation_evidence;

    StorageSnapshot(
        const IStorage & storage_,
        StorageMetadataPtr metadata_);

    StorageSnapshot(
        const IStorage & storage_,
        StorageMetadataPtr metadata_,
        DataPtr data_);

    std::shared_ptr<StorageSnapshot> clone(DataPtr data_) const;
    std::shared_ptr<StorageSnapshot> clone(StorageMetadataPtr metadata_, DataPtr data_) const;

    /// Rechecks an already-admitted mapped read against a quarantine that may
    /// have appeared after this snapshot was created. Physical-only snapshots
    /// are a no-op.
    void assertUDTReadContinuationAllowed() const;

    /// Get columns description
    ColumnsDescription getAllColumnsDescription() const;

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

    /// Get default expression for a column.
    /// Takes into account physical and virtual columns.
    std::optional<ColumnDefault> getDefault(const String & column_name) const;

private:
    struct PreserveUDTReadEvidenceTag
    {
    };
    StorageSnapshot(
        const IStorage & storage_,
        StorageMetadataPtr metadata_,
        DataPtr data_,
        std::shared_ptr<const UDT::AuthorityStorageReadContinuationEvidence> evidence_,
        PreserveUDTReadEvidenceTag);
};

using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

}
