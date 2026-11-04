#pragma once

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Core/Range.h>
#include <base/DayNum.h>

#include <memory>
#include <vector>

namespace DB
{

class Block;
class IDataPartStorage;
class IMergeTreeDataPart;
class WriteBufferFromFileBase;
struct MergeTreeDataPartChecksums;
struct MergeTreeSettings;
using MergeTreeSettingsPtr = std::shared_ptr<const MergeTreeSettings>;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Index that for each part stores min and max values of a set of columns. This allows quickly excluding
/// parts based on conditions on these columns imposed by a query.
/// Currently this index is built using only columns required by partition expression, but in principle it
/// can be built using any set of columns.
/// Not nested in `IMergeTreeDataPart`, so that `IMergeTreeDataPartInfoForReader` can forward-declare it.
struct MergeTreePartMinMaxIndex
{
    using Checksums = MergeTreeDataPartChecksums;

    /// A direct product of ranges for each key column. See Storages/MergeTree/KeyCondition.cpp for details.
    Ranges hyperrectangle;
    bool initialized = false;

public:
    MergeTreePartMinMaxIndex() = default;

    /// For month-based partitioning.
    MergeTreePartMinMaxIndex(DayNum min_date, DayNum max_date)
        : hyperrectangle(1, Range(min_date, true, max_date, true))
        , initialized(true)
    {
    }

    void load(const IMergeTreeDataPart & part);

    using WrittenFiles = std::vector<std::unique_ptr<WriteBufferFromFileBase>>;

    [[nodiscard]] WrittenFiles store(StorageMetadataPtr metadata_snapshot, IDataPartStorage & part_storage, Checksums & checksums, const MergeTreeSettingsPtr & storage_settings) const;
    [[nodiscard]] WrittenFiles store(const NamesAndTypesList & columns, IDataPartStorage & part_storage, Checksums & checksums, const MergeTreeSettingsPtr & storage_settings) const;

    void update(const Block & block, const NamesAndTypesList & columns);
    void merge(const MergeTreePartMinMaxIndex & other);

    /// Repair the block column ranges of an index inherited from `source_part` when that part does not
    /// know them: grow the index to the current set of minmax columns and re-derive the ranges of
    /// `_block_number` / `_block_offset` that came back as the whole universe. See the implementation
    /// for when each range is recoverable. No-op for an uninitialized index, projection and patch parts.
    void repairInheritedBlockColumns(const IMergeTreeDataPart & source_part, const StorageMetadataPtr & metadata_snapshot);
    Names getProbablyWrittenFiles(const IMergeTreeDataPart & part) const;
    /// For Store
    static String getFileColumnName(const String & column_name, const MergeTreeSettingsPtr & storage_settings_, const IDataPartStorage & data_part_storage);
    /// For Load
    static String getFileColumnName(const String & column_name, const Checksums & checksums_);
};

using MergeTreePartMinMaxIndexPtr = std::shared_ptr<MergeTreePartMinMaxIndex>;

}
