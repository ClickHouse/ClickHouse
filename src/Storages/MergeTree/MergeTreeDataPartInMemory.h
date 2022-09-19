#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

class UncompressedCache;

class MergeTreeDataPartInMemory : public IMergeTreeDataPart
{
public:
    MergeTreeDataPartInMemory(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const VolumePtr & volume_,
        const std::optional<String> & relative_path_ = {},
        const IMergeTreeDataPart * parent_part_ = nullptr);

    MergeTreeDataPartInMemory(
        MergeTreeData & storage_,
        const String & name_,
        const VolumePtr & volume_,
        const std::optional<String> & relative_path_ = {},
        const IMergeTreeDataPart * parent_part_ = nullptr);

    MergeTreeReaderPtr getReader(
        const NamesAndTypesList & columns,
        const StorageMetadataPtr & metadata_snapshot,
        const MarkRanges & mark_ranges,
        UncompressedCache * uncompressed_cache,
        MarkCache * mark_cache,
        const MergeTreeReaderSettings & reader_settings_,
        const ValueSizeMap & avg_value_size_hints,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback) const override;

    MergeTreeWriterPtr getWriter(
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        const MergeTreeIndexGranularity & computed_index_granularity) const override;

    bool isStoredOnDisk() const override { return false; }
    bool isStoredOnRemoteDisk() const override { return false; }
    bool hasColumnFiles(const NameAndTypePair & column) const override { return !!getColumnPosition(column.getNameInStorage()); }
    String getFileNameForColumn(const NameAndTypePair & /* column */) const override { return ""; }
    void renameTo(const String & new_relative_path, bool remove_new_dir_if_exists) const override;
    void makeCloneInDetached(const String & prefix, const StorageMetadataPtr & metadata_snapshot) const override;

    void flushToDisk(const String & base_path, const String & new_relative_path, const StorageMetadataPtr & metadata_snapshot) const;

    /// Returns hash of parts's block
    Checksum calculateBlockChecksum() const;

    mutable Block block;

private:
    mutable std::condition_variable is_merged;

    /// Calculates uncompressed sizes in memory.
    void calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const override;
};

using DataPartInMemoryPtr = std::shared_ptr<const MergeTreeDataPartInMemory>;
DataPartInMemoryPtr asInMemoryPart(const MergeTreeDataPartPtr & part);

}
