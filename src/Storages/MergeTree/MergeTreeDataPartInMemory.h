#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

class MergeTreeDataPartInMemory : public IMergeTreeDataPart
{
public:
    MergeTreeDataPartInMemory(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const DiskPtr & disk_,
        const std::optional<String> & relative_path_ = {});

    MergeTreeDataPartInMemory(
        MergeTreeData & storage_,
        const String & name_,
        const DiskPtr & disk_,
        const std::optional<String> & relative_path_ = {});

    MergeTreeReaderPtr getReader(
        const NamesAndTypesList & columns,
        const MarkRanges & mark_ranges,
        UncompressedCache * uncompressed_cache,
        MarkCache * mark_cache,
        const MergeTreeReaderSettings & reader_settings_,
        const ValueSizeMap & avg_value_size_hints,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback) const override;

    MergeTreeWriterPtr getWriter(
        const NamesAndTypesList & columns_list,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        const MergeTreeIndexGranularity & computed_index_granularity) const override;

    bool isStoredOnDisk() const override { return false; }
    bool hasColumnFiles(const String & column_name, const IDataType & /* type */) const override { return !!getColumnPosition(column_name); }
    String getFileNameForColumn(const NameAndTypePair & /* column */) const override { return ""; }
    void renameTo(const String & /*new_relative_path*/, bool /*remove_new_dir_if_exists*/) const override {}
    void makeCloneInDetached(const String & prefix) const override;

    bool waitUntilMerged(size_t timeout) const override;
    void notifyMerged() const override;

    mutable Block block;

private:
    mutable std::condition_variable is_merged;

    void checkConsistency(bool /* require_part_metadata */) const override {}

    /// Loads marks index granularity into memory
    void loadIndexGranularity() override;

    /// Compact parts doesn't support per column size, only total size
    void calculateEachColumnSizesOnDisk(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const override;
};

using DataPartInMemoryPtr = std::shared_ptr<const MergeTreeDataPartInMemory>;

}
