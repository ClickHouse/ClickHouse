#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

/** In compact format all columns are stored in one file (`data.parquet`).
  */
class MergeTreeDataPartParquet : public IMergeTreeDataPart
{
public:
    static constexpr auto DATA_FILE_NAME = "data";
    static constexpr auto DATA_FILE_NAME_WITH_EXTENSION = "data.parquet";

    MergeTreeDataPartParquet(
        const MergeTreeData & storage_,
        const MergeTreeSettings & storage_settings,
        const String & name_,
        const MergeTreePartInfo & info_,
        const MutableDataPartStoragePtr & data_part_storage_,
        const IMergeTreeDataPart * parent_part_ = nullptr);

    bool isStoredOnReadonlyDisk() const override;

    bool isStoredOnRemoteDisk() const override;

    bool isStoredOnRemoteDiskWithZeroCopySupport() const override;

    bool hasColumnFiles(const NameAndTypePair & column) const override;

    std::optional<time_t> getColumnModificationTime(const String & column_name) const override;

    std::optional<String> getFileNameForColumn(const NameAndTypePair & /* column */) const override { return DATA_FILE_NAME; }

    void loadMarksToCache(const Names & column_names, MarkCache * mark_cache) const override;
    void removeMarksFromCache(MarkCache * mark_cache) const override;

    ~MergeTreeDataPartParquet() override;

    static void loadIndexGranularityImpl(
        MergeTreeIndexGranularityPtr & index_granularity_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        size_t marks_per_granule,
        const IDataPartStorage & data_part_storage_,
        const MergeTreeSettings & storage_settings);

protected:
    void doCheckConsistency(bool require_part_metadata) const override;

private:
     /// Loads marks index granularity into memory
     void loadIndexGranularity() override;

     /// Compact parts don't support per column size, only total size
     void calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const override;
};

}
