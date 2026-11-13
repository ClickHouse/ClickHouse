#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

/** In compact format all columns are stored in one file (`data.bin`).
  * Data is split in granules.
  * Several granules of one column are joined into stripes.
  * Stripes are written one by one in data file.
  * Marks are also stored in single file (`data.mrk3`).
  * In compact format one mark is an array of marks for every column and a number of rows in granule.
  * Format of other data part files is not changed.
  * It's considered to store only small parts in compact format (up to 10M).
  * NOTE: New Compact parts aren't created for tables with non-adaptive granularity (a Compact
  *       part is always adaptive), but an existing Compact part can still be loaded by such a table.
  * NOTE: In compact part compressed and uncompressed size of single column is unknown.
  */

/** Examples of layout of data.bin file:
  * Consider we have 3 columns in table (c1, c2, c3)
  * Number of granules in stripe is controlled by settings
  * 'compact_parts_max_bytes_to_buffer' and 'compact_parts_max_granules_to_buffer'.
  *
  * With 1 granule in stripe (old format of compact parts):
  * |--------+----------+--------|
  * | column | granule  | stripe |
  * |--------+----------+--------|
  * | c1     | 1        | 1      |
  * | c2     | 1        | 2      |
  * | c3     | 1        | 3      |
  * | c1     | 2        | 4      |
  * | c2     | 2        | 5      |
  * | c3     | 2        | 6      |
  * | c1     | 3        | 7      |
  * | c2     | 3        | 8      |
  * | c3     | 3        | 9      |
  * |--------+----------+--------|
  *
  * With 3 granules in stripe:
  * |--------+----------+--------|
  * | column | granule  | stripe |
  * |--------+----------+--------|
  * | c1     | 1        | 1      |
  * | c1     | 2        | 1      |
  * | c1     | 3        | 1      |
  * | c2     | 1        | 2      |
  * | c2     | 2        | 2      |
  * | c2     | 3        | 2      |
  * | c3     | 1        | 3      |
  * | c3     | 2        | 3      |
  * | c3     | 3        | 3      |
  * |--------+----------+--------|
  *
  * With 'compress_per_column_in_compact_parts' every column in every granule starts a new compressed block,
  * otherwise the columns of a stripe share compressed blocks, which are cut only at the end of the stripe.
  *
  * data.mrk3 file is not split to stripes
  * and is written as there is always one granule in stripe:
  *
  *  mark1_col1, mark1_col2, mark1_col3, num_rows_in_granule
  *  mark2_col1, mark2_col2, mark2_col3, num_rows_in_granule
  *  mark3_col1, mark3_col2, mark3_col3, num_rows_in_granule
  */
class MergeTreeDataPartCompact : public IMergeTreeDataPart
{
public:
    static constexpr auto DATA_FILE_NAME = "data";
    static constexpr auto DATA_FILE_NAME_WITH_EXTENSION = "data.bin";

    MergeTreeDataPartCompact(
        const MergeTreeData & storage_,
        const MergeTreeSettings & storage_settings,
        const String & name_,
        const MergeTreePartInfo & info_,
        const MutableDataPartStoragePtr & data_part_storage_,
        const IMergeTreeDataPart * parent_part_,
        PartDirIntent intent);

    Strings getPreferredFileOrder() const override;

    bool isStoredOnReadonlyDisk() const override;

    bool isStoredOnRemoteDisk() const override;

    bool isStoredOnRemoteDiskWithZeroCopySupport() const override;

    bool hasColumnFiles(const NameAndTypePair & column) const override;

    std::optional<time_t> getColumnModificationTime(const String & column_name) const override;

    std::optional<String> getFirstFileNameForColumn(const NameAndTypePair & /* column */) const override { return DATA_FILE_NAME; }

    void loadMarksToCache(const Names & column_names, MarkCache * mark_cache) const override;
    void removeMarksFromCache(MarkCache * mark_cache) const override;

    ~MergeTreeDataPartCompact() override;

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
