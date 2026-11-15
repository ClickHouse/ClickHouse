#pragma once

#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/StreamWithSizeHint.h>

#include <functional>

namespace DB
{

/** In wide format data of each column is stored in one or several (for complex types) files.
  * Every data file is followed by marks file.
  * Can be used in tables with both adaptive and non-adaptive granularity.
  * This is the regular format of parts for MergeTree and suitable for big parts, as it's the most efficient.
  * Data part would be created in wide format if it's uncompressed size in bytes or number of rows would exceed
  * thresholds `min_bytes_for_wide_part` and `min_rows_for_wide_part`.
  */
class MergeTreeDataPartWide : public IMergeTreeDataPart
{
public:
    MergeTreeDataPartWide(
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

    std::optional<String> getFirstFileNameForColumn(const NameAndTypePair & column) const override;

    ~MergeTreeDataPartWide() override;

    bool hasColumnFiles(const NameAndTypePair & column) const override;

    std::optional<time_t> getColumnModificationTime(const String & column_name) const override;

    void loadMarksToCache(const Names & column_names, MarkCache * mark_cache) const override;
    void removeMarksFromCache(MarkCache * mark_cache) const override;

    static void loadIndexGranularityImpl(
        MergeTreeIndexGranularityPtr & index_granularity_ptr,
        MergeTreeIndexGranularityInfo & index_granularity_info_,
        const IDataPartStorage & data_part_storage_,
        const std::string & any_column_file_name,
        const MergeTreeSettings & storage_settings);

protected:
    void doCheckConsistency(bool require_part_metadata) const override;

private:
    /// Loads marks index granularity into memory
    void loadIndexGranularity() override;

    ColumnSize getColumnSizeImpl(const NameAndTypePair & column, std::unordered_set<String> * processed_substreams) const;

    void calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const override;

    std::vector<ColumnSize> calculateSubcolumnSizes(const NamesAndTypesList & subcolumns) const override;

    void addStreamToColumnSize(const String & stream_name, ColumnSize & size) const;

    /// Adds the stream's share of its file where it has a size hint, else the whole file.
    void addStreamWithSizeHintToColumnSize(const StreamWithSizeHint & stream, ColumnSize & size) const;

    /// The names of the column's streams that exist in the part, from columns_substreams.txt where the part has it, else
    /// from the serialization, whose state-less enumeration misses the data-dependent streams of dynamic subcolumns.
    void forEachExistingStreamOfColumn(const String & column_name, const std::function<void(const String & stream_name)> & callback) const;

    /// The streams a read of the columns touches, by requested name, listed by one reader after it has read the prefixes.
    std::unordered_map<String, std::vector<StreamWithSizeHint>> getListOfStreamsForColumns(const NamesAndTypesList & columns) const;
};

}
