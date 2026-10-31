#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

/** In Parquet format all columns are stored in one file (`data.parquet`) in the Parquet format,
  * so it can be read by external applications. The other files of the part (primary index, skip indices,
  * statistics, metadata files) are the same as in other formats.
  *
  * The granules of the part are the row groups of the Parquet file: there is one mark and one primary key
  * value for each row group. The size of row groups is controlled by the settings
  * `parquet_row_group_granularity_rows` and `parquet_row_group_granularity_bytes` instead of `index_granularity`.
  *
  * Marks are stored in single file (`data.mrk5`), with the same layout as in Compact parts:
  * one mark is an array of marks for every column and a number of rows in the row group.
  * For Parquet parts, `MarkInCompressedFile::offset_in_compressed_file` is the offset of the data of the column in
  * the row group (the column chunks of all its Parquet leaf columns, which are contiguous) in `data.parquet`,
  * and `MarkInCompressedFile::offset_in_decompressed_block` is the size of this data in bytes.
  * The final mark points to the end of the data, and instead of the size of the data it has the size of
  * the column in memory (the uncompressed size), so the sizes of the columns can be calculated from the marks.
  *
  * The data types of the columns are mapped to similar Parquet types. A column of a type that does not have
  * a lossless representation in Parquet is stored as a `BYTE_ARRAY` column with values in the ClickHouse
  * binary format (see `IDataType::getDefaultSerialization` and `ISerialization::serializeBinary`).
  * The list of the columns with their ClickHouse types and encodings is stored in the key-value metadata of
  * the Parquet file under the key `clickhouse.columns`.
  *
  * The codecs of the columns are mapped to similar Parquet compression methods and encodings.
  *
  * Embedded indices of the Parquet file (column statistics, page index, bloom filters) are written for
  * external applications, but ClickHouse uses the usual index files of the part.
  */
class MergeTreeDataPartParquet : public IMergeTreeDataPart
{
public:
    static constexpr auto DATA_FILE_NAME = "data";
    static constexpr auto DATA_FILE_EXTENSION = ".parquet";
    static constexpr auto DATA_FILE_NAME_WITH_EXTENSION = "data.parquet";

    /// The key in the key-value metadata of the Parquet file with the description of the columns.
    static constexpr auto COLUMNS_METADATA_KEY = "clickhouse.columns";

    MergeTreeDataPartParquet(
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

    void setWrittenColumnSizes(ColumnSizeByName sizes) override { written_column_sizes = std::move(sizes); }

    /// The `Parquet` file is not compressed as a whole, so its checksum has no uncompressed size:
    /// the uncompressed sizes of the columns are added instead. Requires loaded index granularity
    /// or the sizes provided by the writer.
    UInt64 calculateBytesUncompressedOnDisk() const override;

    void loadMarksToCache(const Names & column_names, MarkCache * mark_cache) const override;
    void removeMarksFromCache(MarkCache * mark_cache) const override;

    ~MergeTreeDataPartParquet() override;

protected:
    void doCheckConsistency(bool require_part_metadata) const override;

private:
    /// Loads marks index granularity into memory
    void loadIndexGranularity() override;

    /// The sizes of columns are calculated from the marks, or provided by the writer.
    void calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const override;

    std::optional<ColumnSizeByName> written_column_sizes;
};

}
