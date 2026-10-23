#pragma once

#include "config.h"

#if USE_PARQUET

#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Processors/Formats/Impl/Parquet/Write.h>


namespace DB
{

/// Writes data part in Parquet format. See `MergeTreeDataPartParquet`.
class MergeTreeDataPartWriterParquet : public MergeTreeDataPartWriterOnDisk
{
public:
    MergeTreeDataPartWriterParquet(
        const String & data_part_name_,
        const String & logger_name_,
        const SerializationByName & serializations_,
        MutableDataPartStoragePtr data_part_storage_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        const MergeTreeSettingsPtr & storage_settings_,
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot_,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const String & marks_file_extension,
        const CompressionCodecPtr & default_codec,
        const MergeTreeWriterSettings & settings,
        MergeTreeIndexGranularityPtr index_granularity_);

    void write(const Block & block, const IColumnPermutation * permutation, Block * permuted_columns_cache) override;

    void finalizeIndexGranularity() final;
    void fillChecksums(MergeTreeDataPartChecksums & checksums, NameSet & checksums_to_remove) final;
    void finish(bool sync) override;
    void cancel() noexcept override;

    size_t getNumberOfOpenStreams() const override { return 1; }

    std::optional<std::unordered_map<String, ColumnSize>> getWrittenColumnSizes() const override;

    /// Whether values of the type can be stored in a Parquet column of a similar type and read back without loss.
    /// Otherwise, the values are stored in the ClickHouse binary format in a `BYTE_ARRAY` column.
    static bool canStoreTypeInParquet(const DataTypePtr & type);

private:
    /// The columns are written in Parquet format, not with the serializations of the columns,
    /// so there are no streams.
    void addStreams(const NameAndTypePair &) override {}
    ISerialization::SerializeBinaryBulkSettings getSerializationSettings() const override { return {}; }

    void fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block) override;

    /// Like `computeIndexGranularity`, but the granules are the row groups of the Parquet file.
    size_t computeRowGroupGranularity(const Block & block) const;

    void writeDataBlockPrimaryIndexAndSkipIndices(const Block & block, const Granules & granules);

    /// Write a row group for each granule into the Parquet file, and the marks of the row groups.
    void writeRowGroups(const Block & block, const Granules & granules);
    void writeColumnChunks(size_t column_idx, const ColumnPtr & column, WriteBuffer & marks_out);

    void fillDataChecksums(MergeTreeDataPartChecksums & checksums);
    void finishDataSerialization(bool sync);

    struct ColumnToWrite
    {
        NameAndTypePair name_and_type;
        /// The values are stored in the ClickHouse binary format in a `BYTE_ARRAY` column.
        bool binary = false;
        /// Includes the compression method and the encoding for the codec of the column.
        Parquet::WriteOptions options;
        /// The size of the written values in memory.
        UInt64 uncompressed_bytes = 0;
        /// The size of the written data of the column in the file.
        UInt64 compressed_bytes = 0;
        /// The positions in `schema` of the leaves of the column, in the order of its column chunks.
        std::vector<size_t> leaf_schema_indices;
    };

    std::vector<ColumnToWrite> columns_to_write;
    Parquet::SchemaElements schema;
    Parquet::FileWriteState file_state;
    Parquet::WriteOptions file_options;
    /// Added to the key-value metadata of the file. Describes the columns, see `MergeTreeDataPartParquet`.
    std::vector<std::pair<String, String>> key_value_metadata;

    Block header;

    /// Rows that are accumulated until there are enough of them for a row group.
    MutableColumns buffered_columns;
    size_t buffered_rows = 0;

    /// data_hashing -> data_file
    std::unique_ptr<WriteBufferFromFileBase> data_file;
    std::unique_ptr<HashingWriteBuffer> data_hashing;

    /// If marks are uncompressed, the data is written to 'marks_file_hashing' for hash calculation and then to the 'marks_file'.
    std::unique_ptr<WriteBufferFromFileBase> marks_file;
    std::unique_ptr<HashingWriteBuffer> marks_file_hashing;

    /// If marks are compressed, the data is written to 'marks_source_hashing' for hash calculation,
    /// then to 'marks_compressor' for compression,
    /// then to 'marks_file_hashing' for calculation of hash of compressed data,
    /// then finally to 'marks_file'.
    std::unique_ptr<CompressedWriteBuffer> marks_compressor;
    std::unique_ptr<HashingWriteBuffer> marks_source_hashing;
};

}

#endif
