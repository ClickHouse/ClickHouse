#pragma once

#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>


namespace DB
{

/// Writes data part in compact format.
class MergeTreeDataPartWriterCompact : public MergeTreeDataPartWriterOnDisk
{
public:
    MergeTreeDataPartWriterCompact(
        const String & data_part_name_,
        const String & logger_name_,
        const SerializationByName & serializations_,
        MutableDataPartStoragePtr data_part_storage_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        const MergeTreeSettingsPtr & storage_settings_,
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot_,
        const VirtualsDescriptionPtr & virtual_columns_,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const ColumnsStatistics & stats_to_recalc,
        const String & marks_file_extension,
        const CompressionCodecPtr & default_codec,
        const MergeTreeWriterSettings & settings,
        const MergeTreeIndexGranularity & index_granularity);

    void write(const Block & block, const IColumn::Permutation * permutation) override;

    void fillChecksums(MergeTreeDataPartChecksums & checksums, NameSet & checksums_to_remove) override;
    void finish(bool sync) override;

private:
    /// Finish serialization of the data. Flush rows in buffer to disk, compute checksums.
    void fillDataChecksums(MergeTreeDataPartChecksums & checksums);
    void finishDataSerialization(bool sync);

    void fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block) override;

    /// Write block of rows into .bin file and marks in .mrk files
    void writeDataBlock(const Block & block, const Granules & granules);

    /// Write block of rows into .bin file and marks in .mrk files, primary index in .idx file
    /// and skip indices in their corresponding files.
    void writeDataBlockPrimaryIndexAndSkipIndices(const Block & block, const Granules & granules);

    void addToChecksums(MergeTreeDataPartChecksums & checksums);

    void addStreams(const NameAndTypePair & name_and_type, const ColumnPtr & column, const ASTPtr & effective_codec_desc);

    void initDynamicStreamsIfNeeded(const Block & block);

    Block header;

    /** Simplified SquashingTransform. The original one isn't suitable in this case
      *  as it can return smaller block from buffer without merging it with larger block if last is enough size.
      * But in compact parts we should guarantee, that written block is larger or equals than index_granularity.
      */
    class ColumnsBuffer
    {
    public:
        void add(MutableColumns && columns);
        size_t size() const;
        Columns releaseColumns();
    private:
        MutableColumns accumulated_columns;
    };

    ColumnsBuffer columns_buffer;

    /// hashing_buf -> compressed_buf -> plain_hashing -> plain_file
    std::unique_ptr<WriteBufferFromFileBase> plain_file;
    HashingWriteBuffer plain_hashing;

    /// Compressed stream which allows to write with codec.
    struct CompressedStream
    {
        CompressedWriteBuffer compressed_buf;
        HashingWriteBuffer hashing_buf;

        CompressedStream(WriteBuffer & buf, const CompressionCodecPtr & codec)
            : compressed_buf(buf, codec)
            , hashing_buf(compressed_buf) {}
    };

    using CompressedStreamPtr = std::shared_ptr<CompressedStream>;

    /// Create compressed stream for every different codec. All streams write to
    /// a single file on disk.
    std::unordered_map<UInt64, CompressedStreamPtr> streams_by_codec;

    /// Stream for each column's substreams path (look at addStreams).
    std::unordered_map<String, CompressedStreamPtr> compressed_streams;

    /// If marks are uncompressed, the data is written to 'marks_file_hashing' for hash calculation and then to the 'marks_file'.
    std::unique_ptr<WriteBufferFromFileBase> marks_file;
    std::unique_ptr<HashingWriteBuffer> marks_file_hashing;

    /// If marks are compressed, the data is written to 'marks_source_hashing' for hash calculation,
    /// then to 'marks_compressor' for compression,
    /// then to 'marks_file_hashing' for calculation of hash of compressed data,
    /// then finally to 'marks_file'.
    std::unique_ptr<CompressedWriteBuffer> marks_compressor;
    std::unique_ptr<HashingWriteBuffer> marks_source_hashing;

    bool is_dynamic_streams_initialized = false;
};

}
