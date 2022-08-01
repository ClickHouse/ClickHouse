#pragma once
#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>

namespace DB
{

/// Writes data part in compact format.
class MergeTreeDataPartWriterCompact : public MergeTreeDataPartWriterOnDisk
{
public:
    MergeTreeDataPartWriterCompact(
        const MergeTreeData::DataPartPtr & data_part,
        DataPartStorageBuilderPtr data_part_storage_builder_,
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot_,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const String & marks_file_extension,
        const CompressionCodecPtr & default_codec,
        const MergeTreeWriterSettings & settings,
        const MergeTreeIndexGranularity & index_granularity);

    ~MergeTreeDataPartWriterCompact() override;

    void write(const Block & block, const IColumn::Permutation * permutation) override;

    void fillChecksums(IMergeTreeDataPart::Checksums & checksums) override;
    void finish(bool sync) override;

private:
    /// Finish serialization of the data. Flush rows in buffer to disk, compute checksums.
    void fillDataChecksums(IMergeTreeDataPart::Checksums & checksums);
    void finishDataSerialization(bool sync);

    void fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block) override;

    /// Write block of rows into .bin file and marks in .mrk files
    void writeDataBlock(const Block & block, const Granules & granules);

    /// Write block of rows into .bin file and marks in .mrk files, primary index in .idx file
    /// and skip indices in their corresponding files.
    void writeDataBlockPrimaryIndexAndSkipIndices(const Block & block, const Granules & granules);

    void addToChecksums(MergeTreeDataPartChecksums & checksums);

    void addStreams(const NameAndTypePair & column, const ASTPtr & effective_codec_desc);

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

    /// marks -> marks_file
    std::unique_ptr<WriteBufferFromFileBase> marks_file;
    HashingWriteBuffer marks;
};

}
