#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>

namespace DB
{

/// Writes data part in compact format.
class MergeTreeDataPartWriterCompact : public MergeTreeDataPartWriterOnDisk
{
public:
    MergeTreeDataPartWriterCompact(
        const MergeTreeData::DataPartPtr & data_part,
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot_,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const String & marks_file_extension,
        const CompressionCodecPtr & default_codec,
        const MergeTreeWriterSettings & settings,
        const MergeTreeIndexGranularity & index_granularity);

    void write(const Block & block, const IColumn::Permutation * permutation,
        const Block & primary_key_block, const Block & skip_indexes_block) override;

    void finishDataSerialization(IMergeTreeDataPart::Checksums & checksums, bool sync) override;

protected:
    void fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block) override;

private:
    void writeBlock(const Block & block);

    void addToChecksums(MergeTreeDataPartChecksums & checksums);

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

    struct CompressedStream
    {
        CompressedWriteBuffer compressed_buf;
        HashingWriteBuffer hashing_buf;

        CompressedStream(WriteBuffer & buf, const CompressionCodecPtr & codec)
            : compressed_buf(buf, codec), hashing_buf(compressed_buf) {}
    };

    using CompressedStreamPtr = std::shared_ptr<CompressedStream>;

    /// Create compressed stream for every different codec.
    std::unordered_map<UInt64, CompressedStreamPtr> streams_by_codec;

    /// For better performance save pointer to stream by every column.
    std::vector<CompressedStreamPtr> compressed_streams;

    /// marks -> marks_file
    std::unique_ptr<WriteBufferFromFileBase> marks_file;
    HashingWriteBuffer marks;

    /// Write single granule of one column (rows between 2 marks)
    static void writeColumnSingleGranule(
        const ColumnWithTypeAndName & column,
        const CompressedStreamPtr & stream,
        size_t from_row,
        size_t number_of_rows);
};

}
