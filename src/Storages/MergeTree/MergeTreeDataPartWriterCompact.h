#pragma once

#include <list>
#include <map>

#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <DataTypes/Serializations/ISerialization.h>


namespace DB
{

/// Writes data part in compact format.
class MergeTreeDataPartWriterCompact : public MergeTreeDataPartWriterOnDisk
{
    using Base = MergeTreeDataPartWriterOnDisk;

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

private:
    /// Finish serialization of the data. Flush rows in buffer to disk, compute checksums.
    void fillDataChecksums(MergeTreeDataPartChecksums & checksums);
    void finishDataSerialization(bool sync);

    void fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block) override;

    using BlockAndGranules = std::pair<Block, Granules>;
    using BlocksAndGranules = std::vector<BlockAndGranules>;

    /// Write blocks of rows into .bin file in stripes and marks in .mrk files
    void writeDataBlock(const BlocksAndGranules & blocks_and_granules);

    /// Write blocks of rows into .bin file and marks in .mrk files, primary index in .idx file
    /// and skip indices in their corresponding files.
    void writeDataBlockPrimaryIndexAndSkipIndices(const BlocksAndGranules & blocks_and_granules);

    void addToChecksums(MergeTreeDataPartChecksums & checksums);

    void addStreams(const NameAndTypePair & name_and_type) override;

    ISerialization::SerializeBinaryBulkSettings getSerializationSettings() const override;

    Block header;

    /// Accumulates the columns to write with the granules they form,
    /// so that the data is written by whole granules and whole stripes.
    class ColumnsBuffer
    {
    public:
        using ColumnsAndGranules = std::vector<std::pair<Columns, Granules>>;

        /// `granules` are the complete granules of `columns`, the rows after them form the incomplete granule.
        void add(Columns && columns, Granules && granules);
        /// Releases the first `num_granules` complete granules, grouped by the columns they belong to.
        ColumnsAndGranules flush(size_t num_granules);
        /// Releases all complete granules and the incomplete one, if any.
        ColumnsAndGranules flushAll();

        [[nodiscard]] size_t getNumBytes() const;
        [[nodiscard]] size_t getNumCompleteGranules() const { return complete_granules.size(); }
        [[nodiscard]] size_t getNumRowsInLastIncompleteGranule() const;
        [[nodiscard]] bool empty() const { return complete_granules.empty() && getNumRowsInLastIncompleteGranule() == 0; }

    private:
        /// Columns of the added blocks that hold complete granules.
        std::list<Columns> complete_granules_columns;
        /// Rows of the last granule that is not complete yet.
        MutableColumns incomplete_granule_columns;
        /// Complete granules with the columns they refer to.
        std::vector<std::pair<Granule, std::list<Columns>::iterator>> complete_granules;
        ssize_t last_complete_granule_mark_number = -1;
    };

    ColumnsBuffer columns_buffer;

    /// Blocks with the header of the part from the columns released by `columns_buffer`.
    [[nodiscard]] BlocksAndGranules makeBlocksAndGranules(ColumnsBuffer::ColumnsAndGranules && columns_and_granules) const;

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
    /// Use std::map for deterministic iteration order — the order affects
    /// the uncompressed_hash computation in addToChecksums.
    std::map<UInt64, CompressedStreamPtr> streams_by_codec;

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
};

}
