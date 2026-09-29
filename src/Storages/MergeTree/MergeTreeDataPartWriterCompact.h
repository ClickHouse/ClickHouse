#pragma once

#include <map>

#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>


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
        MergeTreeIndexGranularityPtr index_granularity_,
        const PlannedMapKeyColumnsKeys & map_key_columns_keys_ = {});

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

    /// Write block of rows into .bin file and marks in .mrk files
    void writeDataBlock(const Block & block, const Granules & granules);

    /// Write one granule of one ordinary column, recording its marks. `prev_stream` carries
    /// the pending compressed stream of the previous column within the granule.
    void writeColumnGranule(
        const Block & block,
        const NameAndTypePair & name_and_type,
        const Granule & granule,
        bool is_first_granule_of_block,
        WriteBuffer & marks_out,
        MarksInCompressedFile::PlainArray * cached_marks_);

    /// Write one granule of a `with_key_columns` Map column: one compressed block per key
    /// substream, so that reading `m['k']` decodes only that key's streams.
    void writeMapKeyColumnsGranule(
        const Block & block,
        const Granule & granule,
        const NameAndTypePair & name_and_type,
        WriteBuffer & marks_out,
        MarksInCompressedFile::PlainArray * cached_marks_);

    /// Write block of rows into .bin file and marks in .mrk files, primary index in .idx file
    /// and skip indices in their corresponding files.
    void writeDataBlockPrimaryIndexAndSkipIndices(const Block & block, const Granules & granules);

    void addToChecksums(MergeTreeDataPartChecksums & checksums);

    void addStreams(const NameAndTypePair & name_and_type, const ASTPtr & effective_codec_desc) override;

    ISerialization::SerializeBinaryBulkSettings getSerializationSettings() const override;

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
    /// Use std::map for deterministic iteration order — the order affects
    /// the uncompressed_hash computation in addToChecksums.
    std::map<UInt64, CompressedStreamPtr> streams_by_codec;

    /// Stream for each column's substreams path (look at addStreams).
    std::unordered_map<String, CompressedStreamPtr> compressed_streams;

    /// The compressed stream used last within the current granule. Every column's first
    /// substream flushes it before starting, so that every column (and every per-key
    /// substream of a `with_key_columns` Map column) starts a fresh compressed block.
    CompressedStreamPtr prev_stream;
    /// The substream name `prev_stream` was last used for; two substreams sharing one
    /// compressed stream (same codec) still need a flush between them.
    String prev_stream_name;

    /// Register a lazily discovered stream (e.g. a per-key stream of a
    /// `with_key_columns` Map, whose stream set is fixed only when the first
    /// block is written) with the shared compressed stream of its codec and
    /// record it in columns_substreams, so marks written for it are aligned
    /// with the reader's expectations.
    void addStream(const NameAndTypePair & name_and_type, const ISerialization::SubstreamPath & substream_path, const ASTPtr & effective_codec_desc);

    /// The pre-planned key sets of `with_key_columns` Map columns (computed over
    /// the complete part block by `MergeTreeDataWriter::writeTempPartImpl`, or the
    /// planned union for merges). The compact writer must fix the key set before
    /// the first mark is recorded, so it seeds the serialize state of such
    /// columns (and opens their per-key streams) on the first written block.
    PlannedMapKeyColumnsKeys map_key_columns_keys;
    /// Serialize state of a seeded `with_key_columns` Map column, reused across
    /// the granules of the part (a `SeedKeysState` before the first granule).
    std::unordered_map<String, ISerialization::SerializeBinaryBulkStatePtr> map_key_columns_states;

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
