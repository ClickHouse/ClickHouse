#pragma once

#include <map>

#include <Core/Field.h>
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

    /// Write block of rows into .bin file and marks in .mrk files
    void writeDataBlock(const Block & block, const Granules & granules);

    /// Write block of rows into .bin file and marks in .mrk files, primary index in .idx file
    /// and skip indices in their corresponding files.
    void writeDataBlockPrimaryIndexAndSkipIndices(const Block & block, const Granules & granules);

    void addToChecksums(MergeTreeDataPartChecksums & checksums);

    void addStreams(const NameAndTypePair & name_and_type, const ASTPtr & effective_codec_desc) override;

    ISerialization::SerializeBinaryBulkSettings getSerializationSettings() const override;

    /// `with_key_columns` Map support. Such a column cannot decide its physical stream set
    /// block by block (each distinct key is a separate stream, and Compact marks require the
    /// substream set to be identical in every granule). So the writer buffers the whole part,
    /// freezes the union of keys before the first granule, and only then writes data.bin plus a
    /// plain `<column>.key_columns.txt` per Map column.
    bool hasMapKeyColumns() const { return !map_key_columns.empty(); }
    /// Flush the fully buffered part: freeze keys, init streams, write every granule, and the
    /// per-column `key_columns.txt` files. Called from finalizeIndexGranularity in the frozen path.
    void writeBufferedMapKeyColumnsPart();
    /// Build columns_substreams honouring the frozen key set (no template streams).
    void initFrozenColumnsSubstreams();
    void writeMapKeyColumnsFiles(MergeTreeDataPartChecksums & checksums);

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

    /// Names of columns whose serialization is `SerializationMapWithKeyColumns`. When non-empty
    /// the writer runs the buffered/frozen Compact path instead of the streaming one.
    Names map_key_columns;
    /// Frozen key set for each `with_key_columns` Map column, filled by
    /// writeBufferedMapKeyColumnsPart before the first granule is written. Referenced by the
    /// serialize settings so the serialization registers exactly these keys and writes no template.
    std::unordered_map<String, std::vector<Field>> map_key_columns_frozen_keys;
    bool map_key_columns_frozen = false;
    /// Open `<column>.key_columns.txt` files, kept until finish so they can be synced with data.bin.
    std::vector<std::unique_ptr<WriteBufferFromFileBase>> key_columns_files;

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
