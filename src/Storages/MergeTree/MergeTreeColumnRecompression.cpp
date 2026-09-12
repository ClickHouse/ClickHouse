#include <Storages/MergeTree/MergeTreeColumnRecompression.h>

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Compression/CompressedReadBufferBase.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <Common/PODArray.h>
#include <Common/ProfileEvents.h>
#include <Core/Defines.h>

#include <city.h>
#include <Poco/String.h>

#include <functional>
#include <unordered_map>
#include <unordered_set>

namespace ProfileEvents
{
    extern const Event MutationRecompressedBlocks;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsNonZeroUInt64 marks_compress_block_size;
    extern const MergeTreeSettingsString marks_compression_codec;
}

namespace
{

/// Maps an offset of the start of a compressed block in the source `.bin` file to the offset of
/// the same block in the recompressed `.bin` file. Also contains the mapping of the total file
/// size (for marks that point at the end of the file).
using OffsetMap = std::unordered_map<UInt64, UInt64>;

/// Exposes the protected raw-block API of `CompressedReadBufferBase` so we can read and decompress
/// one compressed block at a time. Codec detection is self-describing (via the block header).
class RawCompressedBlockReader : public CompressedReadBufferBase
{
public:
    explicit RawCompressedBlockReader(std::unique_ptr<ReadBufferFromFileBase> file_)
        : CompressedReadBufferBase(file_.get(), /*allow_different_codecs_=*/ true)
        , file(std::move(file_))
    {
    }

    /// Reads one compressed block. Returns the number of compressed bytes consumed (including the
    /// checksum), or 0 at end of file. On success `decompressed` holds the block's decompressed bytes.
    size_t readBlock(PODArray<char> & decompressed)
    {
        size_t size_decompressed = 0;
        size_t size_compressed_without_checksum = 0;
        size_t consumed = readCompressedData(size_decompressed, size_compressed_without_checksum, /*always_copy=*/ false);
        if (consumed == 0)
            return 0;

        /// Some codecs (LZ4) decompress in wide copies that legitimately overrun the exact end of the
        /// output by up to `getAdditionalSizeAtTheEndOfBuffer` bytes, so the destination must have that
        /// much extra capacity (mirrors `CompressedReadBuffer::nextImpl`). Keep the logical size at the
        /// exact decompressed size -- callers use `decompressed.size()` as the block's decompressed length.
        const size_t additional_size = codec->getAdditionalSizeAtTheEndOfBuffer();
        decompressed.reserve(size_decompressed + additional_size);
        decompressed.resize(size_decompressed);
        decompressTo(decompressed.data(), size_decompressed, size_compressed_without_checksum);
        return consumed;
    }

private:
    std::unique_ptr<ReadBufferFromFileBase> file;
};

/// Writes a single compressed block in the exact on-disk format produced by `CompressedWriteBuffer`:
/// [16-byte CityHash128 checksum][compression header + compressed body].
void writeCompressedBlock(WriteBuffer & out, const ICompressionCodec & codec, const char * data, UInt32 size, PODArray<char> & scratch)
{
    UInt32 reserve = codec.getCompressedReserveSize(size);
    scratch.resize(reserve);
    UInt32 compressed_size = codec.compress(data, size, scratch.data());

    CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(scratch.data(), compressed_size);
    writeBinaryLittleEndian(checksum.low64, out);
    writeBinaryLittleEndian(checksum.high64, out);
    out.write(scratch.data(), compressed_size);
}

/// Reads every compressed block of `bin_name` from `source_storage`, recompresses it with
/// `new_codec` and writes it to `bin_name` in `new_storage`. Blocks are re-emitted one-to-one, so
/// the decompressed content and block boundaries are preserved. Fills `offset_map` with the
/// source-offset -> new-offset mapping of every block start (plus the end-of-file offset) and
/// returns the checksum of the written file.
MergeTreeDataPartChecksum recompressBinFile(
    const IDataPartStorage & source_storage,
    IDataPartStorage & new_storage,
    const String & bin_name,
    const ICompressionCodec & new_codec,
    const MergeTreeDataPartChecksum & source_checksum,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    OffsetMap & offset_map)
{
    RawCompressedBlockReader reader(source_storage.readFile(bin_name, read_settings, std::nullopt));
    auto out = new_storage.writeFile(bin_name, DBMS_DEFAULT_BUFFER_SIZE, write_settings);
    HashingWriteBuffer bin_hashing(*out);

    PODArray<char> decompressed;
    PODArray<char> scratch;
    UInt64 source_offset = 0;

    while (true)
    {
        size_t consumed = reader.readBlock(decompressed);
        if (consumed == 0)
            break;

        offset_map[source_offset] = bin_hashing.count();
        source_offset += consumed;
        writeCompressedBlock(bin_hashing, new_codec, decompressed.data(), static_cast<UInt32>(decompressed.size()), scratch);
        ProfileEvents::increment(ProfileEvents::MutationRecompressedBlocks);
    }

    bin_hashing.finalize();
    /// A mark of the last granule may point at the end of the file.
    offset_map[source_offset] = bin_hashing.count();
    out->finalize();

    MergeTreeDataPartChecksum checksum;
    checksum.is_compressed = true;
    /// The decompressed content is byte-identical to the source, so the uncompressed hash does not
    /// change (this is exactly what `CHECK TABLE` recomputes by decompressing the file).
    checksum.uncompressed_size = source_checksum.uncompressed_size;
    checksum.uncompressed_hash = source_checksum.uncompressed_hash;
    checksum.file_size = bin_hashing.count();
    checksum.file_hash = bin_hashing.getHash();
    return checksum;
}

/// Rewrites the marks file `mrk_name`: every mark's `offset_in_compressed_file` is remapped through
/// `offset_map`; the `offset_in_decompressed_block` and per-granule row count are copied verbatim
/// (block boundaries are preserved by `recompressBinFile`). Returns the checksum of the written file.
MergeTreeDataPartChecksum rewriteMarksFile(
    const IDataPartStorage & source_storage,
    IDataPartStorage & new_storage,
    const String & mrk_name,
    size_t mark_size,
    bool compressed_marks,
    const CompressionCodecPtr & marks_codec,
    size_t marks_compress_block_size,
    const OffsetMap & offset_map,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings)
{
    /// Read the (decompressed) marks.
    String old_marks;
    {
        auto file = source_storage.readFile(mrk_name, read_settings, std::nullopt);
        if (compressed_marks)
        {
            CompressedReadBufferFromFile marks_reader(std::move(file));
            readStringUntilEOF(old_marks, marks_reader);
        }
        else
        {
            readStringUntilEOF(old_marks, *file);
        }
    }

    if (mark_size == 0 || old_marks.size() % mark_size != 0)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Marks file {} has size {} which is not a multiple of the mark size {}",
            mrk_name, old_marks.size(), mark_size);

    const size_t num_marks = old_marks.size() / mark_size;
    const size_t tail_size = mark_size - sizeof(UInt64);

    /// Build the new (uncompressed) marks with remapped compressed offsets.
    WriteBufferFromOwnString new_marks_buf;
    ReadBufferFromMemory marks_in(old_marks.data(), old_marks.size());
    char tail[sizeof(UInt64) * 2];
    for (size_t i = 0; i < num_marks; ++i)
    {
        UInt64 source_offset = 0;
        readBinaryLittleEndian(source_offset, marks_in);
        marks_in.readStrict(tail, tail_size);

        auto it = offset_map.find(source_offset);
        if (it == offset_map.end())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Mark #{} in {} points at compressed offset {} which is not a block boundary",
                i, mrk_name, source_offset);

        writeBinaryLittleEndian(it->second, new_marks_buf);
        new_marks_buf.write(tail, tail_size);
    }
    new_marks_buf.finalize();
    const String & new_marks = new_marks_buf.str();

    auto out = new_storage.writeFile(mrk_name, 4096, write_settings);
    HashingWriteBuffer file_hashing(*out);

    MergeTreeDataPartChecksum checksum;
    if (compressed_marks)
    {
        CompressedWriteBuffer compressor(file_hashing, marks_codec, marks_compress_block_size);
        HashingWriteBuffer uncompressed_hashing(compressor);
        uncompressed_hashing.write(new_marks.data(), new_marks.size());

        uncompressed_hashing.finalize();
        compressor.finalize();
        file_hashing.finalize();

        checksum.is_compressed = true;
        checksum.uncompressed_size = uncompressed_hashing.count();
        checksum.uncompressed_hash = uncompressed_hashing.getHash();
    }
    else
    {
        file_hashing.write(new_marks.data(), new_marks.size());
        file_hashing.finalize();
    }
    out->finalize();

    checksum.file_size = file_hashing.count();
    checksum.file_hash = file_hashing.getHash();
    return checksum;
}

/// Enumerates the data streams of `column` that actually exist in `part`, invoking `callback` with
/// the on-disk stream name (without extension) and the substream path for each. Mirrors the stream
/// enumeration done by the wide-part writer, so the same set of `.bin`/marks files is produced.
void forEachColumnStream(
    const IMergeTreeDataPart & part,
    const NameAndTypePair & column,
    const std::function<void(const String & stream_name, const ISerialization::SubstreamPath & substream_path)> & callback)
{
    auto serialization = part.getSerialization(column.name);
    std::unordered_set<String> processed_streams;

    ISerialization::StreamCallback stream_callback = [&](const ISerialization::SubstreamPath & substream_path)
    {
        if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
            return;

        /// Resolve the stream's on-disk name against the part's recorded files (its checksums) rather
        /// than recomputing it from the table's *current* `replace_long_file_name_to_hash` /
        /// `max_file_name_length` settings. Those settings can change after the part is written, and a
        /// name recomputed from the current settings would then not match the file actually on disk:
        /// the stream would be treated as absent, silently skipped, and the recompression would become
        /// a no-op. `getStreamNameForColumn` tries both the plain and the hashed name (and the
        /// alternative stream-file-name settings) and returns the one the part actually has, or
        /// `nullopt` when the stream is genuinely not present in this part.
        auto stream_name_opt = IMergeTreeDataPart::getStreamNameForColumn(
            column, substream_path, IMergeTreeDataPart::DATA_FILE_EXTENSION, part.checksums, part.storage.getSettings());
        if (!stream_name_opt)
            return;
        const String & stream_name = *stream_name_opt;

        /// Shared offsets substream of a Nested type appears once per element but is written only once.
        if (!processed_streams.insert(stream_name).second)
            return;

        callback(stream_name, substream_path);
    };

    serialization->enumerateStreams(stream_callback, column.type);
}

}

NameSet getColumnDataStreamFileNames(
    const IMergeTreeDataPart & part,
    const NameAndTypePair & column)
{
    const String marks_extension = part.index_granularity_info.mark_type.getFileExtension();
    NameSet result;
    forEachColumnStream(part, column, [&](const String & stream_name, const ISerialization::SubstreamPath &)
    {
        result.insert(stream_name + IMergeTreeDataPart::DATA_FILE_EXTENSION);
        result.insert(stream_name + marks_extension);
    });
    return result;
}

void recompressColumnStreams(
    const IMergeTreeDataPart & source_part,
    IMergeTreeDataPart & new_data_part,
    const NameAndTypePair & column,
    const StorageMetadataPtr & metadata_snapshot,
    const CompressionCodecPtr & default_codec,
    const MergeTreeSettings & storage_settings,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    NameSet & recompressed_streams,
    MergeTreeDataPartChecksums & checksums)
{
    /// Resolve the column's effective codec exactly as the wide-part writer does
    /// (`IMergeTreeDataPartWriter::getCodecDescOrDefault`).
    ASTPtr effective_codec_desc = default_codec->getFullCodecDesc();
    if (const auto * column_desc = metadata_snapshot->getColumns().tryGet(column.name))
        if (column_desc->codec)
            effective_codec_desc = column_desc->codec;

    /// Marks parameters (same for every substream of a wide part).
    const auto & granularity_info = source_part.index_granularity_info;
    const bool compressed_marks = granularity_info.mark_type.compressed;
    const size_t mark_size = granularity_info.getMarkSizeInBytes(1);
    const String marks_extension = granularity_info.mark_type.getFileExtension();

    CompressionCodecPtr marks_codec;
    size_t marks_compress_block_size = 0;
    if (compressed_marks)
    {
        ParserCodec codec_parser;
        auto ast = parseQuery(
            codec_parser,
            "(" + Poco::toUpper(String(storage_settings[MergeTreeSetting::marks_compression_codec])) + ")",
            0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        marks_codec = CompressionCodecFactory::instance().get(ast, nullptr);
        marks_compress_block_size = std::min<size_t>(
            storage_settings[MergeTreeSetting::marks_compress_block_size], MergeTreeWriterSettings::MAX_COMPRESS_BLOCK_SIZE);
    }

    const IDataPartStorage & source_storage = source_part.getDataPartStorage();
    IDataPartStorage & new_storage = new_data_part.getDataPartStorage();

    forEachColumnStream(source_part, column, [&](const String & stream_name, const ISerialization::SubstreamPath & substream_path)
    {
        /// A stream shared by several recompressed columns (the offsets stream of `Nested` siblings
        /// with `share_nested_offsets`, where `n.a`/`n.b` share `n.size0`) must be rewritten exactly
        /// once. The first column to reach it rewrites it; the rest skip it, mirroring the wide-part
        /// writer, which also writes a shared offsets stream only once. Skipping here also leaves the
        /// checksums this stream already produced untouched.
        if (!recompressed_streams.insert(stream_name).second)
            return;

        const String bin_name = stream_name + IMergeTreeDataPart::DATA_FILE_EXTENSION;

        const auto & subtype = substream_path.back().data.type;
        CompressionCodecPtr new_codec;
        if (ISerialization::isSpecialCompressionAllowed(substream_path))
            new_codec = CompressionCodecFactory::instance().get(effective_codec_desc, subtype.get(), default_codec);
        else
            new_codec = CompressionCodecFactory::instance().get(effective_codec_desc, nullptr, default_codec, /*only_generic=*/ true);

        const auto & source_bin_checksum = checksums.files.at(bin_name);

        OffsetMap offset_map;
        checksums.files[bin_name] = recompressBinFile(
            source_storage, new_storage, bin_name, *new_codec, source_bin_checksum, read_settings, write_settings, offset_map);

        const String mrk_name = stream_name + marks_extension;
        checksums.files[mrk_name] = rewriteMarksFile(
            source_storage, new_storage, mrk_name, mark_size, compressed_marks, marks_codec, marks_compress_block_size,
            offset_map, read_settings, write_settings);
    });
}

}
