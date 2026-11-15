#include <Storages/MergeTree/MergeTreeOneRowPerBlockLayout.h>

#include <Compression/CompressionInfo.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/NullWriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/Hex.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>

#include <city.h>
#include <fmt/format.h>


namespace DB
{

namespace
{

constexpr auto DATA_FILE_EXTENSION = ".bin";

std::optional<UInt64> getUInt64ColumnSetting(const ColumnDescription & column_description, const String & name)
{
    const Field * setting = column_description.settings.tryGet(name);
    if (!setting)
        return std::nullopt;
    return setting->safeGet<UInt64>();
}

std::optional<String> getStreamFileName(const IMergeTreeDataPart & part, const NameAndTypePair & column, ISerialization::Substream::Type substream)
{
    ISerialization::SubstreamPath path;
    path.push_back(substream);
    const auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(column, path, DATA_FILE_EXTENSION, part.checksums, part.storage.getSettings());
    if (!stream_name)
        return std::nullopt;
    return *stream_name + DATA_FILE_EXTENSION;
}

}

UInt64 getOneRowPerBlockHeaderBytes()
{
    return sizeof(CityHash_v1_0_2::uint128) + COMPRESSED_BLOCK_HEADER_SIZE;
}

String getOneRowPerBlockMarkerName(
    const String & stream_name, const MergeTreeDataPartChecksum & data_checksum, const MergeTreeDataPartChecksum & sizes_checksum)
{
    SipHash hash;
    hash.update(stream_name);
    for (const auto * checksum : {&data_checksum, &sizes_checksum})
    {
        hash.update(checksum->file_size);
        hash.update(checksum->file_hash.low64);
        hash.update(checksum->file_hash.high64);
    }
    return getHexUIntLowercase(hash.get128()) + ONE_ROW_PER_BLOCK_MARKER_EXTENSION;
}

String makeOneRowPerBlockMarker(
    UInt64 row_bytes, UInt64 array_size, const MergeTreeDataPartChecksum & data_checksum, const MergeTreeDataPartChecksum & sizes_checksum)
{
    return fmt::format(
        "one row per block, version 1\nrow_bytes: {}\narray_size: {}\nfile_size: {}\nfile_hash: {}_{}\nsizes_file_size: {}\nsizes_file_hash: {}_{}\n",
        row_bytes, array_size, data_checksum.file_size, data_checksum.file_hash.low64, data_checksum.file_hash.high64,
        sizes_checksum.file_size, sizes_checksum.file_hash.low64, sizes_checksum.file_hash.high64);
}

MergeTreeDataPartChecksum getOneRowPerBlockMarkerChecksum(const String & content)
{
    /// The same hash as the writer records for the file, see `MergeTreeDataPartWriterWide::fillDataChecksums`.
    NullWriteBuffer null_out;
    HashingWriteBuffer hashing(null_out);
    hashing.write(content.data(), content.size());
    hashing.finalize();
    return MergeTreeDataPartChecksum(hashing.count(), hashing.getHash());
}

bool declaredCodecStoresDataVerbatim(const ASTPtr & codec_desc)
{
    const auto * codec_func = codec_desc ? codec_desc->as<ASTFunction>() : nullptr;
    if (!codec_func || !codec_func->arguments || codec_func->arguments->children.empty())
        return false;

    for (const auto & codec : codec_func->arguments->children)
    {
        String name;
        if (const auto * identifier = codec->as<ASTIdentifier>())
            name = identifier->name();
        else if (const auto * function = codec->as<ASTFunction>())
            name = function->name;

        if (!equalsCaseInsensitive(name, "NONE") && !equalsCaseInsensitive(name, "Quantized"))
            return false;
    }
    return true;
}

bool canHaveOneRowPerBlockLayout(const DataTypePtr & type)
{
    const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
    if (!array_type)
        return false;
    const WhichDataType which(array_type->getNestedType());
    return which.isBFloat16() || which.isFloat32() || which.isFloat64() || which.isInt8();
}

std::optional<OneRowPerBlockLayout> tryGetOneRowPerBlockLayout(
    const IMergeTreeDataPart & part,
    const NameAndTypePair & column,
    const StorageInMemoryMetadata & metadata)
{
    /// In a Compact part, the data of all columns is in one file.
    if (part.getType() != MergeTreeDataPartType::Wide)
        return std::nullopt;

    if (part.rows_count == 0 || !canHaveOneRowPerBlockLayout(column.type))
        return std::nullopt;

    const ColumnDescription * column_description = metadata.getColumns().tryGet(column.getNameInStorage());
    if (!column_description)
        return std::nullopt;

    /// The sizes of the file below match those of blocks stored without compression also for a codec that compresses
    /// to exactly the same size, so the codec is checked first.
    if (!declaredCodecStoresDataVerbatim(column_description->codec))
        return std::nullopt;

    /// The writer reads these column-level settings for every stream of the column, see `MergeTreeDataPartWriterWide`.
    /// They describe the parts written from now on, not necessarily this part, so the marker of the part is checked below.
    const auto min_compress_block_size = getUInt64ColumnSetting(*column_description, "min_compress_block_size");
    const auto max_compress_block_size = getUInt64ColumnSetting(*column_description, "max_compress_block_size");
    if (!min_compress_block_size || !max_compress_block_size || *min_compress_block_size != *max_compress_block_size)
        return std::nullopt;

    const size_t element_bytes = assert_cast<const DataTypeArray &>(*column.type).getNestedType()->getSizeOfValueInMemory();
    if (*max_compress_block_size == 0 || *max_compress_block_size % element_bytes != 0)
        return std::nullopt;

    const auto file_name = getStreamFileName(part, column, ISerialization::Substream::ArrayElements);
    if (!file_name)
        return std::nullopt;

    const auto checksum_it = part.checksums.files.find(*file_name);
    if (checksum_it == part.checksums.files.end() || !checksum_it->second.is_compressed)
        return std::nullopt;

    OneRowPerBlockLayout layout;
    layout.file_name = *file_name;
    layout.row_bytes = *max_compress_block_size;
    layout.array_size = layout.row_bytes / element_bytes;
    layout.header_bytes = getOneRowPerBlockHeaderBytes();
    layout.block_bytes = layout.header_bytes + layout.row_bytes;

    /// Every block has a header and is not empty, so both sizes match only if the file has exactly `rows_count`
    /// blocks, all stored without compression. This is necessary, but not sufficient: blocks of other sizes can add up
    /// to the same totals, and rows of other lengths can fill blocks of the same size. So the part must also have the
    /// marker of the writer for exactly this layout and this file, see `ONE_ROW_PER_BLOCK_MARKER_EXTENSION`.
    const auto & data_checksum = checksum_it->second;
    if (data_checksum.uncompressed_size != part.rows_count * layout.row_bytes
        || data_checksum.file_size != part.rows_count * layout.block_bytes)
        return std::nullopt;

    /// The file of the array sizes defines where the rows begin, so the marker is valid only for these sizes as well.
    const auto sizes_file_name = getStreamFileName(part, column, ISerialization::Substream::ArraySizes);
    if (!sizes_file_name)
        return std::nullopt;
    const auto sizes_checksum_it = part.checksums.files.find(*sizes_file_name);
    if (sizes_checksum_it == part.checksums.files.end())
        return std::nullopt;
    const auto & sizes_checksum = sizes_checksum_it->second;

    const String stream_name = layout.file_name.substr(0, layout.file_name.size() - strlen(DATA_FILE_EXTENSION));
    const auto marker_it = part.checksums.files.find(getOneRowPerBlockMarkerName(stream_name, data_checksum, sizes_checksum));
    if (marker_it == part.checksums.files.end())
        return std::nullopt;

    const auto expected_marker = getOneRowPerBlockMarkerChecksum(
        makeOneRowPerBlockMarker(layout.row_bytes, layout.array_size, data_checksum, sizes_checksum));
    if (marker_it->second.file_size != expected_marker.file_size || marker_it->second.file_hash != expected_marker.file_hash)
        return std::nullopt;

    return layout;
}

}
