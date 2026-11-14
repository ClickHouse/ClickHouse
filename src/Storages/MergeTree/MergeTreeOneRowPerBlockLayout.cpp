#include <Storages/MergeTree/MergeTreeOneRowPerBlockLayout.h>

#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressionInfo.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/StringUtils.h>
#include <base/unaligned.h>

#include <city.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
}

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

/// Whether every codec of the declared codec of a column stores the data verbatim: `NONE`, or `Quantized`, which only adds
/// another stream. A column without a declared codec uses the default codec, which compresses.
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
    /// They describe the parts written from now on, not necessarily this part, so the sizes of the file are checked below.
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
    layout.header_bytes = sizeof(CityHash_v1_0_2::uint128) + COMPRESSED_BLOCK_HEADER_SIZE;
    layout.block_bytes = layout.header_bytes + layout.row_bytes;

    /// Every block has a header and is not empty, so both sizes match only if the file has exactly `rows_count`
    /// blocks, all stored without compression. With all rows of `array_size` elements, which the caller checks, this is
    /// necessary, but not sufficient, for one row per block: the writer fills blocks up to the maximum block size `M` and
    /// may end a shorter block at a granule boundary, so a part written with `M` slightly larger than `row_bytes` and
    /// small granules can also have blocks of different sizes. The reader checks the size of every block it reads, and
    /// throws an exception on a mismatch.
    const auto & checksum = checksum_it->second;
    if (checksum.uncompressed_size != part.rows_count * layout.row_bytes
        || checksum.file_size != part.rows_count * layout.block_bytes)
        return std::nullopt;

    return layout;
}

bool checkOneRowPerBlockArraySizes(
    const IMergeTreeDataPart & part, const NameAndTypePair & column, UInt64 array_size, const ReadSettings & read_settings)
{
    const auto file_name = getStreamFileName(part, column, ISerialization::Substream::ArraySizes);
    if (!file_name)
        return false;

    CompressedReadBufferFromFile in(part.getDataPartStorage().readFile(*file_name, read_settings, std::nullopt));

    /// The sizes are stored as the number of elements of every row, see `serializeArraySizesPositionIndependent`.
    constexpr size_t sizes_per_read = 8192;
    PODArray<char> buffer(sizes_per_read * sizeof(UInt64));
    UInt64 rows_left = part.rows_count;
    while (rows_left > 0)
    {
        const size_t rows = std::min<UInt64>(rows_left, sizes_per_read);
        const size_t bytes = rows * sizeof(UInt64);
        if (in.readBig(buffer.data(), bytes) != bytes)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read {} array sizes from {}", part.rows_count, *file_name);

        for (size_t i = 0; i < rows; ++i)
        {
            if (unalignedLoadLittleEndian<UInt64>(buffer.data() + i * sizeof(UInt64)) != array_size)
                return false;
        }
        rows_left -= rows;
    }

    return true;
}

}
