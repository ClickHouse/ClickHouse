#include <Storages/MergeTree/MergeTreeOneRowPerBlockLayout.h>

#include <Columns/ColumnArray.h>
#include <Compression/CompressionInfo.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>

#include <city.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

constexpr auto DATA_FILE_EXTENSION = ".bin";

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

bool declaredCodecStoresDataVerbatim(const ASTPtr & codec_desc)
{
    /// A chain of several codecs is stored with another header for each block, even of codecs that store the data verbatim.
    const auto * codec_func = codec_desc ? codec_desc->as<ASTFunction>() : nullptr;
    if (!codec_func || !codec_func->arguments || codec_func->arguments->children.size() != 1)
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

UInt64 getUncompressedFixedArraySize(const ColumnDescription & column)
{
    const Field * setting = column.settings.tryGet(UNCOMPRESSED_FIXED_ARRAY_SIZE_SETTING);
    return setting ? setting->safeGet<UInt64>() : 0;
}

void validateUncompressedFixedArraySize(const ColumnDescription & column)
{
    if (!column.settings.tryGet(UNCOMPRESSED_FIXED_ARRAY_SIZE_SETTING))
        return;

    auto error = [&](std::string_view reason)
    {
        return Exception(ErrorCodes::BAD_ARGUMENTS, "Column {} cannot have the setting `{}`: {}",
            backQuote(column.name), UNCOMPRESSED_FIXED_ARRAY_SIZE_SETTING, reason);
    };

    const UInt64 array_size = getUncompressedFixedArraySize(column);
    if (array_size == 0)
        throw error("the value must be greater than 0");

    if (!canHaveOneRowPerBlockLayout(column.type))
        throw error(fmt::format(
            "its type is {}, expected `Array(Float32)`, `Array(Float64)`, `Array(BFloat16)` or `Array(Int8)`", column.type->getName()));

    if (column.default_desc.kind == ColumnDefaultKind::Alias || column.default_desc.kind == ColumnDefaultKind::Ephemeral)
        throw error("the column is not stored");

    if (column.codec && !declaredCodecStoresDataVerbatim(column.codec))
        throw error("its codec must be a single `NONE` or `Quantized` codec, which store the data as is");

    if (!column.subcolumn_codecs.empty())
        throw error("it has codecs of subcolumns");

    /// An expired TTL replaces the values with the default, which can have another number of elements.
    if (column.ttl)
        throw error("it has a TTL");

    for (const auto & setting : {"min_compress_block_size", "max_compress_block_size"})
        if (column.settings.tryGet(setting))
            throw error(fmt::format("it also has the setting `{}`, while the size of the blocks is defined by `{}`",
                setting, UNCOMPRESSED_FIXED_ARRAY_SIZE_SETTING));

    const size_t element_bytes = assert_cast<const DataTypeArray &>(*column.type).getNestedType()->getSizeOfValueInMemory();
    if (array_size > MergeTreeWriterSettings::MAX_COMPRESS_BLOCK_SIZE / element_bytes)
        throw error(fmt::format("a row of {} elements is larger than the maximum size of a block, {} bytes",
            array_size, MergeTreeWriterSettings::MAX_COMPRESS_BLOCK_SIZE));
}

namespace
{

bool hasIdentifier(const IAST & ast)
{
    if (ast.as<ASTIdentifier>())
        return true;
    for (const auto & child : ast.children)
        if (hasIdentifier(*child))
            return true;
    return false;
}

}

bool hasDefaultExpressionWithIdentifiers(const StorageInMemoryMetadata & metadata)
{
    for (const auto & column : metadata.getColumns())
        if (column.default_desc.expression && hasIdentifier(*column.default_desc.expression))
            return true;
    for (const auto & column : metadata.virtuals)
        if (column.default_desc.expression && hasIdentifier(*column.default_desc.expression))
            return true;
    return false;
}

void checkUncompressedFixedArraySizes(const Block & block, const StorageInMemoryMetadata & metadata)
{
    for (const auto & column : block)
    {
        const ColumnDescription * column_description = metadata.getColumns().tryGet(column.name);
        if (!column_description)
            continue;
        const UInt64 array_size = getUncompressedFixedArraySize(*column_description);
        if (!array_size)
            continue;

        const auto full_column = column.column->convertToFullColumnIfConst();
        const auto * column_array = typeid_cast<const ColumnArray *>(full_column.get());
        if (!column_array)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} with the setting `{}` is written as {}",
                backQuote(column.name), UNCOMPRESSED_FIXED_ARRAY_SIZE_SETTING, full_column->getName());

        const auto & offsets = column_array->getOffsets();
        for (size_t row = 0; row < offsets.size(); ++row)
        {
            const UInt64 size = offsets[row] - offsets[ssize_t(row) - 1];
            if (size != array_size)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "A row of column {} has {} elements, while the setting `{}` of the column requires exactly {}",
                    backQuote(column.name), size, UNCOMPRESSED_FIXED_ARRAY_SIZE_SETTING, array_size);
        }
    }
}

void checkNoUncompressedFixedArrayColumns(const ColumnsDescription & columns, std::string_view operation)
{
    for (const auto & column : columns)
        if (getUncompressedFixedArraySize(column))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not supported for a table with a column with the setting `{}`: column {}",
                operation, UNCOMPRESSED_FIXED_ARRAY_SIZE_SETTING, backQuote(column.name));
}

bool isTopLevelArrayElementsStream(const ISerialization::SubstreamPath & path)
{
    return path.size() == 2 && path[0].type == ISerialization::Substream::ArrayElements
        && path[1].type == ISerialization::Substream::Regular;
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

    /// The setting can be specified only in `CREATE TABLE`, and `ALTER` can change neither it, nor the type, nor the codec of
    /// the column, so every part of the table is written with it, see `validateUncompressedFixedArraySize`.
    const UInt64 array_size = getUncompressedFixedArraySize(*column_description);
    if (array_size == 0)
        return std::nullopt;

    const auto file_name = getStreamFileName(part, column, ISerialization::Substream::ArrayElements);
    if (!file_name)
        return std::nullopt;

    const auto checksum_it = part.checksums.files.find(*file_name);
    if (checksum_it == part.checksums.files.end())
        return std::nullopt;

    OneRowPerBlockLayout layout;
    layout.file_name = *file_name;
    layout.array_size = array_size;
    layout.row_bytes = array_size * assert_cast<const DataTypeArray &>(*column.type).getNestedType()->getSizeOfValueInMemory();
    layout.header_bytes = getOneRowPerBlockHeaderBytes();
    layout.block_bytes = layout.header_bytes + layout.row_bytes;

    /// The writer checks the blocks of every part it writes, and the ways to bring in other parts are not supported, see
    /// `checkNoUncompressedFixedArrayColumns`, so this is only a check against a part put into the directory of the table
    /// by hand. Every block is also checked when it is read.
    const auto & data_checksum = checksum_it->second;
    if (!data_checksum.is_compressed || data_checksum.uncompressed_size != part.rows_count * layout.row_bytes
        || data_checksum.file_size != part.rows_count * layout.block_bytes)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "The file {} of part {} has {} bytes, {} uncompressed, while the column {} with the setting `{}` = {} must have "
            "{} bytes, {} uncompressed, for {} rows",
            layout.file_name, part.name, data_checksum.file_size, data_checksum.uncompressed_size, backQuote(column.name),
            UNCOMPRESSED_FIXED_ARRAY_SIZE_SETTING, array_size, part.rows_count * layout.block_bytes,
            part.rows_count * layout.row_bytes, part.rows_count);

    return layout;
}

}
