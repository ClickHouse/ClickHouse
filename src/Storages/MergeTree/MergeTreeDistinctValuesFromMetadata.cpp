#include <Storages/MergeTree/MergeTreeDistinctValuesFromMetadata.h>

#include <Compression/ICompressionCodec.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/Statistics/DistinctValuesFromCompressedSize.h>

#include <Poco/String.h>

#include <algorithm>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool enable_adaptive_codec_selection;
}

namespace
{

/// The compressed size tells the number of distinct values only for the values themselves: not for `Array`, `Map` or
/// `Tuple`, whose sizes add up their elements, and not for JSON or `Dynamic` with their own streams.
bool hasScalarValues(const DataTypePtr & type)
{
    const auto nested = removeNullable(removeLowCardinality(type));
    return nested->isValueRepresentedByNumber() || isStringOrFixedString(nested) || isUUID(nested) || isIPv6(nested);
}

/// Whether every codec of `codec` (a `CODEC(...)` description) is a general purpose one. `Delta`, `DoubleDelta`,
/// `Gorilla`, `T64`, `GCD`, `FPC` and the like compress by the order of the values rather than by their repetition,
/// so the compressed size says nothing about the number of distinct values. `Default` stands for the default codec
/// of the part, which can be a specialized one as well (`default_compression_codec`); `uses_default` tells whether it
/// is among the codecs.
bool hasOnlyGeneralPurposeCodecs(const ASTPtr & codec, bool & uses_default)
{
    const auto * function = codec ? codec->as<ASTFunction>() : nullptr;
    if (!function || !function->arguments)
        return false;

    for (const auto & child : function->arguments->children)
    {
        String name;
        if (const auto * identifier = child->as<ASTIdentifier>())
            name = identifier->name();
        else if (const auto * inner_function = child->as<ASTFunction>())
            name = inner_function->name;
        else
            return false;

        name = Poco::toUpper(name);
        if (name == "DEFAULT")
            uses_default = true;
        else if (name != "NONE" && name != "LZ4" && name != "LZ4HC" && name != "ZSTD")
            return false;
    }
    return true;
}

/// Whether the default codec of the part, that compresses the columns without a codec or with `CODEC(Default)`,
/// is known and a general purpose one.
bool hasGeneralPurposeDefaultCodec(const IMergeTreeDataPart & part)
{
    /// A codec recovered from the files of the part may be of another family than the one it was written with.
    if (!part.default_codec || part.default_codec_is_approximate)
        return false;

    bool uses_default = false;
    return hasOnlyGeneralPurposeCodecs(part.default_codec->getFullCodecDescription(), uses_default) && !uses_default;
}

/// Whether the column of the part is stored by general purpose codecs. The codec declared for the column is the
/// current one, while a codec-only `ALTER` leaves the parts as they were written: a part written with
/// `CODEC(Delta, ZSTD)` keeps it under a column altered to `CODEC(ZSTD)`. So the codec is taken from the data files of
/// the column, which costs a small read per file, once for a part.
///
/// On a remote disk that is a request per file of every part, too much for the planning of a query, so there the
/// declaration is taken instead. A replicated table tells whether a part was written with the current metadata, and
/// a part written with another one leaves the column without an estimate. The metadata version of other tables does
/// not change, so on a remote disk their parts are taken as declared: a codec-only `ALTER` is not noticed there.
bool isStoredWithGeneralPurposeCodecs(const IMergeTreeDataPart & part, const String & column_name, const StorageInMemoryMetadata & metadata)
{
    if (!part.isStoredOnRemoteDisk())
        return part.isColumnStoredWithGeneralPurposeCodecs(column_name);

    if (!part.storage.supportsReplication())
        return true;

    return !part.old_part_with_no_metadata_version_on_disk && part.getMetadataVersion() == metadata.getMetadataVersion();
}

}

std::unordered_map<String, UInt64> estimateDistinctValuesFromMetadata(
    const RangesInDataParts & parts, const StorageMetadataPtr & metadata, const Names & column_names)
{
    const auto & columns = metadata->getColumns();

    if (parts.empty())
        return {};

    /// Adaptive codec selection compresses the columns with the default codec by a specialized codec where it is
    /// smaller, and the parts do not tell for which blocks it did. Only the current setting of the table is known,
    /// not the one each part was written with.
    const bool adaptive_default_codec = (*parts.front().data_part->storage.getSettings())[MergeTreeSetting::enable_adaptive_codec_selection];

    /// The rows and the compressed bytes of a column in the parts that keep the sizes of their columns.
    struct CompressedSize
    {
        UInt64 rows = 0;
        UInt64 compressed_bytes = 0;
        /// The column is compressed by the default codec of each part (without a codec, or with `CODEC(Default)`).
        bool uses_default_codec = false;
    };
    /// The codec compresses the sequence of the values, so a column in the order of the sorting key compresses by
    /// its runs of equal values rather than by their number: 5000 values repeated 20 times each take less than a bit
    /// per row. Columns of the sorting key get no estimate.
    const Names sorting_key_columns = metadata->getColumnsRequiredForSortingKey();

    std::unordered_map<String, CompressedSize> sizes;
    for (const auto & column_name : column_names)
    {
        const auto * column = columns.tryGet(column_name);
        if (!column || column->default_desc.kind == ColumnDefaultKind::Alias || column->default_desc.kind == ColumnDefaultKind::Ephemeral)
            continue;
        if (std::ranges::find(sorting_key_columns, column_name) != sorting_key_columns.end())
            continue;
        if (!hasScalarValues(column->type))
            continue;

        bool uses_default_codec = !column->codec;
        if (column->codec && !hasOnlyGeneralPurposeCodecs(column->codec, uses_default_codec))
            continue;
        if (uses_default_codec && adaptive_default_codec)
            continue;
        sizes.emplace(column_name, CompressedSize{.uses_default_codec = uses_default_codec});
    }

    for (const auto & part : parts)
    {
        const auto & data_part = part.data_part;
        if (data_part->getType() != MergeTreeDataPartType::Wide)
            continue;

        /// The default codec of a part is the one of the table (`default_compression_codec`), of the `<compression>`
        /// configuration of the server, or of a `TTL ... RECOMPRESS`. When it is specialized or unknown in any part,
        /// the columns that it compresses get no estimate.
        if (!hasGeneralPurposeDefaultCodec(*data_part))
            std::erase_if(sizes, [](const auto & column) { return column.second.uses_default_codec; });

        if (sizes.empty())
            return {};

        const auto part_sizes = data_part->getColumnSizes();
        for (auto it = sizes.begin(); it != sizes.end();)
        {
            auto & [column_name, size] = *it;
            /// A part without the column (added by `ALTER` and not materialized) keeps no size for it.
            const auto size_it = part_sizes->find(column_name);
            if (size_it == part_sizes->end() || size_it->second.data_compressed == 0)
            {
                ++it;
                continue;
            }

            /// A part that stores the column by another codec than the declared one leaves it without an estimate.
            if (!isStoredWithGeneralPurposeCodecs(*data_part, column_name, *metadata))
            {
                it = sizes.erase(it);
                continue;
            }

            size.rows += data_part->rows_count;
            size.compressed_bytes += size_it->second.data_compressed;
            ++it;
        }
    }

    std::unordered_map<String, UInt64> result;
    for (const auto & [column_name, size] : sizes)
        if (auto distinct_values = estimateDistinctValuesFromCompressedSize(size.rows, size.compressed_bytes))
            result.emplace(column_name, *distinct_values);
    return result;
}

}
