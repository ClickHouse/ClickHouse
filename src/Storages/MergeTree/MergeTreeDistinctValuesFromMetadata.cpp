#include <Storages/MergeTree/MergeTreeDistinctValuesFromMetadata.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/Statistics/DistinctValuesFromCompressedSize.h>

#include <Poco/String.h>

namespace DB
{

namespace
{

/// The compressed size tells the number of distinct values only for the values themselves: not for `Array`, `Map` or
/// `Tuple`, whose sizes add up their elements, and not for JSON or `Dynamic` with their own streams.
bool hasScalarValues(const DataTypePtr & type)
{
    const auto nested = removeNullable(removeLowCardinality(type));
    return nested->isValueRepresentedByNumber() || isStringOrFixedString(nested) || isUUID(nested) || isIPv6(nested);
}

/// Without a codec the column is compressed by the default codec of the table, which is a general purpose one.
/// `Delta`, `DoubleDelta`, `Gorilla`, `T64`, `GCD`, `FPC` and the like compress by the order of the values rather
/// than by their repetition, so the compressed size says nothing about the number of distinct values.
bool hasGeneralPurposeCodec(const ASTPtr & codec)
{
    if (!codec)
        return true;

    const auto * function = codec->as<ASTFunction>();
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
        if (name != "DEFAULT" && name != "NONE" && name != "LZ4" && name != "LZ4HC" && name != "ZSTD")
            return false;
    }
    return true;
}

}

std::unordered_map<String, UInt64> estimateDistinctValuesFromMetadata(
    const RangesInDataParts & parts, const StorageMetadataPtr & metadata, const Names & column_names)
{
    const auto & columns = metadata->getColumns();

    /// The rows and the compressed bytes of a column in the parts that keep the sizes of their columns.
    struct CompressedSize
    {
        UInt64 rows = 0;
        UInt64 compressed_bytes = 0;
    };
    std::unordered_map<String, CompressedSize> sizes;
    for (const auto & column_name : column_names)
    {
        const auto * column = columns.tryGet(column_name);
        if (!column || column->default_desc.kind == ColumnDefaultKind::Alias || column->default_desc.kind == ColumnDefaultKind::Ephemeral)
            continue;
        if (hasScalarValues(column->type) && hasGeneralPurposeCodec(column->codec))
            sizes.emplace(column_name, CompressedSize{});
    }

    if (sizes.empty())
        return {};

    for (const auto & part : parts)
    {
        const auto & data_part = part.data_part;
        if (data_part->getType() != MergeTreeDataPartType::Wide)
            continue;

        const auto part_sizes = data_part->getColumnSizes();
        for (auto & [column_name, size] : sizes)
        {
            /// A part without the column (added by `ALTER` and not materialized) keeps no size for it.
            const auto it = part_sizes->find(column_name);
            if (it == part_sizes->end() || it->second.data_compressed == 0)
                continue;

            size.rows += data_part->rows_count;
            size.compressed_bytes += it->second.data_compressed;
        }
    }

    std::unordered_map<String, UInt64> result;
    for (const auto & [column_name, size] : sizes)
        if (auto distinct_values = estimateDistinctValuesFromCompressedSize(size.rows, size.compressed_bytes))
            result.emplace(column_name, *distinct_values);
    return result;
}

}
