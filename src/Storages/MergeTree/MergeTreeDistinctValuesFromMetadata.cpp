#include <Storages/MergeTree/MergeTreeDistinctValuesFromMetadata.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/Statistics/DistinctValuesFromCompressedSize.h>

#include <Common/SipHash.h>
#include <Common/typeid_cast.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Poco/String.h>

#include <cmath>
#include <unordered_set>

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

/// The numbers of distinct values of the columns of the primary key in `column_names` from the values of the primary key
/// index, the first row of every granule (and the last row of a part with the final mark).
///
/// A part is sorted by the first column of the key, so its granules with a different value at the next mark tell
/// where the values change. If a fraction `p` of the granules has a change, and the lengths of the runs of equal
/// values are exponentially distributed, a granule has on average `-ln(1 - p)` values starting in it, so the part
/// has `granules * -ln(1 - p)` distinct values. This is the number of changes when the runs are long, and grows
/// without limit when every granule has a change: then the runs are shorter than granules and there is no estimate.
/// The parts mostly overlap in their values, so the sum over the parts is scaled by the ratio of the distinct values
/// of the marks of all parts to the sum of the distinct values of the marks of each part.
///
/// The other columns of the key are not sorted, and the marks are taken as a sample of the rows: the number of
/// distinct values is estimated by the GEE estimator (Charikar et al., 2000), `sqrt(rows / sample) * f1 + f2 + ...`,
/// where `fj` is the number of values that occur `j` times in the sample.
std::unordered_map<String, UInt64> estimateDistinctValuesFromPrimaryIndex(
    const RangesInDataParts & parts, const StorageMetadataPtr & metadata, const NameSet & column_names)
{
    if (!metadata->hasPrimaryKey())
        return {};

    const auto & key_columns = metadata->getPrimaryKey().column_names;
    const auto & columns = metadata->getColumns();

    std::unordered_map<String, UInt64> result;
    for (size_t key_position = 0; key_position < key_columns.size(); ++key_position)
    {
        const auto & column_name = key_columns[key_position];
        /// Only a column of the key itself, not an expression.
        if (!column_names.contains(column_name) || !columns.hasPhysical(column_name))
            continue;

        UInt64 total_rows = 0;
        UInt64 total_marks = 0;
        /// Hashes of the values of the marks of all parts, with the number of their occurrences.
        std::unordered_map<UInt128, UInt64> occurrences;

        /// For the first column.
        Float64 sum_part_distinct_values = 0;
        UInt64 sum_part_mark_distinct_values = 0;
        bool runs_shorter_than_granules = false;

        bool complete = true;
        for (const auto & part : parts)
        {
            const auto & data_part = part.data_part;
            const auto index = data_part->getIndex();
            const size_t marks = data_part->index_granularity->getMarksCount();
            /// The column is not kept in memory (see `primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns`).
            if (!index || index->size() <= key_position || (*index)[key_position]->size() != marks)
            {
                complete = false;
                break;
            }
            if (marks == 0)
                continue;

            const auto & column = *(*index)[key_position];
            total_rows += data_part->rows_count;
            total_marks += marks;

            std::unordered_set<UInt128> part_hashes;
            for (size_t i = 0; i < marks; ++i)
            {
                SipHash hash;
                column.updateHashWithValue(i, hash);
                const auto value_hash = hash.get128();
                ++occurrences[value_hash];
                if (key_position == 0)
                    part_hashes.insert(value_hash);
            }

            if (key_position != 0)
                continue;

            /// The pairs of adjacent marks; without the final mark the last granule has no next mark.
            const size_t pairs = marks - 1;
            size_t changes = 0;
            for (size_t i = 0; i < pairs; ++i)
                changes += column.compareAt(i, i + 1, column, /*nan_direction_hint=*/ 1) != 0;

            sum_part_mark_distinct_values += part_hashes.size();
            if (pairs == 0)
            {
                sum_part_distinct_values += 1;
            }
            else if (changes == pairs)
            {
                runs_shorter_than_granules = true;
            }
            else
            {
                const Float64 changed_fraction = static_cast<Float64>(changes) / static_cast<Float64>(pairs);
                /// The values of the first mark and those starting in the granules.
                sum_part_distinct_values += 1 + static_cast<Float64>(pairs) * -std::log1p(-changed_fraction);
            }
        }

        if (!complete || total_rows == 0 || occurrences.empty())
            continue;

        Float64 distinct_values = 0;
        if (key_position == 0)
        {
            if (runs_shorter_than_granules)
                continue;
            const Float64 overlap = static_cast<Float64>(occurrences.size()) / static_cast<Float64>(sum_part_mark_distinct_values);
            distinct_values = sum_part_distinct_values * overlap;
        }
        else
        {
            UInt64 singletons = 0;
            for (const auto & [_, count] : occurrences)
                singletons += count == 1;
            distinct_values = std::sqrt(static_cast<Float64>(total_rows) / static_cast<Float64>(total_marks)) * static_cast<Float64>(singletons)
                + static_cast<Float64>(occurrences.size() - singletons);
        }

        result.emplace(column_name, std::clamp<UInt64>(static_cast<UInt64>(std::llround(distinct_values)), 1, total_rows));
    }
    return result;
}

/// Reads only so many keys of a dictionary, which is enough for a dictionary written by default
/// (`low_cardinality_max_dictionary_size` is 8192), and bounds the reads of the estimation.
constexpr UInt64 max_dictionary_keys_to_read = 65536;

/// The number of keys of the dictionary of a `LowCardinality` column in a wide part, if it has a single dictionary.
/// The `DictionaryKeys` stream of the column holds the version of the serialization and then the dictionaries: one at
/// the end of writing, or one every time the dictionary of the part reached `low_cardinality_max_dictionary_size`
/// keys, after which the rows that do not fit carry their own keys. So the first dictionary has all values of the
/// part when it is the last thing in the stream, otherwise the part has more values than it, and there is no number.
std::optional<UInt64> readLowCardinalityDictionarySize(const IMergeTreeDataPart & part, const NameAndTypePair & column)
{
    const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(column.type.get());
    if (!low_cardinality_type || part.getType() != MergeTreeDataPartType::Wide || !part.areChecksumsLoaded())
        return std::nullopt;

    ISerialization::SubstreamPath path;
    path.push_back(ISerialization::Substream::DictionaryKeys);
    const auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(column, path, ".bin", part.checksums, part.storage.getSettings());
    if (!stream_name)
        return std::nullopt;

    CompressedReadBufferFromFile in(part.getDataPartStorage().readFile(*stream_name + ".bin", part.storage.getContext()->getReadSettings(), std::nullopt));

    UInt64 keys_version = 0;
    readBinaryLittleEndian(keys_version, in);
    if (in.eof())
        return std::nullopt;

    UInt64 num_keys = 0;
    readBinaryLittleEndian(num_keys, in);
    if (num_keys > max_dictionary_keys_to_read)
        return std::nullopt;

    const auto keys_type = removeNullable(low_cardinality_type->getDictionaryType());
    auto keys = keys_type->createColumn();
    keys_type->getDefaultSerialization()->deserializeBinaryBulk(*keys, in, num_keys, 0);
    if (keys->size() != num_keys || !in.eof())
        return std::nullopt;

    /// The dictionary of a `Nullable` column has a placeholder for NULL, which is not a distinct value.
    if (low_cardinality_type->getDictionaryType()->isNullable() && num_keys > 0)
        --num_keys;
    return std::max<UInt64>(num_keys, 1);
}

/// The numbers of distinct values of the `LowCardinality` columns in `column_names` from the dictionaries of the
/// largest wide parts. The parts share most of their values, so the number of a column is the largest one of the
/// parts; a part with more values than its first dictionary gives no number for the column.
std::unordered_map<String, UInt64> estimateDistinctValuesFromLowCardinalityDictionaries(
    const RangesInDataParts & parts, const StorageMetadataPtr & metadata, const Names & column_names)
{
    /// Each part costs a read, so only a few of the largest parts are read.
    constexpr size_t max_parts_to_read = 8;

    std::vector<NameAndTypePair> low_cardinality_columns;
    for (const auto & column_name : column_names)
        if (auto column = metadata->getColumns().tryGetPhysical(column_name); column && column->type->lowCardinality())
            low_cardinality_columns.push_back(*column);
    if (low_cardinality_columns.empty())
        return {};

    std::vector<const IMergeTreeDataPart *> largest_parts;
    for (const auto & part : parts)
        if (part.data_part->getType() == MergeTreeDataPartType::Wide && part.data_part->rows_count > 0)
            largest_parts.push_back(part.data_part.get());
    const size_t num_parts_to_read = std::min(largest_parts.size(), max_parts_to_read);
    std::partial_sort(largest_parts.begin(), largest_parts.begin() + num_parts_to_read, largest_parts.end(),
        [](const auto * lhs, const auto * rhs) { return lhs->rows_count > rhs->rows_count; });
    largest_parts.resize(num_parts_to_read);

    std::unordered_map<String, UInt64> result;
    for (const auto & column : low_cardinality_columns)
    {
        UInt64 distinct_values = 0;
        bool complete = !largest_parts.empty();
        for (const auto * part : largest_parts)
        {
            /// A part without the column (added by `ALTER`) has only defaults in it.
            if (!part->getColumns().contains(column.name))
                continue;
            const auto part_distinct_values = readLowCardinalityDictionarySize(*part, column);
            if (!part_distinct_values)
            {
                complete = false;
                break;
            }
            distinct_values = std::max(distinct_values, *part_distinct_values);
        }
        if (complete && distinct_values > 0)
            result.emplace(column.name, distinct_values);
    }
    return result;
}

}

std::unordered_map<String, UInt64> estimateDistinctValuesFromMetadata(
    const RangesInDataParts & parts, const StorageMetadataPtr & metadata, const Names & column_names)
{
    const auto & columns = metadata->getColumns();

    /// The columns of the primary key are sorted (the first one) or sampled by the marks of the primary key index,
    /// which tells their numbers of distinct values better than the compressed size: a sorted column compresses by
    /// the lengths of its runs of equal values rather than by its number of distinct values.
    auto result = estimateDistinctValuesFromPrimaryIndex(parts, metadata, NameSet(column_names.begin(), column_names.end()));

    /// The dictionaries of `LowCardinality` columns have their distinct values.
    for (auto & [column_name, distinct_values] : estimateDistinctValuesFromLowCardinalityDictionaries(parts, metadata, column_names))
        result.try_emplace(column_name, distinct_values);

    /// The rows and the compressed bytes of a column in the parts that keep the sizes of their columns.
    struct CompressedSize
    {
        UInt64 rows = 0;
        UInt64 compressed_bytes = 0;
    };
    std::unordered_map<String, CompressedSize> sizes;
    for (const auto & column_name : column_names)
    {
        if (result.contains(column_name))
            continue;
        const auto * column = columns.tryGet(column_name);
        if (!column || column->default_desc.kind == ColumnDefaultKind::Alias || column->default_desc.kind == ColumnDefaultKind::Ephemeral)
            continue;
        if (hasScalarValues(column->type) && hasGeneralPurposeCodec(column->codec))
            sizes.emplace(column_name, CompressedSize{});
    }

    if (sizes.empty())
        return result;

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

    for (const auto & [column_name, size] : sizes)
        if (auto distinct_values = estimateDistinctValuesFromCompressedSize(size.rows, size.compressed_bytes))
            result.emplace(column_name, *distinct_values);
    return result;
}

}
