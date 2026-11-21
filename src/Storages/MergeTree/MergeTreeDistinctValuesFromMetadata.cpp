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

#include <Common/SipHash.h>
#include <Common/typeid_cast.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>

#include <Poco/String.h>

#include <algorithm>
#include <cmath>
#include <unordered_set>

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

/// The number of distinct values of the first column of the primary key from the values of the primary key index, the
/// first row of every granule, or `std::nullopt` without an estimate.
///
/// A part is sorted by the first column of the key, so its granules with a different value at the next mark tell
/// where the values change. If a fraction `p` of the granules has a change, and the lengths of the runs of equal
/// values are exponentially distributed, a granule has on average `-ln(1 - p)` values starting in it, so the part
/// has `granules * -ln(1 - p)` distinct values. This is the number of changes when the runs are long, and grows
/// without limit when every granule has a change: then the runs are shorter than granules and there is no estimate.
/// The parts mostly overlap in their values, so the sum over the parts is scaled by the ratio of the distinct values
/// of the marks of all parts to the sum of the distinct values of the marks of each part.
///
/// The final mark holds the last row of the part rather than the start of a granule: it equals the start of the last
/// granule when that one has a single row, so it is not used.
///
/// The other columns of the key are not estimated: the marks are not a random sample of their rows, as they are sorted
/// within the runs of the preceding columns, and the marks can take the same position in every run (e.g. the first
/// value of a sequence repeated for each value of the first column).
///
/// To bound the work of planning on large tables, at most `max_sampled_marks` marks are taken, evenly spaced in each
/// part, each with its next mark. Only the index that is already in memory is used.
std::optional<UInt64> estimateDistinctValuesOfFirstKeyColumnFromPrimaryIndex(const RangesInDataParts & parts)
{
    static constexpr size_t max_sampled_marks = 4096;

    UInt64 total_rows = 0;
    size_t total_granules = 0;
    for (const auto & part : parts)
    {
        const auto & data_part = part.data_part;
        /// The index is not loaded: it is not worth reading from disk for an estimate.
        if (!data_part->isIndexInMemory())
            return std::nullopt;
        total_rows += data_part->rows_count;
        total_granules += data_part->index_granularity->getMarksCountWithoutFinal();
    }
    if (total_rows == 0 || total_granules == 0)
        return std::nullopt;

    const size_t stride = std::max<size_t>(1, (total_granules + max_sampled_marks - 1) / max_sampled_marks);

    struct PartIndex
    {
        IMergeTreeDataPart::IndexPtr index;
        size_t granules = 0;
    };
    std::vector<PartIndex> part_indexes;
    part_indexes.reserve(parts.size());

    /// First the changes, which tell whether there is an estimate, without hashing the values.
    Float64 sum_part_distinct_values = 0;
    for (const auto & part : parts)
    {
        const auto & data_part = part.data_part;
        const size_t marks = data_part->index_granularity->getMarksCount();
        const size_t granules = data_part->index_granularity->getMarksCountWithoutFinal();
        const auto index = data_part->getIndex();
        /// The column is not kept in memory (see `primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns`).
        if (!index || index->empty() || (*index)[0]->size() != marks)
            return std::nullopt;
        if (granules == 0)
            continue;

        const auto & column = *(*index)[0];
        size_t pairs = 0;
        size_t changes = 0;
        for (size_t i = 0; i + 1 < granules; i += stride)
        {
            ++pairs;
            changes += column.compareAt(i, i + 1, column, /*nan_direction_hint=*/ 1) != 0;
        }

        if (pairs != 0 && changes == pairs)
            return std::nullopt;

        /// The value of the first mark and those starting in the granules.
        const size_t part_pairs = granules - 1;
        if (pairs != 0)
        {
            const Float64 changed_fraction = static_cast<Float64>(changes) / static_cast<Float64>(pairs);
            sum_part_distinct_values += 1 + static_cast<Float64>(part_pairs) * -std::log1p(-changed_fraction);
        }
        else
        {
            sum_part_distinct_values += 1;
        }
        part_indexes.push_back({index, granules});
    }

    Float64 overlap = 1;
    if (part_indexes.size() > 1)
    {
        std::unordered_set<UInt128> all_hashes;
        UInt64 sum_part_mark_distinct_values = 0;
        for (const auto & [index, granules] : part_indexes)
        {
            const auto & column = *(*index)[0];
            std::unordered_set<UInt128> part_hashes;
            for (size_t i = 0; i < granules; i += stride)
            {
                SipHash hash;
                column.updateHashWithValue(i, hash);
                const auto value_hash = hash.get128();
                part_hashes.insert(value_hash);
                all_hashes.insert(value_hash);
            }
            sum_part_mark_distinct_values += part_hashes.size();
        }
        overlap = static_cast<Float64>(all_hashes.size()) / static_cast<Float64>(sum_part_mark_distinct_values);
    }

    return std::clamp<UInt64>(static_cast<UInt64>(std::llround(sum_part_distinct_values * overlap)), 1, total_rows);
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

/// Reads only so many keys of a dictionary, which is enough for a dictionary written by default
/// (`low_cardinality_max_dictionary_size` is 8192), and bounds the reads of the estimation.
constexpr UInt64 max_dictionary_keys_to_read = 65536;

/// Reads only so many bytes (uncompressed) of the indexes of a `LowCardinality` column in a part, see
/// `hasAdditionalLowCardinalityKeys`. It is a read of the whole stream, so a part with more gives no number.
constexpr UInt64 max_indexes_bytes_to_read = 8 * 1024 * 1024;

/// Whether some rows of a `LowCardinality` column in a wide part carry keys that are not in the dictionaries of the
/// `DictionaryKeys` stream, or `std::nullopt` when it is not known.
///
/// Once the dictionary of the part has reached `low_cardinality_max_dictionary_size` keys, the keys that do not fit
/// are written beside the indexes of their rows, in the `DictionaryIndexes` stream. With
/// `low_cardinality_use_single_dictionary_for_part` the part still has one dictionary then, and without it as well if
/// this happens in the last rows that are written. The size of the dictionary the part was written with is not kept,
/// so a full dictionary cannot be told from its number of keys.
///
/// The stream is a sequence of blocks of rows: the type of the indexes with the flags, the additional keys if the
/// flag says so, the number of rows and the indexes. The granules do not tell where the blocks start (a granule can
/// be written in parts), so the blocks are read in order, skipping the indexes.
std::optional<bool> hasAdditionalLowCardinalityKeys(const IMergeTreeDataPart & part, const NameAndTypePair & column)
{
    /// See `IndexesSerializationType` in `SerializationLowCardinality.cpp`.
    static constexpr UInt64 has_additional_keys_bit = 1u << 9u;
    static constexpr UInt64 flags_mask = 0x700;
    static constexpr UInt64 max_index_type = 3;

    ISerialization::SubstreamPath path;
    path.push_back(ISerialization::Substream::DictionaryIndexes);
    const auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(column, path, ".bin", part.checksums, part.storage.getSettings());
    if (!stream_name)
        return std::nullopt;

    const auto checksum = part.checksums.files.find(*stream_name + ".bin");
    if (checksum == part.checksums.files.end() || !checksum->second.is_compressed
        || checksum->second.uncompressed_size > max_indexes_bytes_to_read)
        return std::nullopt;

    CompressedReadBufferFromFile in(part.getDataPartStorage().readFile(*stream_name + ".bin", part.storage.getContext()->getReadSettings(), std::nullopt));

    UInt64 rows = 0;
    while (!in.eof())
    {
        UInt64 index_type = 0;
        readBinaryLittleEndian(index_type, in);
        if (index_type & has_additional_keys_bit)
            return true;

        const UInt64 index_size_degree = index_type & ~flags_mask;
        if (index_size_degree > max_index_type)
            return std::nullopt;

        UInt64 block_rows = 0;
        readBinaryLittleEndian(block_rows, in);
        if (block_rows > part.rows_count - rows)
            return std::nullopt;
        in.ignore(block_rows << index_size_degree);
        rows += block_rows;
    }
    if (rows != part.rows_count)
        return std::nullopt;
    return false;
}

/// The number of keys of the dictionary of a `LowCardinality` column in a wide part, if it has all values of the part.
/// The `DictionaryKeys` stream of the column holds the version of the serialization and then the dictionaries: one at
/// the end of writing, or one every time the dictionary of the part reached `low_cardinality_max_dictionary_size`
/// keys. So the first dictionary has all values of the part when it is the last thing in the stream and no rows carry
/// additional keys (see `hasAdditionalLowCardinalityKeys`), otherwise the part has more values than it, and there is
/// no number.
std::optional<UInt64> readLowCardinalityDictionarySize(const IMergeTreeDataPart & part, const NameAndTypePair & column)
{
    if (part.getType() != MergeTreeDataPartType::Wide || !part.areChecksumsLoaded())
        return std::nullopt;

    /// The part keeps the column in the type it was written with, which is another one than the type of the table
    /// until an `ALTER ... MODIFY COLUMN` is applied to the part: its dictionary is serialized differently, and the
    /// conversion may merge its values.
    const auto column_in_part = part.getColumns().tryGetByName(column.name);
    if (!column_in_part || !column_in_part->type->equals(*column.type))
        return std::nullopt;

    const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(column.type.get());
    if (!low_cardinality_type)
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

    const auto has_additional_keys = hasAdditionalLowCardinalityKeys(part, column);
    if (!has_additional_keys || *has_additional_keys)
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

    if (parts.empty())
        return {};

    std::unordered_map<String, UInt64> result;

    /// The first column of the primary key is sorted in every part, so the marks of the primary key index tell its
    /// number of distinct values. Its compressed size does not (see below).
    if (metadata->hasPrimaryKey() && !metadata->getPrimaryKey().column_names.empty())
    {
        const String & first_key_column = metadata->getPrimaryKey().column_names.front();
        /// Only a column of the key itself, not an expression.
        if (std::ranges::find(column_names, first_key_column) != column_names.end() && columns.hasPhysical(first_key_column))
            if (auto distinct_values = estimateDistinctValuesOfFirstKeyColumnFromPrimaryIndex(parts))
                result.emplace(first_key_column, *distinct_values);
    }

    /// Adaptive codec selection compresses the columns with the default codec by a specialized codec where it is
    /// smaller, and the parts do not tell for which blocks it did. Only the current setting of the table is known,
    /// not the one each part was written with.
    const bool adaptive_default_codec = (*parts.front().data_part->storage.getSettings())[MergeTreeSetting::enable_adaptive_codec_selection];

    /// The dictionaries of `LowCardinality` columns have their distinct values.
    for (auto & [column_name, distinct_values] : estimateDistinctValuesFromLowCardinalityDictionaries(parts, metadata, column_names))
        result.try_emplace(column_name, distinct_values);

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
        if (result.contains(column_name))
            continue;
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
            return result;

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
