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

#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>

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
/// A part of a single granule has no next mark to compare with, so it adds nothing to the estimate, and a table
/// whose parts all have a single granule gets no estimate.
///
/// The final mark holds the last row of the part rather than the start of a granule: it equals the start of the last
/// granule when that one has a single row, so it is not used.
///
/// The other columns of the key are not estimated: the marks are not a random sample of their rows, as they are sorted
/// within the runs of the preceding columns, and the marks can take the same position in every run (e.g. the first
/// value of a sequence repeated for each value of the first column).
///
/// To bound the work of planning on large tables, at most `max_sampled_marks` marks are taken, each with its next
/// mark: one from every `stride` consecutive marks of a part, at a position that varies from one interval to the next
/// (see `sampledMark`). Only the index that is already in memory is used.
/// The mark sampled from the `interval`-th `stride` consecutive marks. The position within the interval is a hash of
/// its number rather than a fixed one: with a fixed position, runs of a length that divides `stride` granules would
/// be sampled at the same place in every run, e.g. always inside a run, and no change would ever be seen. It is the
/// same in every planning of a query, so the estimate does not change between them.
size_t sampledMark(size_t interval, size_t stride)
{
    return interval * stride + intHash64(interval) % stride;
}

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
        for (size_t interval = 0; interval * stride + 1 < granules; ++interval)
        {
            /// The last interval can be shorter than `stride`: its pair is the last one of the part at most.
            const size_t i = std::min(sampledMark(interval, stride), granules - 2);
            ++pairs;
            changes += column.compareAt(i, i + 1, column, /*nan_direction_hint=*/ 1) != 0;
        }

        /// A part of a single granule has one mark, which tells nothing about the values of its rows: it is left out.
        if (pairs == 0)
            continue;

        if (changes == pairs)
            return std::nullopt;

        /// The value of the first mark and those starting in the granules.
        const size_t part_pairs = granules - 1;
        const Float64 changed_fraction = static_cast<Float64>(changes) / static_cast<Float64>(pairs);
        sum_part_distinct_values += 1 + static_cast<Float64>(part_pairs) * -std::log1p(-changed_fraction);
        part_indexes.push_back({index, granules});
    }

    /// Every part has a single granule, e.g. a small table: no estimate.
    if (part_indexes.empty())
        return std::nullopt;

    Float64 overlap = 1;
    if (part_indexes.size() > 1)
    {
        std::unordered_set<UInt128> all_hashes;
        UInt64 sum_part_mark_distinct_values = 0;
        for (const auto & [index, granules] : part_indexes)
        {
            const auto & column = *(*index)[0];
            std::unordered_set<UInt128> part_hashes;
            for (size_t interval = 0; interval * stride < granules; ++interval)
            {
                const size_t i = std::min(sampledMark(interval, stride), granules - 1);
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

UnorderedMapWithMemoryTracking<String, UInt64> estimateDistinctValuesFromMetadata(
    const RangesInDataParts & parts, const StorageMetadataPtr & metadata, const Names & column_names)
{
    const auto & columns = metadata->getColumns();

    if (parts.empty())
        return {};

    UnorderedMapWithMemoryTracking<String, UInt64> result;

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

    UnorderedMapWithMemoryTracking<String, CompressedSize> sizes;
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

    for (const auto & [column_name, size] : sizes)
        if (auto distinct_values = estimateDistinctValuesFromCompressedSize(size.rows, size.compressed_bytes))
            result.emplace(column_name, *distinct_values);
    return result;
}

}
