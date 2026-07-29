#pragma once
/// VectorSearchFilter uses std::min/std::max while intersecting dense bitmaps with coarse mark ranges.
#include <algorithm>
#include <Core/Types.h>
/// VectorSearchParameters stores the user-selected hybrid filtering strategy.
#include <Core/SettingsEnums.h>
#include <Common/VectorWithMemoryTracking.h>

/// Dense bitmap plumbing needs optional counters, pair ranges, and word storage.
#include <optional>
#include <utility>
#include <vector>

namespace DB
{

/// A vehicle to transport elements of the SELECT query into the vector similarity index.
struct VectorSearchParameters
{
    /// Elements of the SELECT query
    String column;
    String distance_function;
    size_t limit;
    VectorWithMemoryTracking<Float64> reference_vector;

    /// Other metadata
    bool additional_filters_present; /// SELECT contains a WHERE or PREWHERE clause
    bool return_distances;
    /// The QueryPlan optimization records the requested strategy here; the concrete decision is made
    /// later per part when MergeTree index analysis knows which scalar filters are available.
    VectorSearchFilterStrategy filter_strategy = VectorSearchFilterStrategy::AUTO;
};

using OptionalVectorSearchParameters = std::optional<VectorSearchParameters>;

/// Row-level predicate passed from MergeTree index analysis into the vector similarity index.
///
/// The vector similarity index stores USearch keys as row offsets local to the vector index granule.
/// The filter is intentionally a physical row predicate, not a SQL expression evaluator: ANN search
/// runs during index analysis, before ordinary columns are read and before the final FilterStep.
///
/// Only exact dense row bitmaps are supported as vector-search filters. Coarse MarkRanges may still
/// be used to intersect such a bitmap before search, but they are not passed to USearch as a
/// standalone filter.
struct VectorSearchFilter
{
    using RowRange = std::pair<UInt64, UInt64>; /// [begin, end), local to the vector index granule.

    /// Packed row bits; bit 1 means the row satisfies WHERE and may enter vector-search candidates.
    std::vector<UInt64> bitmap_words;
    /// Number of valid row positions represented by the bitmap. Tail bits beyond this are ignored.
    UInt64 bitmap_total_rows = 0;
    /// Number of allowed rows represented by set bits within [0, bitmap_total_rows).
    UInt64 bitmap_set_bits = 0;

    void setDenseBitmap(std::vector<UInt64> words, UInt64 total_rows, std::optional<UInt64> set_bits = std::nullopt)
    {
        /// Store an exact row-level predicate for one vector index granule.
        bitmap_words = std::move(words);
        bitmap_total_rows = total_rows;
        bitmap_set_bits = set_bits.value_or(countSetBits(bitmap_words, bitmap_total_rows));
    }

    void intersectAllowedRanges(const std::vector<RowRange> & ranges)
    {
        if (noAllowedRows())
            return;

        /// Convert the surviving mark-derived ranges to a temporary bitmap so exact row predicates
        /// can be intersected with coarse mark pruning using word operations.
        std::vector<UInt64> range_words((bitmap_total_rows + BITS_PER_WORD - 1) / BITS_PER_WORD, 0);
        for (const auto & range : ranges)
        {
            const UInt64 begin = std::min(range.first, bitmap_total_rows);
            const UInt64 end = std::min(range.second, bitmap_total_rows);
            for (UInt64 row = begin; row < end; ++row)
                range_words[row / BITS_PER_WORD] |= 1ULL << (row % BITS_PER_WORD);
        }

        /// The current implementation only supports positive WHERE bitmaps, so range intersection is
        /// a straight AND between the exact row bitmap and the coarse mark-derived bitmap.
        for (size_t i = 0; i < bitmap_words.size(); ++i)
            bitmap_words[i] &= i < range_words.size() ? range_words[i] : 0;

        bitmap_set_bits = countSetBits(bitmap_words, bitmap_total_rows);
    }

    bool noAllowedRows() const
    {
        /// In a positive WHERE bitmap, zero set bits means no row can enter vector-search candidates.
        return bitmap_set_bits == 0;
    }

    bool allRowsAllowed() const
    {
        /// In a positive WHERE bitmap, all rows are accepted only when every valid row bit is set.
        return bitmap_total_rows > 0 && bitmap_set_bits >= bitmap_total_rows;
    }

    bool needsFiltering() const
    {
        /// filtered_search() is useful only for a proper subset of rows. Empty filters are handled by
        /// returning no ANN results, and all-allowed filters should use regular search().
        return !noAllowedRows() && !allRowsAllowed();
    }

    /// Backward-compatible name for callers that used an empty range filter to mean "no row can be
    /// accepted". Do not use this to decide between filtered_search() and search(); use
    /// needsFiltering() instead so dense all-allowed bitmaps do not force a filtered ANN path.
    bool empty() const { return noAllowedRows(); }

    std::optional<UInt64> totalRows() const
    {
        /// Dense bitmaps are row-exact and know the vector-granule row count.
        return bitmap_total_rows;
    }

    std::optional<UInt64> allowedRows() const
    {
        /// Report the exact number of rows that may enter vector-search candidates. The exact
        /// row_bitmap fallback uses this to decide whether the filtered row domain is small enough
        /// to scan inside the vector index.
        /// Return the exact positive bitmap cardinality. Complement bitmaps are intentionally not
        /// modeled until the scalar side can actually build and validate them.
        return bitmap_set_bits;
    }

    bool contains(UInt64 local_row) const
    {
        if (local_row >= bitmap_total_rows)
            return false;

        const UInt64 word_index = local_row / BITS_PER_WORD;
        const UInt64 bit_index = local_row % BITS_PER_WORD;
        const bool bit_is_set = word_index < bitmap_words.size() && ((bitmap_words[word_index] >> bit_index) & 1ULL);
        /// Only positive WHERE bitmaps are supported: set bits are eligible rows.
        return bit_is_set;
    }

private:
    /// Bitmap word width used consistently by row_bitmap construction, remapping, and lookup.
    static constexpr UInt64 BITS_PER_WORD = 64;

    static UInt64 countSetBits(const std::vector<UInt64> & words, UInt64 total_rows)
    {
        /// Count only valid row bits. The final word may contain unused tail bits that must not affect
        /// allRowsAllowed() or noAllowedRows() decisions.
        UInt64 count = 0;
        UInt64 remaining_rows = total_rows;

        for (UInt64 word : words)
        {
            if (remaining_rows == 0)
                break;

            if (remaining_rows < BITS_PER_WORD)
            {
                const UInt64 mask = (1ULL << remaining_rows) - 1;
                word &= mask;
            }

            count += static_cast<UInt64>(__builtin_popcountll(word));
            remaining_rows = remaining_rows > BITS_PER_WORD ? remaining_rows - BITS_PER_WORD : 0;
        }

        return count;
    }
};

struct NearestNeighbours
{
    std::vector<UInt64> rows;
    std::optional<std::vector<float>> distances;
};

}
