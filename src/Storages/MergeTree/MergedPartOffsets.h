#pragma once

#include <algorithm>
#include <optional>

#include <base/types.h>
#include <Common/Allocator.h>
#include <Common/Arena.h>
#include <Common/BitHelpers.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

/// Stores _part_offset mapping in a memory-efficient format.
/// When parts merge, source rows may scatter into the destination,
/// but their relative ordering is preserved:
///
/// Part A:              part B:             After merge (alternating):
/// row | _part_offset   row | _part_offset  row from | original_offset | new_offset
/// A0  | 0              B0  | 0                A0    | 0               | 0
/// A1  | 1              B1  | 1                B0    | 0               | 1
/// A2  | 2              B2  | 2                A1    | 1               | 2
///                                             B1    | 1               | 3
///                                             A2    | 2               | 4
///                                             B2    | 2               | 5
///
/// Part A mapping: 0→0, 1→2, 2→4 (still monotonically increasing)
/// Part B mapping: 0→1, 1→3, 2→5 (still monotonically increasing)
///
/// Uses fixed-size pages for efficient random access.
class PackedPartOffsets
{
private:
    /// Represents a compressed page of monotonically increasing _part_offset values.
    /// Uses Frame-of-Reference encoding: stores the minimum value and differences from it.
    /// Each difference is stored using a fixed number of bits determined by the range of values.
    struct Page
    {
    public:
        /// Page of consecutive values: min_val and num_vals describe it fully, nothing is packed
        Page(UInt64 min_val_, size_t num_vals_)
            : num_vals(num_vals_)
            , min_val(min_val_)
            , bits_per_val(0)
            , compressed_data(nullptr)
        {
        }

        /// @param vals Vector of monotonic increasing _part_offset values
        /// @param bits_per_val_ Number of bits used to represent each value
        /// @param compressed_data_ Pre-allocated memory to store compressed values into 64-bit words
        Page(const PODArray<UInt64> & vals, size_t bits_per_val_, UInt64 * compressed_data_)
            : num_vals(vals.size())
            , min_val(vals.front())
            , bits_per_val(bits_per_val_)
            , compressed_data(compressed_data_)
        {
            size_t pos = 0;
            size_t offset = 0;

            // Skip first value (minimum value) and compress subsequent values
            for (size_t i = 1; i < num_vals; ++i)
            {
                auto val = vals[i] - min_val;

                // Pack value into compressed storage
                if (offset == 0)
                {
                    compressed_data[pos] = val;
                }
                else
                {
                    compressed_data[pos] |= val << offset;

                    // Handle overflow to next 64-bit word
                    if (offset + bits_per_val > 64)
                        compressed_data[pos + 1] = val >> (64 - offset);
                }

                // Update position and offset
                offset += bits_per_val;
                if (offset >= 64)
                {
                    ++pos;
                    offset -= 64;
                }
            }
        }

        /// Retrieves a value from the compressed page
        UInt64 operator[](size_t i) const
        {
            chassert(i < num_vals);

            // Nothing is packed for a run of consecutive values, and min_val is also the first value of a packed page
            if (bits_per_val == 0 || i == 0)
                return min_val + i;

            // Calculate bit position and decode compressed value
            size_t bits = (i - 1) * bits_per_val;
            size_t pos = bits / 64;
            size_t offset = bits % 64;

            UInt64 value = compressed_data[pos] >> offset;

            // Handle value spanning multiple 64-bit words
            if (offset + bits_per_val > 64)
                value |= compressed_data[pos + 1] << (64 - offset);

            return min_val + (value & maskLowBits<UInt64>(static_cast<unsigned char>(bits_per_val)));
        }

        size_t num_vals;
        UInt64 min_val;
        size_t bits_per_val;
        UInt64 * compressed_data;
    };

    static constexpr UInt8 PACKED_PAGE_SIZE_DEGREE = 10;
    static constexpr size_t PACKED_PAGE_SIZE = 1 << PACKED_PAGE_SIZE_DEGREE;
    static constexpr size_t PACKED_PAGE_MASK = PACKED_PAGE_SIZE - 1;

    PODArray<Page> pages;
    PODArray<UInt64> current_page_values;
    Arena arena;
    size_t num_values = 0;
    bool allow_repeated_values = false;

public:
    /// Allows a value equal to the previous one, which the mapping with dropped rows inserts (see MergedPartOffsets).
    void allowRepeatedValues() { allow_repeated_values = true; }

    /// @param val The value to insert (must be greater than all previously inserted values, or equal if repeated values are allowed)
    void insert(UInt64 val)
    {
        if (current_page_values.size() >= PACKED_PAGE_SIZE)
            flush();

        chassert(current_page_values.empty() || current_page_values.back() < val || (allow_repeated_values && current_page_values.back() == val));
        current_page_values.push_back(val);
        ++num_values;
    }

    /// Number of inserted values.
    size_t size() const { return num_values; }

    /// Compresses and finalizes the current page of values.
    /// Called automatically when a page is full or at the end to finalize the structure.
    void flush()
    {
        if (current_page_values.empty())
            return;

        UInt64 range = current_page_values.back() - current_page_values.front();

        /// A merge that does not interleave this part's rows - parts covering disjoint ranges of the sorting
        /// key, or a table without one - inserts consecutive runs, which span exactly size() - 1.
        /// With repeated values such a span is a consecutive run only if no value repeats.
        if (range == current_page_values.size() - 1
            && (!allow_repeated_values
                || std::adjacent_find(current_page_values.begin(), current_page_values.end()) == current_page_values.end()))
        {
            pages.emplace_back(current_page_values.front(), current_page_values.size());
            current_page_values.clear();
            return;
        }

        /// A page that repeats a single value is packed with one (zero) bit per value,
        /// because a page with no bits per value is a run of consecutive values.
        size_t bits_per_val = range == 0 ? 1 : 64 - getLeadingZeroBits(range);
        chassert(bits_per_val >= 1);
        size_t num_uint64 = (((current_page_values.size() - 1) * bits_per_val) + 63) / 64;
        chassert(num_uint64 >= 1);

        pages.emplace_back(
            current_page_values,
            bits_per_val,
            reinterpret_cast<UInt64 *>(arena.alignedAlloc(num_uint64 * sizeof(UInt64), alignof(UInt64))));
        current_page_values.clear();
    }

    /// Decompresses the _part_offset value at the specified index
    UInt64 operator[](size_t i) const
    {
        size_t page_pos = i >> PACKED_PAGE_SIZE_DEGREE;
        chassert(page_pos < pages.size());
        size_t page_idx = i & PACKED_PAGE_MASK;
        return pages[page_pos][page_idx];
    }

    void clearTemporaryStorage() { current_page_values = {}; }

    size_t totalAllocatedMemory() const { return pages.allocated_bytes() + current_page_values.allocated_bytes() + arena.allocatedBytes(); }
};


/// Manages _part_offset mapping during data part merges.
/// Tracks how rows from original parts are positioned in the merged result.
/// Provides efficient lookup from original _part_offset to new _part_offset in merged data.
///
/// In EnabledWithDrops mode the mapping additionally records source rows dropped by the merge
/// (e.g. by ReplacingMergeTree, lightweight deletes or TTL). Each source row of a part has one
/// entry indexed by its original _part_offset:
///   surviving row with new offset n -> (n << 1) | 1
///   dropped row                     -> (m + 1) << 1, where m is the new offset of the part's
///                                      previous surviving row (or -1 if there is none yet)
/// Values stay non-decreasing per part, which keeps the frame-of-reference pages compact.
class MergedPartOffsets
{
public:
    enum class MappingMode
    {
        Enabled,          /// Full offset mapping is required, every source row survives the merge
        EnabledWithDrops, /// Full offset mapping is required, the merge may drop source rows
        Disabled          /// No mapping needed (e.g., no sorting key)
    };

    explicit MergedPartOffsets(size_t num_parts, MappingMode mode_ = MappingMode::Enabled)
        : mode(mode_)
        , offset_maps(mode == MappingMode::Enabled ? num_parts : 0)
        , finalized(mode == MappingMode::Disabled)
    {
        chassert(mode != MappingMode::EnabledWithDrops);
    }

    /// EnabledWithDrops mode: per-part numbers of source rows are required
    /// to record the rows dropped after each part's last surviving row.
    explicit MergedPartOffsets(std::vector<UInt64> part_rows_)
        : mode(MappingMode::EnabledWithDrops)
        , offset_maps(part_rows_.size())
        , part_rows(std::move(part_rows_))
        , next_part_offsets(part_rows.size())
        , dropped_values(part_rows.size())
        , surviving_rows(part_rows.size())
        , finalized(false)
    {
        /// Consecutive dropped rows of a part have equal values.
        for (auto & map : offset_maps)
            map.allowRepeatedValues();
    }

    /// Records _part_offset mappings for a batch of _part_index values.
    void insert(const UInt64 * begin_part_index, const UInt64 * end_part_index)
    {
        chassert(mode == MappingMode::Enabled);
        for (const UInt64 * it = begin_part_index; it != end_part_index; ++it)
        {
            offset_maps[*it].insert(num_rows);
            ++num_rows;
        }
    }

    /// Records mappings for a batch of (_part_index, _part_offset) pairs of surviving rows.
    /// Source rows missing from the per-part offset sequences are recorded as dropped.
    void insert(const UInt64 * begin_part_index, const UInt64 * end_part_index, const UInt64 * begin_part_offset)
    {
        chassert(mode == MappingMode::EnabledWithDrops);

        const UInt64 * offset_it = begin_part_offset;
        for (const UInt64 * it = begin_part_index; it != end_part_index; ++it, ++offset_it)
        {
            UInt64 part_index = *it;
            UInt64 part_offset = *offset_it;
            chassert(part_index < offset_maps.size());

            if (part_offset >= part_rows[part_index])
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Got row with offset {} for source part {} that has only {} rows",
                    part_offset, part_index, part_rows[part_index]);

            UInt64 & next_offset = next_part_offsets[part_index];

            /// Surviving rows of one part must keep their relative order in the merged part.
            /// A violation means the data is corrupted, e.g. the sign column
            /// of CollapsingMergeTree has values other than 1 and -1.
            if (part_offset < next_offset)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Rows of source part {} are merged out of order: got row with offset {} after {} rows of the part have been consumed. "
                    "It may be caused by corrupted data, e.g. incorrect values of the sign column in CollapsingMergeTree",
                    part_index, part_offset, next_offset);

            auto & offset_map = offset_maps[part_index];
            UInt64 & dropped_value = dropped_values[part_index];

            for (; next_offset < part_offset; ++next_offset)
            {
                offset_map.insert(dropped_value);
                ++num_dropped;
            }

            offset_map.insert((num_rows << 1) | 1);
            dropped_value = (num_rows + 1) << 1;
            ++next_offset;
            ++surviving_rows[part_index];
            ++num_rows;
        }
    }

    /// Looks up the new _part_offset in the merged data.
    UInt64 operator[](UInt64 part_index, UInt64 part_offset) const
    {
        chassert(mode == MappingMode::Enabled);
        chassert(part_index < offset_maps.size());
        return offset_maps[part_index][part_offset];
    }

    /// Looks up the new _part_offset in the merged data.
    /// Returns std::nullopt if the source row was dropped by the merge.
    std::optional<UInt64> tryGetNewOffset(UInt64 part_index, UInt64 part_offset) const
    {
        chassert(mode != MappingMode::Disabled);
        chassert(part_index < offset_maps.size());

        if (mode == MappingMode::Enabled)
            return offset_maps[part_index][part_offset];

        UInt64 value = offset_maps[part_index][part_offset];
        if (!(value & 1))
            return std::nullopt;
        return value >> 1;
    }

    /// Whether every source row of the part in [first, last] was dropped by the merge.
    /// Values of a part do not decrease and each surviving row has its own odd value,
    /// so the rows are all dropped if and only if both ends hold the same even value.
    bool areRowsDropped(UInt64 part_index, UInt64 first, UInt64 last) const
    {
        chassert(mode == MappingMode::EnabledWithDrops);
        chassert(part_index < offset_maps.size());
        chassert(first <= last && last < offset_maps[part_index].size());

        UInt64 value = offset_maps[part_index][first];
        return !(value & 1) && offset_maps[part_index][last] == value;
    }

    /// Number of rows of the part, which is the number of its mapped offsets.
    /// In EnabledWithDrops mode it includes the dropped rows once the mapping is flushed.
    size_t getPartRowsCount(UInt64 part_index) const
    {
        chassert(mode != MappingMode::Disabled);
        chassert(part_index < offset_maps.size());
        return offset_maps[part_index].size();
    }

    /// Number of source rows of the part that survive the merge.
    UInt64 getPartSurvivingRowsCount(UInt64 part_index) const
    {
        chassert(mode == MappingMode::EnabledWithDrops);
        chassert(part_index < surviving_rows.size());
        return surviving_rows[part_index];
    }

    /// Finalizes all _part_offset maps and releases temporary buffers.
    /// Must be called after all offsets have been inserted.
    void flush()
    {
        if (mode == MappingMode::Disabled)
            return;

        chassert(!finalized);
        finalized = true;

        if (mode == MappingMode::EnabledWithDrops)
        {
            /// Record the rows dropped after the last surviving row of each part.
            for (size_t part_index = 0; part_index < offset_maps.size(); ++part_index)
            {
                for (UInt64 & next_offset = next_part_offsets[part_index]; next_offset < part_rows[part_index]; ++next_offset)
                {
                    offset_maps[part_index].insert(dropped_values[part_index]);
                    ++num_dropped;
                }
            }
        }

        if (num_rows == 0 && num_dropped == 0)
            return;

        size_t total_allocated_memory = 0;
        for (auto & map : offset_maps)
        {
            map.flush();
            map.clearTemporaryStorage();
            total_allocated_memory += map.totalAllocatedMemory();
        }

        LOG_DEBUG(
            logger,
            "Holding {} merged _part_offset ({} dropped) in memory with {} total allocated memory",
            num_rows,
            num_dropped,
            formatReadableSizeWithBinarySuffix(total_allocated_memory));
    }

    bool isFinalized() const { return finalized; }
    bool isMappingEnabled() const { return mode != MappingMode::Disabled; }
    bool isMappingWithDrops() const { return mode == MappingMode::EnabledWithDrops; }

    bool hasDroppedRows() const
    {
        chassert(finalized);
        return num_dropped > 0;
    }

    bool empty() const { return num_rows == 0; }
    size_t size() const { return num_rows; }

    void clear()
    {
        offset_maps.clear();
        part_rows.clear();
        next_part_offsets.clear();
        dropped_values.clear();
        surviving_rows.clear();
        num_rows = 0;
    }

private:
    MappingMode mode;
    std::vector<PackedPartOffsets> offset_maps;

    /// Used only in EnabledWithDrops mode.
    std::vector<UInt64> part_rows;          /// Number of source rows per part
    std::vector<UInt64> next_part_offsets;  /// Next expected source offset per part
    std::vector<UInt64> dropped_values;     /// Encoded value for dropped rows per part
    std::vector<UInt64> surviving_rows;     /// Number of surviving source rows per part

    bool finalized;

    size_t num_rows = 0;
    size_t num_dropped = 0;
    LoggerPtr logger = getLogger("MergedPartOffsets");
};

}
