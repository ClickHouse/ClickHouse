#pragma once

#include <base/types.h>
#include <Common/Allocator.h>
#include <Common/Arena.h>
#include <Common/BitHelpers.h>
#include <Common/PODArray.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

namespace DB
{

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
        /// Special page that holds only one value
        explicit Page(UInt64 val)
            : num_vals(1)
            , min_val(val)
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

            // First value is always the minimum value
            if (i == 0)
                return min_val;

            // Calculate bit position and decode compressed value
            size_t bits = (i - 1) * bits_per_val;
            size_t pos = bits / 64;
            size_t offset = bits % 64;

            UInt64 value = compressed_data[pos] >> offset;

            // Handle value spanning multiple 64-bit words
            if (offset + bits_per_val > 64)
                value |= compressed_data[pos + 1] << (64 - offset);

            return min_val + (value & maskLowBits<UInt64>(bits_per_val));
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

public:
    /// @param val The _part_offset value to insert (must be greater than all previously inserted values)
    void insert(UInt64 val)
    {
        if (current_page_values.size() >= PACKED_PAGE_SIZE)
            flush();

        chassert(current_page_values.empty() || current_page_values.back() < val);
        current_page_values.push_back(val);
    }

    /// Compresses and finalizes the current page of values.
    /// Called automatically when a page is full or at the end to finalize the structure.
    void flush()
    {
        if (current_page_values.empty())
            return;

        if (current_page_values.size() == 1)
        {
            /// Construct a single value page
            pages.emplace_back(current_page_values[0]);
            return;
        }

        size_t bits_per_val = 64 - getLeadingZeroBits(current_page_values.back() - current_page_values.front());
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
class MergedPartOffsets
{
public:
    enum class MappingMode
    {
        Enabled, /// Full offset mapping is required
        Disabled /// No mapping needed (e.g., no sorting key)
    };

    explicit MergedPartOffsets(size_t num_parts, MappingMode mode_ = MappingMode::Enabled)
        : mode(mode_)
        , offset_maps(mode == MappingMode::Enabled ? num_parts : 0)
        , finalized(mode == MappingMode::Disabled)
    {
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

    /// Looks up the new _part_offset in the merged data.
    UInt64 operator[](UInt64 part_index, UInt64 part_offset) const
    {
        chassert(mode == MappingMode::Enabled);
        chassert(part_index < offset_maps.size());
        return offset_maps[part_index][part_offset];
    }

    /// Finalizes all _part_offset maps and releases temporary buffers.
    /// Must be called after all offsets have been inserted.
    void flush()
    {
        if (mode == MappingMode::Disabled)
            return;

        chassert(!finalized);
        finalized = true;

        if (num_rows == 0)
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
            "Holding {} merged _part_offset in memory with {} total allocated memory",
            num_rows,
            formatReadableSizeWithBinarySuffix(total_allocated_memory));
    }

    bool isFinalized() const { return finalized; }
    bool isMappingEnabled() const { return mode == MappingMode::Enabled; }

    bool empty() const { return num_rows == 0; }
    size_t size() const { return num_rows; }

    void clear()
    {
        offset_maps.clear();
        num_rows = 0;
    }

private:
    MappingMode mode;
    std::vector<PackedPartOffsets> offset_maps;
    bool finalized;

    size_t num_rows = 0;
    LoggerPtr logger = getLogger("MergedPartOffsets");
};

}
