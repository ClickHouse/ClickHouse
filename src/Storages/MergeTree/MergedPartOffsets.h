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

    static constexpr UInt8 PAGE_SIZE_DEGREE = 10;
    static constexpr size_t PAGE_SIZE = 1 << PAGE_SIZE_DEGREE;
    static constexpr size_t PAGE_MASK = PAGE_SIZE - 1;

    PODArray<Page> pages;
    PODArray<UInt64> current_page_values;
    Arena arena;

public:
    /// @param val The _part_offset value to insert (must be greater than all previously inserted values)
    void insert(UInt64 val)
    {
        if (current_page_values.size() >= PAGE_SIZE)
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

        size_t bits_per_val = 64 - getLeadingZeroBits(current_page_values.back() - current_page_values.front());
        chassert(bits_per_val >= 1);
        size_t num_uint64 = ((current_page_values.size() * bits_per_val) + 63) / 64;
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
        size_t page_pos = i >> PAGE_SIZE_DEGREE;
        chassert(page_pos < pages.size());
        size_t page_idx = i & PAGE_MASK;
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
    MergedPartOffsets() = default;

    /// @starting_offsets: Starting cumulative _part_offset for each data part.
    /// @total_rows: Total number of rows across all merging parts.
    MergedPartOffsets(std::vector<UInt64> starting_offsets_, size_t total_rows_)
        : starting_offsets(std::move(starting_offsets_))
        , total_rows(total_rows_)
        , num_parts(starting_offsets.size())
        , offset_maps(num_parts)
    {
    }

    MergedPartOffsets(MergedPartOffsets && other) noexcept
    {
        std::swap(*this, other);
    }

    MergedPartOffsets & operator=(MergedPartOffsets && other) noexcept
    {
        std::swap(*this, other);
        return *this;
    }

    /// Records _part_offset mappings for a batch of cumulated _part_offset values.
    /// Determines which part each offset belongs to and maintains mapping to new _part_offset.
    void insert(const UInt64 * begin_cumulated_offset, const UInt64 * end_cumulated_offset)
    {
        for (const UInt64 * it = begin_cumulated_offset; it != end_cumulated_offset; ++it)
        {
            /// Find which original part this cumulated offset belongs to
            size_t part_idx = 0;
            while (part_idx < num_parts - 1)
            {
                if (starting_offsets[part_idx + 1] > *it)
                    break;

                ++part_idx;
            }

            /// Record the mapping from original _part_offset to new _part_offset
            offset_maps[part_idx].insert(num_rows);
            ++num_rows;
        }
    }

    /// Looks up the new _part_offset in the merged data for a given original cumulated offset.
    UInt64 operator[](UInt64 cumulated_offset) const
    {
        /// Find which part this cumulated_offset belongs to
        size_t part_idx = 0;
        while (part_idx < num_parts - 1)
        {
            if (starting_offsets[part_idx + 1] > cumulated_offset)
                break;

            ++part_idx;
        }

        chassert(part_idx < offset_maps.size());

        /// Calculate relative offset within the part and look up new _part_offset
        UInt64 offset = cumulated_offset - starting_offsets[part_idx];
        return offset_maps[part_idx][offset];
    }

    /// Finalizes all _part_offset maps and releases temporary buffers.
    /// Must be called after all offsets have been inserted.
    void flush()
    {
        chassert(!finalized);
        finalized = true;

        if (num_rows == 0)
            return;

        chassert(total_rows == num_rows);
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
            total_rows,
            formatReadableSizeWithBinarySuffix(total_allocated_memory));
    }

    bool isFinalized() const { return finalized; }
    bool empty() const { return num_rows == 0; }
    size_t size() const { return num_rows; }

private:
    std::vector<UInt64> starting_offsets;
    size_t total_rows = 0;
    size_t num_parts = 0;
    std::vector<PackedPartOffsets> offset_maps;
    size_t num_rows = 0;
    bool finalized = false;
    LoggerPtr logger = getLogger("MergedPartOffsets");
};

}
