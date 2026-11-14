#pragma once
#include <algorithm>
#include <atomic>
#include <utility>
#include <vector>
#include <Core/Joins.h>
#include <Interpreters/joinDispatch.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace JoinStuff
{

/// Flags needed to implement RIGHT and FULL JOINs.
class JoinUsedFlags
{
public:
    using UsedFlagsForColumns = std::vector<std::atomic_bool>;

    using PendingPerRowFlags = std::vector<std::pair<UInt32, UsedFlagsForColumns>>;

    /// Per-row flags filled during the build phase: (block_no, flags) for each stored block.
    /// One list per build stream, so that appending needs no synchronization.
    std::vector<PendingPerRowFlags> pending_per_build_stream;

    /// Call before the build starts: resizing later would race with the appends.
    void setNumBuildStreams(size_t num_build_streams) { pending_per_build_stream.resize(num_build_streams); }

    /// Dense flags indexed by block_no, built from `pending_per_build_stream` when the build finishes.
    /// The probe and non-joined phases read and write only this.
    std::vector<UsedFlagsForColumns> per_row_flags;

    /// For single disjunct we store all flags in a dedicated container to avoid calculating hash(nullptr) on each access.
    /// Index is the offset in FindResult
    /// While the probe runs, accessed only through `std::atomic_ref`: `offsetFlag`, `setUsedAt` and `setUsedOnceAt`.
    std::vector<UInt8> per_offset_flags;

    std::atomic_ref<UInt8> offsetFlag(size_t offset) const
    {
        return std::atomic_ref<UInt8>(const_cast<UInt8 &>(per_offset_flags[offset]));
    }

    /// For a loop that sets the used flags of many keys: `setUsedAt` and `setUsedOnceAt` on this pointer skip the bounds check
    /// of `per_offset_flags`.
    UInt8 * offsetFlags() { return per_offset_flags.data(); }

    /// Could be set simultaneously from different threads.
    static void setUsedAt(UInt8 * flags, size_t offset)
    {
        std::atomic_ref<UInt8> flag(flags[offset]);
        if (!flag.load(std::memory_order_relaxed))
            flag.store(1, std::memory_order_relaxed);
    }

    /// Returns whether this call set the flag.
    static bool setUsedOnceAt(UInt8 * flags, size_t offset)
    {
        std::atomic_ref<UInt8> flag(flags[offset]);
        /// fast check to prevent heavy CAS with seq_cst order
        if (flag.load(std::memory_order_relaxed))
            return false;
        UInt8 expected = 0;
        return flag.compare_exchange_strong(expected, 1);
    }

    bool need_flags{};

    /// Update size for vector with flags.
    /// Calling this method invalidates existing flags.
    /// It can be called several times, but all of them should happen before using this structure.
    template <JoinKind KIND, JoinStrictness STRICTNESS, JoinMapsKind maps_kind>
    void reinit(size_t size)
    {
        if constexpr (MapGetter<KIND, STRICTNESS, maps_kind>::flagged)
        {
            chassert(per_offset_flags.size() <= size);
            need_flags = true;
            // For one disjunct clause case, we don't need to reinit each time we call addBlockToJoin.
            // and there is no value inserted in this JoinUsedFlags before addBlockToJoin finish.
            // So we reinit only when the hash table is rehashed to a larger size.
            if (per_offset_flags.size() < size) [[unlikely]]
                per_offset_flags = std::vector<UInt8>(size);
        }
    }

    /// Update size for vector with flags same as `reinit` but allows the updated size to be smaller.
    /// Must be called only before using this structure.
    template <JoinKind KIND, JoinStrictness STRICTNESS, JoinMapsKind maps_kind>
    void reinitAllowShrinking(size_t size)
    {
        if constexpr (MapGetter<KIND, STRICTNESS, maps_kind>::flagged)
        {
            need_flags = true;
            per_offset_flags = std::vector<UInt8>(size);
        }
    }

    template <JoinKind KIND, JoinStrictness STRICTNESS, JoinMapsKind maps_kind>
    void reinit(size_t stream, UInt32 block_no, size_t rows)
    {
        if constexpr (MapGetter<KIND, STRICTNESS, maps_kind>::flagged)
        {
            chassert(stream < pending_per_build_stream.size());
            pending_per_build_stream[stream].emplace_back(block_no, UsedFlagsForColumns(rows));
        }
    }

    /// Initialize the dense per-row flags for blocks of the given row counts, indexed by their
    /// position. For users outside of the hash-join map dispatch (the templated `reinit`
    /// overloads above), such as the INNER ANY claim-once flags of `MergeJoin`.
    void reinitPerRowFlags(const std::vector<size_t> & rows_per_block)
    {
        need_flags = true;
        per_row_flags = std::vector<UsedFlagsForColumns>(rows_per_block.size());
        for (size_t block_no = 0; block_no < rows_per_block.size(); ++block_no)
            per_row_flags[block_no] = UsedFlagsForColumns(rows_per_block[block_no]);
    }

    /// Call once no build stream can still be appending.
    void finalizePerRowFlags(size_t num_blocks)
    {
        if (std::ranges::all_of(pending_per_build_stream, [](const auto & pending) { return pending.empty(); }))
            return;

        need_flags = true;
        if (per_row_flags.size() < num_blocks)
            per_row_flags.resize(num_blocks);

        for (auto & pending : pending_per_build_stream)
        {
            for (auto & [block_no, flags] : pending)
            {
                if (block_no >= per_row_flags.size() || !per_row_flags[block_no].empty())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinUsedFlags: unexpected per-row flags for block {}", block_no);
                per_row_flags[block_no] = std::move(flags);
            }
            pending.clear();
        }
    }

    bool getUsedSafe(size_t i) const { return offsetFlag(i).load() != 0; }

    bool getUsedSafe(UInt32 block_no, size_t row_idx) const
    {
        if (block_no < per_row_flags.size() && !per_row_flags[block_no].empty())
            return per_row_flags[block_no][row_idx].load();
        return !need_flags;
    }

    template <bool use_flags, bool flag_per_row>
    void setUsed(UInt32 block_no, size_t row_num, size_t offset)
    {
        if constexpr (!use_flags)
            return;

        /// Could be set simultaneously from different threads.
        if constexpr (flag_per_row)
        {
            auto & flag = per_row_flags[block_no][row_num];
            if (!flag.load(std::memory_order_relaxed))
                flag.store(true, std::memory_order_relaxed);
        }
        else
            setUsedAt(per_offset_flags.data(), offset);
    }

    template <bool use_flags, bool flag_per_row>
    bool setUsedOnce(UInt32 block_no, size_t row_num, size_t offset)
    {
        if constexpr (!use_flags)
            return true;

        if constexpr (flag_per_row)
        {
            auto & flag = per_row_flags[block_no][row_num];

            /// fast check to prevent heavy CAS with seq_cst order
            if (flag.load(std::memory_order_relaxed))
                return false;

            bool expected = false;
            return flag.compare_exchange_strong(expected, true);
        }
        else
            return setUsedOnceAt(per_offset_flags.data(), offset);
    }
};

}
}
