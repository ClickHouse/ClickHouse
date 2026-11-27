#pragma once
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
    PendingPerRowFlags pending_per_row_flags;

    /// Dense flags indexed by block_no, that are built from `pending_per_row_flags` when the build phase finishes.
    /// The probe and non-joined phases read and write only this.
    std::vector<UsedFlagsForColumns> per_row_flags;

    /// For single disjunct we store all flags in a dedicated container to avoid calculating hash(nullptr) on each access.
    /// Index is the offset in FindResult
    UsedFlagsForColumns per_offset_flags;

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
                per_offset_flags = std::vector<std::atomic_bool>(size);
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
            per_offset_flags = std::vector<std::atomic_bool>(size);
        }
    }

    template <JoinKind KIND, JoinStrictness STRICTNESS, JoinMapsKind maps_kind>
    void reinit(UInt32 block_no, size_t rows, const ScatteredBlock::Selector & selector)
    {
        if constexpr (MapGetter<KIND, STRICTNESS, maps_kind>::flagged)
        {
            need_flags = true;
            auto & flags = pending_per_row_flags.emplace_back(block_no, UsedFlagsForColumns(rows)).second;

            /// Mark all rows outside of selector as used.
            /// We should not emit them in RIGHT/FULL JOIN result,
            /// since they belongs to another shard, which will handle flags for these rows
            for (auto & flag : flags)
                flag.store(true);
            for (size_t index : selector)
                flags[index].store(false);
        }
    }

    /// Move the source pending per-block flags into the dense `per_row_flags` vector, which is
    /// sized to cover every block_no of the `StoredColumnsIndex`.
    void finalizePerRowFlags(JoinUsedFlags & source, size_t num_blocks)
    {
        if (source.pending_per_row_flags.empty())
            return;

        auto source_pending_flags = std::exchange(source.pending_per_row_flags, PendingPerRowFlags{});

        need_flags = true;
        if (per_row_flags.size() < num_blocks)
            per_row_flags.resize(num_blocks);

        for (auto & [block_no, flags] : source_pending_flags)
        {
            if (block_no >= per_row_flags.size() || !per_row_flags[block_no].empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinUsedFlags: unexpected per-row flags for block {}", block_no);
            per_row_flags[block_no] = std::move(flags);
        }
    }

    bool getUsedSafe(size_t i) const { return per_offset_flags[i].load(); }

    bool getUsedSafe(UInt32 block_no, size_t row_idx) const
    {
        if (block_no < per_row_flags.size() && !per_row_flags[block_no].empty())
            return per_row_flags[block_no][row_idx].load();
        return !need_flags;
    }

    template <bool use_flags, bool flag_per_row, typename FindResult>
    void setUsed(const FindResult & f)
    {
        if constexpr (!use_flags)
            return;

        /// Could be set simultaneously from different threads.
        if constexpr (flag_per_row)
        {
            auto & mapped = f.getMapped();
            if constexpr (std::is_same_v<std::decay_t<decltype(mapped)>, RowRefList>)
            {
                for (const UInt64 ref_word : refsOf(mapped.word))
                {
                    auto & flag = per_row_flags[refWordBlockNo(ref_word)][refWordRowNo(ref_word)];
                    if (!flag.load(std::memory_order_relaxed))
                        flag.store(true, std::memory_order_relaxed);
                }
            }
            else
            {
                auto & flag = headRowFlag(mapped);
                if (!flag.load(std::memory_order_relaxed))
                    flag.store(true, std::memory_order_relaxed);
            }
        }
        else
        {
            auto & flag = per_offset_flags[f.getOffset()];
            if (!flag.load(std::memory_order_relaxed))
                flag.store(true, std::memory_order_relaxed);
        }
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
        {
            auto & flag = per_offset_flags[offset];
            if (!flag.load(std::memory_order_relaxed))
                flag.store(true, std::memory_order_relaxed);
        }
    }

    /// The flag of the key's FIRST row: for RowRefList this preserves the semantics of the
    /// old RowRefList, whose head row fields were used through the RowRef base class.
    template <typename Mapped>
    std::atomic_bool & headRowFlag(const Mapped & mapped)
    {
        /// firstRefWord dispatches on Mapped exactly as needed: RowRefList -> firstWord (the head
        /// row of the key), RowRef -> encode. refWordBlockNo/refWordRowNo of that word equal
        /// blockNo()/rowNo() for a RowRef (same 8-byte layout), so this is one uniform decode.
        const UInt64 ref_word = firstRefWord(mapped);
        return per_row_flags[refWordBlockNo(ref_word)][refWordRowNo(ref_word)];
    }

    template <bool use_flags, bool flag_per_row, typename FindResult>
    bool getUsed(const FindResult & f)
    {
        if constexpr (!use_flags)
            return true;

        if constexpr (flag_per_row)
        {
            return headRowFlag(f.getMapped()).load();
        }
        else
        {
            return per_offset_flags[f.getOffset()].load();
        }
    }

    template <bool use_flags, bool flag_per_row, typename FindResult>
    bool setUsedOnce(const FindResult & f)
    {
        if constexpr (!use_flags)
            return true;

        if constexpr (flag_per_row)
        {
            auto & flag = headRowFlag(f.getMapped());

            /// fast check to prevent heavy CAS with seq_cst order
            if (flag.load(std::memory_order_relaxed))
                return false;

            bool expected = false;
            return flag.compare_exchange_strong(expected, true);
        }
        else
        {
            auto off = f.getOffset();

            /// fast check to prevent heavy CAS with seq_cst order
            if (per_offset_flags[off].load(std::memory_order_relaxed))
                return false;

            bool expected = false;
            return per_offset_flags[off].compare_exchange_strong(expected, true);
        }

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
        {
            /// fast check to prevent heavy CAS with seq_cst order
            if (per_offset_flags[offset].load(std::memory_order_relaxed))
                return false;

            bool expected = false;
            return per_offset_flags[offset].compare_exchange_strong(expected, true);
        }
    }

    /// Are all offset flags set? (index 0 is skipped as it is a service index)
    bool allOffsetFlagsSet() const noexcept
    {
        for (const auto & per_offset_flag : per_offset_flags)
            if (!per_offset_flag.load(std::memory_order_relaxed))
                return false;
        return true;
    }
};

}
}
