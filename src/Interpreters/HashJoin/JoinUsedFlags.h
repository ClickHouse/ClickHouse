#pragma once
#include <atomic>
#include <utility>
#include <vector>
#include <Core/Joins.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
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
    /// One list per build worker, so that appending needs no synchronization.
    std::vector<PendingPerRowFlags> pending_per_worker;

    /// Call before the build starts: resizing later would race with the appends.
    /// `need_flags` is the fallback `getUsedSafe` reads, so publish it here, not from each worker.
    void setPendingFlagWorkers(size_t num_workers, bool need_flags_ = false)
    {
        pending_per_worker.resize(num_workers);
        if (need_flags_)
            need_flags = true;
    }

    /// Dense flags indexed by block_no, built from `pending_per_worker` when the build finishes.
    /// The probe and non-joined phases read and write only this.
    std::vector<UsedFlagsForColumns> per_row_flags;

    /// For single disjunct we store all flags in a dedicated container to avoid calculating hash(nullptr) on each access.
    /// Index is the offset in FindResult
    UsedFlagsForColumns per_offset_flags;

    bool need_flags{};

    /// Update size for vector with flags.
    /// Calling this method invalidates existing flags.
    /// It can be called several times, but all of them should happen before using this structure.
    template <JoinKind KIND, JoinStrictness STRICTNESS, bool prefer_use_maps_all> // NOLINT(readability-identifier-naming)
    void reinit(size_t size)
    {
        if constexpr (MapGetter<KIND, STRICTNESS, prefer_use_maps_all>::flagged)
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
    template <JoinKind KIND, JoinStrictness STRICTNESS, bool prefer_use_maps_all> // NOLINT(readability-identifier-naming)
    void reinitAllowShrinking(size_t size)
    {
        if constexpr (MapGetter<KIND, STRICTNESS, prefer_use_maps_all>::flagged)
        {
            need_flags = true;
            per_offset_flags = std::vector<std::atomic_bool>(size);
        }
    }

    template <JoinKind KIND, JoinStrictness STRICTNESS, bool prefer_use_maps_all> // NOLINT(readability-identifier-naming)
    void reinit(size_t worker_id, UInt32 block_no, size_t rows, const ScatteredBlock::Selector & selector)
    {
        if constexpr (MapGetter<KIND, STRICTNESS, prefer_use_maps_all>::flagged)
        {
            chassert(worker_id < pending_per_worker.size());
            auto & flags = pending_per_worker[worker_id].emplace_back(block_no, UsedFlagsForColumns(rows)).second;

            /// Mark all rows outside of selector as used.
            /// We should not emit them in RIGHT/FULL JOIN result,
            /// since they belongs to another shard, which will handle flags for these rows
            for (auto & flag : flags)
                flag.store(true);
            for (size_t index : selector)
                flags[index].store(false);
        }
    }

    /// Call once no build worker can still be appending.
    void finalizePerRowFlags(size_t num_blocks)
    {
        bool any_pending = false;
        for (const auto & pending : pending_per_worker)
            any_pending |= !pending.empty();
        if (!any_pending)
            return;

        need_flags = true;
        if (per_row_flags.size() < num_blocks)
            per_row_flags.resize(num_blocks);

        for (auto & pending : pending_per_worker)
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

    bool getUsedSafe(size_t i) const { return per_offset_flags[i].load(); }

    bool getUsedSafe(UInt32 block_no, size_t row_idx) const
    {
        if (block_no < per_row_flags.size() && !per_row_flags[block_no].empty())
            return per_row_flags[block_no][row_idx].load();
        return !need_flags;
    }

    template <bool use_flags, bool flag_per_row, typename FindResult>
    void setUsed(const FindResult & f [[maybe_unused]])
    {
        if constexpr (use_flags)
        {
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
                markPerOffsetUsed(f.getOffset());
            }
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
            markPerOffsetUsed(offset);
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
    bool getUsed(const FindResult & f [[maybe_unused]])
    {
        /// `if constexpr` rather than an early return, so the no-flags instantiation never
        /// mentions `getOffset`, which its `FindResult` does not have.
        if constexpr (use_flags)
        {
            if constexpr (flag_per_row)
                return headRowFlag(f.getMapped()).load();
            else
                return per_offset_flags[f.getOffset()].load();
        }
        else
        {
            return true;
        }
    }

    template <bool use_flags, bool flag_per_row, typename FindResult>
    bool setUsedOnce(const FindResult & f [[maybe_unused]])
    {
        if constexpr (use_flags)
        {
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
                return markPerOffsetUsedOnce(f.getOffset());
            }
        }
        else
        {
            return true;
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
            return markPerOffsetUsedOnce(offset);
        }
    }

    /// Occupied keys, not cells: an empty cell has no flag to set and would never be counted off.
    void setUnsetOffsetCount(size_t count) { unset_offset_flags.store(count, std::memory_order_relaxed); }

    /// A counter rather than a scan: RIGHT/FULL asks once per non-joined stream, and the vector is
    /// as large as the hash table.
    bool allOffsetFlagsSet() const noexcept { return unset_offset_flags.load(std::memory_order_relaxed) == 0; }

private:
    void markPerOffsetUsed(size_t offset)
    {
        auto & flag = per_offset_flags[offset];
        /// fast check to avoid a dirtying RMW on every re-match of the same key
        if (flag.load(std::memory_order_relaxed))
            return;
        /// the exchange (not a plain store) keeps `unset_offset_flags` decremented exactly once
        if (!flag.exchange(true, std::memory_order_relaxed))
            unset_offset_flags.fetch_sub(1, std::memory_order_relaxed);
    }

    bool markPerOffsetUsedOnce(size_t offset)
    {
        auto & flag = per_offset_flags[offset];

        /// fast check to prevent heavy CAS with seq_cst order
        if (flag.load(std::memory_order_relaxed))
            return false;

        bool expected = false;
        if (!flag.compare_exchange_strong(expected, true))
            return false;

        unset_offset_flags.fetch_sub(1, std::memory_order_relaxed);
        return true;
    }

    std::atomic<size_t> unset_offset_flags{0};
};

}
}
