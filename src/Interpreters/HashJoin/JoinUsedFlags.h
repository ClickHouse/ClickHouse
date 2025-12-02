#pragma once
#include <vector>
#include <atomic>
#include <unordered_map>
#include <Core/Joins.h>
#include <Interpreters/joinDispatch.h>

namespace DB
{
namespace JoinStuff
{

/// Flags needed to implement RIGHT and FULL JOINs.
class JoinUsedFlags
{
    using RawColumnsPtr = const Columns *;
    using UsedFlagsForColumns = std::vector<std::atomic_bool>;

    /// For multiple disjuncts each entry in hashmap stores flags for particular block
    std::unordered_map<RawColumnsPtr, UsedFlagsForColumns> per_row_flags;

    /// For single disjunct we store all flags in a dedicated container to avoid calculating hash(nullptr) on each access.
    /// Index is the offset in FindResult
    UsedFlagsForColumns per_offset_flags;

    bool need_flags;

public:
    /// Update size for vector with flags.
    /// Calling this method invalidates existing flags.
    /// It can be called several times, but all of them should happen before using this structure.
    template <JoinKind KIND, JoinStrictness STRICTNESS, bool prefer_use_maps_all>
    void reinit(size_t size)
    {
        if constexpr (MapGetter<KIND, STRICTNESS, prefer_use_maps_all>::flagged)
        {
            assert(per_offset_flags.size() <= size);
            need_flags = true;
            // For one disjunct clause case, we don't need to reinit each time we call addBlockToJoin.
            // and there is no value inserted in this JoinUsedFlags before addBlockToJoin finish.
            // So we reinit only when the hash table is rehashed to a larger size.
            if (per_offset_flags.size() < size) [[unlikely]]
                per_offset_flags = std::vector<std::atomic_bool>(size);
        }
    }

    template <JoinKind KIND, JoinStrictness STRICTNESS, bool prefer_use_maps_all>
    void reinit(const Columns * columns)
    {
        if constexpr (MapGetter<KIND, STRICTNESS, prefer_use_maps_all>::flagged)
        {
            assert(per_row_flags[columns].size() <= columns->at(0)->size());
            need_flags = true;
            per_row_flags[columns] = std::vector<std::atomic_bool>(columns->at(0)->size());
        }
    }

    bool getUsedSafe(size_t i) const { return per_offset_flags[i].load(); }

    bool getUsedSafe(const Columns * columns, size_t row_idx) const
    {
        if (auto it = per_row_flags.find(columns); it != per_row_flags.end())
            return it->second[row_idx].load();
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
                for (auto it = mapped.begin(); it.ok(); ++it)
                    per_row_flags[&it->columns_info->columns][it->row_num].store(true, std::memory_order_relaxed);
            }
            else
                per_row_flags[&mapped.columns_info->columns][mapped.row_num].store(true, std::memory_order_relaxed);
        }
        else
        {
            per_offset_flags[f.getOffset()].store(true, std::memory_order_relaxed);
        }
    }

    template <bool use_flags, bool flag_per_row>
    void setUsed(const Columns * columns, size_t row_num, size_t offset)
    {
        if constexpr (!use_flags)
            return;

        /// Could be set simultaneously from different threads.
        if constexpr (flag_per_row)
        {
            per_row_flags[columns][row_num].store(true, std::memory_order_relaxed);
        }
        else
        {
            per_offset_flags[offset].store(true, std::memory_order_relaxed);
        }
    }

    template <bool use_flags, bool flag_per_row, typename FindResult>
    bool getUsed(const FindResult & f)
    {
        if constexpr (!use_flags)
            return true;

        if constexpr (flag_per_row)
        {
            auto & mapped = f.getMapped();
            return per_row_flags[&mapped.columns_info->columns][mapped.row_num].load();
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
            auto & mapped = f.getMapped();

            /// fast check to prevent heavy CAS with seq_cst order
            if (per_row_flags[&mapped.columns_info->columns][mapped.row_num].load(std::memory_order_relaxed))
                return false;

            bool expected = false;
            return per_row_flags[&mapped.columns_info->columns][mapped.row_num].compare_exchange_strong(expected, true);
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
    bool setUsedOnce(const Columns * columns, size_t row_num, size_t offset)
    {
        if constexpr (!use_flags)
            return true;

        if constexpr (flag_per_row)
        {
            /// fast check to prevent heavy CAS with seq_cst order
            if (per_row_flags[columns][row_num].load(std::memory_order_relaxed))
                return false;

            bool expected = false;
            return per_row_flags[columns][row_num].compare_exchange_strong(expected, true);
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
};

}
}
