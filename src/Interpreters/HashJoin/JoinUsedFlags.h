#pragma once
#include <vector>
#include <atomic>
#include <unordered_map>
#include <Core/Joins.h>
#include <Core/Block.h>
#include <Interpreters/joinDispatch.h>
#include <mutex>

namespace DB
{
namespace JoinStuff
{

struct AtomicBoolWrapper
{
    std::atomic<bool> value;

    AtomicBoolWrapper() : value(false) {}
    explicit AtomicBoolWrapper(bool x) : value(x) {}

    // Move constructor copies the internal bool value
    AtomicBoolWrapper(AtomicBoolWrapper && other) noexcept
        : value(other.value.load(std::memory_order_relaxed))
    {}

    AtomicBoolWrapper & operator=(AtomicBoolWrapper && other) noexcept
    {
        value.store(other.value.load(std::memory_order_relaxed), std::memory_order_relaxed);
        return *this;
    }

    // Delete copy constructors, so no accidental copying
    AtomicBoolWrapper(const AtomicBoolWrapper &) = delete;
    AtomicBoolWrapper & operator=(const AtomicBoolWrapper &) = delete;
};

/// Flags needed to implement RIGHT and FULL JOINs.
class JoinUsedFlags
{
    using RawBlockPtr = const Block *;
    using UsedFlagsForBlock = std::vector<AtomicBoolWrapper>;

    /// For multiple dijuncts each empty in hashmap stores flags for particular block
    /// For single dicunct we store all flags in `nullptr` entry, index is the offset in FindResult
    std::unordered_map<RawBlockPtr, UsedFlagsForBlock> flags;

    bool need_flags = false;
    mutable std::mutex mutex;

public:

    std::unordered_map<RawBlockPtr, UsedFlagsForBlock> & getFlagsMap()
    {
        return flags;
    }

    const std::unordered_map<RawBlockPtr, UsedFlagsForBlock> & getFlagsMap() const
    {
        return flags;
    }

    /// Update size for vector with flags.
    /// Calling this method invalidates existing flags.
    /// It can be called several times, but all of them should happen before using this structure.
    template <JoinKind KIND, JoinStrictness STRICTNESS, bool prefer_use_maps_all>
    void reinit(size_t size)
    {
        std::lock_guard lock(mutex);
        if constexpr (MapGetter<KIND, STRICTNESS, prefer_use_maps_all>::flagged)
        {
            assert(flags[nullptr].size() <= size);
            need_flags = true;
            // For one disjunct clause case, we don't need to reinit each time we call addBlockToJoin.
            // and there is no value inserted in this JoinUsedFlags before addBlockToJoin finish.
            // So we reinit only when the hash table is rehashed to a larger size.
            if (flags.empty() || flags[nullptr].size() < size) [[unlikely]]
            {
                flags[nullptr] = std::vector<AtomicBoolWrapper>(size);
            }
        }
    }

    template <JoinKind KIND, JoinStrictness STRICTNESS, bool prefer_use_maps_all>
    void reinit(const Block * block_ptr)
    {
        std::lock_guard lock(mutex);
        if constexpr (MapGetter<KIND, STRICTNESS, prefer_use_maps_all>::flagged)
        {
            assert(flags[block_ptr].size() <= block_ptr->rows());
            need_flags = true;
            flags[block_ptr] = std::vector<AtomicBoolWrapper>(block_ptr->rows());
        }
    }

    bool getUsedSafe(size_t i) const
    {
        return getUsedSafe(nullptr, i);
    }
    bool getUsedSafe(const Block * block_ptr, size_t row_idx) const
    {
        if (auto it = flags.find(block_ptr); it != flags.end())
        {
            if (row_idx < it->second.size())
                return it->second[row_idx].value.load();
            return false;
        }
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
                    flags[it->block][it->row_num].value.store(true, std::memory_order_relaxed);
            }
            else
                flags[mapped.block][mapped.row_num].value.store(true, std::memory_order_relaxed);
        }
        else
        {
            flags[nullptr][f.getOffset()].value.store(true, std::memory_order_relaxed);
        }
    }

    template <bool use_flags, bool flag_per_row>
    void setUsed(const Block * block, size_t row_num, size_t offset)
    {
        if constexpr (!use_flags)
            return;

        /// Could be set simultaneously from different threads.
        if constexpr (flag_per_row)
        {
            flags[block][row_num].value.store(true, std::memory_order_relaxed);
        }
        else
        {
            flags[nullptr][offset].value.store(true, std::memory_order_relaxed);
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
            return flags[mapped.block][mapped.row_num].value.load();
        }
        else
        {
            return flags[nullptr][f.getOffset()].value.load();
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
            if (flags[mapped.block][mapped.row_num].value.load(std::memory_order_relaxed))
                return false;

            bool expected = false;
            return flags[mapped.block][mapped.row_num].value.compare_exchange_strong(expected, true);
        }
        else
        {
            auto off = f.getOffset();

            /// fast check to prevent heavy CAS with seq_cst order
            if (flags[nullptr][off].value.load(std::memory_order_relaxed))
                return false;

            bool expected = false;
            return flags[nullptr][off].value.compare_exchange_strong(expected, true);
        }

    }
    template <bool use_flags, bool flag_per_row>
    bool setUsedOnce(const Block * block, size_t row_num, size_t offset)
    {
        if constexpr (!use_flags)
            return true;

        if constexpr (flag_per_row)
        {
            /// fast check to prevent heavy CAS with seq_cst order
            if (flags[block][row_num].value.load(std::memory_order_relaxed))
                return false;

            bool expected = false;
            return flags[block][row_num].value.compare_exchange_strong(expected, true);
        }
        else
        {
            /// fast check to prevent heavy CAS with seq_cst order
            if (flags[nullptr][offset].value.load(std::memory_order_relaxed))
                return false;

            bool expected = false;
            return flags[nullptr][offset].value.compare_exchange_strong(expected, true);
        }
    }

    void mergeFrom(const JoinUsedFlags & other)
    {
        for (const auto & [block_ptr, other_vec] : other.flags)
        {
            auto & vec = flags[block_ptr];
            if (vec.size() < other_vec.size())
                vec.resize(other_vec.size());  // new entries default false

            for (size_t i = 0; i < other_vec.size(); ++i)
            {
                // Combine current and other by load-OR-store
                bool combined = vec[i].value.load(std::memory_order_relaxed)
                              || other_vec[i].value.load(std::memory_order_relaxed);
                vec[i].value.store(combined, std::memory_order_relaxed);
            }
        }
    }
};

}
}
