#pragma once

#include <Common/CurrentThread.h>
#include <Common/HashTable/FixedHashTable.h>
#include <Common/HashTable/HashMap.h>
#include <Common/ThreadPool.h>
#include <Common/scope_guard_safe.h>

namespace CurrentMetrics
{
    extern const Metric AggregatorThreads;
    extern const Metric AggregatorThreadsActive;
    extern const Metric AggregatorThreadsScheduled;
}

template <typename Key, typename TMapped, typename TState = HashTableNoState>
struct FixedHashMapCell
{
    using Mapped = TMapped;
    using State = TState;

    using value_type = PairNoInit<Key, Mapped>;
    using mapped_type = TMapped;

    bool full;
    Mapped mapped;

    FixedHashMapCell() {} /// NOLINT
    FixedHashMapCell(const Key &, const State &) : full(true) {}
    FixedHashMapCell(const value_type & value_, const State &) : full(true), mapped(value_.second) {}

    const VoidKey getKey() const { return {}; } /// NOLINT
    Mapped & getMapped() { return mapped; }
    const Mapped & getMapped() const { return mapped; }

    bool isZero(const State &) const { return !full; }
    void setZero() { full = false; }

    /// Similar to FixedHashSetCell except that we need to contain a pointer to the Mapped field.
    ///  Note that we have to assemble a continuous layout for the value_type on each call of getValue().
    struct CellExt
    {
        CellExt() {} /// NOLINT
        CellExt(Key && key_, const FixedHashMapCell * ptr_) : key(key_), ptr(const_cast<FixedHashMapCell *>(ptr_)) {}
        void update(Key && key_, const FixedHashMapCell * ptr_)
        {
            key = key_;
            ptr = const_cast<FixedHashMapCell *>(ptr_);
        }
        Key key;
        FixedHashMapCell * ptr;

        const Key & getKey() const { return key; }
        Mapped & getMapped() { return ptr->mapped; }
        const Mapped & getMapped() const { return ptr->mapped; }
        const value_type getValue() const { return {key, ptr->mapped}; } /// NOLINT
    };
};


/// In case when we can encode empty cells with zero mapped values.
template <typename Key, typename TMapped, typename TState = HashTableNoState>
struct FixedHashMapImplicitZeroCell
{
    using Mapped = TMapped;
    using State = TState;

    using value_type = PairNoInit<Key, Mapped>;
    using mapped_type = TMapped;

    Mapped mapped;

    FixedHashMapImplicitZeroCell() {} /// NOLINT
    FixedHashMapImplicitZeroCell(const Key &, const State &) {}
    FixedHashMapImplicitZeroCell(const value_type & value_, const State &) : mapped(value_.second) {}

    const VoidKey getKey() const { return {}; } /// NOLINT
    Mapped & getMapped() { return mapped; }
    const Mapped & getMapped() const { return mapped; }

    bool isZero(const State &) const { return !mapped; }
    void setZero() { mapped = {}; }

    /// Similar to FixedHashSetCell except that we need to contain a pointer to the Mapped field.
    ///  Note that we have to assemble a continuous layout for the value_type on each call of getValue().
    struct CellExt
    {
        CellExt() {} /// NOLINT
        CellExt(Key && key_, const FixedHashMapImplicitZeroCell * ptr_) : key(key_), ptr(const_cast<FixedHashMapImplicitZeroCell *>(ptr_)) {}
        void update(Key && key_, const FixedHashMapImplicitZeroCell * ptr_)
        {
            key = key_;
            ptr = const_cast<FixedHashMapImplicitZeroCell *>(ptr_);
        }
        Key key;
        FixedHashMapImplicitZeroCell * ptr;

        const Key & getKey() const { return key; }
        Mapped & getMapped() { return ptr->mapped; }
        const Mapped & getMapped() const { return ptr->mapped; }
        const value_type getValue() const { return {key, ptr->mapped}; } /// NOLINT
    };
};


template <
    typename Key,
    typename Mapped,
    typename Cell = FixedHashMapCell<Key, Mapped>,
    typename Size = FixedHashTableStoredSize<Cell>,
    typename Allocator = HashTableAllocator>
class FixedHashMap : public FixedHashTable<Key, Cell, Size, Allocator>
{
    static constexpr size_t BUCKET_SIZE = (1ULL << (sizeof(Key) * 8)) > 256 ? 256 : 16;

public:
    using Base = FixedHashTable<Key, Cell, Size, Allocator>;
    using Self = FixedHashMap;
    using LookupResult = typename Base::LookupResult;

    using Base::Base;

    FixedHashMap() = default;
    FixedHashMap(size_t ) {} /// NOLINT

    template <typename Func, bool>
    void ALWAYS_INLINE mergeToViaEmplace(Self & that, Func && func)
    {
        auto thread_pool = std::make_unique<ThreadPool>(
            CurrentMetrics::AggregatorThreads, CurrentMetrics::AggregatorThreadsActive, CurrentMetrics::AggregatorThreadsScheduled);

        std::mutex mutex;

        auto merge = [&that, &func, &mutex](auto & begin, auto & end, DB::ThreadGroupPtr thread_group)
        {
            SCOPE_EXIT_SAFE(
                if (thread_group)
                    DB::CurrentThread::detachFromGroupIfNotDetached();
            );

            if (thread_group)
                DB::CurrentThread::attachToGroupIfDetached(thread_group);

            std::lock_guard lock(mutex);
            for (auto it = begin; it < end; ++it)
            {
                typename Self::LookupResult res_it;
                bool inserted;
                that.emplace(it->getKey(), res_it, inserted, it.getHash());
                func(res_it->getMapped(), it->getMapped(), inserted);
            }
        };

        size_t offset = 0;
        while (offset < Base::NUM_CELLS)
        {
            auto begin = this->getIteratorWithOffsetAndLimit(offset, BUCKET_SIZE);
            auto end = this->getIteratorWithOffset(offset + Base::BUCKET_SIZE);
            auto task
                = [group = DB::CurrentThread::getGroup(), &merge, &begin, &end] { merge(begin, end, group); };

            thread_pool->scheduleOrThrowOnError(task);
            offset += Base::BUCKET_SIZE;
        }
        thread_pool->wait();
    }

    template <typename Func>
    void ALWAYS_INLINE mergeToViaFind(Self & that, Func && func)
    {
        auto thread_pool = std::make_unique<ThreadPool>(
            CurrentMetrics::AggregatorThreads, CurrentMetrics::AggregatorThreadsActive, CurrentMetrics::AggregatorThreadsScheduled);

        auto merge = [&that, &func](auto begin, auto & end, DB::ThreadGroupPtr thread_group)
        {
            SCOPE_EXIT_SAFE(
                if (thread_group)
                    DB::CurrentThread::detachFromGroupIfNotDetached();
            );

            if (thread_group)
                DB::CurrentThread::attachToGroupIfDetached(thread_group);

            for (auto it = begin; it != end; ++it)
            {
                auto res_it = that.find(it->getKey(), it.getHash());
                if (!res_it)
                    func(it->getMapped(), it->getMapped(), false);
                else
                    func(res_it->getMapped(), it->getMapped(), true);
            }
        };

        size_t offset = 0;
        while (offset < Base::NUM_CELLS)
        {
            auto begin = this->getIteratorWithOffset(offset);
            auto end = this->getIteratorWithOffset(offset + BUCKET_SIZE);
            auto task
                = [group = DB::CurrentThread::getGroup(), &merge, &begin, &end] { merge(begin, end, group); };

            thread_pool->scheduleOrThrowOnError(task);
            offset += BUCKET_SIZE;
        }
        thread_pool->wait();
    }

    template <typename Func>
    void forEachValue(Func && func)
    {
        for (auto & v : *this)
            func(v.getKey(), v.getMapped());
    }

    template <typename Func>
    void forEachMapped(Func && func)
    {
        for (auto & v : *this)
            func(v.getMapped());
    }

    Mapped & ALWAYS_INLINE operator[](const Key & x)
    {
        LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);
        if (inserted)
            new (&it->getMapped()) Mapped();

        return it->getMapped();
    }
};


template <typename Key, typename Mapped, typename Allocator = HashTableAllocator>
using FixedImplicitZeroHashMap = FixedHashMap<
    Key,
    Mapped,
    FixedHashMapImplicitZeroCell<Key, Mapped>,
    FixedHashTableStoredSize<FixedHashMapImplicitZeroCell<Key, Mapped>>,
    Allocator>;

template <typename Key, typename Mapped, typename Allocator = HashTableAllocator>
using FixedImplicitZeroHashMapWithCalculatedSize = FixedHashMap<
    Key,
    Mapped,
    FixedHashMapImplicitZeroCell<Key, Mapped>,
    FixedHashTableCalculatedSize<FixedHashMapImplicitZeroCell<Key, Mapped>>,
    Allocator>;
