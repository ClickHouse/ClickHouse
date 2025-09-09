#pragma once

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/Prefetching.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
}

struct ABHashTableHash
{
    size_t ALWAYS_INLINE operator()(StringRef key) const
    {
        // if (key.data == nullptr)
        //     return 0;

        // if ((reinterpret_cast<uintptr_t>(key.data) >> 63) == 1) [[unlikely]]
        //     return key.size;

        // size_t inlined_size = reinterpret_cast<uintptr_t>(key.data) >> 59;
        // if (inlined_size > 0)
        //     return StringRefHash()({reinterpret_cast<const char *>(reinterpret_cast<uintptr_t>(&key) + 1), inlined_size});

        return key.size;
    }
};

template <typename TMapped>
struct ABHashMapCell
{
    using Key = StringRef;
    using Mapped = TMapped;
    using State = HashTableNoState;

    using value_type = PairNoInit<Key, TMapped>;
    using mapped_type = Mapped;
    using key_type = Key;

    value_type value;

    ABHashMapCell() = default;
    ABHashMapCell(const Key & key_, const State &)
        : value(key_, NoInitTag())
    {
    }
    ABHashMapCell(const value_type & value_, const State &)
        : value(value_)
    {
    }

    /// Get the key (externally).
    StringRef ALWAYS_INLINE getKey() const
    {
        // if (value.first.data == nullptr)
        //     return {};

        // if ((reinterpret_cast<uintptr_t>(value.first.data) >> 63) == 1)
        //     return {reinterpret_cast<const char *>(reinterpret_cast<uintptr_t>(value.first.data) & ~(1ULL << 63)), value.first.size};

        // size_t inlined_size = reinterpret_cast<uintptr_t>(value.first.data) >> 59;
        // if (inlined_size > 0)
        //     return {reinterpret_cast<const char *>(reinterpret_cast<uintptr_t>(&value.first) + 1), inlined_size};

        return StringRef(value.first.data, value.first.size >> 32);
    }

    Mapped & getMapped() { return value.second; }
    const Mapped & getMapped() const { return value.second; }
    const value_type & getValue() const { return value; }

    /// Get the key (internally).
    static const Key & getKey(const value_type & value) { return value.first; }

    bool ALWAYS_INLINE keyEquals(const Key & key_) const
    {
        if (value.first.size != key_.size)
            return false;

        // if (value.first.data == nullptr)
        //     return key_.data == nullptr;

//         if ((reinterpret_cast<uintptr_t>(value.first.data) >> 63) == 1)
//         {
//             if ((reinterpret_cast<uintptr_t>(key_.data) >> 63) == 1)
//             {
//                 if (value.first.size != key_.size)
//                     return false;
// #if defined(__SSE2__) || (defined(__aarch64__) && defined(__ARM_NEON))
//                 return memequalWide(
//                     reinterpret_cast<const char *>(reinterpret_cast<uintptr_t>(value.first.data) & ~(1ULL << 63)),
//                     reinterpret_cast<const char *>(reinterpret_cast<uintptr_t>(key_.data) & ~(1ULL << 63)),
//                     key_.size);
// #else
//                 return 0
//                     == memcmp(
//                            reinterpret_cast<const char *>(reinterpret_cast<uintptr_t>(value.first.data) & ~(1ULL << 63)),
//                            reinterpret_cast<const char *>(reinterpret_cast<uintptr_t>(key_.data) & ~(1ULL << 63)),
//                            key_.size);
// #endif
//             }
//             else
//             {
//                 return false;
//             }
//         }

//         size_t inlined_size = reinterpret_cast<uintptr_t>(value.first.data) >> 59;

//         if (inlined_size > 0)
//             return value.first.data == key_.data && value.first.size == key_.size;

//         size_t key_inlined_size = reinterpret_cast<uintptr_t>(key_.data) >> 59;
//         if (key_inlined_size > 0)
//             return false;

        size_t size = value.first.size >> 32;

#if defined(__SSE2__) || (defined(__aarch64__) && defined(__ARM_NEON))
        return memequalWide(value.first.data, key_.data, size);
#else
        return 0 == memcmp(value.first.data, key_.data, size);
#endif
    }

    bool ALWAYS_INLINE keyEquals(const Key & key_, size_t /* hash_ */) const { return keyEquals(key_); }
    bool ALWAYS_INLINE keyEquals(const Key & key_, size_t /* hash_ */, const State & /*state*/) const { return keyEquals(key_); }

    void setHash(size_t /*hash_value*/) { }
    size_t getHash(const ABHashTableHash & hash) const { return hash(value.first); }

    bool isZero(const State & state) const { return isZero(value.first, state); }
    static bool isZero(const Key & key, const State & /*state*/) { return key.data == nullptr; }

    /// Set the key value to zero.
    void setZero() { value.first.data = nullptr; }

    /// Do I need to store the zero key separately (that is, can a zero key be inserted into the hash table).
    static constexpr bool need_zero_value_storage = true;

    void setMapped(const value_type & value_) { value.second = value_.second; }

    /// Serialization, in binary and text form.
    void write(DB::WriteBuffer & wb) const
    {
        DB::writeBinary(value.first, wb);
        DB::writeBinary(value.second, wb);
    }

    void writeText(DB::WriteBuffer & wb) const
    {
        DB::writeDoubleQuoted(value.first, wb);
        DB::writeChar(',', wb);
        DB::writeDoubleQuoted(value.second, wb);
    }

    /// Deserialization, in binary and text form.
    void read(DB::ReadBuffer & rb)
    {
        DB::readBinary(value.first, rb);
        DB::readBinary(value.second, rb);
    }

    void readText(DB::ReadBuffer & rb)
    {
        DB::readDoubleQuoted(value.first, rb);
        DB::assertChar(',', rb);
        DB::readDoubleQuoted(value.second, rb);
    }

    template <size_t I>
    auto & get() &
    {
        if constexpr (I == 0)
            return value.first;
        else if constexpr (I == 1)
            return value.second;
    }

    template <size_t I>
    auto const & get() const &
    {
        if constexpr (I == 0)
            return value.first;
        else if constexpr (I == 1)
            return value.second;
    }

    template <size_t I>
    auto && get() &&
    {
        if constexpr (I == 0)
            return std::move(value.first);
        else if constexpr (I == 1)
            return std::move(value.second);
    }
};

template <typename TMapped>
class ABHashMap
    : public HashTable<StringRef, ABHashMapCell<TMapped>, ABHashTableHash, HashTableGrowerWithPrecalculation<>, HashTableAllocator>
{
public:
    using Self = ABHashMap;
    using Base = HashTable<StringRef, ABHashMapCell<TMapped>, ABHashTableHash, HashTableGrowerWithPrecalculation<>, HashTableAllocator>;
    using LookupResult = typename Base::LookupResult;
    using Iterator = typename Base::iterator;
    using Cell = ABHashMapCell<TMapped>;
    using Key = StringRef;

    using Base::Base;
    using Base::prefetch;

    /// Merge every cell's value of current map into the destination map via emplace.
    ///  Func should have signature void(Mapped & dst, Mapped & src, bool emplaced).
    ///  Each filled cell in current map will invoke func once. If that map doesn't
    ///  have a key equals to the given cell, a new cell gets emplaced into that map,
    ///  and func is invoked with the third argument emplaced set to true. Otherwise
    ///  emplaced is set to false.
    template <typename Func, bool prefetch = false>
    void ALWAYS_INLINE mergeToViaEmplace(Self & that, Func && func)
    {
        DB::PrefetchingHelper prefetching;
        size_t prefetch_look_ahead = DB::PrefetchingHelper::getInitialLookAheadValue();

        size_t i = 0;
        auto prefetch_it = advanceIterator(this->begin(), prefetch_look_ahead);

        for (auto it = this->begin(), end = this->end(); it != end; ++it, ++i)
        {
            if constexpr (prefetch)
            {
                if (i == DB::PrefetchingHelper::iterationsToMeasure())
                {
                    prefetch_look_ahead = prefetching.calcPrefetchLookAhead();
                    prefetch_it = advanceIterator(prefetch_it, prefetch_look_ahead - DB::PrefetchingHelper::getInitialLookAheadValue());
                }

                if (prefetch_it != end)
                {
                    that.prefetchByHash(prefetch_it.getHash());
                    ++prefetch_it;
                }
            }

            typename Self::LookupResult res_it;
            bool inserted;
            that.emplace(Cell::getKey(it->getValue()), res_it, inserted, it.getHash());
            func(res_it->getMapped(), it->getMapped(), inserted);
        }
    }

    /// Merge every cell's value of current map into the destination map via find.
    ///  Func should have signature void(Mapped & dst, Mapped & src, bool exist).
    ///  Each filled cell in current map will invoke func once. If that map doesn't
    ///  have a key equals to the given cell, func is invoked with the third argument
    ///  exist set to false. Otherwise exist is set to true.
    template <typename Func>
    void ALWAYS_INLINE mergeToViaFind(Self & that, Func && func)
    {
        for (auto it = this->begin(), end = this->end(); it != end; ++it)
        {
            auto res_it = that.find(Cell::getKey(it->getValue()), it.getHash());
            if (!res_it)
                func(it->getMapped(), it->getMapped(), false);
            else
                func(res_it->getMapped(), it->getMapped(), true);
        }
    }

    /// Call func(const Key &, Mapped &) for each hash map element.
    template <typename Func>
    void forEachValue(Func && func)
    {
        for (auto & v : *this)
            func(v.getKey(), v.getMapped());
    }

    /// Call func(Mapped &) for each hash map element.
    template <typename Func>
    void forEachMapped(Func && func)
    {
        for (auto & v : *this)
            func(v.getMapped());
    }

    typename Cell::Mapped & ALWAYS_INLINE operator[](const Key & x)
    {
        LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);

        /** It may seem that initialization is not necessary for POD-types (or __has_trivial_constructor),
          *  since the hash table memory is initially initialized with zeros.
          * But, in fact, an empty cell may not be initialized with zeros in the following cases:
          * - ZeroValueStorage (it only zeros the key);
          * - after resizing and moving a part of the cells to the new half of the hash table, the old cells also have only the key to zero.
          *
          * On performance, there is almost always no difference, due to the fact that it->second is usually assigned immediately
          *  after calling `operator[]`, and since `operator[]` is inlined, the compiler removes unnecessary initialization.
          *
          * Sometimes due to initialization, the performance even grows. This occurs in code like `++map[key]`.
          * When we do the initialization, for new cells, it's enough to make `store 1` right away.
          * And if we did not initialize, then even though there was zero in the cell,
          *  the compiler can not guess about this, and generates the `load`, `increment`, `store` code.
          */
        if (inserted)
            new (reinterpret_cast<void *>(&it->getMapped())) typename Cell::Mapped();

        return it->getMapped();
    }

    /// Only inserts the value if key isn't already present
    void ALWAYS_INLINE insertIfNotPresent(const Key & x, const typename Cell::Mapped & value)
    {
        LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);
        if (inserted)
        {
            new (&it->getMapped()) typename Cell::Mapped();
            it->getMapped() = value;
        }
    }

    const typename Cell::Mapped & ALWAYS_INLINE at(const Key & x) const
    {
        if (auto it = this->find(x); it != this->end())
            return it->getMapped();
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot find element in HashMap::at method");
    }

private:
    Iterator advanceIterator(Iterator it, size_t n)
    {
        size_t i = 0;
        while (i < n && it != this->end())
        {
            ++i;
            ++it;
        }
        return it;
    }
};
