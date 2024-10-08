#pragma once

#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashTable.h>
#include <Common/HashTable/HashTableAllocator.h>
#include <Common/HashTable/TwoLevelHashTable.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

/** NOTE HashSet could only be used for memmoveable (position independent) types.
  * Example: std::string is not position independent in libstdc++ with C++11 ABI or in libc++.
  * Also, key must be of type, that zero bytes is compared equals to zero key.
  */


template <
    typename Key,
    typename TCell,
    typename Hash = DefaultHash<Key>,
    typename Grower = HashTableGrowerWithPrecalculation<>,
    typename Allocator = HashTableAllocator>
class HashSetTable : public HashTable<Key, TCell, Hash, Grower, Allocator>
{
public:
    using Self = HashSetTable;
    using Cell = TCell;

    using Base = HashTable<Key, TCell, Hash, Grower, Allocator>;
    using typename Base::LookupResult;

    using Base::Base;

    void merge(const Self & rhs)
    {
        if (!this->hasZero() && rhs.hasZero())
        {
            this->setHasZero();
            ++this->m_size;
        }

        for (size_t i = 0; i < rhs.grower.bufSize(); ++i)
            if (!rhs.buf[i].isZero(*this))
                this->insert(rhs.buf[i].getValue());
    }
};


template <
    typename Key,
    typename TCell, /// Supposed to have no state (HashTableNoState)
    typename Hash = DefaultHash<Key>,
    typename Grower = TwoLevelHashTableGrower<>,
    typename Allocator = HashTableAllocator>
class TwoLevelHashSetTable
    : public TwoLevelHashTable<Key, TCell, Hash, Grower, Allocator, HashSetTable<Key, TCell, Hash, Grower, Allocator>>
{
public:
    using Self = TwoLevelHashSetTable;
    using Base = TwoLevelHashTable<Key, TCell, Hash, Grower, Allocator, HashSetTable<Key, TCell, Hash, Grower, Allocator>>;

    using Base::Base;

    /// Writes its content in a way that it will be correctly read by HashSetTable.
    /// Used by uniqExact to preserve backward compatibility.
    void writeAsSingleLevel(DB::WriteBuffer & wb) const
    {
        DB::writeVarUInt(this->size(), wb);

        bool zero_written = false;
        for (size_t i = 0; i < Base::NUM_BUCKETS; ++i)
        {
            if (this->impls[i].hasZero())
            {
                if (zero_written)
                    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "No more than one zero value expected");
                this->impls[i].zeroValue()->write(wb);
                zero_written = true;
            }
        }

        static constexpr HashTableNoState state;
        for (auto ptr = this->begin(); ptr != this->end(); ++ptr)
            if (!ptr.getPtr()->isZero(state))
                ptr.getPtr()->write(wb);
    }
};


template <typename Key, typename Hash, typename TState = HashTableNoState>
struct HashSetCellWithSavedHash : public HashTableCell<Key, Hash, TState>
{
    using Base = HashTableCell<Key, Hash, TState>;

    size_t saved_hash;

    HashSetCellWithSavedHash() : Base() {}
    HashSetCellWithSavedHash(const Key & key_, const typename Base::State & state) : Base(key_, state) {}

    bool keyEquals(const Key & key_) const { return bitEquals(this->key, key_); }
    bool keyEquals(const Key & key_, size_t hash_) const { return saved_hash == hash_ && bitEquals(this->key, key_); }
    bool keyEquals(const Key & key_, size_t hash_, const typename Base::State &) const { return keyEquals(key_, hash_); }

    void setHash(size_t hash_value) { saved_hash = hash_value; }
    size_t getHash(const Hash & /*hash_function*/) const { return saved_hash; }
};

template <
    typename Key,
    typename Hash = DefaultHash<Key>,
    typename Grower = HashTableGrowerWithPrecalculation<>,
    typename Allocator = HashTableAllocator>
using HashSet = HashSetTable<Key, HashTableCell<Key, Hash>, Hash, Grower, Allocator>;

template <
    typename Key,
    typename Hash = DefaultHash<Key>,
    typename Grower = TwoLevelHashTableGrower<>,
    typename Allocator = HashTableAllocator>
using TwoLevelHashSet = TwoLevelHashSetTable<Key, HashTableCell<Key, Hash>, Hash, Grower, Allocator>;

template <typename Key, typename Hash, size_t initial_size_degree>
using HashSetWithStackMemory = HashSet<
    Key,
    Hash,
    HashTableGrower<initial_size_degree>,
    HashTableAllocatorWithStackMemory<
        (1ULL << initial_size_degree)
        * sizeof(HashTableCell<Key, Hash>)>>;

template <
    typename Key,
    typename Hash = DefaultHash<Key>,
    typename Grower = HashTableGrowerWithPrecalculation<>,
    typename Allocator = HashTableAllocator>
using HashSetWithSavedHash = HashSetTable<Key, HashSetCellWithSavedHash<Key, Hash>, Hash, Grower, Allocator>;

template <typename Key, typename Hash, size_t initial_size_degree>
using HashSetWithSavedHashWithStackMemory = HashSetWithSavedHash<
    Key,
    Hash,
    HashTableGrower<initial_size_degree>,
    HashTableAllocatorWithStackMemory<
        (1ULL << initial_size_degree)
        * sizeof(HashSetCellWithSavedHash<Key, Hash>)>>;
