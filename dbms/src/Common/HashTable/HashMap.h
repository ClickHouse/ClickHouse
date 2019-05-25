#pragma once

#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashTable.h>
#include <Common/HashTable/HashTableAllocator.h>


/** NOTE HashMap could only be used for memmoveable (position independent) types.
  * Example: std::string is not position independent in libstdc++ with C++11 ABI or in libc++.
  * Also, key in hash table must be of type, that zero bytes is compared equals to zero key.
  */


struct NoInitTag
{
};

/// A pair that does not initialize the elements, if not needed.
template <typename First, typename Second>
struct PairNoInit
{
    First first;
    Second second;

    PairNoInit() {}

    template <typename First_>
    PairNoInit(First_ && first_, NoInitTag) : first(std::forward<First_>(first_))
    {
    }

    template <typename First_, typename Second_>
    PairNoInit(First_ && first_, Second_ && second_) : first(std::forward<First_>(first_)), second(std::forward<Second_>(second_))
    {
    }
};


template <typename Key, typename TMapped, typename Hash, typename TState = HashTableNoState>
struct HashMapCell
{
    using Mapped = TMapped;
    using State = TState;

    using value_type = PairNoInit<Key, Mapped>;
    value_type value;

    HashMapCell() {}
    HashMapCell(const Key & key_, const State &) : value(key_, NoInitTag()) {}
    HashMapCell(const value_type & value_, const State &) : value(value_) {}

    Key & getFirstMutable() { return value.first; }
    const Key & getFirst() const { return value.first; }
    Mapped & getSecond() { return value.second; }
    const Mapped & getSecond() const { return value.second; }

    const value_type & getValue() const { return value; }

    Key & getKey() { return value.first; }
    static const Key & getKey(const value_type & value) { return value.first; }

    bool keyEquals(const Key & key_) const { return value.first == key_; }
    bool keyEquals(const Key & key_, size_t /*hash_*/) const { return value.first == key_; }
    bool keyEquals(const Key & key_, size_t /*hash_*/, const State & /*state*/) const { return value.first == key_; }

    void setHash(size_t /*hash_value*/) {}
    size_t getHash(const Hash & hash) const { return hash(value.first); }

    bool isZero(const State & state) const { return isZero(value.first, state); }
    static bool isZero(const Key & key, const State & /*state*/) { return ZeroTraits::check(key); }

    /// Set the key value to zero.
    void setZero() { ZeroTraits::set(value.first); }

    /// Do I need to store the zero key separately (that is, can a zero key be inserted into the hash table).
    static constexpr bool need_zero_value_storage = true;

    /// Whether the cell was deleted.
    bool isDeleted() const { return false; }

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
};


template <typename Key, typename TMapped, typename Hash, typename TState = HashTableNoState>
struct HashMapCellWithSavedHash : public HashMapCell<Key, TMapped, Hash, TState>
{
    using Base = HashMapCell<Key, TMapped, Hash, TState>;

    size_t saved_hash;

    using Base::Base;

    bool keyEquals(const Key & key_) const { return this->value.first == key_; }
    bool keyEquals(const Key & key_, size_t hash_) const { return saved_hash == hash_ && this->value.first == key_; }
    bool keyEquals(const Key & key_, size_t hash_, const typename Base::State &) const { return keyEquals(key_, hash_); }

    void setHash(size_t hash_value) { saved_hash = hash_value; }
    size_t getHash(const Hash & /*hash_function*/) const { return saved_hash; }
};


template <
    typename Key,
    typename Cell,
    typename Hash = DefaultHash<Key>,
    typename Grower = HashTableGrower<>,
    typename Allocator = HashTableAllocator>
class HashMapTable : public HashTable<Key, Cell, Hash, Grower, Allocator>
{
public:
    using Self = HashMapTable;
    using key_type = Key;
    using mapped_type = typename Cell::Mapped;
    using value_type = typename Cell::value_type;

    using HashTable<Key, Cell, Hash, Grower, Allocator>::HashTable;

    /// Merge every cell's value of current map into the destination map via emplace.
    ///  Func should have signature void(Mapped & dst, Mapped & src, bool emplaced).
    ///  Each filled cell in current map will invoke func once. If that map doesn't
    ///  have a key equals to the given cell, a new cell gets emplaced into that map,
    ///  and func is invoked with the third argument emplaced set to true. Otherwise
    ///  emplaced is set to false.
    template <typename Func>
    void ALWAYS_INLINE mergeToViaEmplace(Self & that, Func && func)
    {
        for (auto it = this->begin(), end = this->end(); it != end; ++it)
        {
            decltype(it) res_it;
            bool inserted;
            that.emplace(it->getFirst(), res_it, inserted, it.getHash());
            func(res_it->getSecond(), it->getSecond(), inserted);
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
            decltype(it) res_it = that.find(it->getFirst(), it.getHash());
            if (res_it == that.end())
                func(it->getSecond(), it->getSecond(), false);
            else
                func(res_it->getSecond(), it->getSecond(), true);
        }
    }

    template <typename Func>
    void forEachValue(Func && func)
    {
        for (auto & v : *this)
            func(v);
    }

    template <typename Func>
    void forEachMapped(Func && func)
    {
        for (auto & v : *this)
            func(v.getSecond());
    }

    mapped_type & ALWAYS_INLINE operator[](Key x)
    {
        typename HashMapTable::iterator it;
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
            new (&it->getSecond()) mapped_type();

        return it->getSecond();
    }
};


template <
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = HashTableGrower<>,
    typename Allocator = HashTableAllocator>
using HashMap = HashMapTable<Key, HashMapCell<Key, Mapped, Hash>, Hash, Grower, Allocator>;


template <
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = HashTableGrower<>,
    typename Allocator = HashTableAllocator>
using HashMapWithSavedHash = HashMapTable<Key, HashMapCellWithSavedHash<Key, Mapped, Hash>, Hash, Grower, Allocator>;
