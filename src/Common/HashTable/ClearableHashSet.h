#pragma once

#include <type_traits>
#include <Common/HashTable/HashSet.h>


/** A hash table that allows you to clear the table in O(1).
  * Even simpler than HashSet: Key and Mapped must be POD-types.
  *
  * Instead of this class, you could just use the pair (version, key) in the HashSet as the key
  * but then the table would accumulate all the keys that it ever stored, and it was unreasonably growing.
  * This class goes a step further and considers the keys with the old version empty in the hash table.
  */


struct ClearableHashSetState
{
    UInt32 version = 1;

    /// Serialization, in binary and text form.
    void write(DB::WriteBuffer & wb) const         { DB::writeBinary(version, wb); }
    void writeText(DB::WriteBuffer & wb) const     { DB::writeText(version, wb); }

    /// Deserialization, in binary and text form.
    void read(DB::ReadBuffer & rb)                 { DB::readBinary(version, rb); }
    void readText(DB::ReadBuffer & rb)             { DB::readText(version, rb); }
};


template <typename Key, typename BaseCell>
struct ClearableHashTableCell : public BaseCell
{
    using State = ClearableHashSetState;
    using value_type = typename BaseCell::value_type;

    UInt32 version;

    bool isZero(const State & state) const { return version != state.version; }
    static bool isZero(const Key & /*key*/, const State & /*state*/) { return false; }

    /// Set the key value to zero.
    void setZero() { version = 0; }

    /// Do I need to store the zero key separately (that is, can a zero key be inserted into the hash table).
    static constexpr bool need_zero_value_storage = false;

    ClearableHashTableCell() {} //-V730
    ClearableHashTableCell(const Key & key_, const State & state) : BaseCell(key_, state), version(state.version) {}
};

template
<
    typename Key,
    typename Hash = DefaultHash<Key>,
    typename Grower = HashTableGrower<>,
    typename Allocator = HashTableAllocator
>
class ClearableHashSet : public HashTable<Key, ClearableHashTableCell<Key, HashTableCell<Key, Hash, ClearableHashSetState>>, Hash, Grower, Allocator>
{
public:
    using Base = HashTable<Key, ClearableHashTableCell<Key, HashTableCell<Key, Hash, ClearableHashSetState>>, Hash, Grower, Allocator>;
    using typename Base::LookupResult;

    void clear()
    {
        ++this->version;
        this->m_size = 0;
    }
};

template
<
    typename Key,
    typename Hash = DefaultHash<Key>,
    typename Grower = HashTableGrower<>,
    typename Allocator = HashTableAllocator
>
class ClearableHashSetWithSavedHash: public HashTable<Key, ClearableHashTableCell<Key, HashSetCellWithSavedHash<Key, Hash, ClearableHashSetState>>, Hash, Grower, Allocator>
{
public:
    void clear()
    {
        ++this->version;
        this->m_size = 0;
    }
};

template <typename Key, typename Hash, size_t initial_size_degree>
using ClearableHashSetWithStackMemory = ClearableHashSet<
    Key,
    Hash,
    HashTableGrower<initial_size_degree>,
    HashTableAllocatorWithStackMemory<
        (1ULL << initial_size_degree)
        * sizeof(
            ClearableHashTableCell<
                Key,
                HashTableCell<Key, Hash, ClearableHashSetState>>)>>;
