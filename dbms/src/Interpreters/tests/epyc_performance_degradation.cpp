#include <iostream>
#include <Core/Types.h>

#include <string.h>

#include <math.h>

#include <utility>

#include <boost/noncopyable.hpp>

#include <common/likely.h>

#include <Core/Defines.h>
#include <Core/Types.h>
#include <Common/Exception.h>


/// It is reasonable to use for UInt8, UInt16 with sufficient hash table size.
struct TrivialHash
{
    template <typename T>
    size_t operator() (T key) const
    {
        return key;
    }
};


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}


/** The state of the hash table that affects the properties of its cells.
  * Used as a template parameter.
  * For example, there is an implementation of an instantly clearable hash table - ClearableHashMap.
  * For it, each cell holds the version number, and in the hash table itself is the current version.
  *  When clearing, the current version simply increases; All cells with a mismatching version are considered empty.
  *  Another example: for an approximate calculation of the number of unique visitors, there is a hash table for UniquesHashSet.
  *  It has the concept of "degree". At each overflow, cells with keys that do not divide by the corresponding power of the two are deleted.
  */
struct HashTableNoState
{
};


/// These functions can be overloaded for custom types.
namespace ZeroTraits
{

template <typename T>
bool check(const T x) { return x == 0; }

template <typename T>
void set(T & x) { x = 0; }

}


/** Compile-time interface for cell of the hash table.
  * Different cell types are used to implement different hash tables.
  * The cell must contain a key.
  * It can also contain a value and arbitrary additional data
  *  (example: the stored hash value; version number for ClearableHashMap).
  */
template <typename Key, typename Hash, typename TState = HashTableNoState>
struct HashTableCell
{
    using State = TState;

    using value_type = Key;
    Key key;

    HashTableCell() {}

    /// Create a cell with the given key / key and value.
    HashTableCell(const Key & key_, const State &) : key(key_) {}
/// HashTableCell(const value_type & value_, const State & state) : key(value_) {}

    /// Get what the value_type of the container will be.
    value_type & getValue()             { return key; }
    const value_type & getValue() const { return key; }

    /// Get the key.
    static Key & getKey(value_type & value)             { return value; }
    static const Key & getKey(const value_type & value) { return value; }

    /// Are the keys at the cells equal?
    bool keyEquals(const Key & key_) const { return key == key_; }
    bool keyEquals(const Key & key_, size_t /*hash_*/) const { return key == key_; }
    bool keyEquals(const Key & key_, size_t /*hash_*/, const State & /*state*/) const { return key == key_; }

    /// If the cell can remember the value of the hash function, then remember it.
    void setHash(size_t /*hash_value*/) {}

    /// If the cell can store the hash value in itself, then return the stored value.
    /// It must be at least once calculated before.
    /// If storing the hash value is not provided, then just compute the hash.
    size_t getHash(const Hash & hash) const { return hash(key); }

    /// Whether the key is zero. In the main buffer, cells with a zero key are considered empty.
    /// If zero keys can be inserted into the table, then the cell for the zero key is stored separately, not in the main buffer.
    /// Zero keys must be such that the zeroed-down piece of memory is a zero key.
    bool isZero(const State & state) const { return isZero(key, state); }
    static bool isZero(const Key & key, const State & /*state*/) { return ZeroTraits::check(key); }

    /// Set the key value to zero.
    void setZero() { ZeroTraits::set(key); }

    /// Do the hash table need to store the zero key separately (that is, can a zero key be inserted into the hash table).
    static constexpr bool need_zero_value_storage = true;

    /// Whether the cell is deleted.
    bool isDeleted() const { return false; }

    /// Set the mapped value, if any (for HashMap), to the corresponding `value`.
    void setMapped(const value_type & /*value*/) {}
};


/** When used as a Grower, it turns a hash table into something like a lookup table.
  * It remains non-optimal - the cells store the keys.
  * Also, the compiler can not completely remove the code of passing through the collision resolution chain, although it is not needed.
  * TODO Make a proper lookup table.
  */
template <size_t key_bits>
struct HashTableFixedGrower
{
    size_t bufSize() const               { return 1ULL << key_bits; }
    size_t place(size_t x) const         { return x; }
    /// You could write __builtin_unreachable(), but the compiler does not optimize everything, and it turns out less efficiently.
    size_t next(size_t pos) const        { return pos + 1; }
    bool overflow(size_t /*elems*/) const { return false; }

    void increaseSize() { __builtin_unreachable(); }
    void set(size_t /*num_elems*/) {}
    void setBufSize(size_t /*buf_size_*/) {}
};


/** If you want to store the zero key separately - a place to store it. */
template <bool need_zero_value_storage, typename Cell>
struct ZeroValueStorage;

template <typename Cell>
struct ZeroValueStorage<true, Cell>
{
private:
    bool has_zero = false;
    std::aligned_storage_t<sizeof(Cell), alignof(Cell)> zero_value_storage; /// Storage of element with zero key.

public:
    bool hasZero() const { return has_zero; }
    void setHasZero() { has_zero = true; }
    void clearHasZero() { has_zero = false; }

    Cell * zeroValue()              { return reinterpret_cast<Cell*>(&zero_value_storage); }
    const Cell * zeroValue() const  { return reinterpret_cast<const Cell*>(&zero_value_storage); }
};

template <typename Cell>
struct ZeroValueStorage<false, Cell>
{
    bool hasZero() const { return false; }
    void setHasZero() { throw DB::Exception("HashTable: logical error", DB::ErrorCodes::LOGICAL_ERROR); }
    void clearHasZero() {}

    Cell * zeroValue()              { return nullptr; }
    const Cell * zeroValue() const  { return nullptr; }
};


template
<
    typename Key,
    typename Cell,
    typename Hash,
    typename Grower
>
class HashTable :
    private boost::noncopyable,
    protected Hash,
    protected Cell::State,
    protected ZeroValueStorage<Cell::need_zero_value_storage, Cell>     /// empty base optimization
{
protected:
    friend class const_iterator;
    friend class iterator;

    using HashValue = size_t;
    using Self = HashTable;
    using cell_type = Cell;

    size_t m_size = 0;        /// Amount of elements
    Cell * buf;               /// A piece of memory for all elements except the element with zero key.
    Grower grower;

    /// Find a cell with the same key or an empty cell, starting from the specified position and further along the collision resolution chain.
    template <typename ObjectToCompareWith>
    size_t ALWAYS_INLINE findCell(const ObjectToCompareWith & x, size_t hash_value, size_t place_value) const
    {
        while (!buf[place_value].isZero(*this) && !buf[place_value].keyEquals(x, hash_value, *this))
            place_value = grower.next(place_value);

        return place_value;
    }


    /// Find an empty cell, starting with the specified position and further along the collision resolution chain.
    size_t ALWAYS_INLINE findEmptyCell(size_t place_value) const
    {
        while (!buf[place_value].isZero(*this))
            place_value = grower.next(place_value);

        return place_value;
    }

    void alloc(const Grower & new_grower)
    {
        buf = reinterpret_cast<Cell *>(calloc(new_grower.bufSize() * sizeof(Cell), 1));
        grower = new_grower;
    }

    void free()
    {
        if (buf)
        {
            ::free(buf);
            buf = nullptr;
        }
    }


    /// Increase the size of the buffer.
    void resize()
    {
    }


    /** Paste into the new buffer the value that was in the old buffer.
      * Used when increasing the buffer size.
      */
    void reinsert(Cell & x, size_t hash_value)
    {
        size_t place_value = grower.place(hash_value);

        /// If the element is in its place.
        if (&x == &buf[place_value])
            return;

        /// Compute a new location, taking into account the collision resolution chain.
        place_value = findCell(Cell::getKey(x.getValue()), hash_value, place_value);

        /// If the item remains in its place in the old collision resolution chain.
        if (!buf[place_value].isZero(*this))
            return;

        /// Copy to a new location and zero the old one.
        x.setHash(hash_value);
        memcpy(static_cast<void*>(&buf[place_value]), &x, sizeof(x));
        x.setZero();

        /// Then the elements that previously were in collision with this can move to the old place.
    }


    void destroyElements()
    {
        if (!std::is_trivially_destructible_v<Cell>)
            for (iterator it = begin(), it_end = end(); it != it_end; ++it)
                it.ptr->~Cell();
    }


    template <typename Derived, bool is_const>
    class iterator_base
    {
        using Container = std::conditional_t<is_const, const Self, Self>;
        using cell_type = std::conditional_t<is_const, const Cell, Cell>;

        Container * container;
        cell_type * ptr;

        friend class HashTable;

    public:
        iterator_base() {}
        iterator_base(Container * container_, cell_type * ptr_) : container(container_), ptr(ptr_) {}

        bool operator== (const iterator_base & rhs) const { return ptr == rhs.ptr; }
        bool operator!= (const iterator_base & rhs) const { return ptr != rhs.ptr; }

        Derived & operator++()
        {
            /// If iterator was pointed to ZeroValueStorage, move it to the beginning of the main buffer.
            if (unlikely(ptr->isZero(*container)))
                ptr = container->buf;
            else
                ++ptr;

            /// Skip empty cells in the main buffer.
            auto buf_end = container->buf + container->grower.bufSize();
            while (ptr < buf_end && ptr->isZero(*container))
                ++ptr;

            return static_cast<Derived &>(*this);
        }

        auto & operator* () const { return ptr->getValue(); }
        auto * operator->() const { return &ptr->getValue(); }

        auto getPtr() const { return ptr; }
        size_t getHash() const { return ptr->getHash(*container); }

        size_t getCollisionChainLength() const
        {
            return container->grower.place((ptr - container->buf) - container->grower.place(getHash()));
        }
    };


public:
    using key_type = Key;
    using value_type = typename Cell::value_type;

    size_t hash(const Key & x) const { return Hash::operator()(x); }


    HashTable()
    {
        if (Cell::need_zero_value_storage)
            this->zeroValue()->setZero();
        alloc(grower);
    }

    HashTable(size_t reserve_for_num_elements)
    {
        if (Cell::need_zero_value_storage)
            this->zeroValue()->setZero();
        grower.set(reserve_for_num_elements);
        alloc(grower);
    }

    ~HashTable()
    {
        destroyElements();
        free();
    }

    class iterator : public iterator_base<iterator, false>
    {
    public:
        using iterator_base<iterator, false>::iterator_base;
    };

    class const_iterator : public iterator_base<const_iterator, true>
    {
    public:
        using iterator_base<const_iterator, true>::iterator_base;
    };


    const_iterator begin() const
    {
        if (!buf)
            return end();

        if (this->hasZero())
            return iteratorToZero();

        const Cell * ptr = buf;
        auto buf_end = buf + grower.bufSize();
        while (ptr < buf_end && ptr->isZero(*this))
            ++ptr;

        return const_iterator(this, ptr);
    }

    const_iterator cbegin() const { return begin(); }

    iterator begin()
    {
        if (!buf)
            return end();

        if (this->hasZero())
            return iteratorToZero();

        Cell * ptr = buf;
        auto buf_end = buf + grower.bufSize();
        while (ptr < buf_end && ptr->isZero(*this))
            ++ptr;

        return iterator(this, ptr);
    }

    const_iterator end() const         { return const_iterator(this, buf + grower.bufSize()); }
    const_iterator cend() const        { return end(); }
    iterator end()                     { return iterator(this, buf + grower.bufSize()); }


protected:
    const_iterator iteratorTo(const Cell * ptr) const { return const_iterator(this, ptr); }
    iterator iteratorTo(Cell * ptr)                   { return iterator(this, ptr); }
    const_iterator iteratorToZero() const             { return iteratorTo(this->zeroValue()); }
    iterator iteratorToZero()                         { return iteratorTo(this->zeroValue()); }


    /// If the key is zero, insert it into a special place and return true.
    bool ALWAYS_INLINE emplaceIfZero(Key x, iterator & it, bool & inserted, size_t hash_value)
    {
        /// If it is claimed that the zero key can not be inserted into the table.
        if (!Cell::need_zero_value_storage)
            return false;

        if (Cell::isZero(x, *this))
        {
            it = iteratorToZero();
            if (!this->hasZero())
            {
                ++m_size;
                this->setHasZero();
                it.ptr->setHash(hash_value);
                inserted = true;
            }
            else
                inserted = false;

            return true;
        }

        return false;
    }


    /// Only for non-zero keys. Find the right place, insert the key there, if it does not already exist. Set iterator to the cell in output parameter.
    void ALWAYS_INLINE emplaceNonZero(Key x, iterator & it, bool & inserted, size_t hash_value)
    {
        size_t place_value = findCell(x, hash_value, grower.place(hash_value));

        it = iterator(this, &buf[place_value]);

        if (!buf[place_value].isZero(*this))
        {
            inserted = false;
            return;
        }

        new(&buf[place_value]) Cell(x, *this);
        buf[place_value].setHash(hash_value);
        inserted = true;
        ++m_size;

        if (unlikely(grower.overflow(m_size)))
        {
            try
            {
                resize();
            }
            catch (...)
            {
                /** If we have not resized successfully, then there will be problems.
                  * There remains a key, but uninitialized mapped-value,
                  *  which, perhaps, can not even be called a destructor.
                  */
                --m_size;
                buf[place_value].setZero();
                throw;
            }

            it = find(x, hash_value);
        }
    }


public:
    /// Insert a value. In the case of any more complex values, it is better to use the `emplace` function.
    std::pair<iterator, bool> ALWAYS_INLINE insert(const value_type & x)
    {
        std::pair<iterator, bool> res;

        size_t hash_value = hash(Cell::getKey(x));
        if (!emplaceIfZero(Cell::getKey(x), res.first, res.second, hash_value))
            emplaceNonZero(Cell::getKey(x), res.first, res.second, hash_value);

        if (res.second)
            res.first.ptr->setMapped(x);

        return res;
    }


    /// Reinsert node pointed to by iterator
    void ALWAYS_INLINE reinsert(iterator & it, size_t hash_value)
    {
        reinsert(*it.getPtr(), hash_value);
    }


    /** Insert the key,
      * return an iterator to a position that can be used for `placement new` of value,
      * as well as the flag - whether a new key was inserted.
      *
      * You have to make `placement new` of value if you inserted a new key,
      * since when destroying a hash table, it will call the destructor!
      *
      * Example usage:
      *
      * Map::iterator it;
      * bool inserted;
      * map.emplace(key, it, inserted);
      * if (inserted)
      *     new(&it->second) Mapped(value);
      */
    void ALWAYS_INLINE emplace(Key x, iterator & it, bool & inserted)
    {
        size_t hash_value = hash(x);
        if (!emplaceIfZero(x, it, inserted, hash_value))
            emplaceNonZero(x, it, inserted, hash_value);
    }


    /// Same, but with a precalculated value of hash function.
    void ALWAYS_INLINE emplace(Key x, iterator & it, bool & inserted, size_t hash_value)
    {
        if (!emplaceIfZero(x, it, inserted, hash_value))
            emplaceNonZero(x, it, inserted, hash_value);
    }


    /// Copy the cell from another hash table. It is assumed that the cell is not zero, and also that there was no such key in the table yet.
    void ALWAYS_INLINE insertUniqueNonZero(const Cell * cell, size_t hash_value)
    {
        size_t place_value = findEmptyCell(grower.place(hash_value));

        memcpy(static_cast<void*>(&buf[place_value]), cell, sizeof(*cell));
        ++m_size;

        if (unlikely(grower.overflow(m_size)))
            resize();
    }


    template <typename ObjectToCompareWith>
    iterator ALWAYS_INLINE find(ObjectToCompareWith x)
    {
        if (Cell::isZero(x, *this))
            return this->hasZero() ? iteratorToZero() : end();

        size_t hash_value = hash(x);
        size_t place_value = findCell(x, hash_value, grower.place(hash_value));
        return !buf[place_value].isZero(*this) ? iterator(this, &buf[place_value]) : end();
    }


    template <typename ObjectToCompareWith>
    const_iterator ALWAYS_INLINE find(ObjectToCompareWith x) const
    {
        if (Cell::isZero(x, *this))
            return this->hasZero() ? iteratorToZero() : end();

        size_t hash_value = hash(x);
        size_t place_value = findCell(x, hash_value, grower.place(hash_value));
        return !buf[place_value].isZero(*this) ? const_iterator(this, &buf[place_value]) : end();
    }


    template <typename ObjectToCompareWith>
    iterator ALWAYS_INLINE find(ObjectToCompareWith x, size_t hash_value)
    {
        if (Cell::isZero(x, *this))
            return this->hasZero() ? iteratorToZero() : end();

        size_t place_value = findCell(x, hash_value, grower.place(hash_value));
        return !buf[place_value].isZero(*this) ? iterator(this, &buf[place_value]) : end();
    }


    template <typename ObjectToCompareWith>
    const_iterator ALWAYS_INLINE find(ObjectToCompareWith x, size_t hash_value) const
    {
        if (Cell::isZero(x, *this))
            return this->hasZero() ? iteratorToZero() : end();

        size_t place_value = findCell(x, hash_value, grower.place(hash_value));
        return !buf[place_value].isZero(*this) ? const_iterator(this, &buf[place_value]) : end();
    }


    bool ALWAYS_INLINE has(Key x) const
    {
        if (Cell::isZero(x, *this))
            return this->hasZero();

        size_t hash_value = hash(x);
        size_t place_value = findCell(x, hash_value, grower.place(hash_value));
        return !buf[place_value].isZero(*this);
    }


    bool ALWAYS_INLINE has(Key x, size_t hash_value) const
    {
        if (Cell::isZero(x, *this))
            return this->hasZero();

        size_t place_value = findCell(x, hash_value, grower.place(hash_value));
        return !buf[place_value].isZero(*this);
    }


    size_t size() const
    {
        return m_size;
    }

    bool empty() const
    {
        return 0 == m_size;
    }

    void clear()
    {
        destroyElements();
        this->clearHasZero();
        m_size = 0;

        memset(static_cast<void*>(buf), 0, grower.bufSize() * sizeof(*buf));
    }

    /// After executing this function, the table can only be destroyed,
    ///  and also you can use the methods `size`, `empty`, `begin`, `end`.
    void clearAndShrink()
    {
        destroyElements();
        this->clearHasZero();
        m_size = 0;
        free();
    }

    size_t getBufferSizeInBytes() const
    {
        return grower.bufSize() * sizeof(Cell);
    }

    size_t getBufferSizeInCells() const
    {
        return grower.bufSize();
    }
};


struct NoInitTag {};

/// A pair that does not initialize the elements, if not needed.
template <typename First, typename Second>
struct PairNoInit
{
    First first;
    Second second;

    PairNoInit() {}

    template <typename First_>
    PairNoInit(First_ && first_, NoInitTag)
        : first(std::forward<First_>(first_)) {}

    template <typename First_, typename Second_>
    PairNoInit(First_ && first_, Second_ && second_)
        : first(std::forward<First_>(first_)), second(std::forward<Second_>(second_)) {}
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

    value_type & getValue() { return value; }
    const value_type & getValue() const { return value; }

    static Key & getKey(value_type & value) { return value.first; }
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
};


template
<
    typename Key,
    typename Cell,
    typename Hash,
    typename Grower
>
class HashMapTable : public HashTable<Key, Cell, Hash, Grower>
{
public:
    using key_type = Key;
    using mapped_type = typename Cell::Mapped;
    using value_type = typename Cell::value_type;

    using HashTable<Key, Cell, Hash, Grower>::HashTable;

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
            new(&it->second) mapped_type();

        return it->second;
    }
};


template
<
    typename Key,
    typename Mapped,
    typename Hash,
    typename Grower
>
using HashMap = HashMapTable<Key, HashMapCell<Key, Mapped, Hash>, Hash, Grower>;



int main(int, char **)
{
    using namespace DB;

    HashMap<UInt64, UInt64, TrivialHash, HashTableFixedGrower<16>> map;

    map[12345] = 1;

    for (auto it = map.begin(); it != map.end(); ++it)
        std::cerr << it->first << ": " << it->second << "\n";

    return 0;
}
