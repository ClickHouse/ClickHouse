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
    typename Hash
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

    void alloc()
    {
        buf = reinterpret_cast<Cell *>(calloc(0x100000 * sizeof(Cell), 1));
    }

    void free()
    {
        if (buf)
        {
            ::free(buf);
            buf = nullptr;
        }
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

        bool operator!= (const iterator_base & rhs) const { return ptr != rhs.ptr; }

        Derived & operator++()
        {
            /// If iterator was pointed to ZeroValueStorage, move it to the beginning of the main buffer.
            if (unlikely(ptr->isZero(*container)))
                ptr = container->buf;
            else
                ++ptr;

            /// Skip empty cells in the main buffer.
            auto buf_end = container->buf + 0x100000;
            while (ptr < buf_end && ptr->isZero(*container))
                ++ptr;

            return static_cast<Derived &>(*this);
        }

        auto * operator->() const { return &ptr->getValue(); }
    };


public:
    using key_type = Key;
    using value_type = typename Cell::value_type;

    size_t hash(const Key & x) const { return Hash::operator()(x); }


    HashTable()
    {
        if (Cell::need_zero_value_storage)
            this->zeroValue()->setZero();
        alloc();
    }

    ~HashTable()
    {
        free();
    }

    class iterator : public iterator_base<iterator, false>
    {
    public:
        using iterator_base<iterator, false>::iterator_base;
    };

    iterator begin()
    {
        if (!buf)
            return end();

        Cell * ptr = buf;
        auto buf_end = buf + 0x100000;
        while (ptr < buf_end && ptr->isZero(*this))
            ++ptr;

        return iterator(this, ptr);
    }

    iterator end()                     { return iterator(this, buf + 0x100000); }


protected:
    /// Only for non-zero keys. Find the right place, insert the key there, if it does not already exist. Set iterator to the cell in output parameter.
    void ALWAYS_INLINE emplaceNonZero(Key x)
    {
        buf[x].value.first = x;
        ++m_size;
    }


public:
    void insert(const value_type & x)
    {
        emplaceNonZero(x.first);
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


int main(int, char **)
{
    using namespace DB;

    HashTable<UInt64, HashMapCell<UInt64, UInt64, TrivialHash>, TrivialHash> map;

    map.insert({12345, 1});

    for (auto it = map.begin(); it != map.end(); ++it)
        std::cerr << it->first << ": " << it->second << "\n";

    return 0;
}
