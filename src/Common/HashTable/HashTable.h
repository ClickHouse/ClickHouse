#pragma once

#include <string.h>

#include <math.h>

#include <utility>

#include <boost/noncopyable.hpp>

#include <Core/Defines.h>
#include <common/types.h>
#include <Common/Exception.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>

#include <Common/HashTable/HashTableAllocator.h>
#include <Common/HashTable/HashTableKeyHolder.h>

#ifdef DBMS_HASH_MAP_DEBUG_RESIZES
    #include <iostream>
    #include <iomanip>
    #include <Common/Stopwatch.h>
#endif

/** NOTE HashTable could only be used for memmoveable (position independent) types.
  * Example: std::string is not position independent in libstdc++ with C++11 ABI or in libc++.
  * Also, key in hash table must be of type, that zero bytes is compared equals to zero key.
  */


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NO_AVAILABLE_DATA;
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
    /// Serialization, in binary and text form.
    void write(DB::WriteBuffer &) const         {}
    void writeText(DB::WriteBuffer &) const     {}

    /// Deserialization, in binary and text form.
    void read(DB::ReadBuffer &)                 {}
    void readText(DB::ReadBuffer &)             {}
};


/// These functions can be overloaded for custom types.
namespace ZeroTraits
{

template <typename T>
bool check(const T x) { return x == 0; }

template <typename T>
void set(T & x) { x = 0; }

}


/** Numbers are compared bitwise.
  * Complex types are compared by operator== as usual (this is important if there are gaps).
  *
  * This is needed if you use floats as keys. They are compared by bit equality.
  * Otherwise the invariants in hash table probing do not met when NaNs are present.
  */
template <typename T>
inline bool bitEquals(T && a, T && b)
{
    using RealT = std::decay_t<T>;

    if constexpr (std::is_floating_point_v<RealT>)
        return 0 == memcmp(&a, &b, sizeof(RealT));  /// Note that memcmp with constant size is compiler builtin.
    else
        return a == b;
}


/**
  * getKey/Mapped -- methods to get key/"mapped" values from the LookupResult returned by find() and
  * emplace() methods of HashTable. Must not be called for a null LookupResult.
  *
  * We don't use iterators for lookup result. Instead, LookupResult is a pointer of some kind. There
  * are methods getKey/Mapped, that return references or values to key/"mapped" values.
  *
  * Different hash table implementations support this interface to a varying degree:
  *
  * 1) Hash tables that store neither the key in its original form, nor a "mapped" value:
  *    FixedHashTable or StringHashTable. Neither GetKey nor GetMapped are supported, the only valid
  *    operation is checking LookupResult for null.
  *
  * 2) Hash maps that do not store the key, e.g. FixedHashMap or StringHashMap. Only GetMapped is
  *    supported.
  *
  * 3) Hash tables that store the key and do not have a "mapped" value, e.g. the normal HashTable.
  *    GetKey returns the key, and GetMapped returns a zero void pointer. This simplifies generic
  *    code that works with mapped values: it can overload on the return type of GetMapped(), and
  *    doesn't need other parameters. One example is insertSetMapped() function.
  *
  * 4) Hash tables that store both the key and the "mapped" value, e.g. HashMap. Both GetKey and
  *    GetMapped are supported.
  *
  * The implementation side goes as follows:
  *
  * for (1), LookupResult->getKey = const VoidKey, LookupResult->getMapped = VoidMapped;
  *
  * for (2), LookupResult->getKey = const VoidKey, LookupResult->getMapped = Mapped &;
  *
  * for (3) and (4), LookupResult->getKey = const Key [&], LookupResult->getMapped = Mapped &;
  * VoidKey and VoidMapped may have specialized function overloads for generic code.
  */

struct VoidKey {};
struct VoidMapped
{
    template <typename T>
    auto & operator=(const T &)
    {
        return *this;
    }
};

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

    using key_type = Key;
    using value_type = Key;
    using mapped_type = VoidMapped;

    Key key;

    HashTableCell() {}

    /// Create a cell with the given key / key and value.
    HashTableCell(const Key & key_, const State &) : key(key_) {}

    /// Get the key (externally).
    const Key & getKey() const { return key; }
    VoidMapped getMapped() const { return {}; }
    const value_type & getValue() const { return key; }

    /// Get the key (internally).
    static const Key & getKey(const value_type & value) { return value; }

    /// Are the keys at the cells equal?
    bool keyEquals(const Key & key_) const { return bitEquals(key, key_); }
    bool keyEquals(const Key & key_, size_t /*hash_*/) const { return bitEquals(key, key_); }
    bool keyEquals(const Key & key_, size_t /*hash_*/, const State & /*state*/) const { return bitEquals(key, key_); }

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

    /// Serialization, in binary and text form.
    void write(DB::WriteBuffer & wb) const         { DB::writeBinary(key, wb); }
    void writeText(DB::WriteBuffer & wb) const     { DB::writeDoubleQuoted(key, wb); }

    /// Deserialization, in binary and text form.
    void read(DB::ReadBuffer & rb)        { DB::readBinary(key, rb); }
    void readText(DB::ReadBuffer & rb)    { DB::readDoubleQuoted(key, rb); }
};

/**
  * A helper function for HashTable::insert() to set the "mapped" value.
  * Overloaded on the mapped type, does nothing if it's VoidMapped.
  */
template <typename ValueType>
void insertSetMapped(VoidMapped /* dest */, const ValueType & /* src */) {}

template <typename MappedType, typename ValueType>
void insertSetMapped(MappedType & dest, const ValueType & src) { dest = src.second; }


/** Determines the size of the hash table, and when and how much it should be resized.
  */
template <size_t initial_size_degree = 8>
struct HashTableGrower
{
    /// The state of this structure is enough to get the buffer size of the hash table.

    UInt8 size_degree = initial_size_degree;
    static constexpr auto initial_count = 1ULL << initial_size_degree;

    /// The size of the hash table in the cells.
    size_t bufSize() const               { return 1ULL << size_degree; }

    size_t maxFill() const               { return 1ULL << (size_degree - 1); }
    size_t mask() const                  { return bufSize() - 1; }

    /// From the hash value, get the cell number in the hash table.
    size_t place(size_t x) const         { return x & mask(); }

    /// The next cell in the collision resolution chain.
    size_t next(size_t pos) const        { ++pos; return pos & mask(); }

    /// Whether the hash table is sufficiently full. You need to increase the size of the hash table, or remove something unnecessary from it.
    bool overflow(size_t elems) const    { return elems > maxFill(); }

    /// Increase the size of the hash table.
    void increaseSize()
    {
        size_degree += size_degree >= 23 ? 1 : 2;
    }

    /// Set the buffer size by the number of elements in the hash table. Used when deserializing a hash table.
    void set(size_t num_elems)
    {
        size_degree = num_elems <= 1
             ? initial_size_degree
             : ((initial_size_degree > static_cast<size_t>(log2(num_elems - 1)) + 2)
                 ? initial_size_degree
                 : (static_cast<size_t>(log2(num_elems - 1)) + 2));
    }

    void setBufSize(size_t buf_size_)
    {
        size_degree = static_cast<size_t>(log2(buf_size_ - 1) + 1);
    }
};


/** When used as a Grower, it turns a hash table into something like a lookup table.
  * It remains non-optimal - the cells store the keys.
  * Also, the compiler can not completely remove the code of passing through the collision resolution chain, although it is not needed.
  * NOTE: Better to use FixedHashTable instead.
  */
template <size_t key_bits>
struct HashTableFixedGrower
{
    static constexpr auto initial_count = 1ULL << key_bits;
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

    void setHasZero()
    {
        has_zero = true;
        new (zeroValue()) Cell();
    }

    void clearHasZero()
    {
        has_zero = false;
        zeroValue()->~Cell();
    }

    Cell * zeroValue()             { return reinterpret_cast<Cell*>(&zero_value_storage); }
    const Cell * zeroValue() const { return reinterpret_cast<const Cell*>(&zero_value_storage); }
};

template <typename Cell>
struct ZeroValueStorage<false, Cell>
{
    bool hasZero() const { return false; }
    void setHasZero() { throw DB::Exception("HashTable: logical error", DB::ErrorCodes::LOGICAL_ERROR); }
    void clearHasZero() {}

    Cell * zeroValue()             { return nullptr; }
    const Cell * zeroValue() const { return nullptr; }
};


// The HashTable
template
<
    typename Key,
    typename Cell,
    typename Hash,
    typename Grower,
    typename Allocator
>
class HashTable :
    private boost::noncopyable,
    protected Hash,
    protected Allocator,
    protected Cell::State,
    protected ZeroValueStorage<Cell::need_zero_value_storage, Cell>     /// empty base optimization
{
public:
    // If we use an allocator with inline memory, check that the initial
    // size of the hash table is in sync with the amount of this memory.
    static constexpr size_t initial_buffer_bytes
        = Grower::initial_count * sizeof(Cell);
    static_assert(allocatorInitialBytes<Allocator> == 0
        || allocatorInitialBytes<Allocator> == initial_buffer_bytes);

protected:
    friend class const_iterator;
    friend class iterator;
    friend class Reader;

    template <typename, typename, typename, typename, typename, typename, size_t>
    friend class TwoLevelHashTable;

    template <typename, typename, size_t>
    friend class TwoLevelStringHashTable;

    template <typename SubMaps>
    friend class StringHashTable;

    using HashValue = size_t;
    using Self = HashTable;

    size_t m_size = 0;        /// Amount of elements
    Cell * buf;               /// A piece of memory for all elements except the element with zero key.
    Grower grower;

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
    mutable size_t collisions = 0;
#endif

    /// Find a cell with the same key or an empty cell, starting from the specified position and further along the collision resolution chain.
    size_t ALWAYS_INLINE findCell(const Key & x, size_t hash_value, size_t place_value) const
    {
        while (!buf[place_value].isZero(*this) && !buf[place_value].keyEquals(x, hash_value, *this))
        {
            place_value = grower.next(place_value);
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
            ++collisions;
#endif
        }

        return place_value;
    }


    /// Find an empty cell, starting with the specified position and further along the collision resolution chain.
    size_t ALWAYS_INLINE findEmptyCell(size_t place_value) const
    {
        while (!buf[place_value].isZero(*this))
        {
            place_value = grower.next(place_value);
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
            ++collisions;
#endif
        }

        return place_value;
    }

    void alloc(const Grower & new_grower)
    {
        buf = reinterpret_cast<Cell *>(Allocator::alloc(new_grower.bufSize() * sizeof(Cell)));
        grower = new_grower;
    }

    void free()
    {
        if (buf)
        {
            Allocator::free(buf, getBufferSizeInBytes());
            buf = nullptr;
        }
    }


    /// Increase the size of the buffer.
    void resize(size_t for_num_elems = 0, size_t for_buf_size = 0)
    {
#ifdef DBMS_HASH_MAP_DEBUG_RESIZES
        Stopwatch watch;
#endif

        size_t old_size = grower.bufSize();

        /** In case of exception for the object to remain in the correct state,
          *  changing the variable `grower` (which determines the buffer size of the hash table)
          *  is postponed for a moment after a real buffer change.
          * The temporary variable `new_grower` is used to determine the new size.
          */
        Grower new_grower = grower;

        if (for_num_elems)
        {
            new_grower.set(for_num_elems);
            if (new_grower.bufSize() <= old_size)
                return;
        }
        else if (for_buf_size)
        {
            new_grower.setBufSize(for_buf_size);
            if (new_grower.bufSize() <= old_size)
                return;
        }
        else
            new_grower.increaseSize();

        /// Expand the space.
        buf = reinterpret_cast<Cell *>(Allocator::realloc(buf, getBufferSizeInBytes(), new_grower.bufSize() * sizeof(Cell)));
        grower = new_grower;

        /** Now some items may need to be moved to a new location.
          * The element can stay in place, or move to a new location "on the right",
          *  or move to the left of the collision resolution chain, because the elements to the left of it have been moved to the new "right" location.
          */
        size_t i = 0;
        for (; i < old_size; ++i)
            if (!buf[i].isZero(*this) && !buf[i].isDeleted())
                reinsert(buf[i], buf[i].getHash(*this));

        /** There is also a special case:
          *    if the element was to be at the end of the old buffer,                  [        x]
          *    but is at the beginning because of the collision resolution chain,      [o       x]
          *    then after resizing, it will first be out of place again,               [        xo        ]
          *    and in order to transfer it where necessary,
          *    after transferring all the elements from the old halves you need to     [         o   x    ]
          *    process tail from the collision resolution chain immediately after it   [        o    x    ]
          */
        for (; !buf[i].isZero(*this) && !buf[i].isDeleted(); ++i)
            reinsert(buf[i], buf[i].getHash(*this));

#ifdef DBMS_HASH_MAP_DEBUG_RESIZES
        watch.stop();
        std::cerr << std::fixed << std::setprecision(3)
            << "Resize from " << old_size << " to " << grower.bufSize() << " took " << watch.elapsedSeconds() << " sec."
            << std::endl;
#endif
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

        auto & operator* () const { return *ptr; }
        auto * operator->() const { return ptr; }

        auto getPtr() const { return ptr; }
        size_t getHash() const { return ptr->getHash(*container); }

        size_t getCollisionChainLength() const
        {
            return container->grower.place((ptr - container->buf) - container->grower.place(getHash()));
        }

        /**
          * A hack for HashedDictionary.
          *
          * The problem: std-like find() returns an iterator, which has to be
          * compared to end(). On the other hand, HashMap::find() returns
          * LookupResult, which is compared to nullptr. HashedDictionary has to
          * support both hash maps with the same code, hence the need for this
          * hack.
          *
          * The proper way would be to remove iterator interface from our
          * HashMap completely, change all its users to the existing internal
          * iteration interface, and redefine end() to return LookupResult for
          * compatibility with std find(). Unfortunately, now is not the time to
          * do this.
          */
        operator Cell * () const { return nullptr; }
    };


public:
    using key_type = Key;
    using mapped_type = typename Cell::mapped_type;
    using value_type = typename Cell::value_type;
    using cell_type = Cell;

    using LookupResult = Cell *;
    using ConstLookupResult = const Cell *;

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

    HashTable(HashTable && rhs)
        : buf(nullptr)
    {
        *this = std::move(rhs);
    }

    ~HashTable()
    {
        destroyElements();
        free();
    }

    HashTable & operator= (HashTable && rhs)
    {
        destroyElements();
        free();

        std::swap(buf, rhs.buf);
        std::swap(m_size, rhs.m_size);
        std::swap(grower, rhs.grower);

        Hash::operator=(std::move(rhs));
        Allocator::operator=(std::move(rhs));
        Cell::State::operator=(std::move(rhs));
        ZeroValueStorage<Cell::need_zero_value_storage, Cell>::operator=(std::move(rhs));

        return *this;
    }

    class Reader final : private Cell::State
    {
    public:
        Reader(DB::ReadBuffer & in_)
            : in(in_)
        {
        }

        Reader(const Reader &) = delete;
        Reader & operator=(const Reader &) = delete;

        bool next()
        {
            if (!is_initialized)
            {
                Cell::State::read(in);
                DB::readVarUInt(size, in);
                is_initialized = true;
            }

            if (read_count == size)
            {
                is_eof = true;
                return false;
            }

            cell.read(in);
            ++read_count;

            return true;
        }

        inline const value_type & get() const
        {
            if (!is_initialized || is_eof)
                throw DB::Exception("No available data", DB::ErrorCodes::NO_AVAILABLE_DATA);

            return cell.getValue();
        }

    private:
        DB::ReadBuffer & in;
        Cell cell;
        size_t read_count = 0;
        size_t size = 0;
        bool is_eof = false;
        bool is_initialized = false;
    };


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

    const_iterator end() const
    {
        /// Avoid UBSan warning about adding zero to nullptr. It is valid in C++20 (and earlier) but not valid in C.
        return const_iterator(this, buf ? buf + grower.bufSize() : buf);
    }

    const_iterator cend() const
    {
        return end();
    }

    iterator end()
    {
        return iterator(this, buf ? buf + grower.bufSize() : buf);
    }


protected:
    const_iterator iteratorTo(const Cell * ptr) const { return const_iterator(this, ptr); }
    iterator iteratorTo(Cell * ptr)                   { return iterator(this, ptr); }
    const_iterator iteratorToZero() const             { return iteratorTo(this->zeroValue()); }
    iterator iteratorToZero()                         { return iteratorTo(this->zeroValue()); }


    /// If the key is zero, insert it into a special place and return true.
    /// We don't have to persist a zero key, because it's not actually inserted.
    /// That's why we just take a Key by value, an not a key holder.
    bool ALWAYS_INLINE emplaceIfZero(const Key & x, LookupResult & it, bool & inserted, size_t hash_value)
    {
        /// If it is claimed that the zero key can not be inserted into the table.
        if (!Cell::need_zero_value_storage)
            return false;

        if (Cell::isZero(x, *this))
        {
            it = this->zeroValue();

            if (!this->hasZero())
            {
                ++m_size;
                this->setHasZero();
                this->zeroValue()->setHash(hash_value);
                inserted = true;
            }
            else
                inserted = false;

            return true;
        }

        return false;
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplaceNonZeroImpl(size_t place_value, KeyHolder && key_holder,
                                          LookupResult & it, bool & inserted, size_t hash_value)
    {
        it = &buf[place_value];

        if (!buf[place_value].isZero(*this))
        {
            keyHolderDiscardKey(key_holder);
            inserted = false;
            return;
        }

        keyHolderPersistKey(key_holder);
        const auto & key = keyHolderGetKey(key_holder);

        new (&buf[place_value]) Cell(key, *this);
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

            // The hash table was rehashed, so we have to re-find the key.
            size_t new_place = findCell(key, hash_value, grower.place(hash_value));
            assert(!buf[new_place].isZero(*this));
            it = &buf[new_place];
        }
    }

    /// Only for non-zero keys. Find the right place, insert the key there, if it does not already exist. Set iterator to the cell in output parameter.
    template <typename KeyHolder>
    void ALWAYS_INLINE emplaceNonZero(KeyHolder && key_holder, LookupResult & it,
                                      bool & inserted, size_t hash_value)
    {
        const auto & key = keyHolderGetKey(key_holder);
        size_t place_value = findCell(key, hash_value, grower.place(hash_value));
        emplaceNonZeroImpl(place_value, key_holder, it, inserted, hash_value);
    }


public:
    void reserve(size_t num_elements)
    {
        resize(num_elements);
    }

    /// Insert a value. In the case of any more complex values, it is better to use the `emplace` function.
    std::pair<LookupResult, bool> ALWAYS_INLINE insert(const value_type & x)
    {
        std::pair<LookupResult, bool> res;

        size_t hash_value = hash(Cell::getKey(x));
        if (!emplaceIfZero(Cell::getKey(x), res.first, res.second, hash_value))
        {
            emplaceNonZero(Cell::getKey(x), res.first, res.second, hash_value);
        }

        if (res.second)
            insertSetMapped(res.first->getMapped(), x);

        return res;
    }


    /// Reinsert node pointed to by iterator
    void ALWAYS_INLINE reinsert(iterator & it, size_t hash_value)
    {
        reinsert(*it.getPtr(), hash_value);
    }


    /** Insert the key.
      * Return values:
      * 'it' -- a LookupResult pointing to the corresponding key/mapped pair.
      * 'inserted' -- whether a new key was inserted.
      *
      * You have to make `placement new` of value if you inserted a new key,
      * since when destroying a hash table, it will call the destructor!
      *
      * Example usage:
      *
      * Map::LookupResult it;
      * bool inserted;
      * map.emplace(key, it, inserted);
      * if (inserted)
      *     new (&it->getMapped()) Mapped(value);
      */
    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted)
    {
        const auto & key = keyHolderGetKey(key_holder);
        emplace(key_holder, it, inserted, hash(key));
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it,
                                  bool & inserted, size_t hash_value)
    {
        const auto & key = keyHolderGetKey(key_holder);
        if (!emplaceIfZero(key, it, inserted, hash_value))
            emplaceNonZero(key_holder, it, inserted, hash_value);
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

    LookupResult ALWAYS_INLINE find(const Key & x)
    {
        if (Cell::isZero(x, *this))
            return this->hasZero() ? this->zeroValue() : nullptr;

        size_t hash_value = hash(x);
        size_t place_value = findCell(x, hash_value, grower.place(hash_value));
        return !buf[place_value].isZero(*this) ? &buf[place_value] : nullptr;
    }

    ConstLookupResult ALWAYS_INLINE find(const Key & x) const
    {
        return const_cast<std::decay_t<decltype(*this)> *>(this)->find(x);
    }

    LookupResult ALWAYS_INLINE find(const Key & x, size_t hash_value)
    {
        if (Cell::isZero(x, *this))
            return this->hasZero() ? this->zeroValue() : nullptr;

        size_t place_value = findCell(x, hash_value, grower.place(hash_value));
        return !buf[place_value].isZero(*this) ? &buf[place_value] : nullptr;
    }

    ConstLookupResult ALWAYS_INLINE find(const Key & x, size_t hash_value) const
    {
        return const_cast<std::decay_t<decltype(*this)> *>(this)->find(x, hash_value);
    }

    bool ALWAYS_INLINE has(const Key & x) const
    {
        if (Cell::isZero(x, *this))
            return this->hasZero();

        size_t hash_value = hash(x);
        size_t place_value = findCell(x, hash_value, grower.place(hash_value));
        return !buf[place_value].isZero(*this);
    }


    bool ALWAYS_INLINE has(const Key & x, size_t hash_value) const
    {
        if (Cell::isZero(x, *this))
            return this->hasZero();

        size_t place_value = findCell(x, hash_value, grower.place(hash_value));
        return !buf[place_value].isZero(*this);
    }


    void write(DB::WriteBuffer & wb) const
    {
        Cell::State::write(wb);
        DB::writeVarUInt(m_size, wb);

        if (this->hasZero())
            this->zeroValue()->write(wb);

        if (!buf)
            return;

        for (auto ptr = buf, buf_end = buf + grower.bufSize(); ptr < buf_end; ++ptr)
            if (!ptr->isZero(*this))
                ptr->write(wb);
    }

    void writeText(DB::WriteBuffer & wb) const
    {
        Cell::State::writeText(wb);
        DB::writeText(m_size, wb);

        if (this->hasZero())
        {
            DB::writeChar(',', wb);
            this->zeroValue()->writeText(wb);
        }

        if (!buf)
            return;

        for (auto ptr = buf, buf_end = buf + grower.bufSize(); ptr < buf_end; ++ptr)
        {
            if (!ptr->isZero(*this))
            {
                DB::writeChar(',', wb);
                ptr->writeText(wb);
            }
        }
    }

    void read(DB::ReadBuffer & rb)
    {
        Cell::State::read(rb);

        destroyElements();
        this->clearHasZero();
        m_size = 0;

        size_t new_size = 0;
        DB::readVarUInt(new_size, rb);

        free();
        Grower new_grower = grower;
        new_grower.set(new_size);
        alloc(new_grower);

        for (size_t i = 0; i < new_size; ++i)
        {
            Cell x;
            x.read(rb);
            insert(x.getValue());
        }
    }

    void readText(DB::ReadBuffer & rb)
    {
        Cell::State::readText(rb);

        destroyElements();
        this->clearHasZero();
        m_size = 0;

        size_t new_size = 0;
        DB::readText(new_size, rb);

        free();
        Grower new_grower = grower;
        new_grower.set(new_size);
        alloc(new_grower);

        for (size_t i = 0; i < new_size; ++i)
        {
            Cell x;
            DB::assertChar(',', rb);
            x.readText(rb);
            insert(x.getValue());
        }
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

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
    size_t getCollisions() const
    {
        return collisions;
    }
#endif
};
