#include <iostream>
#include <Core/Types.h>

#include <string.h>

#include <math.h>

#include <utility>

#include <boost/noncopyable.hpp>

#include <common/likely.h>

#include <Core/Defines.h>
#include <Core/Types.h>


template <typename Key>
struct HashTableCell
{
    using value_type = Key;
    Key key;

    HashTableCell() {}

    HashTableCell(const Key & key_) : key(key_) {}

    value_type & getValue()             { return key; }
    const value_type & getValue() const { return key; }

    static Key & getKey(value_type & value)             { return value; }
    static const Key & getKey(const value_type & value) { return value; }

    bool keyEquals(const Key & key_) const { return key == key_; }

    bool isZero() const { return key == 0; }

    /// Set the key value to zero.
    void setZero() { key = 0; }
};


template <typename Cell>
struct ZeroValueStorage
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


template
<
    typename Key,
    typename Cell
>
class HashTable :
    protected ZeroValueStorage<Cell>     /// empty base optimization
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
            if (unlikely(ptr->isZero()))
                ptr = container->buf;
            else
                ++ptr;

            /// Skip empty cells in the main buffer.
            auto buf_end = container->buf + 0x100000;
            while (ptr < buf_end && ptr->isZero())
                ++ptr;

            return static_cast<Derived &>(*this);
        }

        auto * operator->() const { return &ptr->getValue(); }
    };


public:
    using key_type = Key;
    using value_type = typename Cell::value_type;

    HashTable()
    {
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
        while (ptr < buf_end && ptr->isZero())
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


template <typename Key, typename TMapped>
struct HashMapCell
{
    using Mapped = TMapped;

    using value_type = PairNoInit<Key, Mapped>;
    value_type value;

    HashMapCell() {}
    HashMapCell(const Key & key_) : value(key_, NoInitTag()) {}
    HashMapCell(const value_type & value_) : value(value_) {}

    value_type & getValue() { return value; }
    const value_type & getValue() const { return value; }

    static Key & getKey(value_type & value) { return value.first; }
    static const Key & getKey(const value_type & value) { return value.first; }

    bool keyEquals(const Key & key_) const { return value.first == key_; }

    bool isZero() const { return value.first == 0; }
    static bool isZero(const Key & key) { return key == 0; }

    /// Set the key value to zero.
    void setZero() { value.first = 0; }

    void setMapped(const value_type & value_) { value.second = value_.second; }
};


int main(int, char **)
{
    using namespace DB;

    HashTable<UInt64, HashMapCell<UInt64, UInt64>> map;

    map.insert({12345, 1});

    for (auto it = map.begin(); it != map.end(); ++it)
        std::cerr << it->first << ": " << it->second << "\n";

    return 0;
}
