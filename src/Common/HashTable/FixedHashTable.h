#pragma once

#include <Common/HashTable/HashTable.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int NO_AVAILABLE_DATA;
    }
}

template <typename Key, typename TState = HashTableNoState>
struct FixedHashTableCell
{
    using State = TState;

    using value_type = Key;
    using mapped_type = VoidMapped;
    bool full;

    FixedHashTableCell() {} //-V730 /// NOLINT
    FixedHashTableCell(const Key &, const State &) : full(true) {}

    const VoidKey getKey() const { return {}; } /// NOLINT
    VoidMapped getMapped() const { return {}; }

    bool isZero(const State &) const { return !full; }
    void setZero() { full = false; }
    static constexpr bool need_zero_value_storage = false;

    /// This Cell is only stored inside an iterator. It's used to accommodate the fact
    ///  that the iterator based API always provide a reference to a continuous memory
    ///  containing the Key. As a result, we have to instantiate a real Key field.
    /// All methods that return a mutable reference to the Key field are named with
    ///  -Mutable suffix, indicating this is uncommon usage. As this is only for lookup
    ///  tables, it's totally fine to discard the Key mutations.
    struct CellExt
    {
        Key key;

        const VoidKey getKey() const { return {}; } /// NOLINT
        VoidMapped getMapped() const { return {}; }
        const value_type & getValue() const { return key; }
        void update(Key && key_, FixedHashTableCell *) { key = key_; }
    };
};


/// How to obtain the size of the table.

template <typename Cell>
struct FixedHashTableStoredSize
{
    size_t m_size = 0;

    size_t getSize(const Cell *, const typename Cell::State &, size_t) const { return m_size; }
    bool isEmpty(const Cell *, const typename Cell::State &, size_t) const { return m_size == 0; }

    void increaseSize() { ++m_size; }
    void clearSize() { m_size = 0; }
    void setSize(size_t to) { m_size = to; }
};

template <typename Cell>
struct FixedHashTableCalculatedSize
{
    size_t getSize(const Cell * buf, const typename Cell::State & state, size_t num_cells) const
    {
        size_t res = 0;
        for (const Cell * end = buf + num_cells; buf != end; ++buf)
            if (!buf->isZero(state))
                ++res;
        return res;
    }

    bool isEmpty(const Cell * buf, const typename Cell::State & state, size_t num_cells) const
    {
        for (const Cell * end = buf + num_cells; buf != end; ++buf)
            if (!buf->isZero(state))
                return false;
        return true;
    }

    void increaseSize() {}
    void clearSize() {}
    void setSize(size_t) {}
};


/** Used as a lookup table for small keys such as UInt8, UInt16. It's different
  *  than a HashTable in that keys are not stored in the Cell buf, but inferred
  *  inside each iterator. There are a bunch of to make it faster than using
  *  HashTable: a) It doesn't have a conflict chain; b) There is no key
  *  comparison; c) The number of cycles for checking cell empty is halved; d)
  *  Memory layout is tighter, especially the Clearable variants.
  *
  * NOTE: For Set variants this should always be better. For Map variants
  *  however, as we need to assemble the real cell inside each iterator, there
  *  might be some cases we fall short.
  *
  * TODO: Deprecate the cell API so that end users don't rely on the structure
  *  of cell. Instead iterator should be used for operations such as cell
  *  transfer, key updates (f.g. StringRef) and serde. This will allow
  *  TwoLevelHashSet(Map) to contain different type of sets(maps).
  */
template <typename Key, typename Cell, typename Size, typename Allocator>
class FixedHashTable : private boost::noncopyable, protected Allocator, protected Cell::State, protected Size
{
    static constexpr size_t NUM_CELLS = 1ULL << (sizeof(Key) * 8);

protected:
    friend class const_iterator;
    friend class iterator;
    friend class Reader;

    using Self = FixedHashTable;

    Cell * buf; /// A piece of memory for all elements.

    void alloc() { buf = reinterpret_cast<Cell *>(Allocator::alloc(NUM_CELLS * sizeof(Cell))); }

    void free()
    {
        if (buf)
        {
            Allocator::free(buf, getBufferSizeInBytes());
            buf = nullptr;
        }
    }

    void destroyElements()
    {
        if (!std::is_trivially_destructible_v<Cell>)
            for (iterator it = begin(), it_end = end(); it != it_end; ++it)
                it.ptr->~Cell();
    }


    template <typename Derived, bool is_const>
    class iterator_base /// NOLINT
    {
        using Container = std::conditional_t<is_const, const Self, Self>;
        using cell_type = std::conditional_t<is_const, const Cell, Cell>;

        Container * container;
        cell_type * ptr;

        friend class FixedHashTable;

    public:
        iterator_base() {} /// NOLINT
        iterator_base(Container * container_, cell_type * ptr_) : container(container_), ptr(ptr_)
        {
            cell.update(ptr - container->buf, ptr);
        }

        bool operator==(const iterator_base & rhs) const { return ptr == rhs.ptr; }
        bool operator!=(const iterator_base & rhs) const { return ptr != rhs.ptr; }

        Derived & operator++()
        {
            ++ptr;

            /// Skip empty cells in the main buffer.
            const auto * buf_end = container->buf + container->NUM_CELLS;
            while (ptr < buf_end && ptr->isZero(*container))
                ++ptr;

            return static_cast<Derived &>(*this);
        }

        auto & operator*()
        {
            if (cell.key != ptr - container->buf)
                cell.update(ptr - container->buf, ptr);
            return cell;
        }
        auto * operator-> ()
        {
            if (cell.key != ptr - container->buf)
                cell.update(ptr - container->buf, ptr);
            return &cell;
        }

        auto getPtr() const { return ptr; }
        size_t getHash() const { return ptr - container->buf; }
        size_t getCollisionChainLength() const { return 0; }
        typename cell_type::CellExt cell;
    };


public:
    using key_type = Key;
    using mapped_type = typename Cell::mapped_type;
    using value_type = typename Cell::value_type;
    using cell_type = Cell;

    using LookupResult = Cell *;
    using ConstLookupResult = const Cell *;


    size_t hash(const Key & x) const { return x; }

    FixedHashTable() { alloc(); }

    FixedHashTable(FixedHashTable && rhs) noexcept : buf(nullptr) { *this = std::move(rhs); } /// NOLINT

    ~FixedHashTable()
    {
        destroyElements();
        free();
    }

    FixedHashTable & operator=(FixedHashTable && rhs) noexcept
    {
        destroyElements();
        free();

        std::swap(buf, rhs.buf);
        this->setSize(rhs.size());

        Allocator::operator=(std::move(rhs));
        Cell::State::operator=(std::move(rhs));

        return *this;
    }

    class Reader final : private Cell::State
    {
    public:
        explicit Reader(DB::ReadBuffer & in_) : in(in_) {}

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


    class iterator : public iterator_base<iterator, false> /// NOLINT
    {
    public:
        using iterator_base<iterator, false>::iterator_base;
    };

    class const_iterator : public iterator_base<const_iterator, true> /// NOLINT
    {
    public:
        using iterator_base<const_iterator, true>::iterator_base;
    };


    const_iterator begin() const
    {
        if (!buf)
            return end();

        const Cell * ptr = buf;
        auto buf_end = buf + NUM_CELLS;
        while (ptr < buf_end && ptr->isZero(*this))
            ++ptr;

        return const_iterator(this, ptr);
    }

    const_iterator cbegin() const { return begin(); }

    iterator begin()
    {
        if (!buf)
            return end();

        Cell * ptr = buf;
        auto buf_end = buf + NUM_CELLS;
        while (ptr < buf_end && ptr->isZero(*this))
            ++ptr;

        return iterator(this, ptr);
    }

    const_iterator end() const
    {
        /// Avoid UBSan warning about adding zero to nullptr. It is valid in C++20 (and earlier) but not valid in C.
        return const_iterator(this, buf ? buf + NUM_CELLS : buf);
    }

    const_iterator cend() const
    {
        return end();
    }

    iterator end()
    {
        return iterator(this, buf ? buf + NUM_CELLS : buf);
    }


    /// The last parameter is unused but exists for compatibility with HashTable interface.
    void ALWAYS_INLINE emplace(const Key & x, LookupResult & it, bool & inserted, size_t /* hash */ = 0)
    {
        it = &buf[x];

        if (!buf[x].isZero(*this))
        {
            inserted = false;
            return;
        }

        new (&buf[x]) Cell(x, *this);
        inserted = true;
        this->increaseSize();
    }

    std::pair<LookupResult, bool> ALWAYS_INLINE insert(const value_type & x)
    {
        std::pair<LookupResult, bool> res;
        emplace(Cell::getKey(x), res.first, res.second);
        if (res.second)
            insertSetMapped(res.first->getMapped(), x);

        return res;
    }

    LookupResult ALWAYS_INLINE find(const Key & x) { return !buf[x].isZero(*this) ? &buf[x] : nullptr; }

    ConstLookupResult ALWAYS_INLINE find(const Key & x) const { return const_cast<std::decay_t<decltype(*this)> *>(this)->find(x); }

    LookupResult ALWAYS_INLINE find(const Key &, size_t hash_value) { return !buf[hash_value].isZero(*this) ? &buf[hash_value] : nullptr; }

    ConstLookupResult ALWAYS_INLINE find(const Key & key, size_t hash_value) const
    {
        return const_cast<std::decay_t<decltype(*this)> *>(this)->find(key, hash_value);
    }

    bool ALWAYS_INLINE has(const Key & x) const { return !buf[x].isZero(*this); }
    bool ALWAYS_INLINE has(const Key &, size_t hash_value) const { return !buf[hash_value].isZero(*this); }

    void write(DB::WriteBuffer & wb) const
    {
        Cell::State::write(wb);
        DB::writeVarUInt(size(), wb);

        if (!buf)
            return;

        for (auto ptr = buf, buf_end = buf + NUM_CELLS; ptr < buf_end; ++ptr)
        {
            if (!ptr->isZero(*this))
            {
                DB::writeVarUInt(ptr - buf);
                ptr->write(wb);
            }
        }
    }

    void writeText(DB::WriteBuffer & wb) const
    {
        Cell::State::writeText(wb);
        DB::writeText(size(), wb);

        if (!buf)
            return;

        for (auto ptr = buf, buf_end = buf + NUM_CELLS; ptr < buf_end; ++ptr)
        {
            if (!ptr->isZero(*this))
            {
                DB::writeChar(',', wb);
                DB::writeText(ptr - buf, wb);
                DB::writeChar(',', wb);
                ptr->writeText(wb);
            }
        }
    }

    void read(DB::ReadBuffer & rb)
    {
        Cell::State::read(rb);
        destroyElements();
        size_t m_size;
        DB::readVarUInt(m_size, rb);
        this->setSize(m_size);
        free();
        alloc();

        for (size_t i = 0; i < m_size; ++i)
        {
            size_t place_value = 0;
            DB::readVarUInt(place_value, rb);
            Cell x;
            x.read(rb);
            new (&buf[place_value]) Cell(x, *this);
        }
    }

    void readText(DB::ReadBuffer & rb)
    {
        Cell::State::readText(rb);
        destroyElements();
        size_t m_size;
        DB::readText(m_size, rb);
        this->setSize(m_size);
        free();
        alloc();

        for (size_t i = 0; i < m_size; ++i)
        {
            size_t place_value = 0;
            DB::assertChar(',', rb);
            DB::readText(place_value, rb);
            Cell x;
            DB::assertChar(',', rb);
            x.readText(rb);
            new (&buf[place_value]) Cell(x, *this);
        }
    }

    size_t size() const { return this->getSize(buf, *this, NUM_CELLS); }
    bool empty() const { return this->isEmpty(buf, *this, NUM_CELLS); }

    void clear()
    {
        destroyElements();
        this->clearSize();

        memset(static_cast<void *>(buf), 0, NUM_CELLS * sizeof(*buf));
    }

    /// After executing this function, the table can only be destroyed,
    ///  and also you can use the methods `size`, `empty`, `begin`, `end`.
    void clearAndShrink()
    {
        destroyElements();
        this->clearSize();
        free();
    }

    size_t getBufferSizeInBytes() const { return NUM_CELLS * sizeof(Cell); }

    size_t getBufferSizeInCells() const { return NUM_CELLS; }

    /// Return offset for result in internal buffer.
    /// Result can have value up to `getBufferSizeInCells() + 1`
    /// because offset for zero value considered to be 0
    /// and for other values it will be `offset in buffer + 1`
    size_t offsetInternal(ConstLookupResult ptr) const
    {
        if (ptr->isZero(*this))
            return 0;
        return ptr - buf + 1;
    }

    const Cell * data() const { return buf; }
    Cell * data() { return buf; }

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
    size_t getCollisions() const { return 0; }
#endif
};
