#pragma once

#include <Common/HashTable/HashTable.h>

template <typename Key, typename TState = HashTableNoState>
struct FixedHashTableCell
{
    using State = TState;

    using value_type = Key;
    using mapped_type = VoidMapped;
    bool full;

    FixedHashTableCell() {}
    FixedHashTableCell(const Key &, const State &) : full(true) {}

    bool isZero(const State &) const { return !full; }
    void setZero() { full = false; }
    static const Key & getKey(const value_type & value) { return value; }
    VoidMapped & getSecond() const { return voidMapped; }
};


/** Used as a lookup table for small keys such as UInt8, UInt16. It's different
  *  than a HashTable in that keys are not stored in the Cell buf, but inferred
  *  inside each iterator. There are a bunch of to make it faster than using
  *  HashTable: a) It doesn't have a conflict chain; b) There is no key
  *  comparision; c) The number of cycles for checking cell empty is halved; d)
  *  Memory layout is tighter, especially the Clearable variants.
  *
  * NOTE: For Set variants this should always be better. For Map variants
  *  however, as we need to assemble the real cell inside each iterator, there
  *  might be some cases we fall short.
  */
template <typename Key, typename Cell, typename Allocator>
class FixedHashTable : private boost::noncopyable, protected Allocator, protected Cell::State
{
    static constexpr size_t CELL_NUM = 1ULL << (sizeof(Key) * 8);

protected:
    friend class Reader;

    using Self = FixedHashTable;

    size_t m_size = 0; /// Amount of elements
    Cell * buf; /// A piece of memory for all elements.

    void alloc() { buf = reinterpret_cast<Cell *>(Allocator::alloc(CELL_NUM * sizeof(Cell))); }

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
            forEachCell([&](Key, Cell & cell) { cell.~Cell(); });
    }

public:
    using key_type = Key;
    using value_type = typename Cell::value_type;
    using mapped_type = typename Cell::mapped_type;
    using cell_type = Cell;

    using LookupResult = Cell *;
    using ConstLookupResult = const Cell *;

    size_t hash(const Key & x) const { return x; }

    FixedHashTable() { alloc(); }

    FixedHashTable(FixedHashTable && rhs) : buf(nullptr) { *this = std::move(rhs); }

    ~FixedHashTable()
    {
        destroyElements();
        free();
    }

    FixedHashTable & operator=(FixedHashTable && rhs)
    {
        destroyElements();
        free();

        std::swap(buf, rhs.buf);
        std::swap(m_size, rhs.m_size);

        Allocator::operator=(std::move(rhs));
        Cell::State::operator=(std::move(rhs));

        return *this;
    }

    class Reader final : private Cell::State
    {
    public:
        Reader(DB::ReadBuffer & in_) : in(in_) {}

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
        size_t size;
        bool is_eof = false;
        bool is_initialized = false;
    };

public:
    using Position = Cell *;
    using ConstPosition = const Cell *;

    Position startPos() { return buf; }
    ConstPosition startPos() const { return buf; }

    template <typename TSelf, typename Func, typename TPosition>
    static bool forEachCell(TSelf & self, Func && func, TPosition & pos)
    {
        using TCell = decltype(*std::declval<TSelf>().buf);
        static constexpr bool with_key = std::is_invocable_v<Func, const Key &, TCell>;
        using ReturnType =
            typename std::conditional_t<with_key, std::invoke_result<Func, const Key &, TCell>, std::invoke_result<Func, TCell>>::type;
        static constexpr bool ret_bool = std::is_same_v<bool, ReturnType>;

        size_t i = pos - self.buf;
        for (; i < CELL_NUM; ++i)
        {
            if (!self.buf[i].isZero(self))
            {
                if constexpr (with_key)
                {
                    if constexpr (ret_bool)
                    {
                        if (func(i, self.buf[i]))
                        {
                            pos = &self.buf[i + 1];
                            return true;
                        }
                    }
                    else
                        func(i, self.buf[i]);
                }
                else
                {
                    if constexpr (ret_bool)
                    {
                        if (func(self.buf[i]))
                        {
                            pos = &self.buf[i + 1];
                            return true;
                        }
                    }
                    else
                        func(self.buf[i]);
                }
            }
        }
        pos = &self.buf[i];

        return false;
    }

    /// Iterate over every cell and pass non-zero cells to func.
    ///  Func should have signature (1) void(const Key &, const Cell &); or (2)  void(const Cell &).
    template <typename Func>
    auto forEachCell(Func && func) const
    {
        ConstPosition pos = startPos();
        return forEachCell(*this, std::forward<Func>(func), pos);
    }

    /// Iterate over every cell and pass non-zero cells to func.
    ///  Func should have signature (1) void(const Key &, Cell &); or (2)  void(const Cell &).
    template <typename Func>
    auto forEachCell(Func && func)
    {
        Position pos = startPos();
        return forEachCell(*this, std::forward<Func>(func), pos);
    }

    /// Same as the above functions but with additional position variable to resume last iteration.
    template <typename Func>
    auto forEachCell(Func && func, ConstPosition & pos) const
    {
        return forEachCell(*this, std::forward<Func>(func), pos);
    }

    /// Same as the above functions but with additional position variable to resume last iteration.
    template <typename Func>
    auto forEachCell(Func && func, Position & pos)
    {
        return forEachCell(*this, std::forward<Func>(func), pos);
    }

    /// The last parameter is unused but exists for compatibility with HashTable interface.
    void ALWAYS_INLINE emplace(Key x, LookupResult & it, bool & inserted, size_t /* hash */ = 0)
    {
        it = &buf[x];

        if (!buf[x].isZero(*this))
        {
            inserted = false;
            return;
        }

        new (&buf[x]) Cell(x, *this);
        inserted = true;
        ++m_size;
    }

    std::pair<LookupResult, bool> ALWAYS_INLINE insert(const value_type & x)
    {
        std::pair<LookupResult, bool> res;
        emplace(Cell::getKey(x), res.first, res.second);
        if (res.second)
            insertSetMapped(res.first->getSecond(), x);

        return res;
    }

    LookupResult ALWAYS_INLINE find(Key x) { return !buf[x].isZero(*this) ? &buf[x] : nullptr; }

    ConstLookupResult ALWAYS_INLINE find(Key x) const { return const_cast<std::decay_t<decltype(*this)> *>(this)->find(x); }

    LookupResult ALWAYS_INLINE find(Key, size_t hash_value) { return !buf[hash_value].isZero(*this) ? &buf[hash_value] : nullptr; }

    ConstLookupResult ALWAYS_INLINE find(Key key, size_t hash_value) const
    {
        return const_cast<std::decay_t<decltype(*this)> *>(this)->find(key, hash_value);
    }

    bool ALWAYS_INLINE has(Key x) const { return !buf[x].isZero(*this); }
    bool ALWAYS_INLINE has(Key, size_t hash_value) const { return !buf[hash_value].isZero(*this); }

    void write(DB::WriteBuffer & wb) const
    {
        Cell::State::write(wb);
        DB::writeVarUInt(m_size, wb);

        for (auto ptr = buf, buf_end = buf + CELL_NUM; ptr < buf_end; ++ptr)
            if (!ptr->isZero(*this))
            {
                DB::writeVarUInt(ptr - buf);
                ptr->write(wb);
            }
    }

    void writeText(DB::WriteBuffer & wb) const
    {
        Cell::State::writeText(wb);
        DB::writeText(m_size, wb);

        for (auto ptr = buf, buf_end = buf + CELL_NUM; ptr < buf_end; ++ptr)
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
        DB::readVarUInt(m_size, rb);
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
        DB::readText(m_size, rb);
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

    size_t size() const { return m_size; }

    bool empty() const { return 0 == m_size; }

    void clear()
    {
        destroyElements();
        m_size = 0;

        memset(static_cast<void *>(buf), 0, CELL_NUM * sizeof(*buf));
    }

    /// After executing this function, the table can only be destroyed,
    ///  and also you can use the methods `size`, `empty`, `begin`, `end`.
    void clearAndShrink()
    {
        destroyElements();
        m_size = 0;
        free();
    }

    size_t getBufferSizeInBytes() const { return CELL_NUM * sizeof(Cell); }

    size_t getBufferSizeInCells() const { return CELL_NUM; }

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
    size_t getCollisions() const { return 0; }
#endif
};
