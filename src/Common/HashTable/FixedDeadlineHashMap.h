#pragma once

#include <chrono>
#include <type_traits>
#include <Common/HashTable/HashMap.h>
#include <Common/BitHelpers.h>

using TimePoint = std::chrono::system_clock::time_point;

template <typename TKey, typename TMapped, typename Hash, bool save_hash_in_cell>
struct DeadlineCell :
    public std::conditional_t<save_hash_in_cell,
        HashMapCellWithSavedHash<TKey, TMapped, Hash, HashTableNoState>,
        HashMapCell<TKey, TMapped, Hash, HashTableNoState>>
{
    using Key = TKey;

    using Base = std::conditional_t<save_hash_in_cell,
        HashMapCellWithSavedHash<TKey, TMapped, Hash, HashTableNoState>,
        HashMapCell<TKey, TMapped, Hash, HashTableNoState>>;

    using Mapped = typename Base::Mapped;
    using State = typename Base::State;

    using mapped_type = Mapped;
    using key_type = Key;

    using Base::Base;

    inline TimePoint getDeadline() const { return deadline; }

    void setDeadline(TimePoint & deadline_value) { deadline = deadline_value; }

private:
    TimePoint deadline;
};

template <typename TKey, typename TValue, typename Disposer, typename Hash, bool save_hash_in_cells>
class FixedDeadlineHashMapImpl :
    private HashMapTable<
        TKey,
        DeadlineCell<TKey, TValue, Hash, save_hash_in_cells>,
        Hash,
        HashTableGrower<>,
        HashTableAllocator>
{
    /// TODO: Make custom grower
    using Base = HashMapTable<
        TKey,
        DeadlineCell<TKey, TValue, Hash, save_hash_in_cells>,
        Hash,
        HashTableGrower<>,
        HashTableAllocator>;

    static size_t calculateMaxSize(size_t max_size, size_t max_collision_resolution_chain)
    {
        return roundUpToPowerOfTwoOrZero(std::max(max_size, max_collision_resolution_chain));
    }
public:
    using Cell = DeadlineCell<TKey, TValue, Hash, save_hash_in_cells>;
    using Key = TKey;
    using Value = TValue;
    using Mapped = typename Cell::Mapped;

    explicit FixedDeadlineHashMapImpl(size_t max_size_, size_t max_collision_resolution_chain_, Disposer disposer_ = Disposer())
        : Base(calculateMaxSize(max_size_, max_collision_resolution_chain_))
        , max_collision_resolution_chain(max_collision_resolution_chain_)
        , max_size(max_size_)
        , disposer(std::move(disposer_))
    {
        assert(max_size > 0);
        assert(max_collision_resolution_chain > 0);
    }

    ~FixedDeadlineHashMapImpl()
    {
        clear();
    }

    Cell * get(const Key & key)
    {
        if (Cell::isZero(key, *this))
            return this->hasZero() ? this->zeroValue() : nullptr;

        /// TODO: Optimize

        size_t hash_value = Base::hash(key);
        size_t place_value = Base::grower.place(hash_value);
        size_t resolution_chain = max_collision_resolution_chain;

        while (resolution_chain != 0)
        {
            auto & cell = Base::buf[place_value];

            if (cell.isZero(*this))
                return nullptr;

            if (cell.keyEquals(key, hash_value, *this))
                return &cell;

            place_value = Base::grower.next(place_value);
            --resolution_chain;
        }

        return nullptr;
    }

    const Cell * get(const Key & key) const
    {
        return const_cast<std::decay_t<decltype(*this)> *>(this)->get(key);
    }

    std::pair<Cell *, bool> ALWAYS_INLINE insert(const Key & key, const Value & value)
    {
        return emplace(key, value);
    }

    std::pair<Cell *, bool> ALWAYS_INLINE insert(const Key & key, Value && value)
    {
        return emplace(key, std::move(value));
    }

    template<typename ...Args>
    std::pair<Cell *, bool> ALWAYS_INLINE emplace(const Key & key, Args && ... args)
    {
        size_t hash_value = Base::hash(key);
        std::pair<Cell *, bool> result;

        if (!emplaceIfZero(key, hash_value, result))
            result = emplaceNonZeroImpl(key, hash_value);

        bool was_inserted = result.second;

        if (was_inserted)
            new (&result.first->getMapped()) Value(std::forward<Args>(args)...);

        return result;
    }

    template <typename ...Args>
    void reinsert(Cell * place_to_use, const Key & key, Args && ... args)
    {
        size_t hash_value = Base::hash(key);

        new (place_to_use) Cell(key, *this);
        new (&place_to_use->getMapped()) Value(std::forward<Args>(args)...);
        place_to_use->setHash(hash_value);
    }

    using Base::size;

    using iterator = typename Base::iterator;
    using const_iterator = typename Base::const_iterator;

    using Base::begin;
    using Base::end;

    size_t getMaxSize() const { return max_size; }

    size_t getSizeInBytes() const { return Base::getBufferSizeInBytes(); }

    void clear()
    {
        for (auto & cell : *this)
            disposer(cell.getKey(), cell.getMapped());
    }

private:
    size_t max_collision_resolution_chain;
    size_t max_size;
    Disposer disposer;

    bool emplaceIfZero(const Key & key, size_t hash_value, std::pair<Cell *, bool> & result)
    {
        if (!Cell::isZero(key, *this))
            return false;

        if (this->hasZero())
        {
            result = {this->zeroValue(), false};
            return true;
        }

        ++Base::m_size;

        this->setHasZero();
        this->zeroValue()->setHash(hash_value);
        result = {this->zeroValue(), true};

        return true;
    }

    std::pair<Cell *, bool> emplaceNonZeroImpl(const Key & key, size_t hash_value)
    {
        TimePoint oldest_time = TimePoint::max();
        size_t place_value = Base::grower.place(hash_value);
        size_t resolution_chain = max_collision_resolution_chain;

        bool use_old_value_place = false;
        Cell * place_to_insert = nullptr;

        while (resolution_chain != 0)
        {
            auto & cell = Base::buf[place_value];

            if (cell.isZero(*this))
            {
                use_old_value_place = false;
                place_to_insert = &cell;
                break;
            }

            if (cell.keyEquals(key, hash_value, *this))
                return std::make_pair(&cell, false);

            if (cell.getDeadline() < oldest_time)
            {
                use_old_value_place = true;
                place_to_insert = &cell;
            }

            place_value = Base::grower.next(place_value);
            --resolution_chain;
        }

        if (!place_to_insert)
            place_to_insert = &Base::buf[place_value];

        if (use_old_value_place)
            return std::make_pair(place_to_insert, false);
        else
        {
            ++Base::m_size;

            new (place_to_insert) Cell(key, *this);
            place_to_insert->setHash(hash_value);

            return std::make_pair(place_to_insert, true);
        }
    }
};

template <typename Key, typename Mapped>
struct DefaultFixedHashMapCellDisposer
{
    void operator()(const Key &, const Mapped &) const {}
};

template <typename Key, typename Value, typename Disposer = DefaultFixedHashMapCellDisposer<Key, Value>, typename Hash = DefaultHash<Key>>
using FixedDeadlineHashMap = FixedDeadlineHashMapImpl<Key, Value, Disposer, Hash, false>;

template <typename Key, typename Value, typename Disposer = DefaultFixedHashMapCellDisposer<Key, Value>, typename Hash = DefaultHash<Key>>
using FixedDeadlineHashMapWithSavedHash = FixedDeadlineHashMapImpl<Key, Value, Disposer, Hash, true>;
