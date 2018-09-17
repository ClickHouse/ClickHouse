#pragma once

#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashTable.h>
#include <Common/HashTable/HashTableAllocator.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <ext/range.h>

namespace DB
{

namespace
{
    template <typename ColumnType, bool with_saved_hash, bool has_base_index>
    struct ReverseIndexHashTableState;

    template <typename ColumnType>
    struct ReverseIndexHashTableState<ColumnType, /* with_saved_hash */ false, /* has_base_index */ false>
    {
        constexpr static bool with_saved_hash = false;
        constexpr static bool has_base_index = false;

        ColumnType * index_column;
    };

    template <typename ColumnType>
    struct ReverseIndexHashTableState<ColumnType, /* with_saved_hash */ false, /* has_base_index */ true>
    {
        constexpr static bool with_saved_hash = false;
        constexpr static bool has_base_index = true;

        ColumnType * index_column;
        size_t base_index;
    };

    template <typename ColumnType>
    struct ReverseIndexHashTableState<ColumnType, /* with_saved_hash = */ true, /* has_base_index */ false>
    {
        constexpr static bool with_saved_hash = true;
        constexpr static bool has_base_index = false;

        ColumnType * index_column;
        typename ColumnVector<UInt64>::Container * saved_hash_column;
    };

    template <typename ColumnType>
    struct ReverseIndexHashTableState<ColumnType, /* with_saved_hash = */ true, /* has_base_index */ true>
    {
        constexpr static bool with_saved_hash = true;
        constexpr static bool has_base_index = true;

        ColumnType * index_column;
        typename ColumnVector<UInt64>::Container * saved_hash_column;
        size_t base_index;
    };


    template <typename Hash>
    struct ReverseIndexHash : public Hash
    {
        template <typename T>
        size_t operator()(T) const
        {
            throw Exception("operator()(key) is not implemented for ReverseIndexHash.", ErrorCodes::LOGICAL_ERROR);
        }

        template <typename State, typename T>
        size_t operator()(const State & state, T key) const
        {
            auto index = key;
            if constexpr (State::has_base_index)
                index -= state.base_index;

            return Hash::operator()(state.index_column->getElement(index));
        }
    };

    using ReverseIndexStringHash = ReverseIndexHash<StringRefHash>;

    template <typename IndexType>
    using ReverseIndexNumberHash = ReverseIndexHash<DefaultHash<IndexType>>;


    template <typename IndexType, typename Hash, typename HashTable, typename ColumnType, bool string_hash, bool has_base_index>
    struct  ReverseIndexHashTableCell
        : public HashTableCell<IndexType, Hash, ReverseIndexHashTableState<ColumnType, string_hash, has_base_index>>
    {
        using Base = HashTableCell<IndexType, Hash, ReverseIndexHashTableState<ColumnType, string_hash, has_base_index>>;
        using State = typename Base::State;
        using Base::Base;
        using Base::key;
        using Base::keyEquals;
        using Base::isZero;

        template <typename T>
        static bool isZero(const T &, const State & /*state*/)
        {
            static_assert(!std::is_same_v<typename std::decay<T>::type, typename std::decay<IndexType>::type>);
            return false;
        }
        /// Special case when we want to compare with something not in index_column.
        /// When we compare something inside column default keyEquals checks only that row numbers are equal.
        bool keyEquals(const StringRef & object, size_t hash_ [[maybe_unused]], const State & state) const
        {
            auto index = key;
            if constexpr (has_base_index)
                index -= state.base_index;

            if constexpr (string_hash)
                return hash_ == (*state.saved_hash_column)[index] && object == state.index_column->getDataAt(index);
            else
                return object == state.index_column->getDataAt(index);
        }

        size_t getHash(const Hash & hash) const
        {
            auto index = key;

            /// Hack. HashTable is Hash itself.
            const auto & state = static_cast<const State &>(static_cast<const HashTable &>(hash));

            if constexpr (has_base_index)
                index -= state.base_index;

            if constexpr (string_hash)
                return (*state.saved_hash_column)[index];
            else
                return hash(state, key);
        }
    };


    template <typename Key, typename Cell, typename Hash>
    class HashTableWithPublicState : public HashTable<Key, Cell, Hash, HashTableGrower<>, HashTableAllocator>
    {
        using State = typename Cell::State;
        using Base = HashTable<Key, Cell, Hash, HashTableGrower<>, HashTableAllocator>;

    public:
        using Base::Base;
        State & getState() { return *this; }
    };

    template <typename IndexType, typename ColumnType, bool has_base_index>
    class ReverseIndexStringHashTable : public HashTableWithPublicState<
            IndexType,
            ReverseIndexHashTableCell<
                    IndexType,
                    ReverseIndexStringHash,
                    ReverseIndexStringHashTable<IndexType, ColumnType, has_base_index>,
                    ColumnType,
                    true,
                    has_base_index>,
            ReverseIndexStringHash>
    {
        using Base = HashTableWithPublicState<
                IndexType,
                ReverseIndexHashTableCell<
                        IndexType,
                        ReverseIndexStringHash,
                        ReverseIndexStringHashTable<IndexType, ColumnType, has_base_index>,
                        ColumnType,
                        true,
                        has_base_index>,
                ReverseIndexStringHash>;
    public:
        using Base::Base;
        friend struct ReverseIndexHashTableCell<
                IndexType,
                ReverseIndexStringHash,
                ReverseIndexStringHashTable<IndexType, ColumnType, has_base_index>,
                ColumnType,
                true,
                has_base_index>;
    };

    template <typename IndexType, typename ColumnType, bool has_base_index>
    class ReverseIndexNumberHashTable : public HashTableWithPublicState<
            IndexType,
            ReverseIndexHashTableCell<
                    IndexType,
                    ReverseIndexNumberHash<typename ColumnType::value_type>,
                    ReverseIndexNumberHashTable<IndexType, ColumnType, has_base_index>,
                    ColumnType,
                    false,
                    has_base_index>,
            ReverseIndexNumberHash<typename ColumnType::value_type>>
    {
        using Base = HashTableWithPublicState<
                IndexType,
                ReverseIndexHashTableCell<
                        IndexType,
                        ReverseIndexNumberHash<typename ColumnType::value_type>,
                        ReverseIndexNumberHashTable<IndexType, ColumnType, has_base_index>,
                        ColumnType,
                        false,
                        has_base_index>,
                ReverseIndexNumberHash<typename ColumnType::value_type>>;
    public:
        using Base::Base;
        friend struct ReverseIndexHashTableCell<
                IndexType,
                ReverseIndexNumberHash<typename ColumnType::value_type>,
                ReverseIndexNumberHashTable<IndexType, ColumnType, has_base_index>,
                ColumnType,
                false,
                has_base_index>;
    };


    template <typename IndexType, typename ColumnType, bool has_base_index, bool is_numeric_column>
    struct SelectReverseIndexHashTable;

    template <typename IndexType, typename ColumnType, bool has_base_index>
    struct SelectReverseIndexHashTable<IndexType, ColumnType, has_base_index, true>
    {
        using Type = ReverseIndexNumberHashTable<IndexType, ColumnType, has_base_index>;
    };

    template <typename IndexType, typename ColumnType, bool has_base_index>
    struct SelectReverseIndexHashTable<IndexType, ColumnType, has_base_index, false>
    {
        using Type = ReverseIndexStringHashTable<IndexType, ColumnType, has_base_index>;
    };


    template <typename T>
    constexpr bool isNumericColumn(const T *) { return false; }

    template <typename T>
    constexpr bool isNumericColumn(const ColumnVector<T> *) { return true; }

    static_assert(isNumericColumn(static_cast<ColumnVector<UInt8> *>(nullptr)));
    static_assert(!isNumericColumn(static_cast<ColumnString *>(nullptr)));


    template <typename IndexType, typename ColumnType, bool has_base_index>
    using ReverseIndexHashTable = typename SelectReverseIndexHashTable<IndexType, ColumnType, has_base_index,
            isNumericColumn(static_cast<ColumnType *>(nullptr))>::Type;
}


template <typename IndexType, typename ColumnType>
class ReverseIndex
{
public:
    explicit ReverseIndex(UInt64 num_prefix_rows_to_skip, UInt64 base_index)
            : num_prefix_rows_to_skip(num_prefix_rows_to_skip), base_index(base_index), saved_hash_ptr(nullptr) {}

    void setColumn(ColumnType * column_);

    static constexpr bool is_numeric_column = isNumericColumn(static_cast<ColumnType *>(nullptr));
    static constexpr bool use_saved_hash = !is_numeric_column;

    UInt64 insert(UInt64 from_position);  /// Insert into index column[from_position];
    UInt64 insertFromLastRow();
    UInt64 getInsertionPoint(const StringRef & data);
    UInt64 lastInsertionPoint() const { return size() + base_index; }

    ColumnType * getColumn() const { return column; }
    size_t size() const;

    const UInt64 * tryGetSavedHash() const
    {
        if (!use_saved_hash)
            return nullptr;

        UInt64 * ptr = saved_hash_ptr.load();
        if (!ptr)
        {
            auto hash = calcHashes();
            ptr = &hash->getData()[0];
            UInt64 * expected = nullptr;
            if(saved_hash_ptr.compare_exchange_strong(expected, ptr))
                saved_hash = std::move(hash);
            else
                ptr = expected;
        }

        return ptr;
    }

    size_t allocatedBytes() const { return index ? index->getBufferSizeInBytes() : 0; }

private:
    ColumnType * column = nullptr;
    UInt64 num_prefix_rows_to_skip; /// The number prefix tows in column which won't be sored at index.
    UInt64 base_index; /// This values will be added to row number which is inserted into index.

    using IndexMapType = ReverseIndexHashTable<IndexType, ColumnType, true>;

    /// Lazy initialized.
    std::unique_ptr<IndexMapType> index;
    mutable ColumnUInt64::MutablePtr saved_hash;
    mutable std::atomic<UInt64 *> saved_hash_ptr;

    void buildIndex();

    UInt64 getHash(const StringRef & ref) const
    {
        if constexpr (is_numeric_column)
        {
            using ValueType = typename ColumnType::value_type;
            ValueType value = *reinterpret_cast<const ValueType *>(ref.data);
            return DefaultHash<ValueType>()(value);
        }
        else
            return StringRefHash()(ref);
    }

    ColumnUInt64::MutablePtr calcHashes() const;
};


template <typename IndexType, typename ColumnType>
void ReverseIndex<IndexType, ColumnType>:: setColumn(ColumnType * column_)
{
    if (column != column_)
    {
        index = nullptr;
        saved_hash = nullptr;
    }

    column = column_;
}

template <typename IndexType, typename ColumnType>
size_t ReverseIndex<IndexType, ColumnType>::size() const
{
    if (!column)
        throw Exception("ReverseIndex has not size because index column wasn't set.", ErrorCodes::LOGICAL_ERROR);

    return column->size();
}

template <typename IndexType, typename ColumnType>
void ReverseIndex<IndexType, ColumnType>::buildIndex()
{
    if (index)
        return;

    if (!column)
        throw Exception("ReverseIndex can't build index because index column wasn't set.", ErrorCodes::LOGICAL_ERROR);

    auto size = column->size();
    index = std::make_unique<IndexMapType>(size);

    if constexpr (use_saved_hash)
        saved_hash = calcHashes();

    auto & state = index->getState();
    state.index_column = column;
    state.base_index = base_index;
    if constexpr (use_saved_hash)
        state.saved_hash_column = &saved_hash->getData();

    using IteratorType = typename IndexMapType::iterator;
    IteratorType iterator;
    bool inserted;

    for (auto row : ext::range(num_prefix_rows_to_skip, size))
    {
        UInt64 hash;
        if constexpr (use_saved_hash)
            hash = saved_hash->getElement(row);
        else
            hash = getHash(column->getDataAt(row));

        index->emplace(row + base_index, iterator, inserted, hash);

        if (!inserted)
            throw Exception("Duplicating keys found in ReverseIndex.", ErrorCodes::LOGICAL_ERROR);
    }
}

template <typename IndexType, typename ColumnType>
ColumnUInt64::MutablePtr ReverseIndex<IndexType, ColumnType>::calcHashes() const
{
    if (!column)
        throw Exception("ReverseIndex can't build index because index column wasn't set.", ErrorCodes::LOGICAL_ERROR);

    auto size = column->size();
    auto hash = ColumnUInt64::create(size);

    for (auto row : ext::range(0, size))
        hash->getElement(row) = getHash(column->getDataAt(row));

    return std::move(hash);
}

template <typename IndexType, typename ColumnType>
UInt64 ReverseIndex<IndexType, ColumnType>::insert(UInt64 from_position)
{
    if (!index)
        buildIndex();

    using IteratorType = typename IndexMapType::iterator;
    IteratorType iterator;
    bool inserted;

    auto hash = getHash(column->getDataAt(from_position));

    if constexpr (use_saved_hash)
    {
        auto & data = saved_hash->getData();
        if (data.size() <= from_position)
            data.resize(from_position + 1);
        data[from_position] = hash;
    }

    index->emplace(from_position + base_index, iterator, inserted, hash);

    return *iterator;
}

template <typename IndexType, typename ColumnType>
UInt64 ReverseIndex<IndexType, ColumnType>::insertFromLastRow()
{
    if (!column)
        throw Exception("ReverseIndex can't insert row from column because index column wasn't set.",
                        ErrorCodes::LOGICAL_ERROR);

    UInt64 num_rows = size();

    if (num_rows == 0)
        throw Exception("ReverseIndex can't insert row from column because it is empty.", ErrorCodes::LOGICAL_ERROR);

    UInt64 position = num_rows - 1;
    UInt64 inserted_pos = insert(position);
    if (position + base_index != inserted_pos)
        throw Exception("Can't insert into reverse index from last row (" + toString(position + base_index)
                        + ") because the same row is in position " + toString(inserted_pos), ErrorCodes::LOGICAL_ERROR);

    return inserted_pos;
}

template <typename IndexType, typename ColumnType>
UInt64 ReverseIndex<IndexType, ColumnType>::getInsertionPoint(const StringRef & data)
{
    if (!index)
        buildIndex();

    using IteratorType = typename IndexMapType::iterator;
    IteratorType iterator;

    auto hash = getHash(data);
    iterator = index->find(data, hash);

    return iterator == index->end() ? size() + base_index : *iterator;
}

}
