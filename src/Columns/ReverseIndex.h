#pragma once

#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashTable.h>
#include <Common/HashTable/HashTableAllocator.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <base/range.h>
#include <base/unaligned.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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


struct ReverseIndexHash
{
    template <typename T>
    size_t operator()(T) const
    {
        throw Exception("operator()(key) is not implemented for ReverseIndexHash.", ErrorCodes::LOGICAL_ERROR);
    }
};

template <typename IndexType, typename Hash, typename HashTable, typename ColumnType, bool string_hash, bool has_base_index>
struct ReverseIndexHashTableCell
    : public HashTableCell<IndexType, Hash, ReverseIndexHashTableState<ColumnType, string_hash, has_base_index>>
{
    using Base = HashTableCell<IndexType, Hash, ReverseIndexHashTableState<ColumnType, string_hash, has_base_index>>;
    using State = typename Base::State;
    using Base::Base;
    using Base::isZero;
    using Base::key;
    using Base::keyEquals;

    template <typename T>
    static bool isZero(const T &, const State & /*state*/)
    {
        /// Careful: apparently this uses SFINAE to redefine isZero for all types
        /// except the IndexType, for which the default ZeroTraits::isZero is used.
        static_assert(!std::is_same_v<typename std::decay<T>::type, typename std::decay<IndexType>::type>);
        return false;
    }

    /// Special case when we want to compare with something not in index_column.
    /// When we compare something inside column default keyEquals checks only that row numbers are equal.
    bool keyEquals(StringRef object, size_t hash_ [[maybe_unused]], const State & state) const
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
        {
            using ValueType = typename ColumnType::ValueType;
            ValueType value = unalignedLoad<ValueType>(state.index_column->getDataAt(index).data);
            return DefaultHash<ValueType>()(value);
        }
    }
};


/**
  * ReverseIndexHashTableBase implements a special hash table interface for
  * reverse index.
  *
  * The following requirements are different compared to a plain hash table:
  *
  * 1) Provide public access to 'hash table state' that contains
  * additional data needed to calculate cell hashes.
  *
  * 2) Support emplace() and find() with a Key different from the resulting
  * hash table key. This means emplace() accepts a different kind of object
  * as a key, and then the real key can be read from the returned cell iterator.
  *
  * These requirements are unique to ReverseIndex and are in conflict with
  * supporting hash tables that use alternative key storage, such as FixedHashMap
  * or StringHashMap. Therefore, we implement an interface for ReverseIndex
  * separately.
  */
template <typename Key, typename Cell, typename Hash>
class ReverseIndexHashTableBase : public HashTable<Key, Cell, Hash, HashTableGrowerWithPrecalculation<>, HashTableAllocator>
{
    using State = typename Cell::State;
    using Base = HashTable<Key, Cell, Hash, HashTableGrowerWithPrecalculation<>, HashTableAllocator>;

public:
    using Base::Base;
    using iterator = typename Base::iterator;
    using LookupResult = typename Base::LookupResult;
    State & getState() { return *this; }


    template <typename ObjectToCompareWith>
    size_t ALWAYS_INLINE reverseIndexFindCell(const ObjectToCompareWith & x, size_t hash_value, size_t place_value) const
    {
        while (!this->buf[place_value].isZero(*this) && !this->buf[place_value].keyEquals(x, hash_value, *this))
        {
            place_value = this->grower.next(place_value);
        }

        return place_value;
    }

    template <typename ObjectToCompareWith>
    void ALWAYS_INLINE
    reverseIndexEmplaceNonZero(const Key & key, LookupResult & it, bool & inserted, size_t hash_value, const ObjectToCompareWith & object)
    {
        size_t place_value = reverseIndexFindCell(object, hash_value, this->grower.place(hash_value));
        // emplaceNonZeroImpl() might need to re-find the cell if the table grows,
        // but it will find it correctly by the key alone, so we don't have to
        // pass it the 'object'.
        this->emplaceNonZeroImpl(place_value, key, it, inserted, hash_value);
    }

    /// Searches position by object.
    template <typename ObjectToCompareWith>
    void ALWAYS_INLINE reverseIndexEmplace(Key key, iterator & it, bool & inserted, size_t hash_value, const ObjectToCompareWith & object)
    {
        LookupResult impl_it = nullptr;

        if (!this->emplaceIfZero(key, impl_it, inserted, hash_value))
        {
            reverseIndexEmplaceNonZero(key, impl_it, inserted, hash_value, object);
        }
        assert(impl_it != nullptr);
        it = iterator(this, impl_it);
    }

    template <typename ObjectToCompareWith>
    iterator ALWAYS_INLINE reverseIndexFind(ObjectToCompareWith x, size_t hash_value)
    {
        if (Cell::isZero(x, *this))
            return this->hasZero() ? this->iteratorToZero() : this->end();

        size_t place_value = reverseIndexFindCell(x, hash_value, this->grower.place(hash_value));
        return !this->buf[place_value].isZero(*this) ? iterator(this, &this->buf[place_value]) : this->end();
    }
};

template <typename IndexType, typename ColumnType, bool has_base_index>
class ReverseIndexStringHashTable : public ReverseIndexHashTableBase<
                                        IndexType,
                                        ReverseIndexHashTableCell<
                                            IndexType,
                                            ReverseIndexHash,
                                            ReverseIndexStringHashTable<IndexType, ColumnType, has_base_index>,
                                            ColumnType,
                                            true,
                                            has_base_index>,
                                        ReverseIndexHash>
{
    using Base = ReverseIndexHashTableBase<
        IndexType,
        ReverseIndexHashTableCell<
            IndexType,
            ReverseIndexHash,
            ReverseIndexStringHashTable<IndexType, ColumnType, has_base_index>,
            ColumnType,
            true,
            has_base_index>,
        ReverseIndexHash>;

public:
    using Base::Base;
    friend struct ReverseIndexHashTableCell<
        IndexType,
        ReverseIndexHash,
        ReverseIndexStringHashTable<IndexType, ColumnType, has_base_index>,
        ColumnType,
        true,
        has_base_index>;
};

template <typename IndexType, typename ColumnType, bool has_base_index>
class ReverseIndexNumberHashTable : public ReverseIndexHashTableBase<
                                        IndexType,
                                        ReverseIndexHashTableCell<
                                            IndexType,
                                            ReverseIndexHash,
                                            ReverseIndexNumberHashTable<IndexType, ColumnType, has_base_index>,
                                            ColumnType,
                                            false,
                                            has_base_index>,
                                        ReverseIndexHash>
{
    using Base = ReverseIndexHashTableBase<
        IndexType,
        ReverseIndexHashTableCell<
            IndexType,
            ReverseIndexHash,
            ReverseIndexNumberHashTable<IndexType, ColumnType, has_base_index>,
            ColumnType,
            false,
            has_base_index>,
        ReverseIndexHash>;

public:
    using Base::Base;
    friend struct ReverseIndexHashTableCell<
        IndexType,
        ReverseIndexHash,
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
constexpr bool isNumericColumn(const T *)
{
    return false;
}

template <typename T>
constexpr bool isNumericColumn(const ColumnVector<T> *)
{
    return true;
}

static_assert(isNumericColumn(static_cast<ColumnVector<UInt8> *>(nullptr)));
static_assert(!isNumericColumn(static_cast<ColumnString *>(nullptr)));


template <typename IndexType, typename ColumnType, bool has_base_index>
using ReverseIndexHashTable =
    typename SelectReverseIndexHashTable<IndexType, ColumnType, has_base_index, isNumericColumn(static_cast<ColumnType *>(nullptr))>::Type;


template <typename IndexType, typename ColumnType>
class ReverseIndex
{
public:
    ReverseIndex(UInt64 num_prefix_rows_to_skip_, UInt64 base_index_)
        : num_prefix_rows_to_skip(num_prefix_rows_to_skip_), base_index(base_index_), external_saved_hash_ptr(nullptr) {}

    void setColumn(ColumnType * column_);

    static constexpr bool is_numeric_column = isNumericColumn(static_cast<ColumnType *>(nullptr));
    static constexpr bool use_saved_hash = !is_numeric_column;

    UInt64 insert(StringRef data);

    /// Returns the found data's index in the dictionary. If index is not built, builds it.
    UInt64 getInsertionPoint(StringRef data)
    {
        if (!index)
            buildIndex();
        return getIndexImpl(data);
    }

    /// Returns the found data's index in the dictionary if the #index is built, otherwise, returns a std::nullopt.
    std::optional<UInt64> getIndex(StringRef data) const
    {
        if (!index)
            return {};
        return getIndexImpl(data);
    }

    UInt64 lastInsertionPoint() const { return size() + base_index; }

    ColumnType * getColumn() const { return column; }
    size_t size() const;

    const UInt64 * tryGetSavedHash() const
    {
        if (!use_saved_hash)
            return nullptr;

        UInt64 * ptr = external_saved_hash_ptr.load();
        if (!ptr)
        {
            auto hash = calcHashes();
            ptr = &hash->getData()[0];
            UInt64 * expected = nullptr;
            if (external_saved_hash_ptr.compare_exchange_strong(expected, ptr))
                external_saved_hash = std::move(hash);
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
    /// For usage during GROUP BY
    mutable ColumnUInt64::MutablePtr external_saved_hash;
    mutable std::atomic<UInt64 *> external_saved_hash_ptr;

    void buildIndex();

    UInt64 getHash(StringRef ref) const
    {
        if constexpr (is_numeric_column)
        {
            using ValueType = typename ColumnType::ValueType;
            ValueType value = unalignedLoad<ValueType>(ref.data);
            return DefaultHash<ValueType>()(value);
        }
        else
            return StringRefHash()(ref);
    }

    ColumnUInt64::MutablePtr calcHashes() const;

    UInt64 getIndexImpl(StringRef data) const;
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

    for (auto row : collections::range(num_prefix_rows_to_skip, size))
    {
        UInt64 hash;
        if constexpr (use_saved_hash)
            hash = saved_hash->getElement(row);
        else
            hash = getHash(column->getDataAt(row));

        index->reverseIndexEmplace(row + base_index, iterator, inserted, hash, column->getDataAt(row));

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

    for (auto row : collections::range(0, size))
        hash->getElement(row) = getHash(column->getDataAt(row));

    return hash;
}

template <typename IndexType, typename ColumnType>
UInt64 ReverseIndex<IndexType, ColumnType>::insert(StringRef data)
{
    if (!index)
        buildIndex();

    using IteratorType = typename IndexMapType::iterator;
    IteratorType iterator;
    bool inserted;

    auto hash = getHash(data);
    UInt64 num_rows = size();

    if constexpr (use_saved_hash)
    {
        auto & column_data = saved_hash->getData();
        if (column_data.size() <= num_rows)
            column_data.resize(num_rows + 1);
        column_data[num_rows] = hash;
    }
    else
        column->insertData(data.data, data.size);

    index->reverseIndexEmplace(num_rows + base_index, iterator, inserted, hash, data);

    if constexpr (use_saved_hash)
    {
        if (inserted)
            column->insertData(data.data, data.size);
    }
    else
    {
        if (!inserted)
            column->popBack(1);
    }

    return iterator->getValue();
}

template <typename IndexType, typename ColumnType>
UInt64 ReverseIndex<IndexType, ColumnType>::getIndexImpl(StringRef data) const
{
    using IteratorType = typename IndexMapType::iterator;
    IteratorType iterator;

    auto hash = getHash(data);
    iterator = index->reverseIndexFind(data, hash);

    return iterator == index->end() ? size() + base_index : iterator->getValue();
}
}
