#include <Columns/ColumnLowCardinality.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/ColumnGathererStream.h>
#include <DataTypes/NumberTraits.h>
#include <Common/HashTable/HashMap.h>
#include <Common/WeakHash.h>
#include <Common/assert_cast.h>
#include <common/sort.h>
#include <ext/scope_guard.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
}

namespace
{
    void checkColumn(const IColumn & column)
    {
        if (!dynamic_cast<const IColumnUnique *>(&column))
            throw Exception("ColumnUnique expected as an argument of ColumnLowCardinality.", ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename T>
    PaddedPODArray<T> * getIndexesData(IColumn & indexes)
    {
        auto * column = typeid_cast<ColumnVector<T> *>(&indexes);
        if (column)
            return &column->getData();

        return nullptr;
    }

    template <typename T>
    MutableColumnPtr mapUniqueIndexImplRef(PaddedPODArray<T> & index)
    {
        PaddedPODArray<T> copy(index.cbegin(), index.cend());

        HashMap<T, T> hash_map;
        for (auto val : index)
            hash_map.insert({val, hash_map.size()});

        auto res_col = ColumnVector<T>::create();
        auto & data = res_col->getData();

        data.resize(hash_map.size());
        for (const auto & val : hash_map)
            data[val.getMapped()] = val.getKey();

        for (auto & ind : index)
            ind = hash_map[ind];

        for (size_t i = 0; i < index.size(); ++i)
            if (data[index[i]] != copy[i])
                throw Exception("Expected " + toString(data[index[i]]) + ", but got " + toString(copy[i]), ErrorCodes::LOGICAL_ERROR);

        return res_col;
    }

    template <typename T>
    MutableColumnPtr mapUniqueIndexImpl(PaddedPODArray<T> & index)
    {
        if (index.empty())
            return ColumnVector<T>::create();

        auto size = index.size();

        T max_val = index[0];
        for (size_t i = 1; i < size; ++i)
            max_val = std::max(max_val, index[i]);

        /// May happen when dictionary is shared.
        if (max_val > size)
            return mapUniqueIndexImplRef(index);

        auto map_size = UInt64(max_val) + 1;
        PaddedPODArray<T> map(map_size, 0);
        T zero_pos_value = index[0];
        index[0] = 0;
        T cur_pos = 0;
        for (size_t i = 1; i < size; ++i)
        {
            T val = index[i];
            if (val != zero_pos_value && map[val] == 0)
            {
                ++cur_pos;
                map[val] = cur_pos;
            }

            index[i] = map[val];
        }

        auto res_col = ColumnVector<T>::create(UInt64(cur_pos) + 1);
        auto & data = res_col->getData();
        data[0] = zero_pos_value;
        for (size_t i = 0; i < map_size; ++i)
        {
            auto val = map[i];
            if (val)
                data[val] = static_cast<T>(i);
        }

        return res_col;
    }

    /// Returns unique values of column. Write new index to column.
    MutableColumnPtr mapUniqueIndex(IColumn & column)
    {
        if (auto * data_uint8 = getIndexesData<UInt8>(column))
            return mapUniqueIndexImpl(*data_uint8);
        else if (auto * data_uint16 = getIndexesData<UInt16>(column))
            return mapUniqueIndexImpl(*data_uint16);
        else if (auto * data_uint32 = getIndexesData<UInt32>(column))
            return mapUniqueIndexImpl(*data_uint32);
        else if (auto * data_uint64 = getIndexesData<UInt64>(column))
            return mapUniqueIndexImpl(*data_uint64);
        else
            throw Exception("Indexes column for getUniqueIndex must be ColumnUInt, got" + column.getName(),
                            ErrorCodes::LOGICAL_ERROR);
    }
}


ColumnLowCardinality::ColumnLowCardinality(MutableColumnPtr && column_unique_, MutableColumnPtr && indexes_, bool is_shared)
    : dictionary(std::move(column_unique_), is_shared), idx(std::move(indexes_))
{
    idx.check(getDictionary().size());
}

void ColumnLowCardinality::insert(const Field & x)
{
    compactIfSharedDictionary();
    idx.insertPosition(dictionary.getColumnUnique().uniqueInsert(x));
    idx.check(getDictionary().size());
}

void ColumnLowCardinality::insertDefault()
{
    idx.insertPosition(getDictionary().getDefaultValueIndex());
}

void ColumnLowCardinality::insertFrom(const IColumn & src, size_t n)
{
    const auto * low_cardinality_src = typeid_cast<const ColumnLowCardinality *>(&src);

    if (!low_cardinality_src)
        throw Exception("Expected ColumnLowCardinality, got" + src.getName(), ErrorCodes::ILLEGAL_COLUMN);

    size_t position = low_cardinality_src->getIndexes().getUInt(n);

    if (&low_cardinality_src->getDictionary() == &getDictionary())
    {
        /// Dictionary is shared with src column. Insert only index.
        idx.insertPosition(position);
    }
    else
    {
        compactIfSharedDictionary();
        const auto & nested = *low_cardinality_src->getDictionary().getNestedColumn();
        idx.insertPosition(dictionary.getColumnUnique().uniqueInsertFrom(nested, position));
    }

    idx.check(getDictionary().size());
}

void ColumnLowCardinality::insertFromFullColumn(const IColumn & src, size_t n)
{
    compactIfSharedDictionary();
    idx.insertPosition(dictionary.getColumnUnique().uniqueInsertFrom(src, n));
    idx.check(getDictionary().size());
}

void ColumnLowCardinality::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const auto * low_cardinality_src = typeid_cast<const ColumnLowCardinality *>(&src);

    if (!low_cardinality_src)
        throw Exception("Expected ColumnLowCardinality, got " + src.getName(), ErrorCodes::ILLEGAL_COLUMN);

    if (&low_cardinality_src->getDictionary() == &getDictionary())
    {
        /// Dictionary is shared with src column. Insert only indexes.
        idx.insertPositionsRange(low_cardinality_src->getIndexes(), start, length);
    }
    else
    {
        compactIfSharedDictionary();

        /// TODO: Support native insertion from other unique column. It will help to avoid null map creation.

        auto sub_idx = IColumn::mutate(low_cardinality_src->getIndexes().cut(start, length));
        auto idx_map = mapUniqueIndex(*sub_idx);

        auto src_nested = low_cardinality_src->getDictionary().getNestedColumn();
        auto used_keys = src_nested->index(*idx_map, 0);

        auto inserted_indexes = dictionary.getColumnUnique().uniqueInsertRangeFrom(*used_keys, 0, used_keys->size());
        idx.insertPositionsRange(*inserted_indexes->index(*sub_idx, 0), 0, length);
    }
    idx.check(getDictionary().size());
}

void ColumnLowCardinality::insertRangeFromFullColumn(const IColumn & src, size_t start, size_t length)
{
    compactIfSharedDictionary();
    auto inserted_indexes = dictionary.getColumnUnique().uniqueInsertRangeFrom(src, start, length);
    idx.insertPositionsRange(*inserted_indexes, 0, length);
    idx.check(getDictionary().size());
}

void ColumnLowCardinality::insertRangeFromDictionaryEncodedColumn(const IColumn & keys, const IColumn & positions)
{
    Index(positions.getPtr()).check(keys.size());
    compactIfSharedDictionary();
    auto inserted_indexes = dictionary.getColumnUnique().uniqueInsertRangeFrom(keys, 0, keys.size());
    idx.insertPositionsRange(*inserted_indexes->index(positions, 0), 0, positions.size());
    idx.check(getDictionary().size());
}

void ColumnLowCardinality::insertData(const char * pos, size_t length)
{
    compactIfSharedDictionary();
    idx.insertPosition(dictionary.getColumnUnique().uniqueInsertData(pos, length));
    idx.check(getDictionary().size());
}

StringRef ColumnLowCardinality::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    return getDictionary().serializeValueIntoArena(getIndexes().getUInt(n), arena, begin);
}

const char * ColumnLowCardinality::deserializeAndInsertFromArena(const char * pos)
{
    compactIfSharedDictionary();

    const char * new_pos;
    idx.insertPosition(dictionary.getColumnUnique().uniqueDeserializeAndInsertFromArena(pos, new_pos));

    idx.check(getDictionary().size());
    return new_pos;
}

void ColumnLowCardinality::updateWeakHash32(WeakHash32 & hash) const
{
    auto s = size();

    if (hash.getData().size() != s)
        throw Exception("Size of WeakHash32 does not match size of column: column size is " + std::to_string(s) +
                        ", hash size is " + std::to_string(hash.getData().size()), ErrorCodes::LOGICAL_ERROR);

    const auto & dict = getDictionary().getNestedColumn();
    WeakHash32 dict_hash(dict->size());
    dict->updateWeakHash32(dict_hash);

    idx.updateWeakHash(hash, dict_hash);
}

void ColumnLowCardinality::updateHashFast(SipHash & hash) const
{
    idx.getPositions()->updateHashFast(hash);
    getDictionary().getNestedColumn()->updateHashFast(hash);
}

void ColumnLowCardinality::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

MutableColumnPtr ColumnLowCardinality::cloneResized(size_t size) const
{
    auto unique_ptr = dictionary.getColumnUniquePtr();
    if (size == 0)
        unique_ptr = unique_ptr->cloneEmpty();

    return ColumnLowCardinality::create(IColumn::mutate(std::move(unique_ptr)), getIndexes().cloneResized(size));
}

int ColumnLowCardinality::compareAtImpl(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint, const Collator * collator) const
{
    const auto & low_cardinality_column = assert_cast<const ColumnLowCardinality &>(rhs);
    size_t n_index = getIndexes().getUInt(n);
    size_t m_index = low_cardinality_column.getIndexes().getUInt(m);
    if (collator)
        return getDictionary().getNestedColumn()->compareAtWithCollation(n_index, m_index, *low_cardinality_column.getDictionary().getNestedColumn(), nan_direction_hint, *collator);
    return getDictionary().compareAt(n_index, m_index, low_cardinality_column.getDictionary(), nan_direction_hint);
}

int ColumnLowCardinality::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
{
    return compareAtImpl(n, m, rhs, nan_direction_hint);
}

int ColumnLowCardinality::compareAtWithCollation(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint, const Collator & collator) const
{
    return compareAtImpl(n, m, rhs, nan_direction_hint, &collator);
}

void ColumnLowCardinality::compareColumn(const IColumn & rhs, size_t rhs_row_num,
                                         PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                                         int direction, int nan_direction_hint) const
{
    return doCompareColumn<ColumnLowCardinality>(
            assert_cast<const ColumnLowCardinality &>(rhs), rhs_row_num, row_indexes,
            compare_results, direction, nan_direction_hint);
}

void ColumnLowCardinality::getPermutationImpl(bool reverse, size_t limit, int nan_direction_hint, Permutation & res, const Collator * collator) const
{
    if (limit == 0)
        limit = size();

    size_t unique_limit = getDictionary().size();
    Permutation unique_perm;
    if (collator)
        getDictionary().getNestedColumn()->getPermutationWithCollation(*collator, reverse, unique_limit, nan_direction_hint, unique_perm);
    else
        getDictionary().getNestedColumn()->getPermutation(reverse, unique_limit, nan_direction_hint, unique_perm);

    /// TODO: optimize with sse.

    /// Get indexes per row in column_unique.
    std::vector<std::vector<size_t>> indexes_per_row(getDictionary().size());
    size_t indexes_size = getIndexes().size();
    for (size_t row = 0; row < indexes_size; ++row)
        indexes_per_row[getIndexes().getUInt(row)].push_back(row);

    /// Replicate permutation.
    size_t perm_size = std::min(indexes_size, limit);
    res.resize(perm_size);
    size_t perm_index = 0;
    for (size_t row = 0; row < unique_perm.size() && perm_index < perm_size; ++row)
    {
        const auto & row_indexes = indexes_per_row[unique_perm[row]];
        for (auto row_index : row_indexes)
        {
            res[perm_index] = row_index;
            ++perm_index;

            if (perm_index == perm_size)
                break;
        }
    }
}

template <typename Cmp>
void ColumnLowCardinality::updatePermutationImpl(size_t limit, Permutation & res, EqualRanges & equal_ranges, Cmp comparator) const
{
    if (equal_ranges.empty())
        return;

    if (limit >= size() || limit >= equal_ranges.back().second)
        limit = 0;

    size_t number_of_ranges = equal_ranges.size();
    if (limit)
        --number_of_ranges;

    EqualRanges new_ranges;
    SCOPE_EXIT({equal_ranges = std::move(new_ranges);});

    auto less = [&comparator](size_t lhs, size_t rhs){ return comparator(lhs, rhs) < 0; };

    for (size_t i = 0; i < number_of_ranges; ++i)
    {
        const auto& [first, last] = equal_ranges[i];
        std::sort(res.begin() + first, res.begin() + last, less);

        auto new_first = first;
        for (auto j = first + 1; j < last; ++j)
        {
            if (comparator(res[new_first], res[j]) != 0)
            {
                if (j - new_first > 1)
                    new_ranges.emplace_back(new_first, j);

                new_first = j;
            }
        }
        if (last - new_first > 1)
            new_ranges.emplace_back(new_first, last);
    }

    if (limit)
    {
        const auto & [first, last] = equal_ranges.back();

        if (limit < first || limit > last)
            return;

        /// Since then we are working inside the interval.

        partial_sort(res.begin() + first, res.begin() + limit, res.begin() + last, less);
        auto new_first = first;

        for (auto j = first + 1; j < limit; ++j)
        {
            if (comparator(res[new_first],res[j]) != 0)
            {
                if (j - new_first > 1)
                    new_ranges.emplace_back(new_first, j);

                new_first = j;
            }
        }

        auto new_last = limit;
        for (auto j = limit; j < last; ++j)
        {
            if (comparator(res[new_first], res[j]) == 0)
            {
                std::swap(res[new_last], res[j]);
                ++new_last;
            }
        }
        if (new_last - new_first > 1)
            new_ranges.emplace_back(new_first, new_last);
    }
}

void ColumnLowCardinality::getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const
{
    getPermutationImpl(reverse, limit, nan_direction_hint, res);
}

void ColumnLowCardinality::updatePermutation(bool reverse, size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const
{
    auto comparator = [this, nan_direction_hint, reverse](size_t lhs, size_t rhs)
    {
        int ret = getDictionary().compareAt(getIndexes().getUInt(lhs), getIndexes().getUInt(rhs), getDictionary(), nan_direction_hint);
        return reverse ? -ret : ret;
    };

    updatePermutationImpl(limit, res, equal_ranges, comparator);
}

void ColumnLowCardinality::getPermutationWithCollation(const Collator & collator, bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const
{
    getPermutationImpl(reverse, limit, nan_direction_hint, res, &collator);
}

void ColumnLowCardinality::updatePermutationWithCollation(const Collator & collator, bool reverse, size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_ranges) const
{
    auto comparator = [this, &collator, reverse, nan_direction_hint](size_t lhs, size_t rhs)
    {
        int ret = getDictionary().getNestedColumn()->compareAtWithCollation(getIndexes().getUInt(lhs), getIndexes().getUInt(rhs), *getDictionary().getNestedColumn(), nan_direction_hint, collator);
        return reverse ? -ret : ret;
    };

    updatePermutationImpl(limit, res, equal_ranges, comparator);
}

std::vector<MutableColumnPtr> ColumnLowCardinality::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    auto columns = getIndexes().scatter(num_columns, selector);
    for (auto & column : columns)
    {
        auto unique_ptr = dictionary.getColumnUniquePtr();
        column = ColumnLowCardinality::create(IColumn::mutate(std::move(unique_ptr)), std::move(column));
    }

    return columns;
}

void ColumnLowCardinality::setSharedDictionary(const ColumnPtr & column_unique)
{
    if (!empty())
        throw Exception("Can't set ColumnUnique for ColumnLowCardinality because is't not empty.",
                        ErrorCodes::LOGICAL_ERROR);

    dictionary.setShared(column_unique);
}

ColumnLowCardinality::MutablePtr ColumnLowCardinality::cutAndCompact(size_t start, size_t length) const
{
    auto sub_positions = IColumn::mutate(idx.getPositions()->cut(start, length));
    /// Create column with new indexes and old dictionary.
    /// Dictionary is shared, but will be recreated after compactInplace call.
    auto column = ColumnLowCardinality::create(getDictionary().assumeMutable(), std::move(sub_positions));
    /// Will create new dictionary.
    column->compactInplace();

    return column;
}

void ColumnLowCardinality::compactInplace()
{
    auto positions = idx.detachPositions();
    dictionary.compact(positions);
    idx.attachPositions(std::move(positions));
}

void ColumnLowCardinality::compactIfSharedDictionary()
{
    if (dictionary.isShared())
        compactInplace();
}


ColumnLowCardinality::DictionaryEncodedColumn
ColumnLowCardinality::getMinimalDictionaryEncodedColumn(UInt64 offset, UInt64 limit) const
{
    MutableColumnPtr sub_indexes = IColumn::mutate(idx.getPositions()->cut(offset, limit));
    auto indexes_map = mapUniqueIndex(*sub_indexes);
    auto sub_keys = getDictionary().getNestedColumn()->index(*indexes_map, 0);

    return {std::move(sub_keys), std::move(sub_indexes)};
}

ColumnPtr ColumnLowCardinality::countKeys() const
{
    const auto & nested_column = getDictionary().getNestedColumn();
    size_t dict_size = nested_column->size();

    auto counter = ColumnUInt64::create(dict_size, 0);
    idx.countKeys(counter->getData());
    return counter;
}

bool ColumnLowCardinality::containsNull() const
{
    return getDictionary().nestedColumnIsNullable() && idx.containsDefault();
}


ColumnLowCardinality::Index::Index() : positions(ColumnUInt8::create()), size_of_type(sizeof(UInt8)) {}

ColumnLowCardinality::Index::Index(MutableColumnPtr && positions_) : positions(std::move(positions_))
{
    updateSizeOfType();
}

ColumnLowCardinality::Index::Index(ColumnPtr positions_) : positions(std::move(positions_))
{
    updateSizeOfType();
}

template <typename Callback>
void ColumnLowCardinality::Index::callForType(Callback && callback, size_t size_of_type)
{
    switch (size_of_type)
    {
        case sizeof(UInt8): { callback(UInt8()); break; }
        case sizeof(UInt16): { callback(UInt16()); break; }
        case sizeof(UInt32): { callback(UInt32()); break; }
        case sizeof(UInt64): { callback(UInt64()); break; }
        default: {
            throw Exception("Unexpected size of index type for ColumnLowCardinality: " + toString(size_of_type),
                            ErrorCodes::LOGICAL_ERROR);
        }
    }
}

size_t ColumnLowCardinality::Index::getSizeOfIndexType(const IColumn & column, size_t hint)
{
    auto check_for = [&](auto type) { return typeid_cast<const ColumnVector<decltype(type)> *>(&column) != nullptr; };
    auto try_get_size_for = [&](auto type) -> size_t { return check_for(type) ? sizeof(decltype(type)) : 0; };

    if (hint)
    {
        size_t size = 0;
        callForType([&](auto type) { size = try_get_size_for(type); }, hint);

        if (size)
            return size;
    }

    if (auto size = try_get_size_for(UInt8()))
        return size;
    if (auto size = try_get_size_for(UInt16()))
        return size;
    if (auto size = try_get_size_for(UInt32()))
        return size;
    if (auto size = try_get_size_for(UInt64()))
        return size;

    throw Exception("Unexpected indexes type for ColumnLowCardinality. Expected UInt, got " + column.getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
}

void ColumnLowCardinality::Index::attachPositions(ColumnPtr positions_)
{
    positions = std::move(positions_);
    updateSizeOfType();
}

template <typename IndexType>
typename ColumnVector<IndexType>::Container & ColumnLowCardinality::Index::getPositionsData()
{
    auto * positions_ptr = typeid_cast<ColumnVector<IndexType> *>(positions->assumeMutable().get());
    if (!positions_ptr)
        throw Exception("Invalid indexes type for ColumnLowCardinality."
                        " Expected UInt" + toString(8 * sizeof(IndexType)) + ", got " + positions->getName(),
                        ErrorCodes::LOGICAL_ERROR);

    return positions_ptr->getData();
}

template <typename IndexType>
const typename ColumnVector<IndexType>::Container & ColumnLowCardinality::Index::getPositionsData() const
{
    const auto * positions_ptr = typeid_cast<const ColumnVector<IndexType> *>(positions.get());
    if (!positions_ptr)
        throw Exception("Invalid indexes type for ColumnLowCardinality."
                        " Expected UInt" + toString(8 * sizeof(IndexType)) + ", got " + positions->getName(),
                        ErrorCodes::LOGICAL_ERROR);

    return positions_ptr->getData();
}

template <typename IndexType>
void ColumnLowCardinality::Index::convertPositions()
{
    auto convert = [&](auto x)
    {
        using CurIndexType = decltype(x);
        auto & data = getPositionsData<CurIndexType>();

        if (sizeof(CurIndexType) > sizeof(IndexType))
            throw Exception("Converting indexes to smaller type: from " + toString(sizeof(CurIndexType)) +
                            " to " + toString(sizeof(IndexType)), ErrorCodes::LOGICAL_ERROR);

        if (sizeof(CurIndexType) != sizeof(IndexType))
        {
            size_t size = data.size();
            auto new_positions = ColumnVector<IndexType>::create(size);
            auto & new_data = new_positions->getData();

            /// TODO: Optimize with SSE?
            for (size_t i = 0; i < size; ++i)
                new_data[i] = data[i];

            positions = std::move(new_positions);
            size_of_type = sizeof(IndexType);
        }
    };

    callForType(std::move(convert), size_of_type);

    checkSizeOfType();
}

void ColumnLowCardinality::Index::expandType()
{
    auto expand = [&](auto type)
    {
        using CurIndexType = decltype(type);
        constexpr auto next_size = NumberTraits::nextSize(sizeof(CurIndexType));
        if (next_size == sizeof(CurIndexType))
            throw Exception("Can't expand indexes type for ColumnLowCardinality from type: "
                            + demangle(typeid(CurIndexType).name()), ErrorCodes::LOGICAL_ERROR);

        using NewIndexType = typename NumberTraits::Construct<false, false, next_size>::Type;
        convertPositions<NewIndexType>();
    };

    callForType(std::move(expand), size_of_type);
}

UInt64 ColumnLowCardinality::Index::getMaxPositionForCurrentType() const
{
    UInt64 value = 0;
    callForType([&](auto type) { value = std::numeric_limits<decltype(type)>::max(); }, size_of_type);
    return value;
}

size_t ColumnLowCardinality::Index::getPositionAt(size_t row) const
{
    size_t pos;
    auto get_position = [&](auto type)
    {
        using CurIndexType = decltype(type);
        pos = getPositionsData<CurIndexType>()[row];
    };

    callForType(std::move(get_position), size_of_type);
    return pos;
}

void ColumnLowCardinality::Index::insertPosition(UInt64 position)
{
    while (position > getMaxPositionForCurrentType())
        expandType();

    positions->insert(position);
    checkSizeOfType();
}

void ColumnLowCardinality::Index::insertPositionsRange(const IColumn & column, UInt64 offset, UInt64 limit)
{
    auto insert_for_type = [&](auto type)
    {
        using ColumnType = decltype(type);
        const auto * column_ptr = typeid_cast<const ColumnVector<ColumnType> *>(&column);

        if (!column_ptr)
            return false;

        if (size_of_type < sizeof(ColumnType))
            convertPositions<ColumnType>();

        if (size_of_type == sizeof(ColumnType))
            positions->insertRangeFrom(column, offset, limit);
        else
        {
            auto copy = [&](auto cur_type)
            {
                using CurIndexType = decltype(cur_type);
                auto & positions_data = getPositionsData<CurIndexType>();
                const auto & column_data = column_ptr->getData();

                UInt64 size = positions_data.size();
                positions_data.resize(size + limit);

                for (UInt64 i = 0; i < limit; ++i)
                    positions_data[size + i] = column_data[offset + i];
            };

            callForType(std::move(copy), size_of_type);
        }

        return true;
    };

    if (!insert_for_type(UInt8()) &&
        !insert_for_type(UInt16()) &&
        !insert_for_type(UInt32()) &&
        !insert_for_type(UInt64()))
        throw Exception("Invalid column for ColumnLowCardinality index. Expected UInt, got " + column.getName(),
                        ErrorCodes::ILLEGAL_COLUMN);

    checkSizeOfType();
}

void ColumnLowCardinality::Index::check(size_t /*max_dictionary_size*/)
{
    /// TODO: remove
    /*
    auto check = [&](auto cur_type)
    {
        using CurIndexType = decltype(cur_type);
        auto & positions_data = getPositionsData<CurIndexType>();

        for (size_t i = 0; i < positions_data.size(); ++i)
        {
            if (positions_data[i] >= max_dictionary_size)
            {
                throw Exception("Found index " + toString(positions_data[i]) + " at position " + toString(i)
                                + " which is grated or equal than dictionary size " + toString(max_dictionary_size),
                                ErrorCodes::LOGICAL_ERROR);
            }
        }
    };

    callForType(std::move(check), size_of_type);
     */
}

void ColumnLowCardinality::Index::checkSizeOfType()
{
    if (size_of_type != getSizeOfIndexType(*positions, size_of_type))
        throw Exception("Invalid size of type. Expected " + toString(8 * size_of_type) +
                        ", but positions are " + positions->getName(), ErrorCodes::LOGICAL_ERROR);
}

void ColumnLowCardinality::Index::countKeys(ColumnUInt64::Container & counts) const
{
    auto counter = [&](auto x)
    {
        using CurIndexType = decltype(x);
        auto & data = getPositionsData<CurIndexType>();
        for (auto pos : data)
            ++counts[pos];
    };
    callForType(std::move(counter), size_of_type);
}

bool ColumnLowCardinality::Index::containsDefault() const
{
    bool contains = false;

    auto check_contains_default = [&](auto x)
    {
        using CurIndexType = decltype(x);
        auto & data = getPositionsData<CurIndexType>();
        for (auto pos : data)
        {
            if (pos == 0)
            {
                contains = true;
                break;
            }
        }
    };

    callForType(std::move(check_contains_default), size_of_type);
    return contains;
}

void ColumnLowCardinality::Index::updateWeakHash(WeakHash32 & hash, WeakHash32 & dict_hash) const
{
    auto & hash_data = hash.getData();
    auto & dict_hash_data = dict_hash.getData();

    auto update_weak_hash = [&](auto x)
    {
        using CurIndexType = decltype(x);
        auto & data = getPositionsData<CurIndexType>();
        auto size = data.size();

        for (size_t i = 0; i < size; ++i)
            hash_data[i] = intHashCRC32(dict_hash_data[data[i]], hash_data[i]);
    };

    callForType(std::move(update_weak_hash), size_of_type);
}


ColumnLowCardinality::Dictionary::Dictionary(MutableColumnPtr && column_unique_, bool is_shared)
    : column_unique(std::move(column_unique_)), shared(is_shared)
{
    checkColumn(*column_unique);
}
ColumnLowCardinality::Dictionary::Dictionary(ColumnPtr column_unique_, bool is_shared)
    : column_unique(std::move(column_unique_)), shared(is_shared)
{
    checkColumn(*column_unique);
}

void ColumnLowCardinality::Dictionary::setShared(const ColumnPtr & column_unique_)
{
    checkColumn(*column_unique_);

    column_unique = column_unique_;
    shared = true;
}

void ColumnLowCardinality::Dictionary::compact(ColumnPtr & positions)
{
    auto new_column_unique = column_unique->cloneEmpty();

    auto & unique = getColumnUnique();
    auto & new_unique = static_cast<IColumnUnique &>(*new_column_unique);

    auto indexes = mapUniqueIndex(positions->assumeMutableRef());
    auto sub_keys = unique.getNestedColumn()->index(*indexes, 0);
    auto new_indexes = new_unique.uniqueInsertRangeFrom(*sub_keys, 0, sub_keys->size());

    positions = IColumn::mutate(new_indexes->index(*positions, 0));
    column_unique = std::move(new_column_unique);

    shared = false;
}

}
