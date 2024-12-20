#include <Columns/ColumnLowCardinality.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/NumberTraits.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashMap.h>
#include <Common/WeakHash.h>
#include <Common/assert_cast.h>
#include <base/types.h>
#include <base/sort.h>
#include <base/scope_guard.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

namespace
{
    void checkColumn(const IColumn & column)
    {
        if (!dynamic_cast<const IColumnUnique *>(&column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "ColumnUnique expected as an argument of ColumnLowCardinality.");
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
            hash_map.insert({val, static_cast<T>(hash_map.size())});

        auto res_col = ColumnVector<T>::create();
        auto & data = res_col->getData();

        data.resize(hash_map.size());
        for (const auto & val : hash_map)
            data[val.getMapped()] = val.getKey();

        for (auto & ind : index)
            ind = hash_map[ind];

        for (size_t i = 0; i < index.size(); ++i)
            if (data[index[i]] != copy[i])
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected {}, but got {}", toString(data[index[i]]), toString(copy[i]));

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

        auto map_size = static_cast<UInt64>(max_val) + 1;
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

        auto res_col = ColumnVector<T>::create(static_cast<UInt64>(cur_pos) + 1);
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
        if (auto * data_uint16 = getIndexesData<UInt16>(column))
            return mapUniqueIndexImpl(*data_uint16);
        if (auto * data_uint32 = getIndexesData<UInt32>(column))
            return mapUniqueIndexImpl(*data_uint32);
        if (auto * data_uint64 = getIndexesData<UInt64>(column))
            return mapUniqueIndexImpl(*data_uint64);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Indexes column for getUniqueIndex must be ColumnUInt, got {}", column.getName());
    }
}


ColumnLowCardinality::ColumnLowCardinality(MutableColumnPtr && column_unique_, MutableColumnPtr && indexes_, bool is_shared)
    : dictionary(std::move(column_unique_), is_shared), idx(std::move(indexes_))
{
}

void ColumnLowCardinality::insert(const Field & x)
{
    compactIfSharedDictionary();
    idx.insertPosition(getDictionary().uniqueInsert(x));
}

bool ColumnLowCardinality::tryInsert(const Field & x)
{
    compactIfSharedDictionary();

    size_t index;
    if (!dictionary.getColumnUnique().tryUniqueInsert(x, index))
        return false;

    idx.insertPosition(index);
    return true;
}

void ColumnLowCardinality::insertDefault()
{
    idx.insertPosition(getDictionary().getDefaultValueIndex());
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnLowCardinality::insertFrom(const IColumn & src, size_t n)
#else
void ColumnLowCardinality::doInsertFrom(const IColumn & src, size_t n)
#endif
{
    const auto * low_cardinality_src = typeid_cast<const ColumnLowCardinality *>(&src);

    if (!low_cardinality_src)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected ColumnLowCardinality, got {}", src.getName());

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
        idx.insertPosition(getDictionary().uniqueInsertFrom(nested, position));
    }
}

void ColumnLowCardinality::insertFromFullColumn(const IColumn & src, size_t n)
{
    compactIfSharedDictionary();
    idx.insertPosition(getDictionary().uniqueInsertFrom(src, n));
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnLowCardinality::insertRangeFrom(const IColumn & src, size_t start, size_t length)
#else
void ColumnLowCardinality::doInsertRangeFrom(const IColumn & src, size_t start, size_t length)
#endif
{
    const auto * low_cardinality_src = typeid_cast<const ColumnLowCardinality *>(&src);

    if (!low_cardinality_src)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected ColumnLowCardinality, got {}", src.getName());

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

        auto inserted_indexes = getDictionary().uniqueInsertRangeFrom(*used_keys, 0, used_keys->size());
        idx.insertPositionsRange(*inserted_indexes->index(*sub_idx, 0), 0, length);
    }
}

void ColumnLowCardinality::insertRangeFromFullColumn(const IColumn & src, size_t start, size_t length)
{
    compactIfSharedDictionary();
    auto inserted_indexes = getDictionary().uniqueInsertRangeFrom(src, start, length);
    idx.insertPositionsRange(*inserted_indexes, 0, length);
}

static void checkPositionsAreLimited(const IColumn & positions, UInt64 limit)
{
    auto check_for_type = [&](auto type)
    {
        using ColumnType = decltype(type);
        const auto * column_ptr = typeid_cast<const ColumnVector<ColumnType> *>(&positions);

        if (!column_ptr)
            return false;

        const auto & data = column_ptr->getData();
        size_t num_rows = data.size();
        UInt64 max_position = 0;
        for (size_t i = 0; i < num_rows; ++i)
            max_position = std::max<UInt64>(max_position, data[i]);

        if (max_position >= limit)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                            "Index for LowCardinality is out of range. Dictionary size is {}, "
                            "but found index with value {}", limit, max_position);

        return true;
    };

    if (!check_for_type(UInt8()) &&
        !check_for_type(UInt16()) &&
        !check_for_type(UInt32()) &&
        !check_for_type(UInt64()))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Invalid column for ColumnLowCardinality index. Expected UInt, got {}",
                        positions.getName());
}

void ColumnLowCardinality::insertRangeFromDictionaryEncodedColumn(const IColumn & keys, const IColumn & positions)
{
    checkPositionsAreLimited(positions, keys.size());
    compactIfSharedDictionary();
    auto inserted_indexes = getDictionary().uniqueInsertRangeFrom(keys, 0, keys.size());
    idx.insertPositionsRange(*inserted_indexes->index(positions, 0), 0, positions.size());
}

void ColumnLowCardinality::insertData(const char * pos, size_t length)
{
    compactIfSharedDictionary();
    idx.insertPosition(getDictionary().uniqueInsertData(pos, length));
}

StringRef ColumnLowCardinality::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    return getDictionary().serializeValueIntoArena(getIndexes().getUInt(n), arena, begin);
}

char * ColumnLowCardinality::serializeValueIntoMemory(size_t n, char * memory) const
{
    return getDictionary().serializeValueIntoMemory(getIndexes().getUInt(n), memory);
}

void ColumnLowCardinality::collectSerializedValueSizes(PaddedPODArray<UInt64> & sizes, const UInt8 * is_null) const
{
    /// nullable is handled internally.
    chassert(is_null == nullptr);
    if (empty())
        return;

    size_t rows = size();
    if (sizes.empty())
        sizes.resize_fill(rows);
    else if (sizes.size() != rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Size of sizes: {} doesn't match rows_num: {}. It is a bug", sizes.size(), rows);

    PaddedPODArray<UInt64> dict_sizes;
    getDictionary().collectSerializedValueSizes(dict_sizes, nullptr);
    idx.collectSerializedValueSizes(sizes, dict_sizes);
}

const char * ColumnLowCardinality::deserializeAndInsertFromArena(const char * pos)
{
    compactIfSharedDictionary();

    const char * new_pos;
    idx.insertPosition(getDictionary().uniqueDeserializeAndInsertFromArena(pos, new_pos));

    return new_pos;
}

const char * ColumnLowCardinality::skipSerializedInArena(const char * pos) const
{
    return getDictionary().skipSerializedInArena(pos);
}

WeakHash32 ColumnLowCardinality::getWeakHash32() const
{
    WeakHash32 dict_hash = getDictionary().getNestedColumn()->getWeakHash32();
    return idx.getWeakHash(dict_hash);
}

void ColumnLowCardinality::updateHashFast(SipHash & hash) const
{
    idx.getPositions()->updateHashFast(hash);
    getDictionary().getNestedColumn()->updateHashFast(hash);
}

MutableColumnPtr ColumnLowCardinality::cloneResized(size_t size) const
{
    auto unique_ptr = dictionary.getColumnUniquePtr();
    if (size == 0)
        unique_ptr = unique_ptr->cloneEmpty();

    return ColumnLowCardinality::create(IColumn::mutate(std::move(unique_ptr)), getIndexes().cloneResized(size));
}

MutableColumnPtr ColumnLowCardinality::cloneNullable() const
{
    auto res = cloneFinalized();
    /* Compact required not to share dictionary.
     * If `shared` flag is not set `cloneFinalized` will return shallow copy
     * and `nestedToNullable` will mutate source column.
     */
    assert_cast<ColumnLowCardinality &>(*res).compactInplace();
    assert_cast<ColumnLowCardinality &>(*res).nestedToNullable();
    return res;
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

#if !defined(DEBUG_OR_SANITIZER_BUILD)
int ColumnLowCardinality::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#else
int ColumnLowCardinality::doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#endif
{
    return compareAtImpl(n, m, rhs, nan_direction_hint);
}

int ColumnLowCardinality::compareAtWithCollation(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint, const Collator & collator) const
{
    return compareAtImpl(n, m, rhs, nan_direction_hint, &collator);
}

bool ColumnLowCardinality::hasEqualValues() const
{
    if (getDictionary().size() <= 1)
        return true;
    return getIndexes().hasEqualValues();
}

void ColumnLowCardinality::getPermutationImpl(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                            size_t limit, int nan_direction_hint, Permutation & res, const Collator * collator) const
{
    if (limit == 0)
        limit = size();

    size_t unique_limit = getDictionary().size();
    Permutation unique_perm;
    if (collator)
        getDictionary().getNestedColumn()->getPermutationWithCollation(*collator, direction, stability, unique_limit, nan_direction_hint, unique_perm);
    else
        getDictionary().getNestedColumn()->getPermutation(direction, stability, unique_limit, nan_direction_hint, unique_perm);

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

void ColumnLowCardinality::getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                        size_t limit, int nan_direction_hint, IColumn::Permutation & res) const
{
    getPermutationImpl(direction, stability, limit, nan_direction_hint, res);
}

void ColumnLowCardinality::updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                        size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const
{
    bool ascending = direction == IColumn::PermutationSortDirection::Ascending;

    auto comparator = [this, ascending, stability, nan_direction_hint](size_t lhs, size_t rhs)
    {
        int ret = getDictionary().compareAt(getIndexes().getUInt(lhs), getIndexes().getUInt(rhs), getDictionary(), nan_direction_hint);
        if (unlikely(stability == IColumn::PermutationSortStability::Stable && ret == 0))
            return lhs < rhs;

        if (ascending)
            return ret < 0;
        return ret > 0;
    };

    auto equal_comparator = [this, nan_direction_hint](size_t lhs, size_t rhs)
    {
        int ret = getDictionary().compareAt(getIndexes().getUInt(lhs), getIndexes().getUInt(rhs), getDictionary(), nan_direction_hint);
        return ret == 0;
    };

    updatePermutationImpl(limit, res, equal_ranges, comparator, equal_comparator, DefaultSort(), DefaultPartialSort());
}

void ColumnLowCardinality::getPermutationWithCollation(const Collator & collator, IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                                        size_t limit, int nan_direction_hint, IColumn::Permutation & res) const
{
    getPermutationImpl(direction, stability, limit, nan_direction_hint, res, &collator);
}

void ColumnLowCardinality::updatePermutationWithCollation(const Collator & collator, IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                                        size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const
{
    bool ascending = direction == IColumn::PermutationSortDirection::Ascending;

    auto comparator = [this, &collator, ascending, stability, nan_direction_hint](size_t lhs, size_t rhs)
    {
        auto nested_column = getDictionary().getNestedColumn();
        size_t lhs_index = getIndexes().getUInt(lhs);
        size_t rhs_index = getIndexes().getUInt(rhs);

        int ret = nested_column->compareAtWithCollation(lhs_index, rhs_index, *nested_column, nan_direction_hint, collator);

        if (unlikely(stability == IColumn::PermutationSortStability::Stable && ret == 0))
            return lhs < rhs;

        if (ascending)
            return ret < 0;
        return ret > 0;
    };

    auto equal_comparator = [this, &collator, nan_direction_hint](size_t lhs, size_t rhs)
    {
        int ret = getDictionary().getNestedColumn()->compareAtWithCollation(getIndexes().getUInt(lhs), getIndexes().getUInt(rhs), *getDictionary().getNestedColumn(), nan_direction_hint, collator);
        return ret == 0;
    };

    updatePermutationImpl(limit, res, equal_ranges, comparator, equal_comparator, DefaultSort(), DefaultPartialSort());
}

size_t ColumnLowCardinality::estimateCardinalityInPermutedRange(const Permutation & permutation, const EqualRange & equal_range) const
{
    const size_t range_size = equal_range.size();
    if (range_size <= 1)
        return range_size;

    HashSet<UInt64> elements;
    for (size_t i = equal_range.from; i < equal_range.to; ++i)
    {
        UInt64 index = getIndexes().getUInt(permutation[i]);
        elements.insert(index);
    }
    return elements.size();
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't set ColumnUnique for "
                        "ColumnLowCardinality because is't not empty.");

    dictionary.setShared(column_unique);
}

ColumnLowCardinality::MutablePtr ColumnLowCardinality::cutAndCompact(size_t start, size_t length) const
{
    auto sub_positions = IColumn::mutate(idx.getPositions()->cut(start, length));
    auto new_column_unique = Dictionary::compact(getDictionary(), sub_positions);
    return ColumnLowCardinality::create(std::move(new_column_unique), std::move(sub_positions));
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for ColumnLowCardinality: {}",
                            size_of_type);
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

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected indexes type for ColumnLowCardinality. Expected UInt, got {}",
                    column.getName());
}

void ColumnLowCardinality::Index::attachPositions(MutableColumnPtr positions_)
{
    positions = std::move(positions_);
    updateSizeOfType();
}

template <typename IndexType>
typename ColumnVector<IndexType>::Container & ColumnLowCardinality::Index::getPositionsData()
{
    auto * positions_ptr = typeid_cast<ColumnVector<IndexType> *>(positions->assumeMutable().get());
    if (!positions_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid indexes type for ColumnLowCardinality. Expected UInt{}, got {}",
                        8 * sizeof(IndexType), positions->getName());

    return positions_ptr->getData();
}

template <typename IndexType>
const typename ColumnVector<IndexType>::Container & ColumnLowCardinality::Index::getPositionsData() const
{
    const auto * positions_ptr = typeid_cast<const ColumnVector<IndexType> *>(positions.get());
    if (!positions_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid indexes type for ColumnLowCardinality. Expected UInt{}, got {}",
                        8 * sizeof(IndexType), positions->getName());

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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Converting indexes to smaller type: from {} to {}",
                            sizeof(CurIndexType), sizeof(IndexType));

        if (sizeof(CurIndexType) != sizeof(IndexType))
        {
            size_t size = data.size();
            auto new_positions = ColumnVector<IndexType>::create(size);
            auto & new_data = new_positions->getData();

            /// TODO: Optimize with SSE?
            for (size_t i = 0; i < size; ++i)
                new_data[i] = static_cast<CurIndexType>(data[i]);

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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't expand indexes type for ColumnLowCardinality from type: {}",
                            demangle(typeid(CurIndexType).name()));

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
                    positions_data[size + i] = static_cast<CurIndexType>(column_data[offset + i]);
            };

            callForType(std::move(copy), size_of_type);
        }

        return true;
    };

    if (!insert_for_type(UInt8()) &&
        !insert_for_type(UInt16()) &&
        !insert_for_type(UInt32()) &&
        !insert_for_type(UInt64()))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Invalid column for ColumnLowCardinality index. Expected UInt, got {}",
                        column.getName());

    checkSizeOfType();
}

void ColumnLowCardinality::Index::checkSizeOfType()
{
    if (size_of_type != getSizeOfIndexType(*positions, size_of_type))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid size of type. Expected {}, but positions are {}",
                        8 * size_of_type, positions->getName());
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

WeakHash32 ColumnLowCardinality::Index::getWeakHash(const WeakHash32 & dict_hash) const
{
    WeakHash32 hash(positions->size());
    auto & hash_data = hash.getData();
    const auto & dict_hash_data = dict_hash.getData();

    auto update_weak_hash = [&](auto x)
    {
        using CurIndexType = decltype(x);
        auto & data = getPositionsData<CurIndexType>();
        auto size = data.size();

        for (size_t i = 0; i < size; ++i)
            hash_data[i] = dict_hash_data[data[i]];
    };

    callForType(std::move(update_weak_hash), size_of_type);
    return hash;
}

void ColumnLowCardinality::Index::collectSerializedValueSizes(
    PaddedPODArray<UInt64> & sizes, const PaddedPODArray<UInt64> & dict_sizes) const
{
    auto func = [&](auto x)
    {
        using CurIndexType = decltype(x);
        auto & data = getPositionsData<CurIndexType>();

        size_t rows = sizes.size();
        for (size_t i = 0; i < rows; ++i)
            sizes[i] += dict_sizes[data[i]];
    };
    callForType(std::move(func), size_of_type);
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

void ColumnLowCardinality::Dictionary::compact(MutableColumnPtr & positions)
{
    column_unique = compact(getColumnUnique(), positions);
    shared = false;
}

MutableColumnPtr ColumnLowCardinality::Dictionary::compact(const IColumnUnique & unique, MutableColumnPtr & positions)
{
    auto new_column_unique = unique.cloneEmpty();
    auto & new_unique = static_cast<IColumnUnique &>(*new_column_unique);

    auto indexes = mapUniqueIndex(*positions);
    auto sub_keys = unique.getNestedColumn()->index(*indexes, 0);
    auto new_indexes = new_unique.uniqueInsertRangeFrom(*sub_keys, 0, sub_keys->size());

    positions = IColumn::mutate(new_indexes->index(*positions, 0));
    return new_column_unique;
}

ColumnPtr ColumnLowCardinality::cloneWithDefaultOnNull() const
{
    if (!nestedIsNullable())
        return getPtr();

    auto res = cloneEmpty();
    auto & lc_res = assert_cast<ColumnLowCardinality &>(*res);
    lc_res.nestedRemoveNullable();
    size_t end = size();
    size_t start = 0;
    while (start < end)
    {
        size_t next_null_index = start;
        while (next_null_index < end && !isNullAt(next_null_index))
            ++next_null_index;

        if (next_null_index != start)
            lc_res.insertRangeFrom(*this, start, next_null_index - start);

        if (next_null_index < end)
            lc_res.insertDefault();

        start = next_null_index + 1;
    }

    return res;
}

bool isColumnLowCardinalityNullable(const IColumn & column)
{
    if (const auto * lc_column = checkAndGetColumn<ColumnLowCardinality>(&column))
        return lc_column->nestedIsNullable();
    return false;
}

}
