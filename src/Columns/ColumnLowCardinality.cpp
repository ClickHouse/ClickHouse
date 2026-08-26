#include <Columns/ColumnLowCardinality.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/NumberTraits.h>
#include <Common/Exception.h>
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

void throwUnexpectedLowCardinalityIndexType(size_t size)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for low cardinality column: {}", size);
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
    idx.insertIndex(getDictionary().uniqueInsert(x));
}

bool ColumnLowCardinality::tryInsert(const Field & x)
{
    compactIfSharedDictionary();

    size_t index;
    if (!dictionary.getColumnUnique().tryUniqueInsert(x, index))
        return false;

    idx.insertIndex(index);
    return true;
}

void ColumnLowCardinality::insertDefault()
{
    idx.insertIndex(getDictionary().getDefaultValueIndex());
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
        idx.insertIndex(position);
    }
    else
    {
        compactIfSharedDictionary();
        const auto & nested = *low_cardinality_src->getDictionary().getNestedColumn();
        idx.insertIndex(getDictionary().uniqueInsertFrom(nested, position));
    }
}

void ColumnLowCardinality::insertFromFullColumn(const IColumn & src, size_t n)
{
    compactIfSharedDictionary();
    idx.insertIndex(getDictionary().uniqueInsertFrom(src, n));
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
        idx.insertIndexesRange(low_cardinality_src->getIndexes(), start, length);
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
        idx.insertIndexesRange(*inserted_indexes->index(*sub_idx, 0), 0, length);
    }
}

void ColumnLowCardinality::insertRangeFromFullColumn(const IColumn & src, size_t start, size_t length)
{
    compactIfSharedDictionary();
    auto inserted_indexes = getDictionary().uniqueInsertRangeFrom(src, start, length);
    idx.insertIndexesRange(*inserted_indexes, 0, length);
}

static void checkIndexesAreLimited(const IColumn & indexes, UInt64 limit)
{
    auto check_for_type = [&](auto type)
    {
        using ColumnType = decltype(type);
        const auto * column_ptr = typeid_cast<const ColumnVector<ColumnType> *>(&indexes);

        if (!column_ptr)
            return false;

        const auto & data = column_ptr->getData();
        size_t num_rows = data.size();
        if (num_rows == 0)
            return true;
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
                        indexes.getName());
}

void ColumnLowCardinality::insertRangeFromDictionaryEncodedColumn(const IColumn & keys, const IColumn & indexes)
{
    checkIndexesAreLimited(indexes, keys.size());
    compactIfSharedDictionary();
    auto inserted_indexes = getDictionary().uniqueInsertRangeFrom(keys, 0, keys.size());
    idx.insertIndexesRange(*inserted_indexes->index(indexes, 0), 0, indexes.size());
}

void ColumnLowCardinality::insertData(const char * pos, size_t length)
{
    compactIfSharedDictionary();
    idx.insertIndex(getDictionary().uniqueInsertData(pos, length));
}

std::string_view ColumnLowCardinality::serializeValueIntoArena(
    size_t n, Arena & arena, char const *& begin, const IColumn::SerializationSettings * settings) const
{
    return getDictionary().serializeValueIntoArena(getIndexes().getUInt(n), arena, begin, settings);
}

char * ColumnLowCardinality::serializeValueIntoMemory(size_t n, char * memory, const IColumn::SerializationSettings * settings) const
{
    return getDictionary().serializeValueIntoMemory(getIndexes().getUInt(n), memory, settings);
}

std::optional<size_t> ColumnLowCardinality::getSerializedValueSize(size_t n, const IColumn::SerializationSettings * settings) const
{
    return getDictionary().getSerializedValueSize(getIndexes().getUInt(n), settings);
}

void ColumnLowCardinality::collectSerializedValueSizes(PaddedPODArray<UInt64> & sizes, const UInt8 * is_null, const IColumn::SerializationSettings * settings) const
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
    getDictionary().collectSerializedValueSizes(dict_sizes, nullptr, settings);
    idx.collectSerializedValueSizes(sizes, dict_sizes);
}

void ColumnLowCardinality::deserializeAndInsertFromArena(ReadBuffer & in, const IColumn::SerializationSettings * settings)
{
    compactIfSharedDictionary();
    idx.insertIndex(getDictionary().uniqueDeserializeAndInsertFromArena(in, settings));
}

void ColumnLowCardinality::skipSerializedInArena(ReadBuffer & in) const
{
    getDictionary().skipSerializedInArena(in);
}

WeakHash32 ColumnLowCardinality::getWeakHash32() const
{
    WeakHash32 dict_hash = getDictionary().getNestedColumn()->getWeakHash32();
    return idx.getWeakHash(dict_hash);
}

void ColumnLowCardinality::updateHashFast(SipHash & hash) const
{
    idx.getIndexes()->updateHashFast(hash);
    getDictionary().getNestedColumn()->updateHashFast(hash);
}

MutableColumnPtr ColumnLowCardinality::cloneResized(size_t size) const
{
    auto unique_ptr = dictionary.getColumnUniquePtr();
    if (size == 0)
        unique_ptr = unique_ptr->cloneEmpty();

    return ColumnLowCardinality::create(IColumn::mutate(std::move(unique_ptr)), getIndexes().cloneResized(size), /*is_shared=*/false);
}

MutableColumnPtr ColumnLowCardinality::cloneNullable() const
{
    if (nestedIsNullable())
        return cloneFinalized();

    auto res = cloneFinalized();
    assert_cast<ColumnLowCardinality &>(*res).compactInplaceToNullable();
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

namespace
{

/// Comparator for sorting LowCardinality column with the help of sorted dictionary.
/// NOTE: Dictionary itself must be sorted in ASC or DESC order depending on the requested direction.
template <typename IndexColumn, bool stable>
struct LowCardinalityComparator
{
    const IndexColumn & real_indexes;                   /// Indexes column
    const PaddedPODArray<UInt64> & position_by_index;   /// Maps original dictionary index to position in sorted dictionary

    inline bool operator () (size_t lhs, size_t rhs) const
    {
        int ret;

        const UInt64 lhs_index = real_indexes.getUInt(lhs);
        const UInt64 rhs_index = real_indexes.getUInt(rhs);

        if (lhs_index == rhs_index)
            ret = 0;
        else
            ret = CompareHelper<UInt64>::compare(position_by_index[lhs_index], position_by_index[rhs_index], 0);

        if (stable && ret == 0)
            return lhs < rhs;

        return ret < 0;
    }
};

}

template <typename IndexColumn>
void ColumnLowCardinality::updatePermutationWithIndexType(
    IColumn::PermutationSortStability stability, size_t limit, const PaddedPODArray<UInt64> & position_by_index,
    IColumn::Permutation & res, EqualRanges & equal_ranges) const
{
    /// Cast indexes column to the real type so that compareAt and getUInt methods can be inlined.
    const IndexColumn * real_indexes = assert_cast<const IndexColumn *>(&getIndexes());

    auto equal_comparator = [real_indexes](size_t lhs, size_t rhs)
    {
        return real_indexes->getUInt(lhs) == real_indexes->getUInt(rhs);
    };

    const bool stable = (stability == IColumn::PermutationSortStability::Stable);
    if (stable)
        updatePermutationImpl(limit, res, equal_ranges, LowCardinalityComparator<IndexColumn, true>{*real_indexes, position_by_index}, equal_comparator, DefaultSort(), DefaultPartialSort());
    else
        updatePermutationImpl(limit, res, equal_ranges, LowCardinalityComparator<IndexColumn, false>{*real_indexes, position_by_index}, equal_comparator, DefaultSort(), DefaultPartialSort());
}

void ColumnLowCardinality::updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                        size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const
{
    IColumn::Permutation dict_perm;
    getDictionary().getNestedColumn()->getPermutation(direction, stability, 0, nan_direction_hint, dict_perm);

    /// This is a paranoid check, but in other places in code empty permutation is used to indicate that no sorting is needed.
    if (dict_perm.size() != getDictionary().size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Dictionary permutation size {} is equal to dictionary size {}. It is a bug.",
            dict_perm.size(), getDictionary().size());
    PaddedPODArray<UInt64> position_by_index(dict_perm.size());
    for (size_t i = 0; i < dict_perm.size(); ++i)
        position_by_index[dict_perm[i]] = i;

    /// Dispatch by index column type.
    switch (idx.getSizeOfIndexType())
    {
        case sizeof(UInt8):
            updatePermutationWithIndexType<ColumnUInt8>(stability, limit, position_by_index, res, equal_ranges);
            return;
        case sizeof(UInt16):
            updatePermutationWithIndexType<ColumnUInt16>(stability, limit, position_by_index, res, equal_ranges);
            return;
        case sizeof(UInt32):
            updatePermutationWithIndexType<ColumnUInt32>(stability, limit, position_by_index, res, equal_ranges);
            return;
        case sizeof(UInt64):
            updatePermutationWithIndexType<ColumnUInt64>(stability, limit, position_by_index, res, equal_ranges);
            return;
        default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for low cardinality column.");
    }
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

std::vector<MutableColumnPtr> ColumnLowCardinality::scatter(size_t num_columns, const Selector & selector) const
{
    auto columns = getIndexes().scatter(num_columns, selector);
    ColumnPtr global_unique_ptr = IColumn::mutate(dictionary.getColumnUniquePtr());
    for (auto & column : columns)
    {
        auto unique_ptr = global_unique_ptr->cloneEmpty();
        column = ColumnLowCardinality::create(std::move(unique_ptr), std::move(column), true);
        static_cast<ColumnLowCardinality &>(*column).dictionary.setShared(global_unique_ptr);
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
    auto sub_indexes = IColumn::mutate(idx.getIndexes()->cut(start, length));
    auto new_column_unique = Dictionary::compact(getDictionary(), sub_indexes);
    return ColumnLowCardinality::create(std::move(new_column_unique), std::move(sub_indexes), /*is_shared=*/false);
}

void ColumnLowCardinality::compactInplace()
{
    auto indexes = idx.detachIndexes();
    dictionary.compact(indexes);
    idx.attachIndexes(std::move(indexes));
}

void ColumnLowCardinality::compactInplaceToNullable()
{
    auto indexes = idx.detachIndexes();
    dictionary.compactToNullable(indexes);
    idx.attachIndexes(std::move(indexes));
}

void ColumnLowCardinality::compactIfSharedDictionary()
{
    if (dictionary.isShared())
        compactInplace();
}


ColumnLowCardinality::DictionaryEncodedColumn
ColumnLowCardinality::getMinimalDictionaryEncodedColumn(UInt64 offset, UInt64 limit) const
{
    MutableColumnPtr sub_indexes = IColumn::mutate(idx.getIndexes()->cut(offset, limit));
    auto indexes_map = mapUniqueIndex(*sub_indexes);
    auto sub_keys = getDictionary().getNestedColumn()->index(*indexes_map, 0);

    return {std::move(sub_keys), std::move(sub_indexes)};
}

ColumnPtr ColumnLowCardinality::countKeys() const
{
    const auto & nested_column = getDictionary().getNestedColumn();
    size_t dict_size = nested_column->size();

    auto counter = ColumnUInt64::create(dict_size, 0);
    idx.countRowsInIndexedData(counter->getData());
    return counter;
}

bool ColumnLowCardinality::containsNull() const
{
    return getDictionary().nestedColumnIsNullable() && idx.containsDefault();
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

void ColumnLowCardinality::Dictionary::compact(MutableColumnPtr & indexes)
{
    column_unique = compact(getColumnUnique(), indexes);
    shared = false;
}

void ColumnLowCardinality::Dictionary::compactToNullable(MutableColumnPtr & indexes)
{
    column_unique = compactToNullable(getColumnUnique(), indexes);
    shared = false;
}

MutableColumnPtr ColumnLowCardinality::Dictionary::compact(const IColumnUnique & unique, MutableColumnPtr & indexes)
{
    auto new_column_unique = unique.cloneEmpty();
    auto & new_unique = static_cast<IColumnUnique &>(*new_column_unique);

    auto unique_indexes = mapUniqueIndex(*indexes);
    auto sub_keys = unique.getNestedColumn()->index(*unique_indexes, 0);
    auto new_indexes = new_unique.uniqueInsertRangeFrom(*sub_keys, 0, sub_keys->size());

    indexes = IColumn::mutate(new_indexes->index(*indexes, 0));
    return new_column_unique;
}

MutableColumnPtr ColumnLowCardinality::Dictionary::compactToNullable(const IColumnUnique & unique, MutableColumnPtr & indexes)
{
    auto new_column_unique = unique.cloneEmptyNullable();
    auto & new_unique = static_cast<IColumnUnique &>(*new_column_unique);

    auto unique_indexes = mapUniqueIndex(*indexes);
    auto sub_keys = unique.getNestedColumn()->index(*unique_indexes, 0);
    auto new_indexes = new_unique.uniqueInsertRangeFrom(*sub_keys, 0, sub_keys->size());

    indexes = IColumn::mutate(new_indexes->index(*indexes, 0));
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
