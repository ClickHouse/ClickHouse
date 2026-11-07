#include <Columns/ColumnFixedArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <IO/ReadBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

/// The same limit as for `ColumnArray`: obtaining a large array as a `Field` is slow and consumes a lot of memory.
static constexpr size_t max_array_size_as_field = 1000000;


ColumnFixedArray::ColumnFixedArray(MutableColumnPtr && nested_column, size_t n_)
    : data(std::move(nested_column)), n(n_)
{
    if (n == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The size of ColumnFixedArray cannot be zero");

    if (isColumnConst(*data))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnFixedArray cannot have ColumnConst as its nested column");

    if (data->size() % n != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "The size of the nested column {} of ColumnFixedArray is not a multiple of the array size {}", data->size(), n);
}

std::string ColumnFixedArray::getName() const
{
    return "FixedArray(" + std::to_string(n) + ", " + data->getName() + ")";
}

MutableColumnPtr ColumnFixedArray::cloneResized(size_t to_size) const
{
    auto res = ColumnFixedArray::create(data->cloneEmpty(), n);
    if (to_size == 0)
        return res;

    size_t from_size = size();
    res->data->insertRangeFrom(*data, 0, std::min(from_size, to_size) * n);
    if (to_size > from_size)
        res->data->insertManyDefaults((to_size - from_size) * n);

    return res;
}

Field ColumnFixedArray::operator[](size_t index) const
{
    Field res;
    get(index, res);
    return res;
}

void ColumnFixedArray::get(size_t index, Field & res) const
{
    if (n > max_array_size_as_field)
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Array of size {} is too large to be manipulated as single field, maximum size {}",
            n, max_array_size_as_field);

    res = Array();
    Array & res_arr = res.safeGet<Array>();
    res_arr.reserve(n);

    size_t offset = index * n;
    for (size_t i = 0; i < n; ++i)
        res_arr.push_back((*data)[offset + i]);
}

void ColumnFixedArray::getValueNameImpl(WriteBufferFromOwnString & name_buf, size_t index, const Options & options) const
{
    if (options.notFull(name_buf))
        name_buf << "[";

    size_t offset = index * n;
    for (size_t i = 0; i < n; ++i)
    {
        if (options.notFull(name_buf) && i > 0)
            name_buf << ", ";
        data->getValueNameImpl(name_buf, offset + i, options);
        if (!options.notFull(name_buf))
            break;
    }

    if (options.notFull(name_buf))
        name_buf << "]";
}

std::string_view ColumnFixedArray::getDataAt(size_t index) const
{
    auto first = data->getDataAt(index * n);
    return {first.data(), first.size() * n};
}

bool ColumnFixedArray::isDefaultAt(size_t index) const
{
    size_t offset = index * n;
    for (size_t i = 0; i < n; ++i)
        if (!data->isDefaultAt(offset + i))
            return false;
    return true;
}

void ColumnFixedArray::insertData(const char * pos, size_t length)
{
    size_t value_size = data->sizeOfValueIfFixed();
    if (length != n * value_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect length argument {} for method ColumnFixedArray::insertData, expected {}",
            length, n * value_size);

    for (size_t i = 0; i < n; ++i)
        data->insertData(pos + i * value_size, value_size);
}

std::string_view ColumnFixedArray::serializeValueIntoArena(
    size_t index, Arena & arena, char const *& begin, const IColumn::SerializationSettings * settings) const
{
    /// The size is known from the type, so only the elements are serialized.
    std::string_view res;
    size_t offset = index * n;
    for (size_t i = 0; i < n; ++i)
    {
        auto value_ref = data->serializeValueIntoArena(offset + i, arena, begin, settings);
        res = i == 0 ? value_ref : std::string_view{value_ref.data() - res.size(), res.size() + value_ref.size()};
    }
    return res;
}

char * ColumnFixedArray::serializeValueIntoMemory(size_t index, char * memory, const IColumn::SerializationSettings * settings) const
{
    size_t offset = index * n;
    for (size_t i = 0; i < n; ++i)
        memory = data->serializeValueIntoMemory(offset + i, memory, settings);
    return memory;
}

std::optional<size_t> ColumnFixedArray::getSerializedValueSize(size_t index, const IColumn::SerializationSettings * settings) const
{
    size_t res = 0;
    size_t offset = index * n;
    for (size_t i = 0; i < n; ++i)
    {
        auto element_size = data->getSerializedValueSize(offset + i, settings);
        if (!element_size)
            return std::nullopt;
        res += *element_size;
    }
    return res;
}

void ColumnFixedArray::deserializeAndInsertFromArena(ReadBuffer & in, const IColumn::SerializationSettings * settings)
{
    for (size_t i = 0; i < n; ++i)
        data->deserializeAndInsertFromArena(in, settings);
}

void ColumnFixedArray::updateHashWithValue(size_t index, SipHash & hash) const
{
    size_t offset = index * n;
    for (size_t i = 0; i < n; ++i)
        data->updateHashWithValue(offset + i, hash);
}

void ColumnFixedArray::updateHashWithValueRange(size_t begin, size_t end, SipHash & hash) const
{
    data->updateHashWithValueRange(begin * n, end * n, hash);
}

void ColumnFixedArray::computeHashInto(size_t row_begin, size_t row_end, UInt32 * hash_out, bool initial) const
{
    const size_t num_elems = (row_end - row_begin) * n;
    PaddedPODArray<UInt32> elem_hash(num_elems);
    if (num_elems)
        data->computeHashInto(row_begin * n, row_end * n, elem_hash.data(), true);

    /// The same folding of the element hashes as in `ColumnArray`, so equal arrays hash equally.
    for (size_t i = row_begin; i < row_end; ++i)
    {
        UInt32 acc = static_cast<UInt32>(intHashCRC32(WEAK_HASH32_INITIAL_VALUE));
        const UInt32 * row_hashes = elem_hash.data() + (i - row_begin) * n;
        for (size_t j = 0; j < n; ++j)
            acc = combineWeakHash32(row_hashes[j], acc);

        UInt32 & out = hash_out[i - row_begin];
        out = initial ? acc : combineWeakHash32(acc, out);
    }
}

void ColumnFixedArray::updateHashFast(SipHash & hash) const
{
    hash.update(n);
    data->updateHashFast(hash);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnFixedArray::insertRangeFrom(const IColumn & src, size_t start, size_t length)
#else
void ColumnFixedArray::doInsertRangeFrom(const IColumn & src, size_t start, size_t length)
#endif
{
    const auto & src_concrete = assert_cast<const ColumnFixedArray &>(src);
    if (src_concrete.n != n)
        throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Cannot insert FixedArray of size {} into FixedArray of size {}", src_concrete.n, n);

    if (start > src_concrete.size() || length > src_concrete.size() - start)
        throwInsertRangeFromOutOfBound("ColumnFixedArray", start, length, src_concrete.size());

    data->insertRangeFrom(*src_concrete.data, start * n, length * n);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnFixedArray::insertFrom(const IColumn & src, size_t index)
#else
void ColumnFixedArray::doInsertFrom(const IColumn & src, size_t index)
#endif
{
    const auto & src_concrete = assert_cast<const ColumnFixedArray &>(src);
    chassert(src_concrete.n == n);
    data->insertRangeFrom(*src_concrete.data, index * n, n);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnFixedArray::insertManyFrom(const IColumn & src, size_t position, size_t length)
#else
void ColumnFixedArray::doInsertManyFrom(const IColumn & src, size_t position, size_t length)
#endif
{
    const auto & src_concrete = assert_cast<const ColumnFixedArray &>(src);
    chassert(src_concrete.n == n);
    data->reserve(data->size() + length * n);
    for (size_t i = 0; i < length; ++i)
        data->insertRangeFrom(*src_concrete.data, position * n, n);
}

void ColumnFixedArray::insert(const Field & x)
{
    if (!tryInsert(x))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot insert {} into {}", x.dump(), getName());
}

bool ColumnFixedArray::tryInsert(const Field & x)
{
    if (x.getType() != Field::Types::Which::Array)
        return false;

    const Array & array = x.safeGet<Array>();
    if (array.size() != n)
        return false;

    for (size_t i = 0; i < n; ++i)
    {
        if (!data->tryInsert(array[i]))
        {
            data->popBack(i);
            return false;
        }
    }

    return true;
}

void ColumnFixedArray::popBack(size_t elems)
{
    if (elems > size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot pop {} rows from {}: there are only {} rows", elems, getName(), size());
    data->popBack(elems * n);
}

/// Repeat every byte of the filter `n` times, so that it selects the elements of the selected rows.
static IColumn::Filter expandFilter(const IColumn::Filter & filt, size_t n)
{
    IColumn::Filter res(filt.size() * n);
    for (size_t i = 0; i < filt.size(); ++i)
        memset(&res[i * n], filt[i], n);
    return res;
}

ColumnPtr ColumnFixedArray::filter(const Filter & filt, ssize_t result_size_hint) const
{
    if (filt.size() != size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of filter ({}) doesn't match size of column ({})", filt.size(), size());

    if (n == 1)
        return ColumnFixedArray::create(data->filter(filt, result_size_hint), n);

    return ColumnFixedArray::create(data->filter(expandFilter(filt, n), result_size_hint < 0 ? result_size_hint : result_size_hint * n), n);
}

void ColumnFixedArray::filter(const Filter & filt)
{
    if (filt.size() != size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of filter ({}) doesn't match size of column ({})", filt.size(), size());

    if (n == 1)
        data->filter(filt);
    else
        data->filter(expandFilter(filt, n));
}

void ColumnFixedArray::expand(const Filter & mask, bool inverted)
{
    if (n == 1)
        data->expand(mask, inverted);
    else
        data->expand(expandFilter(mask, n), inverted);
}

template <typename Type>
MutableColumnPtr ColumnFixedArray::expandIndexes(const PaddedPODArray<Type> & indexes, size_t limit) const
{
    auto nested_indexes_column = ColumnUInt64::create(limit * n);
    auto & nested_indexes = nested_indexes_column->getData();
    for (size_t i = 0; i < limit; ++i)
    {
        UInt64 offset = static_cast<UInt64>(indexes[i]) * n;
        for (size_t j = 0; j < n; ++j)
            nested_indexes[i * n + j] = offset + j;
    }
    return nested_indexes_column;
}

ColumnPtr ColumnFixedArray::permute(const Permutation & perm, size_t limit) const
{
    return permuteImpl(*this, perm, limit);
}

ColumnPtr ColumnFixedArray::index(const IColumn & indexes, size_t limit) const
{
    return selectIndexImpl(*this, indexes, limit);
}

template <typename Type>
ColumnPtr ColumnFixedArray::indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const
{
    chassert(limit <= indexes.size());
    if (limit == 0)
        return ColumnFixedArray::create(data->cloneEmpty(), n);

    auto nested_indexes = expandIndexes(indexes, limit);
    return ColumnFixedArray::create(data->index(*nested_indexes, limit * n), n);
}

INSTANTIATE_INDEX_IMPL(ColumnFixedArray)

ColumnPtr ColumnFixedArray::replicate(const Offsets & replicate_offsets) const
{
    size_t col_size = size();
    if (col_size != replicate_offsets.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of offsets doesn't match size of column.");

    if (n == 1)
        return ColumnFixedArray::create(data->replicate(replicate_offsets), n);

    /// Every row is repeated as a whole, so this is a selection of rows by indexes.
    size_t res_size = col_size == 0 ? 0 : replicate_offsets.back();
    PaddedPODArray<UInt64> indexes(res_size);
    IColumn::Offset prev_offset = 0;
    for (size_t i = 0; i < col_size; ++i)
    {
        for (IColumn::Offset j = prev_offset; j < replicate_offsets[i]; ++j)
            indexes[j] = i;
        prev_offset = replicate_offsets[i];
    }

    return indexImpl(indexes, res_size);
}

int ColumnFixedArray::compareAtImpl(size_t lhs, size_t rhs, const IColumn & rhs_column, int nan_direction_hint, const Collator * collator) const
{
    const auto & rhs_concrete = assert_cast<const ColumnFixedArray &>(rhs_column);
    chassert(rhs_concrete.n == n);

    for (size_t i = 0; i < n; ++i)
    {
        int res = collator
            ? data->compareAtWithCollation(lhs * n + i, rhs * n + i, *rhs_concrete.data, nan_direction_hint, *collator)
            : data->compareAt(lhs * n + i, rhs * n + i, *rhs_concrete.data, nan_direction_hint);
        if (res)
            return res;
    }

    return 0;
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
int ColumnFixedArray::compareAt(size_t lhs, size_t rhs, const IColumn & rhs_column, int nan_direction_hint) const
#else
int ColumnFixedArray::doCompareAt(size_t lhs, size_t rhs, const IColumn & rhs_column, int nan_direction_hint) const
#endif
{
    return compareAtImpl(lhs, rhs, rhs_column, nan_direction_hint, nullptr);
}

int ColumnFixedArray::compareAtWithCollation(size_t lhs, size_t rhs, const IColumn & rhs_column, int nan_direction_hint, const Collator & collator) const
{
    return compareAtImpl(lhs, rhs, rhs_column, nan_direction_hint, &collator);
}

struct ColumnFixedArray::ComparatorBase
{
    const ColumnFixedArray & parent;
    int nan_direction_hint;

    ComparatorBase(const ColumnFixedArray & parent_, int nan_direction_hint_)
        : parent(parent_), nan_direction_hint(nan_direction_hint_)
    {
    }

    ALWAYS_INLINE int compare(size_t lhs, size_t rhs) const
    {
        return parent.compareAtImpl(lhs, rhs, parent, nan_direction_hint, nullptr);
    }
};

struct ColumnFixedArray::ComparatorCollationBase
{
    const ColumnFixedArray & parent;
    int nan_direction_hint;
    const Collator * collator;

    ComparatorCollationBase(const ColumnFixedArray & parent_, int nan_direction_hint_, const Collator * collator_)
        : parent(parent_), nan_direction_hint(nan_direction_hint_), collator(collator_)
    {
    }

    ALWAYS_INLINE int compare(size_t lhs, size_t rhs) const
    {
        return parent.compareAtImpl(lhs, rhs, parent, nan_direction_hint, collator);
    }
};

void ColumnFixedArray::getPermutation(PermutationSortDirection direction, PermutationSortStability stability,
                                      size_t limit, int nan_direction_hint, Permutation & res) const
{
    if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorAscendingUnstable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorAscendingStable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorDescendingUnstable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorDescendingStable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
}

void ColumnFixedArray::updatePermutation(PermutationSortDirection direction, PermutationSortStability stability,
                                         size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_ranges) const
{
    auto comparator_equal = ComparatorEqual(*this, nan_direction_hint);

    if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingUnstable(*this, nan_direction_hint), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingStable(*this, nan_direction_hint), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingUnstable(*this, nan_direction_hint), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingStable(*this, nan_direction_hint), comparator_equal, DefaultSort(), DefaultPartialSort());
}

void ColumnFixedArray::getPermutationWithCollation(const Collator & collator, PermutationSortDirection direction, PermutationSortStability stability,
                                                   size_t limit, int nan_direction_hint, Permutation & res) const
{
    if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorCollationAscendingUnstable(*this, nan_direction_hint, &collator), DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorCollationAscendingStable(*this, nan_direction_hint, &collator), DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorCollationDescendingUnstable(*this, nan_direction_hint, &collator), DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorCollationDescendingStable(*this, nan_direction_hint, &collator), DefaultSort(), DefaultPartialSort());
}

void ColumnFixedArray::updatePermutationWithCollation(const Collator & collator, PermutationSortDirection direction, PermutationSortStability stability,
                                                      size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_ranges) const
{
    auto comparator_equal = ComparatorCollationEqual(*this, nan_direction_hint, &collator);

    if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorCollationAscendingUnstable(*this, nan_direction_hint, &collator), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorCollationAscendingStable(*this, nan_direction_hint, &collator), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorCollationDescendingUnstable(*this, nan_direction_hint, &collator), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorCollationDescendingStable(*this, nan_direction_hint, &collator), comparator_equal, DefaultSort(), DefaultPartialSort());
}

void ColumnFixedArray::prepareForSquashing(const VectorWithMemoryTracking<ColumnPtr> & source_columns, size_t factor)
{
    VectorWithMemoryTracking<ColumnPtr> source_data_columns;
    source_data_columns.reserve(source_columns.size());
    for (const auto & source_column : source_columns)
        source_data_columns.push_back(assert_cast<const ColumnFixedArray &>(*source_column).getDataPtr());

    data->prepareForSquashing(source_data_columns, factor);
}

size_t ColumnFixedArray::byteSizeAt(size_t index) const
{
    size_t res = 0;
    size_t offset = index * n;
    for (size_t i = 0; i < n; ++i)
        res += data->byteSizeAt(offset + i);
    return res;
}

void ColumnFixedArray::getExtremes(Field & min, Field & max, size_t start, size_t end) const
{
    min = Array();
    max = Array();

    if (start >= end)
        return;

    size_t min_idx = start;
    size_t max_idx = start;

    for (size_t i = start + 1; i < end; ++i)
    {
        if (compareAtImpl(i, min_idx, *this, /* nan_direction_hint = */ 1, nullptr) < 0)
            min_idx = i;
        else if (compareAtImpl(i, max_idx, *this, /* nan_direction_hint = */ -1, nullptr) > 0)
            max_idx = i;
    }

    get(min_idx, min);
    get(max_idx, max);
}

ColumnPtr ColumnFixedArray::compress(bool force_compression) const
{
    ColumnPtr data_compressed = data->compress(force_compression);
    size_t byte_size = data_compressed->byteSize();
    Columns parts{data_compressed};

    return ColumnCompressed::create(size(), byte_size,
        [my_data_compressed = std::move(data_compressed), my_n = n]
        {
            return ColumnFixedArray::create(my_data_compressed->decompress(), my_n);
        },
        std::move(parts));
}

ColumnCheckpointPtr ColumnFixedArray::getCheckpoint() const
{
    return std::make_shared<ColumnCheckpointWithNested>(size(), data->getCheckpoint());
}

void ColumnFixedArray::updateCheckpoint(ColumnCheckpoint & checkpoint) const
{
    checkpoint.size = size();
    data->updateCheckpoint(*assert_cast<ColumnCheckpointWithNested &>(checkpoint).nested);
}

void ColumnFixedArray::rollback(const ColumnCheckpoint & checkpoint)
{
    data->rollback(*assert_cast<const ColumnCheckpointWithNested &>(checkpoint).nested);
}

void ColumnFixedArray::takeExactDynamicStructureFrom(const IColumn & source)
{
    data->takeExactDynamicStructureFrom(assert_cast<const ColumnFixedArray &>(source).getData());
}

void ColumnFixedArray::chooseDynamicStructureForMerge(const VectorWithMemoryTracking<ColumnPtr> & source_columns, std::optional<size_t> max_dynamic_subcolumns)
{
    VectorWithMemoryTracking<ColumnPtr> nested_source_columns;
    nested_source_columns.reserve(source_columns.size());
    for (const auto & source_column : source_columns)
        nested_source_columns.push_back(assert_cast<const ColumnFixedArray &>(*source_column).getDataPtr());

    data->chooseDynamicStructureForMerge(nested_source_columns, max_dynamic_subcolumns);
}

ColumnPtr ColumnFixedArray::convertToArray() const
{
    size_t rows = size();
    auto offsets = ColumnArray::ColumnOffsets::create(rows);
    auto & offsets_data = offsets->getData();
    for (size_t i = 0; i < rows; ++i)
        offsets_data[i] = (i + 1) * n;
    return ColumnArray::create(data, std::move(offsets));
}


ColumnPtr recursiveRemoveFixedArray(const ColumnPtr & column)
{
    if (const auto * column_fixed_array = typeid_cast<const ColumnFixedArray *>(column.get()))
    {
        auto array = column_fixed_array->convertToArray();
        const auto & column_array = assert_cast<const ColumnArray &>(*array);
        return ColumnArray::create(recursiveRemoveFixedArray(column_array.getDataPtr()), column_array.getOffsetsPtr());
    }

    if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
    {
        auto data = recursiveRemoveFixedArray(column_const->getDataColumnPtr());
        if (data.get() == column_const->getDataColumnPtr().get())
            return column;
        return ColumnConst::create(data, column_const->size());
    }

    if (const auto * column_array = typeid_cast<const ColumnArray *>(column.get()))
    {
        auto data = recursiveRemoveFixedArray(column_array->getDataPtr());
        if (data.get() == column_array->getDataPtr().get())
            return column;
        return ColumnArray::create(data, column_array->getOffsetsPtr());
    }

    if (const auto * column_tuple = typeid_cast<const ColumnTuple *>(column.get()))
    {
        Columns elements = column_tuple->getColumnsCopy();
        bool changed = false;
        for (auto & element : elements)
        {
            auto converted = recursiveRemoveFixedArray(element);
            changed |= converted.get() != element.get();
            element = std::move(converted);
        }
        if (!changed)
            return column;
        return ColumnTuple::create(elements);
    }

    if (const auto * column_map = typeid_cast<const ColumnMap *>(column.get()))
    {
        auto nested = recursiveRemoveFixedArray(column_map->getNestedColumnPtr());
        if (nested.get() == column_map->getNestedColumnPtr().get())
            return column;
        return ColumnMap::create(nested);
    }

    if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(column.get()))
    {
        auto nested = recursiveRemoveFixedArray(column_nullable->getNestedColumnPtr());
        if (nested.get() == column_nullable->getNestedColumnPtr().get())
            return column;
        return ColumnNullable::create(nested, column_nullable->getNullMapColumnPtr());
    }

    /// The lazily replicated column keeps its indexes, only the replicated values are converted.
    if (const auto * column_replicated = typeid_cast<const ColumnReplicated *>(column.get()))
    {
        auto nested = recursiveRemoveFixedArray(column_replicated->getNestedColumn());
        if (nested.get() == column_replicated->getNestedColumn().get())
            return column;
        return ColumnReplicated::create(nested, column_replicated->getIndexesColumn());
    }

    return column;
}

}
