#include <Columns/ColumnExponentialTimeDecaying.h>

#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumnImpl.h>
#include <Common/HashTable/Hash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeExponentialTimeDecayingFloat64.h>
#include <IO/Operators.h>

#include <cmath>

namespace DB
{

namespace
{

MutableColumnPtr buildOrderingPrefix(const IColumn & storage, Float64 decay_length)
{
    const auto & tuple = assert_cast<const ColumnTuple &>(storage);
    chassert(tuple.tupleSize() == 2);

    const auto & values = assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)).getData();
    const auto & times = assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)).getData();

    auto prefix = ColumnUInt64::create();
    prefix->reserve(tuple.size());

    for (size_t row = 0; row < tuple.size(); ++row)
        prefix->insertValue(
            normalizeExponentialTimeDecayingFloat64(values[row], times[row], decay_length).ordering_prefix);

    return prefix;
}

struct ComparatorBase
{
    const ColumnExponentialTimeDecaying & column;
    int nan_direction_hint;

    int compare(size_t lhs, size_t rhs) const
    {
        return column.compareAt(lhs, rhs, column, nan_direction_hint);
    }
};

using ComparatorAscendingUnstable = ComparatorAscendingUnstableImpl<ComparatorBase>;
using ComparatorAscendingStable = ComparatorAscendingStableImpl<ComparatorBase>;
using ComparatorDescendingUnstable = ComparatorDescendingUnstableImpl<ComparatorBase>;
using ComparatorDescendingStable = ComparatorDescendingStableImpl<ComparatorBase>;
using ComparatorEqual = ComparatorEqualImpl<ComparatorBase>;

void updateCanonicalHash(
    const ColumnExponentialTimeDecaying & column, size_t row, SipHash & hash)
{
    const auto & tuple = column.getStorageTuple();
    const Float64 value = assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)).getData()[row];
    const Float64 time = assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)).getData()[row];

    const auto score = getExponentialTimeDecayingOrderingScore(
        value, time, column.getDecayLength());
    const UInt64 prefix = shiftOneBitAndSign(score.high, value);
    const Float64 sign = value == 0 ? 0 : std::copysign(1.0, value);
    const Float64 signed_high = sign * score.high;
    const Float64 signed_low = sign * score.low;

    hash.update(prefix);
    hash.update(signed_high == 0 ? 0 : signed_high);
    hash.update(signed_low == 0 ? 0 : signed_low);
}

UInt32 canonicalWeakHash(
    const ColumnExponentialTimeDecaying & column, size_t row)
{
    SipHash hash;
    updateCanonicalHash(column, row, hash);
    return static_cast<UInt32>(hash.get64());
}

}

ColumnExponentialTimeDecaying::ColumnExponentialTimeDecaying(
    MutableColumnPtr && storage_, Float64 decay_length_)
    : storage(std::move(storage_))
    , decay_length(decay_length_)
{
    const auto & tuple = assert_cast<const ColumnTuple &>(*storage);
    chassert(tuple.tupleSize() == 2);
    rebuildOrderingPrefix();
}

ColumnExponentialTimeDecaying::ColumnExponentialTimeDecaying(
    MutableColumnPtr && storage_,
    MutableColumnPtr && ordering_prefix_,
    Float64 decay_length_)
    : storage(std::move(storage_))
    , ordering_prefix(std::move(ordering_prefix_))
    , decay_length(decay_length_)
{
    const auto & tuple = assert_cast<const ColumnTuple &>(*storage);
    chassert(tuple.tupleSize() == 2);
    chassert(ordering_prefix->size() == storage->size());
}

std::string ColumnExponentialTimeDecaying::getName() const
{
    return "ExponentialTimeDecaying";
}

Field ColumnExponentialTimeDecaying::operator[](size_t n) const
{
    return (*storage)[n];
}

void ColumnExponentialTimeDecaying::get(size_t n, Field & res) const
{
    storage->get(n, res);
}

void ColumnExponentialTimeDecaying::getValueNameImpl(
    WriteBufferFromOwnString & name_buf, size_t n, const Options & options) const
{
    storage->getValueNameImpl(name_buf, n, options);
}

void ColumnExponentialTimeDecaying::appendOrderingPrefix(size_t row)
{
    const auto & tuple = getStorageTuple();
    const Float64 value = assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)).getData()[row];
    const Float64 time = assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)).getData()[row];
    assert_cast<ColumnUInt64 &>(*ordering_prefix).insertValue(
        normalizeExponentialTimeDecayingFloat64(value, time, decay_length).ordering_prefix);
}

void ColumnExponentialTimeDecaying::rebuildOrderingPrefix()
{
    ordering_prefix = buildOrderingPrefix(*storage, decay_length);
}

void ColumnExponentialTimeDecaying::syncOrderingPrefixFrom(size_t previous_size)
{
    if (ordering_prefix->size() > previous_size)
        ordering_prefix->popBack(ordering_prefix->size() - previous_size);

    if (ordering_prefix->size() != previous_size)
    {
        rebuildOrderingPrefix();
        return;
    }

    for (size_t row = previous_size; row < storage->size(); ++row)
        appendOrderingPrefix(row);
}

MutableColumnPtr ColumnExponentialTimeDecaying::cloneResized(size_t new_size) const
{
    if (new_size <= size())
        return ColumnExponentialTimeDecaying::createWithPrefix(
            storage->cloneResized(new_size),
            ordering_prefix->cloneResized(new_size),
            decay_length);

    return ColumnExponentialTimeDecaying::create(storage->cloneResized(new_size), decay_length);
}

void ColumnExponentialTimeDecaying::insertData(const char * pos, size_t length)
{
    const size_t previous_size = size();
    storage->insertData(pos, length);
    syncOrderingPrefixFrom(previous_size);
}

void ColumnExponentialTimeDecaying::insert(const Field & x)
{
    storage->insert(x);
    appendOrderingPrefix(size() - 1);
}

bool ColumnExponentialTimeDecaying::tryInsert(const Field & x)
{
    if (!storage->tryInsert(x))
        return false;

    appendOrderingPrefix(size() - 1);
    return true;
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnExponentialTimeDecaying::insertFrom(const IColumn & src_, size_t n)
{
    const auto & src = assert_cast<const ColumnExponentialTimeDecaying &>(src_);
    storage->insertFrom(src.getStorageColumn(), n);
    ordering_prefix->insertFrom(src.getOrderingPrefixColumn(), n);
}

void ColumnExponentialTimeDecaying::insertManyFrom(const IColumn & src_, size_t position, size_t length)
{
    const auto & src = assert_cast<const ColumnExponentialTimeDecaying &>(src_);
    storage->insertManyFrom(src.getStorageColumn(), position, length);
    ordering_prefix->insertManyFrom(src.getOrderingPrefixColumn(), position, length);
}

void ColumnExponentialTimeDecaying::insertRangeFrom(const IColumn & src_, size_t start, size_t length)
{
    const auto & src = assert_cast<const ColumnExponentialTimeDecaying &>(src_);
    storage->insertRangeFrom(src.getStorageColumn(), start, length);
    ordering_prefix->insertRangeFrom(src.getOrderingPrefixColumn(), start, length);
}
#else
void ColumnExponentialTimeDecaying::doInsertFrom(const IColumn & src_, size_t n)
{
    const auto & src = assert_cast<const ColumnExponentialTimeDecaying &>(src_);
    storage->insertFrom(src.getStorageColumn(), n);
    ordering_prefix->insertFrom(src.getOrderingPrefixColumn(), n);
}

void ColumnExponentialTimeDecaying::doInsertManyFrom(const IColumn & src_, size_t position, size_t length)
{
    const auto & src = assert_cast<const ColumnExponentialTimeDecaying &>(src_);
    storage->insertManyFrom(src.getStorageColumn(), position, length);
    ordering_prefix->insertManyFrom(src.getOrderingPrefixColumn(), position, length);
}

void ColumnExponentialTimeDecaying::doInsertRangeFrom(const IColumn & src_, size_t start, size_t length)
{
    const auto & src = assert_cast<const ColumnExponentialTimeDecaying &>(src_);
    storage->insertRangeFrom(src.getStorageColumn(), start, length);
    ordering_prefix->insertRangeFrom(src.getOrderingPrefixColumn(), start, length);
}
#endif

void ColumnExponentialTimeDecaying::insertDefault()
{
    storage->insertDefault();
    appendOrderingPrefix(size() - 1);
}

void ColumnExponentialTimeDecaying::popBack(size_t n)
{
    storage->popBack(n);
    ordering_prefix->popBack(n);
}

void ColumnExponentialTimeDecaying::deserializeAndInsertFromArena(
    ReadBuffer & in, const IColumn::SerializationSettings * settings)
{
    const size_t previous_size = size();
    storage->deserializeAndInsertFromArena(in, settings);
    syncOrderingPrefixFrom(previous_size);
}

int ColumnExponentialTimeDecaying::compareDirect(
    size_t n, size_t m, const ColumnExponentialTimeDecaying & rhs) const
{
    const auto & left_tuple = getStorageTuple();
    const auto & right_tuple = rhs.getStorageTuple();

    const auto & left_values = assert_cast<const ColumnFloat64 &>(left_tuple.getColumn(0)).getData();
    const auto & left_times = assert_cast<const ColumnFloat64 &>(left_tuple.getColumn(1)).getData();
    const auto & right_values = assert_cast<const ColumnFloat64 &>(right_tuple.getColumn(0)).getData();
    const auto & right_times = assert_cast<const ColumnFloat64 &>(right_tuple.getColumn(1)).getData();

    const Float64 left_value = left_values[n];
    const Float64 right_value = right_values[m];

    if (left_value == 0 || right_value == 0)
    {
        if (left_value < right_value)
            return -1;
        if (left_value > right_value)
            return 1;
        return 0;
    }

    const bool left_negative = std::signbit(left_value);
    const bool right_negative = std::signbit(right_value);
    if (left_negative != right_negative)
        return left_negative ? -1 : 1;

    const auto left_score = getExponentialTimeDecayingOrderingScore(
        left_value, left_times[n], decay_length);
    const auto right_score = getExponentialTimeDecayingOrderingScore(
        right_value, right_times[m], decay_length);

    int result = 0;
    if (left_score.high < right_score.high)
        result = -1;
    else if (left_score.high > right_score.high)
        result = 1;
    else if (left_score.low < right_score.low)
        result = -1;
    else if (left_score.low > right_score.low)
        result = 1;

    return left_negative ? -result : result;
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
int ColumnExponentialTimeDecaying::compareAt(
    size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const
#else
int ColumnExponentialTimeDecaying::doCompareAt(
    size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const
#endif
{
    const auto & rhs = assert_cast<const ColumnExponentialTimeDecaying &>(rhs_);
    chassert(decay_length == rhs.decay_length);

    const int prefix_compare
        = ordering_prefix->compareAt(n, m, rhs.getOrderingPrefixColumn(), nan_direction_hint);
    if (prefix_compare != 0)
        return prefix_compare;

    return compareDirect(n, m, rhs);
}

void ColumnExponentialTimeDecaying::updateHashWithValue(size_t n, SipHash & hash) const
{
    updateCanonicalHash(*this, n, hash);
}

void ColumnExponentialTimeDecaying::updateHashFast(SipHash & hash) const
{
    for (size_t row = 0; row < size(); ++row)
        updateCanonicalHash(*this, row, hash);
}

void ColumnExponentialTimeDecaying::computeHashInto(
    size_t row_begin, size_t row_end, UInt32 * hash_out, bool initial) const
{
    for (size_t row = row_begin; row < row_end; ++row)
    {
        const UInt32 value = canonicalWeakHash(*this, row);
        UInt32 & out = hash_out[row - row_begin];
        out = initial ? value : combineWeakHash32(value, out);
    }
}

void ColumnExponentialTimeDecaying::getPermutation(
    PermutationSortDirection direction,
    PermutationSortStability stability,
    size_t limit,
    int nan_direction_hint,
    Permutation & res) const
{
    ComparatorBase base{*this, nan_direction_hint};
    if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorAscendingUnstable(base), DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorAscendingStable(base), DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorDescendingUnstable(base), DefaultSort(), DefaultPartialSort());
    else
        getPermutationImpl(limit, res, ComparatorDescendingStable(base), DefaultSort(), DefaultPartialSort());
}

void ColumnExponentialTimeDecaying::updatePermutation(
    PermutationSortDirection direction,
    PermutationSortStability stability,
    size_t limit,
    int nan_direction_hint,
    Permutation & res,
    EqualRanges & equal_ranges) const
{
    ComparatorBase base{*this, nan_direction_hint};
    ComparatorEqual equal(base);

    if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingUnstable(base), equal, DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingStable(base), equal, DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingUnstable(base), equal, DefaultSort(), DefaultPartialSort());
    else
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingStable(base), equal, DefaultSort(), DefaultPartialSort());
}

void ColumnExponentialTimeDecaying::getExtremes(
    Field & min, Field & max, size_t start, size_t end) const
{
    if (start >= end)
    {
        min = Field();
        max = Field();
        return;
    }

    size_t min_index = start;
    size_t max_index = start;
    for (size_t i = start + 1; i < end; ++i)
    {
        if (compareAt(i, min_index, *this, 1) < 0)
            min_index = i;
        if (compareAt(i, max_index, *this, -1) > 0)
            max_index = i;
    }

    get(min_index, min);
    get(max_index, max);
}

ColumnPtr ColumnExponentialTimeDecaying::filter(const Filter & filt, ssize_t result_size_hint) const
{
    return ColumnExponentialTimeDecaying::createWithPrefix(
        storage->filter(filt, result_size_hint)->assumeMutable(),
        ordering_prefix->filter(filt, result_size_hint)->assumeMutable(),
        decay_length);
}

void ColumnExponentialTimeDecaying::filter(const Filter & filt)
{
    storage->filter(filt);
    ordering_prefix->filter(filt);
}

void ColumnExponentialTimeDecaying::expand(const Filter & mask, bool inverted)
{
    storage->expand(mask, inverted);
    ordering_prefix->expand(mask, inverted);
}

ColumnPtr ColumnExponentialTimeDecaying::permute(const Permutation & perm, size_t limit) const
{
    return ColumnExponentialTimeDecaying::createWithPrefix(
        storage->permute(perm, limit)->assumeMutable(),
        ordering_prefix->permute(perm, limit)->assumeMutable(),
        decay_length);
}

ColumnPtr ColumnExponentialTimeDecaying::index(const IColumn & indexes, size_t limit) const
{
    return ColumnExponentialTimeDecaying::createWithPrefix(
        storage->index(indexes, limit)->assumeMutable(),
        ordering_prefix->index(indexes, limit)->assumeMutable(),
        decay_length);
}

ColumnPtr ColumnExponentialTimeDecaying::replicate(const Offsets & offsets) const
{
    return ColumnExponentialTimeDecaying::createWithPrefix(
        storage->replicate(offsets)->assumeMutable(),
        ordering_prefix->replicate(offsets)->assumeMutable(),
        decay_length);
}

ColumnPtr ColumnExponentialTimeDecaying::compress(bool force_compression) const
{
    auto compressed = storage->compress(force_compression);
    const auto byte_size = compressed->byteSize();
    return ColumnCompressed::create(
        size(),
        byte_size,
        [my_compressed = std::move(compressed), my_decay_length = decay_length]
        {
            return ColumnExponentialTimeDecaying::create(
                my_compressed->decompress()->assumeMutable(),
                my_decay_length);
        });
}

void ColumnExponentialTimeDecaying::prepareForSquashing(
    const VectorWithMemoryTracking<ColumnPtr> & source_columns, size_t factor)
{
    VectorWithMemoryTracking<ColumnPtr> source_storage;
    VectorWithMemoryTracking<ColumnPtr> source_prefix;
    source_storage.reserve(source_columns.size());
    source_prefix.reserve(source_columns.size());

    for (const auto & source : source_columns)
    {
        const auto & decaying = assert_cast<const ColumnExponentialTimeDecaying &>(*source);
        source_storage.push_back(decaying.getStoragePtr());
        source_prefix.push_back(decaying.ordering_prefix);
    }

    storage->prepareForSquashing(source_storage, factor);
    ordering_prefix->prepareForSquashing(source_prefix, factor);
}

void ColumnExponentialTimeDecaying::rollback(const ColumnCheckpoint & checkpoint)
{
    storage->rollback(checkpoint);
    rebuildOrderingPrefix();
}

void ColumnExponentialTimeDecaying::forEachMutableSubcolumn(MutableColumnCallback callback)
{
    callback(storage);
}

void ColumnExponentialTimeDecaying::forEachMutableSubcolumnRecursively(RecursiveMutableColumnCallback callback)
{
    callback(*storage);
    storage->forEachMutableSubcolumnRecursively(callback);
}

void ColumnExponentialTimeDecaying::forEachSubcolumn(ColumnCallback callback) const
{
    callback(storage);
}

void ColumnExponentialTimeDecaying::forEachSubcolumnRecursively(RecursiveColumnCallback callback) const
{
    callback(*storage);
    storage->forEachSubcolumnRecursively(callback);
}

bool ColumnExponentialTimeDecaying::structureEquals(const IColumn & rhs) const
{
    if (const auto * other = typeid_cast<const ColumnExponentialTimeDecaying *>(&rhs))
        return decay_length == other->decay_length && storage->structureEquals(*other->storage);
    return false;
}

}
