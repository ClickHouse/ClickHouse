#include <Columns/ColumnExponentialTimeDecaying.h>

#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumnImpl.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Common/transformEndianness.h>
#include <DataTypes/DataTypeExponentialTimeDecayingFloat64.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>

#include <cmath>
#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

MutableColumnPtr buildOrderingKey(const IColumn & storage, Float64 decay_length)
{
    const auto & tuple = assert_cast<const ColumnTuple &>(storage);
    chassert(tuple.tupleSize() == 2);

    const auto & values = assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)).getData();
    const auto & times = assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)).getData();

    auto prefix = ColumnUInt64::create();
    prefix->reserve(tuple.size());

    for (size_t row = 0; row < tuple.size(); ++row)
        prefix->insertValue(
            getExponentialTimeDecayingOrderingKey(values[row], times[row], decay_length));

    return prefix;
}

struct ComparatorBase
{
    ComparatorBase(const ColumnExponentialTimeDecaying & column_, int nan_direction_hint_)
        : column(column_)
        , nan_direction_hint(nan_direction_hint_)
    {
    }

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

UInt64 getOrderingKey(
    const ColumnExponentialTimeDecaying & column, size_t row)
{
    const auto & tuple = column.getStorageTuple();
    const Float64 value
        = assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)).getData()[row];
    const Float64 time
        = assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)).getData()[row];
    return getExponentialTimeDecayingOrderingKey(
        value, time, column.getDecayLength());
}

void updateOrderingKeyHash(
    const ColumnExponentialTimeDecaying & column, size_t row, SipHash & hash)
{
    hash.update(getOrderingKey(column, row));
}

UInt32 orderingKeyWeakHash(
    const ColumnExponentialTimeDecaying & column, size_t row)
{
    const UInt64 key = getOrderingKey(column, row);
    return static_cast<UInt32>(
        intHashCRC32(key, WEAK_HASH32_INITIAL_VALUE));
}

}

ColumnExponentialTimeDecaying::ColumnExponentialTimeDecaying(
    MutableColumnPtr && storage_, Float64 decay_length_)
    : storage(std::move(storage_))
    , decay_length(decay_length_)
{
    const auto & tuple = assert_cast<const ColumnTuple &>(*storage);
    chassert(tuple.tupleSize() == 2);
    rebuildOrderingKey();
}

ColumnExponentialTimeDecaying::ColumnExponentialTimeDecaying(
    MutableColumnPtr && storage_,
    MutableColumnPtr && ordering_key_,
    Float64 decay_length_)
    : storage(std::move(storage_))
    , ordering_key(std::move(ordering_key_))
    , decay_length(decay_length_)
{
    const auto & tuple = assert_cast<const ColumnTuple &>(*storage);
    chassert(tuple.tupleSize() == 2);
    chassert(ordering_key->size() == storage->size());
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

void ColumnExponentialTimeDecaying::appendOrderingKey(size_t row)
{
    const auto & tuple = getStorageTuple();
    const Float64 value = assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)).getData()[row];
    const Float64 time = assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)).getData()[row];
    assert_cast<ColumnUInt64 &>(*ordering_key).insertValue(
        getExponentialTimeDecayingOrderingKey(value, time, decay_length));
}

void ColumnExponentialTimeDecaying::rebuildOrderingKey()
{
    ordering_key = buildOrderingKey(*storage, decay_length);
}

void ColumnExponentialTimeDecaying::syncOrderingKeyFrom(size_t previous_size)
{
    if (ordering_key->size() > previous_size)
        ordering_key->popBack(ordering_key->size() - previous_size);

    if (ordering_key->size() != previous_size)
    {
        rebuildOrderingKey();
        return;
    }

    for (size_t row = previous_size; row < storage->size(); ++row)
        appendOrderingKey(row);
}

MutableColumnPtr ColumnExponentialTimeDecaying::cloneResized(size_t new_size) const
{
    if (new_size <= size())
        return ColumnExponentialTimeDecaying::createWithPrefix(
            storage->cloneResized(new_size),
            ordering_key->cloneResized(new_size),
            decay_length);

    return ColumnExponentialTimeDecaying::create(storage->cloneResized(new_size), decay_length);
}

void ColumnExponentialTimeDecaying::insertData(const char * pos, size_t length)
{
    if (length != sizeof(UInt64))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Serialized ExponentialTimeDecaying key must contain {} bytes, got {}",
            sizeof(UInt64),
            length);

    UInt64 key = 0;
    std::memcpy(&key, pos, sizeof(key));
    transformEndianness<std::endian::native, std::endian::little>(key);

    const auto direct
        = getExponentialTimeDecayingCanonicalDirectValue(key);
    if ((direct.value_at_anchor != 0 && !std::isfinite(direct.anchor_time))
        || getExponentialTimeDecayingOrderingKey(
               direct.value_at_anchor, direct.anchor_time, decay_length)
            != key)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Serialized ExponentialTimeDecaying ordering key is invalid");

    storage->insert(
        Tuple{direct.value_at_anchor, direct.anchor_time});
    appendOrderingKey(size() - 1);
}

void ColumnExponentialTimeDecaying::insert(const Field & x)
{
    storage->insert(x);
    appendOrderingKey(size() - 1);
}

bool ColumnExponentialTimeDecaying::tryInsert(const Field & x)
{
    if (!storage->tryInsert(x))
        return false;

    appendOrderingKey(size() - 1);
    return true;
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnExponentialTimeDecaying::insertFrom(const IColumn & src_, size_t n)
{
    const auto & src = assert_cast<const ColumnExponentialTimeDecaying &>(src_);
    storage->insertFrom(src.getStorageColumn(), n);
    ordering_key->insertFrom(src.getOrderingKeyColumn(), n);
}

void ColumnExponentialTimeDecaying::insertManyFrom(const IColumn & src_, size_t position, size_t length)
{
    const auto & src = assert_cast<const ColumnExponentialTimeDecaying &>(src_);
    storage->insertManyFrom(src.getStorageColumn(), position, length);
    ordering_key->insertManyFrom(src.getOrderingKeyColumn(), position, length);
}

void ColumnExponentialTimeDecaying::insertRangeFrom(const IColumn & src_, size_t start, size_t length)
{
    const auto & src = assert_cast<const ColumnExponentialTimeDecaying &>(src_);
    storage->insertRangeFrom(src.getStorageColumn(), start, length);
    ordering_key->insertRangeFrom(src.getOrderingKeyColumn(), start, length);
}
#else
void ColumnExponentialTimeDecaying::doInsertFrom(const IColumn & src_, size_t n)
{
    const auto & src = assert_cast<const ColumnExponentialTimeDecaying &>(src_);
    storage->insertFrom(src.getStorageColumn(), n);
    ordering_key->insertFrom(src.getOrderingKeyColumn(), n);
}

void ColumnExponentialTimeDecaying::doInsertManyFrom(const IColumn & src_, size_t position, size_t length)
{
    const auto & src = assert_cast<const ColumnExponentialTimeDecaying &>(src_);
    storage->insertManyFrom(src.getStorageColumn(), position, length);
    ordering_key->insertManyFrom(src.getOrderingKeyColumn(), position, length);
}

void ColumnExponentialTimeDecaying::doInsertRangeFrom(const IColumn & src_, size_t start, size_t length)
{
    const auto & src = assert_cast<const ColumnExponentialTimeDecaying &>(src_);
    storage->insertRangeFrom(src.getStorageColumn(), start, length);
    ordering_key->insertRangeFrom(src.getOrderingKeyColumn(), start, length);
}
#endif

void ColumnExponentialTimeDecaying::insertDefault()
{
    storage->insertDefault();
    appendOrderingKey(size() - 1);
}

void ColumnExponentialTimeDecaying::popBack(size_t n)
{
    storage->popBack(n);
    ordering_key->popBack(n);
}

std::string_view ColumnExponentialTimeDecaying::serializeValueIntoArena(
    size_t n,
    Arena & arena,
    char const *& begin,
    const IColumn::SerializationSettings *) const
{
    UInt64 key = getOrderingKey(*this, n);
    transformEndianness<std::endian::little>(key);

    char * memory = arena.allocContinue(sizeof(key), begin);
    std::memcpy(memory, &key, sizeof(key));
    return {memory, sizeof(key)};
}

char * ColumnExponentialTimeDecaying::serializeValueIntoMemory(
    size_t n,
    char * memory,
    const IColumn::SerializationSettings *) const
{
    UInt64 key = getOrderingKey(*this, n);
    transformEndianness<std::endian::little>(key);
    std::memcpy(memory, &key, sizeof(key));
    return memory + sizeof(key);
}

void ColumnExponentialTimeDecaying::collectSerializedValueSizes(
    PaddedPODArray<UInt64> & sizes,
    const UInt8 * is_null,
    const IColumn::SerializationSettings *) const
{
    const size_t rows = size();
    if (sizes.empty())
        sizes.resize_fill(rows);
    else if (sizes.size() != rows)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Size of serialized-size array {} does not match ExponentialTimeDecaying column size {}",
            sizes.size(),
            rows);

    if (is_null)
    {
        for (size_t row = 0; row < rows; ++row)
            sizes[row] += 1 + (is_null[row] ? 0 : sizeof(UInt64));
    }
    else
    {
        for (size_t row = 0; row < rows; ++row)
            sizes[row] += sizeof(UInt64);
    }
}

void ColumnExponentialTimeDecaying::deserializeAndInsertFromArena(
    ReadBuffer & in,
    const IColumn::SerializationSettings *)
{
    UInt64 key = 0;
    readBinaryLittleEndian(key, in);

    const auto direct
        = getExponentialTimeDecayingCanonicalDirectValue(key);
    if ((direct.value_at_anchor != 0 && !std::isfinite(direct.anchor_time))
        || getExponentialTimeDecayingOrderingKey(
               direct.value_at_anchor, direct.anchor_time, decay_length)
            != key)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Serialized ExponentialTimeDecaying ordering key is invalid");

    storage->insert(
        Tuple{direct.value_at_anchor, direct.anchor_time});
    appendOrderingKey(size() - 1);
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

    return ordering_key->compareAt(n, m, rhs.getOrderingKeyColumn(), nan_direction_hint);
}

void ColumnExponentialTimeDecaying::updateHashWithValue(size_t n, SipHash & hash) const
{
    updateOrderingKeyHash(*this, n, hash);
}

void ColumnExponentialTimeDecaying::updateHashFast(SipHash & hash) const
{
    for (size_t row = 0; row < size(); ++row)
        updateOrderingKeyHash(*this, row, hash);
}

void ColumnExponentialTimeDecaying::computeHashInto(
    size_t row_begin, size_t row_end, UInt32 * hash_out, bool initial) const
{
    for (size_t row = row_begin; row < row_end; ++row)
    {
        const UInt32 value = orderingKeyWeakHash(*this, row);
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
    if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorAscendingUnstable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorAscendingStable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorDescendingUnstable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
    else
        getPermutationImpl(limit, res, ComparatorDescendingStable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
}

void ColumnExponentialTimeDecaying::updatePermutation(
    PermutationSortDirection direction,
    PermutationSortStability stability,
    size_t limit,
    int nan_direction_hint,
    Permutation & res,
    EqualRanges & equal_ranges) const
{
    ComparatorEqual equal(*this, nan_direction_hint);

    if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingUnstable(*this, nan_direction_hint), equal, DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingStable(*this, nan_direction_hint), equal, DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingUnstable(*this, nan_direction_hint), equal, DefaultSort(), DefaultPartialSort());
    else
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingStable(*this, nan_direction_hint), equal, DefaultSort(), DefaultPartialSort());
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
        ordering_key->filter(filt, result_size_hint)->assumeMutable(),
        decay_length);
}

void ColumnExponentialTimeDecaying::filter(const Filter & filt)
{
    storage->filter(filt);
    ordering_key->filter(filt);
}

void ColumnExponentialTimeDecaying::expand(const Filter & mask, bool inverted)
{
    storage->expand(mask, inverted);
    ordering_key->expand(mask, inverted);
}

ColumnPtr ColumnExponentialTimeDecaying::permute(const Permutation & perm, size_t limit) const
{
    return ColumnExponentialTimeDecaying::createWithPrefix(
        storage->permute(perm, limit)->assumeMutable(),
        ordering_key->permute(perm, limit)->assumeMutable(),
        decay_length);
}

ColumnPtr ColumnExponentialTimeDecaying::index(const IColumn & indexes, size_t limit) const
{
    return ColumnExponentialTimeDecaying::createWithPrefix(
        storage->index(indexes, limit)->assumeMutable(),
        ordering_key->index(indexes, limit)->assumeMutable(),
        decay_length);
}

ColumnPtr ColumnExponentialTimeDecaying::replicate(const Offsets & offsets) const
{
    return ColumnExponentialTimeDecaying::createWithPrefix(
        storage->replicate(offsets)->assumeMutable(),
        ordering_key->replicate(offsets)->assumeMutable(),
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
        source_prefix.push_back(decaying.ordering_key);
    }

    storage->prepareForSquashing(source_storage, factor);
    ordering_key->prepareForSquashing(source_prefix, factor);
}

void ColumnExponentialTimeDecaying::rollback(const ColumnCheckpoint & checkpoint)
{
    storage->rollback(checkpoint);
    rebuildOrderingKey();
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
