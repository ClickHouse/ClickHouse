#include <Columns/ColumnExponentialTimeDecaying.h>

#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumnImpl.h>
#include <Columns/MaskOperations.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Common/transformEndianness.h>
#include <DataTypes/DataTypeExponentialTimeDecaying.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

void writeOrderingKey64(UInt64 key, char * memory)
{
    transformEndianness<std::endian::little>(key);
    std::memcpy(memory, &key, sizeof(key));
}

void writeOrderingKey128(UInt128 key, char * memory)
{
    transformEndianness<std::endian::little>(key);
    std::memcpy(memory, &key, sizeof(key));
}

UInt64 readOrderingKey64(const char * pos)
{
    UInt64 key = 0;
    std::memcpy(&key, pos, sizeof(key));
    transformEndianness<std::endian::native, std::endian::little>(key);
    return key;
}

UInt128 readOrderingKey128(const char * pos)
{
    UInt128 key = 0;
    std::memcpy(&key, pos, sizeof(key));
    transformEndianness<std::endian::native, std::endian::little>(key);
    return key;
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


}

ColumnExponentialTimeDecaying::ColumnExponentialTimeDecaying(
    MutableColumnPtr && storage_,
    Float64 decay_length_,
    ExponentialTimeDecayingKeyWidth key_width_,
    ExponentialTimeDecayingMode mode_)
    : storage(std::move(storage_))
    , decay_length(decay_length_)
    , key_width(key_width_)
    , mode(mode_)
{
    if (mode == ExponentialTimeDecayingMode::Compact)
    {
        if (key_width == ExponentialTimeDecayingKeyWidth::Bits64)
            assert_cast<const ColumnUInt64 &>(*storage);
        else
            assert_cast<const ColumnUInt128 &>(*storage);
    }
    else
    {
        const auto & tuple = assert_cast<const ColumnTuple &>(*storage);
        chassert(tuple.tupleSize() == 3);
        if (key_width == ExponentialTimeDecayingKeyWidth::Bits64)
            assert_cast<const ColumnUInt64 &>(tuple.getColumn(0));
        else
            assert_cast<const ColumnUInt128 &>(tuple.getColumn(0));
        assert_cast<const ColumnFloat64 &>(tuple.getColumn(1));
        assert_cast<const ColumnFloat64 &>(tuple.getColumn(2));
    }
}

std::string ColumnExponentialTimeDecaying::getName() const
{
    return getExponentialTimeDecayingTypeName(key_width);
}

MutableColumnPtr ColumnExponentialTimeDecaying::cloneResized(size_t new_size) const
{
    /// The logical zero uses the midpoint key, not the physical integer default.
    const size_t copied_size = std::min(size(), new_size);
    auto result = ColumnExponentialTimeDecaying::create(
        storage->cloneResized(copied_size),
        decay_length,
        key_width,
        mode);
    result->insertManyDefaults(new_size - copied_size);
    return result;
}

const IColumn & ColumnExponentialTimeDecaying::getOrderingKeyColumn() const
{
    if (mode == ExponentialTimeDecayingMode::Compact)
        return *storage;
    return assert_cast<const ColumnTuple &>(*storage).getColumn(0);
}

IColumn & ColumnExponentialTimeDecaying::getOrderingKeyColumn()
{
    if (mode == ExponentialTimeDecayingMode::Compact)
        return *storage;
    return assert_cast<ColumnTuple &>(*storage).getColumn(0);
}

ExponentialTimeDecayingCanonicalDirectValue ColumnExponentialTimeDecaying::getCanonicalValueAt(size_t n) const
{
    if (key_width == ExponentialTimeDecayingKeyWidth::Bits64)
        return getExponentialTimeDecayingCanonicalDirectValue(
            assert_cast<const ColumnUInt64 &>(getOrderingKeyColumn()).getData()[n]);
    return getExponentialTimeDecayingCanonicalDirectValue(
        assert_cast<const ColumnUInt128 &>(getOrderingKeyColumn()).getData()[n]);
}

ExponentialTimeDecayingCanonicalDirectValue ColumnExponentialTimeDecaying::getDirectValueAt(size_t n) const
{
    if (mode == ExponentialTimeDecayingMode::Direct)
    {
        const auto & tuple = assert_cast<const ColumnTuple &>(*storage);
        return {
            assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)).getData()[n],
            assert_cast<const ColumnFloat64 &>(tuple.getColumn(2)).getData()[n]};
    }

    return getCanonicalValueAt(n);
}

void ColumnExponentialTimeDecaying::setDirectMetadataAt(
    size_t n,
    const ExponentialTimeDecayingCanonicalDirectValue & direct)
{
    chassert(mode == ExponentialTimeDecayingMode::Direct);
    auto & tuple = assert_cast<ColumnTuple &>(*storage);
    assert_cast<ColumnFloat64 &>(tuple.getColumn(1)).getData()[n] = direct.value_at_anchor;
    assert_cast<ColumnFloat64 &>(tuple.getColumn(2)).getData()[n] = direct.anchor_time;
}

Float64 ColumnExponentialTimeDecaying::getUnitTimestampAt(size_t n) const
{
    const auto canonical = getCanonicalValueAt(n);
    if (canonical.value_at_anchor == 0)
        return 0;

    const Float64 edge = std::numeric_limits<Float64>::max();
    if (std::abs(canonical.anchor_time) == edge && std::abs(canonical.value_at_anchor) != 1)
        return canonical.anchor_time;

    const Float64 unit_timestamp = getExponentialTimeDecayingUnitTimestamp(
        canonical.value_at_anchor, canonical.anchor_time, decay_length);
    return std::isfinite(unit_timestamp) ? unit_timestamp : canonical.anchor_time;
}

ExponentialTimeDecayingCanonicalDirectValue ColumnExponentialTimeDecaying::getPrintableValueAt(size_t n) const
{
    const auto canonical = getCanonicalValueAt(n);
    if (canonical.value_at_anchor == 0)
        return {0, 0};

    const Float64 edge = std::numeric_limits<Float64>::max();
    if (std::abs(canonical.anchor_time) == edge && std::abs(canonical.value_at_anchor) != 1)
        return canonical;

    const Float64 unit_timestamp = getExponentialTimeDecayingUnitTimestamp(
        canonical.value_at_anchor, canonical.anchor_time, decay_length);
    if (!std::isfinite(unit_timestamp))
        return canonical;

    return {std::signbit(canonical.value_at_anchor) ? -1.0 : 1.0, unit_timestamp};
}

Int8 ColumnExponentialTimeDecaying::getSignAt(size_t n) const
{
    if (key_width == ExponentialTimeDecayingKeyWidth::Bits64)
    {
        constexpr UInt64 midpoint = UInt64(1) << 63;
        const UInt64 key = assert_cast<const ColumnUInt64 &>(getOrderingKeyColumn()).getData()[n];
        return key == midpoint ? 0 : (key < midpoint ? -1 : 1);
    }

    constexpr UInt128 midpoint = UInt128(1) << 127;
    const UInt128 key = assert_cast<const ColumnUInt128 &>(getOrderingKeyColumn()).getData()[n];
    return key == midpoint ? 0 : (key < midpoint ? -1 : 1);
}

Field ColumnExponentialTimeDecaying::operator[](size_t n) const
{
    Field res;
    get(n, res);
    return res;
}

void ColumnExponentialTimeDecaying::get(size_t n, Field & res) const
{
    const auto value = mode == ExponentialTimeDecayingMode::Direct
        ? getDirectValueAt(n)
        : getCanonicalValueAt(n);
    res = Tuple{value.value_at_anchor, value.anchor_time, decay_length};
}

void ColumnExponentialTimeDecaying::getValueNameImpl(
    WriteBufferFromOwnString & name_buf, size_t n, const Options & options) const
{
    if (options.notFull(name_buf))
        name_buf << "(";

    if (mode == ExponentialTimeDecayingMode::Direct)
    {
        const auto direct = getDirectValueAt(n);
        name_buf << direct.value_at_anchor << ", " << direct.anchor_time;
    }
    else
    {
        const auto printable = getPrintableValueAt(n);
        name_buf << printable.value_at_anchor << ", " << printable.anchor_time;
    }

    name_buf << ", " << decay_length;
    if (options.notFull(name_buf))
        name_buf << ")";
}

void ColumnExponentialTimeDecaying::insertEncoded(
    UInt128 key, std::optional<ExponentialTimeDecayingCanonicalDirectValue> direct)
{
    if (mode == ExponentialTimeDecayingMode::Compact)
    {
        if (key_width == ExponentialTimeDecayingKeyWidth::Bits64)
            assert_cast<ColumnUInt64 &>(*storage).insertValue(static_cast<UInt64>(key));
        else
            assert_cast<ColumnUInt128 &>(*storage).insertValue(key);
        return;
    }

    if (!direct)
        direct = key_width == ExponentialTimeDecayingKeyWidth::Bits64
            ? getExponentialTimeDecayingCanonicalDirectValue(static_cast<UInt64>(key))
            : getExponentialTimeDecayingCanonicalDirectValue(key);

    auto & tuple = assert_cast<ColumnTuple &>(*storage);
    if (key_width == ExponentialTimeDecayingKeyWidth::Bits64)
        assert_cast<ColumnUInt64 &>(tuple.getColumn(0)).insertValue(static_cast<UInt64>(key));
    else
        assert_cast<ColumnUInt128 &>(tuple.getColumn(0)).insertValue(key);
    assert_cast<ColumnFloat64 &>(tuple.getColumn(1)).insertValue(direct->value_at_anchor);
    assert_cast<ColumnFloat64 &>(tuple.getColumn(2)).insertValue(direct->anchor_time);
}

void ColumnExponentialTimeDecaying::insertData(const char * pos, size_t length)
{
    if (key_width == ExponentialTimeDecayingKeyWidth::Bits64)
    {
        if (length != sizeof(UInt64))
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Serialized ExponentialTimeDecaying64 key must contain {} bytes, got {}",
                sizeof(UInt64),
                length);
        insertEncoded(readOrderingKey64(pos), std::nullopt);
        return;
    }

    if (length != sizeof(UInt128))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Serialized ExponentialTimeDecaying128 key must contain {} bytes, got {}",
            sizeof(UInt128),
            length);
    insertEncoded(readOrderingKey128(pos), std::nullopt);
}

void ColumnExponentialTimeDecaying::insert(const Field & x)
{
    const auto & tuple = x.safeGet<Tuple>();
    if (tuple.size() != 3)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "ExponentialTimeDecaying expects (value, timestamp, decay_length)");

    const Float64 value = applyVisitor(FieldVisitorConvertToNumber<Float64>(), tuple[0]);
    const Float64 timestamp = applyVisitor(FieldVisitorConvertToNumber<Float64>(), tuple[1]);
    const Float64 supplied_decay_length
        = applyVisitor(FieldVisitorConvertToNumber<Float64>(), tuple[2]);

    if (!std::isfinite(supplied_decay_length) || supplied_decay_length != decay_length)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "ExponentialTimeDecaying supplied decay length {} does not match type decay length {}",
            supplied_decay_length,
            decay_length);

    if (!isFiniteExponentialTimeDecayingCurve(value, timestamp, decay_length))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid ExponentialTimeDecaying value");

    const auto normalized = normalizeExponentialTimeDecaying(value, timestamp, decay_length, key_width);
    std::optional<ExponentialTimeDecayingCanonicalDirectValue> direct;
    if (mode == ExponentialTimeDecayingMode::Direct)
        direct = ExponentialTimeDecayingCanonicalDirectValue{value, timestamp};
    insertEncoded(normalized.ordering_key, direct);
}

bool ColumnExponentialTimeDecaying::tryInsert(const Field & x)
{
    try
    {
        insert(x);
        return true;
    }
    catch (...)
    {
        // Ok: `tryInsert` reports any invalid public representation as a failed insertion.
        return false;
    }
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnExponentialTimeDecaying::insertFrom(const IColumn & src_, size_t n)
{
    const auto & src = assert_cast<const ColumnExponentialTimeDecaying &>(src_);
    chassert(mode == src.mode);
    storage->insertFrom(src.getStorageColumn(), n);
}

void ColumnExponentialTimeDecaying::insertManyFrom(const IColumn & src_, size_t position, size_t length)
{
    const auto & src = assert_cast<const ColumnExponentialTimeDecaying &>(src_);
    chassert(mode == src.mode);
    storage->insertManyFrom(src.getStorageColumn(), position, length);
}

void ColumnExponentialTimeDecaying::insertRangeFrom(const IColumn & src_, size_t start, size_t length)
{
    const auto & src = assert_cast<const ColumnExponentialTimeDecaying &>(src_);
    chassert(mode == src.mode);
    storage->insertRangeFrom(src.getStorageColumn(), start, length);
}
#else
void ColumnExponentialTimeDecaying::doInsertFrom(const IColumn & src_, size_t n)
{
    const auto & src = assert_cast<const ColumnExponentialTimeDecaying &>(src_);
    chassert(mode == src.mode);
    storage->insertFrom(src.getStorageColumn(), n);
}

void ColumnExponentialTimeDecaying::doInsertManyFrom(const IColumn & src_, size_t position, size_t length)
{
    const auto & src = assert_cast<const ColumnExponentialTimeDecaying &>(src_);
    chassert(mode == src.mode);
    storage->insertManyFrom(src.getStorageColumn(), position, length);
}

void ColumnExponentialTimeDecaying::doInsertRangeFrom(const IColumn & src_, size_t start, size_t length)
{
    const auto & src = assert_cast<const ColumnExponentialTimeDecaying &>(src_);
    chassert(mode == src.mode);
    storage->insertRangeFrom(src.getStorageColumn(), start, length);
}
#endif

bool ColumnExponentialTimeDecaying::isDefaultAt(size_t n) const
{
    return getSignAt(n) == 0;
}

bool ColumnExponentialTimeDecaying::hasOnlyTypeDefaults() const
{
    for (size_t i = 0; i < size(); ++i)
        if (!isDefaultAt(i))
            return false;
    return true;
}

void ColumnExponentialTimeDecaying::insertDefault()
{
    const UInt128 midpoint = key_width == ExponentialTimeDecayingKeyWidth::Bits64
        ? UInt128(UInt64(1) << 63)
        : (UInt128(1) << 127);
    insertEncoded(midpoint, ExponentialTimeDecayingCanonicalDirectValue{0, 0});
}

void ColumnExponentialTimeDecaying::popBack(size_t n)
{
    storage->popBack(n);
}

std::string_view ColumnExponentialTimeDecaying::serializeValueIntoArena(
    size_t n,
    Arena & arena,
    char const *& begin,
    const IColumn::SerializationSettings *) const
{
    const size_t serialized_size = key_width == ExponentialTimeDecayingKeyWidth::Bits64
        ? sizeof(UInt64)
        : sizeof(UInt128);
    char * memory = arena.allocContinue(serialized_size, begin);

    if (key_width == ExponentialTimeDecayingKeyWidth::Bits64)
        writeOrderingKey64(assert_cast<const ColumnUInt64 &>(getOrderingKeyColumn()).getData()[n], memory);
    else
        writeOrderingKey128(assert_cast<const ColumnUInt128 &>(getOrderingKeyColumn()).getData()[n], memory);

    return {memory, serialized_size};
}

char * ColumnExponentialTimeDecaying::serializeValueIntoMemory(
    size_t n,
    char * memory,
    const IColumn::SerializationSettings *) const
{
    if (key_width == ExponentialTimeDecayingKeyWidth::Bits64)
    {
        writeOrderingKey64(assert_cast<const ColumnUInt64 &>(getOrderingKeyColumn()).getData()[n], memory);
        return memory + sizeof(UInt64);
    }

    writeOrderingKey128(assert_cast<const ColumnUInt128 &>(getOrderingKeyColumn()).getData()[n], memory);
    return memory + sizeof(UInt128);
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

    const UInt64 serialized_size = key_width == ExponentialTimeDecayingKeyWidth::Bits64
        ? sizeof(UInt64)
        : sizeof(UInt128);

    for (size_t row = 0; row < rows; ++row)
        sizes[row] += is_null ? 1 + (is_null[row] ? 0 : serialized_size) : serialized_size;
}

void ColumnExponentialTimeDecaying::deserializeAndInsertFromArena(
    ReadBuffer & in,
    const IColumn::SerializationSettings *)
{
    if (key_width == ExponentialTimeDecayingKeyWidth::Bits64)
    {
        UInt64 key = 0;
        readBinaryLittleEndian(key, in);
        insertEncoded(key, std::nullopt);
        return;
    }

    UInt128 key = 0;
    readBinaryLittleEndian(key, in);
    insertEncoded(key, std::nullopt);
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
    chassert(key_width == rhs.key_width);
    return getOrderingKeyColumn().compareAt(n, m, rhs.getOrderingKeyColumn(), nan_direction_hint);
}

void ColumnExponentialTimeDecaying::updateHashWithValue(size_t n, SipHash & hash) const
{
    getOrderingKeyColumn().updateHashWithValue(n, hash);
}

void ColumnExponentialTimeDecaying::updateHashFast(SipHash & hash) const
{
    getOrderingKeyColumn().updateHashFast(hash);
}

void ColumnExponentialTimeDecaying::computeHashInto(
    size_t row_begin, size_t row_end, UInt32 * hash_out, bool initial) const
{
    getOrderingKeyColumn().computeHashInto(row_begin, row_end, hash_out, initial);
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
    return ColumnExponentialTimeDecaying::create(
        storage->filter(filt, result_size_hint)->assumeMutable(),
        decay_length,
        key_width,
        mode);
}

void ColumnExponentialTimeDecaying::filter(const Filter & filt)
{
    storage->filter(filt);
}

void ColumnExponentialTimeDecaying::expand(const Filter & mask, bool inverted)
{
    if (key_width == ExponentialTimeDecayingKeyWidth::Bits64)
        expandDataByMask<UInt64>(
            assert_cast<ColumnUInt64 &>(getOrderingKeyColumn()).getData(), mask, inverted, UInt64(1) << 63);
    else
        expandDataByMask<UInt128>(
            assert_cast<ColumnUInt128 &>(getOrderingKeyColumn()).getData(), mask, inverted, UInt128(1) << 127);

    if (mode == ExponentialTimeDecayingMode::Direct)
    {
        auto & tuple = assert_cast<ColumnTuple &>(*storage);
        tuple.getColumn(1).expand(mask, inverted);
        tuple.getColumn(2).expand(mask, inverted);
    }
}

ColumnPtr ColumnExponentialTimeDecaying::permute(const Permutation & perm, size_t limit) const
{
    return ColumnExponentialTimeDecaying::create(
        storage->permute(perm, limit)->assumeMutable(),
        decay_length,
        key_width,
        mode);
}

ColumnPtr ColumnExponentialTimeDecaying::index(const IColumn & indexes, size_t limit) const
{
    return ColumnExponentialTimeDecaying::create(
        storage->index(indexes, limit)->assumeMutable(),
        decay_length,
        key_width,
        mode);
}

ColumnPtr ColumnExponentialTimeDecaying::replicate(const Offsets & offsets) const
{
    return ColumnExponentialTimeDecaying::create(
        storage->replicate(offsets)->assumeMutable(),
        decay_length,
        key_width,
        mode);
}

ColumnPtr ColumnExponentialTimeDecaying::compress(bool force_compression) const
{
    auto compressed = storage->compress(force_compression);
    const auto byte_size = compressed->byteSize();
    return ColumnCompressed::create(
        size(),
        byte_size,
        [my_compressed = std::move(compressed), my_decay_length = decay_length, my_key_width = key_width, my_mode = mode]
        {
            return ColumnExponentialTimeDecaying::create(
                my_compressed->decompress()->assumeMutable(),
                my_decay_length,
                my_key_width,
                my_mode);
        });
}

void ColumnExponentialTimeDecaying::prepareForSquashing(
    const VectorWithMemoryTracking<ColumnPtr> & source_columns, size_t factor)
{
    VectorWithMemoryTracking<ColumnPtr> source_storage;
    source_storage.reserve(source_columns.size());
    for (const auto & source : source_columns)
        source_storage.push_back(assert_cast<const ColumnExponentialTimeDecaying &>(*source).getStoragePtr());
    storage->prepareForSquashing(source_storage, factor);
}

void ColumnExponentialTimeDecaying::rollback(const ColumnCheckpoint & checkpoint)
{
    storage->rollback(checkpoint);
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
        return decay_length == other->decay_length
            && key_width == other->key_width
            && mode == other->mode
            && storage->structureEquals(*other->storage);
    return false;
}

}
