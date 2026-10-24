#include <algorithm>
#include <iterator>

#include <Common/Arena.h>
#include <Common/ErrnoException.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/StringHashSet.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Columns/ColumnNullable.h>
#include <Columns/IColumnImpl.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/MaskOperations.h>
#include <Columns/findEqualRangeEndAssumeSorted.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/Operators.h>
#include <Common/ProfileEvents.h>

#include <mutex>
#include <sys/mman.h>

#if USE_EMBEDDED_COMPILER
#include <DataTypes/Native.h>
#include <llvm/IR/IRBuilder.h>
#endif


namespace ProfileEvents
{
    extern const Event NullableNullMapMaterializations;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_ALLOCATE_MEMORY;
}


ColumnNullable::ColumnNullable(MutableColumnPtr && nested_column_, MutableColumnPtr && null_map_)
    : nested_column(std::move(nested_column_)), null_map(std::move(null_map_)), has_null_map(static_cast<bool>(null_map))
{
    /// ColumnNullable cannot have constant nested column. But constant argument could be passed. Materialize it.
    nested_column = nested_column->convertToFullColumnIfConst();

    if (!nested_column->canBeInsideNullable())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{} cannot be inside Nullable column", nested_column->getName());

    if (null_map && isColumnConst(*null_map))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "ColumnNullable cannot have constant null map");
}

ColumnNullable::ColumnNullable(MutableColumnPtr && nested_column_)
    : ColumnNullable(std::move(nested_column_), nullptr)
{
}

ColumnNullable::ColumnNullable(const ColumnNullable & other)
    : COWHelper<IColumnHelper<ColumnNullable>, ColumnNullable>(other)
    , nested_column(other.nested_column)
{
    /// The null map of `other` can be created concurrently, read it only if it is published.
    if (other.hasNullMap())
    {
        null_map = other.null_map;
        has_null_map.store(true, std::memory_order_relaxed);
    }
}

namespace
{

/// The null maps created on demand in the const methods of shared columns are rare,
/// so a small set of mutexes shared by all columns is enough.
std::mutex & getNullMapMaterializationMutex(const void * column)
{
    static constexpr size_t num_mutexes = 64;
    static std::mutex mutexes[num_mutexes];
    return mutexes[(reinterpret_cast<uintptr_t>(column) >> 6) % num_mutexes];
}

/// Feeds `size` zero bytes into the hash, as if a zero-filled null map of this size was hashed.
void updateHashWithZeros(SipHash & hash, size_t size)
{
    static constexpr char zeros[4096]{};
    while (size)
    {
        size_t chunk = std::min(size, sizeof(zeros));
        hash.update(zeros, chunk);
        size -= chunk;
    }
}

}

void ColumnNullable::materializeNullMapSlow() const
{
    std::lock_guard lock(getNullMapMaterializationMutex(this));
    if (has_null_map.load(std::memory_order_relaxed))
        return;

    null_map = ColumnUInt8::create(nested_column->size(), static_cast<UInt8>(0));
    has_null_map.store(true, std::memory_order_release);
    ProfileEvents::increment(ProfileEvents::NullableNullMapMaterializations);
}

namespace
{

/// A read-only anonymous mapping, so it does not take memory: its pages are mapped to the zero page on reading.
/// Not a static array in `.bss`, which would take 16 MiB of the 2 GiB reach of the PC-relative references of the binary.
/// Padded on both sides like `PaddedPODArray`, because the readers may read past the end of the null map
/// (e.g. `memcpySmallAllowReadWriteOverflow15` in `GatherUtils`). Never unmapped.
constexpr size_t zeros_size = 16 << 20;
constexpr size_t zeros_pad_left = PADDING_FOR_SIMD;

const UInt8 * getZeros()
{
    static const UInt8 * const zeros = []
    {
        void * mapping = mmap(nullptr, zeros_pad_left + zeros_size + PADDING_FOR_SIMD, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (MAP_FAILED == mapping)
            throw ErrnoException(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Cannot mmap the zero null map");
        return static_cast<const UInt8 *>(mapping) + zeros_pad_left;
    }();
    return zeros;
}

}

const UInt8 * ColumnNullable::getNullMapRawDataOrZeros() const
{
    if (const auto * map = tryGetNullMapData())
        return map->data();
    if (size() <= zeros_size)
        return getZeros();
    return getNullMapData().data();
}

ColumnPtr ColumnNullable::convertToFullIfWrapped() const
{
    ColumnPtr map = tryGetNullMapColumnPtr();
    ColumnPtr new_nested = nested_column->convertToFullIfWrapped();
    ColumnPtr new_map = map ? map->convertToFullIfWrapped() : nullptr;

    if (new_nested.get() == nested_column.get() && new_map.get() == map.get())
        return getPtr();

    return ColumnNullable::create(new_nested, new_map);
}

bool ColumnNullable::dropNullMapIfNoNulls()
{
    const auto * map = tryGetNullMapData();
    if (!map)
        return true;

    if (!memoryIsZero(map->data(), 0, map->size()))
        return false;

    has_null_map.store(false, std::memory_order_relaxed);
    null_map = nullptr;
    return true;
}

std::string_view ColumnNullable::getDataAt(size_t n) const
{
    if (!isNullAt(n))
        return getNestedColumn().getDataAt(n);

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getDataAt is not supported for {} in case if value is NULL", getName());
}

/// The hashes must not depend on whether the column has a null map, because the same values
/// can be hashed in the blocks with and without a null map (e.g. in GROUP BY or DISTINCT).

void ColumnNullable::updateHashWithValue(size_t n, SipHash & hash) const
{
    const auto * arr = tryGetNullMapData();
    const UInt8 is_null = arr ? (*arr)[n] : 0;
    hash.update(is_null);
    if (is_null == 0)
        nested_column->updateHashWithValue(n, hash);
}

void ColumnNullable::updateHashWithValueRange(size_t begin, size_t end, SipHash & hash) const
{
    nested_column->updateHashWithValueRange(begin, end, hash);
    if (const auto * arr = tryGetNullMapData())
        hash.update(reinterpret_cast<const char *>(&(*arr)[begin]), (end - begin) * sizeof((*arr)[0]));
    else
        updateHashWithZeros(hash, end - begin);
}

void ColumnNullable::computeHashInto(size_t row_begin, size_t row_end, UInt32 * hash_out, bool initial) const
{
    /// A NULL row must hash to a fixed value, independent of the bytes that happen to sit in the
    /// nested column for that row, so two SQL-equal NULL keys route identically. The nested hash of
    /// null rows is replaced with `WEAK_HASH32_INITIAL_VALUE`. See IColumn::computeHashInto.
    const auto * map = tryGetNullMapData();
    if (!map)
    {
        nested_column->computeHashInto(row_begin, row_end, hash_out, initial);
        return;
    }

    const UInt8 * null_map_data = map->data() + row_begin;
    const size_t n = row_end - row_begin;

    if (initial)
    {
        nested_column->computeHashInto(row_begin, row_end, hash_out, /*initial=*/true);
        for (size_t i = 0; i < n; ++i)
            if (null_map_data[i])
                hash_out[i] = WEAK_HASH32_INITIAL_VALUE;
        return;
    }

    /// Non-initial: `hash_out` holds the prior key columns' hash, so the nested hash is produced
    /// into a transient scratch buffer, null-selected, then combined into `hash_out`.
    PaddedPODArray<UInt32> nested_hash(n);
    nested_column->computeHashInto(row_begin, row_end, nested_hash.data(), /*initial=*/true);
    for (size_t i = 0; i < n; ++i)
    {
        const UInt32 base = null_map_data[i] ? WEAK_HASH32_INITIAL_VALUE : nested_hash[i];
        hash_out[i] = combineWeakHash32(base, hash_out[i]);
    }
}

void ColumnNullable::updateHashFast(SipHash & hash) const
{
    if (const auto * map = tryGetNullMapColumn())
        map->updateHashFast(hash);
    else
        updateHashWithZeros(hash, size());
    nested_column->updateHashFast(hash);
}

MutableColumnPtr ColumnNullable::cloneResized(size_t new_size) const
{
    MutableColumnPtr new_nested_col = nested_column->cloneResized(new_size);

    const auto * map = tryGetNullMapData();
    /// No NULLs are added if the column is not extended.
    if (new_size == 0 || (!map && new_size <= size()))
        return ColumnNullable::create(std::move(new_nested_col));

    auto new_null_map = ColumnUInt8::create(new_size);

    if (new_size > 0)
    {
        new_null_map->getData().resize_exact(new_size);

        size_t count = std::min(size(), new_size);
        if (map)
            memcpy(new_null_map->getData().data(), map->data(), count * sizeof((*map)[0]));
        else
            memset(new_null_map->getData().data(), 0, count);

        /// If resizing to bigger one, set all new values to NULLs.
        if (new_size > count)
            memset(&new_null_map->getData()[count], 1, new_size - count);
    }

    return ColumnNullable::create(std::move(new_nested_col), std::move(new_null_map));
}


Field ColumnNullable::operator[](size_t n) const
{
    return isNullAt(n) ? Null() : getNestedColumn()[n];
}


void ColumnNullable::get(size_t n, Field & res) const
{
    if (isNullAt(n))
        res = Null();
    else
        getNestedColumn().get(n, res);
}

void ColumnNullable::getValueNameImpl(WriteBufferFromOwnString & name_buf, size_t n, const Options & options) const
{
    if (isNullAt(n))
    {
        if (options.notFull(name_buf))
            name_buf << "NULL";
        return;
    }

    getNestedColumn().getValueNameImpl(name_buf, n, options);
}

Float64 ColumnNullable::getFloat64(size_t n) const
{
    if (isNullAt(n))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of {} at {} is NULL while calling method getFloat64", getName(), n);
    return getNestedColumn().getFloat64(n);
}

Float32 ColumnNullable::getFloat32(size_t n) const
{
    if (isNullAt(n))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of {} at {} is NULL while calling method getFloat32", getName(), n);
    return getNestedColumn().getFloat32(n);
}

UInt64 ColumnNullable::getUInt(size_t n) const
{
    if (isNullAt(n))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of {} at {} is NULL while calling method getUInt", getName(), n);
    return getNestedColumn().getUInt(n);
}

Int64 ColumnNullable::getInt(size_t n) const
{
    if (isNullAt(n))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of {} at {} is NULL while calling method getInt", getName(), n);
    return getNestedColumn().getInt(n);
}

/// The methods inserting a NULL into a column without a null map create the null map before
/// inserting into the nested column, because the null map is created with the size of the nested column.

void ColumnNullable::insertData(const char * pos, size_t length)
{
    if (pos == nullptr)
    {
        materializeNullMap();
        nested_column->insertDefault();
        getNullMapData().push_back(true);
    }
    else
    {
        nested_column->insertData(pos, length);
        if (hasNullMap())
            getNullMapData().push_back(false);
    }
}

std::string_view ColumnNullable::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin, const IColumn::SerializationSettings * settings) const
{
    const auto * arr = tryGetNullMapData();
    const UInt8 is_null = arr ? (*arr)[n] : 0;

    /// First serialize the NULL map byte.
    auto * pos = arena.allocContinue(1, begin);
    *pos = is_null;

    /// If the value is NULL, that's it.
    if (is_null)
        return std::string_view(pos, 1);

    /// Now serialize the nested value. Note that it also uses allocContinue so that the memory range remains contiguous.
    auto nested_ref = getNestedColumn().serializeValueIntoArena(n, arena, begin, settings);

    /// serializeValueIntoArena may reallocate memory. Have to use ptr from nested_ref.data and move it back.
    return std::string_view(nested_ref.data() - 1, nested_ref.size() + 1);
}

char * ColumnNullable::serializeValueIntoMemory(size_t n, char * memory, const IColumn::SerializationSettings * settings) const
{
    const auto * arr = tryGetNullMapData();
    const UInt8 is_null = arr ? (*arr)[n] : 0;

    *memory = is_null;
    ++memory;

    if (is_null)
        return memory;

    return getNestedColumn().serializeValueIntoMemory(n, memory, settings);
}

std::optional<size_t> ColumnNullable::getSerializedValueSize(size_t n, const IColumn::SerializationSettings * settings) const
{
    if (isNullAt(n))
        return 1; /// Just the null mask byte for NULL values.

    auto nested_size = getNestedColumn().getSerializedValueSize(n, settings);
    if (!nested_size)
        return std::nullopt;
    return 1 + *nested_size; /// +1 for null mask byte.
}

void ColumnNullable::deserializeAndInsertFromArena(ReadBuffer & in, const IColumn::SerializationSettings * settings)
{
    UInt8 val = 0;
    readBinaryLittleEndian<UInt8>(val, in);

    if (val != 0)
        materializeNullMap();

    if (hasNullMap())
        getNullMapData().push_back(val);

    if (val == 0)
        nested_column->deserializeAndInsertFromArena(in, settings);
    else
        nested_column->insertDefault();
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnNullable::insertRangeFrom(const IColumn & src, size_t start, size_t length)
#else
void ColumnNullable::doInsertRangeFrom(const IColumn & src, size_t start, size_t length)
#endif
{
    const ColumnNullable & nullable_col = assert_cast<const ColumnNullable &>(src);
    const auto * src_map = nullable_col.tryGetNullMapColumn();

    /// Keep the column without a null map while the inserted rows have no NULLs.
    if (src_map && !hasNullMap() && !memoryIsZero(src_map->getData().data(), start, start + length))
        materializeNullMap();

    if (hasNullMap())
    {
        auto & map = getNullMapColumn();
        if (src_map)
            map.insertRangeFrom(*src_map, start, length);
        else
            map.getData().resize_fill(map.size() + length);
    }

    nested_column->insertRangeFrom(*nullable_col.nested_column, start, length);
}

void ColumnNullable::insert(const Field & x)
{
    if (x.isNull())
    {
        materializeNullMap();
        nested_column->insertDefault();
        getNullMapData().push_back(true);
    }
    else
    {
        nested_column->insert(x);
        if (hasNullMap())
            getNullMapData().push_back(false);
    }
}

bool ColumnNullable::tryInsert(const Field & x)
{
    if (x.isNull())
    {
        materializeNullMap();
        nested_column->insertDefault();
        getNullMapData().push_back(true);
        return true;
    }

    if (!nested_column->tryInsert(x))
        return false;

    if (hasNullMap())
        getNullMapData().push_back(false);
    return true;
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnNullable::insertFrom(const IColumn & src, size_t n)
#else
void ColumnNullable::doInsertFrom(const IColumn & src, size_t n)
#endif
{
    const ColumnNullable & src_concrete = assert_cast<const ColumnNullable &>(src);
    const UInt8 is_null = src_concrete.isNullAt(n) ? src_concrete.getNullMapData()[n] : 0;
    if (is_null)
        materializeNullMap();
    nested_column->insertFrom(src_concrete.getNestedColumn(), n);
    if (hasNullMap())
        getNullMapData().push_back(is_null);
}


#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnNullable::insertManyFrom(const IColumn & src, size_t position, size_t length)
#else
void ColumnNullable::doInsertManyFrom(const IColumn & src, size_t position, size_t length)
#endif
{
    const ColumnNullable & src_concrete = assert_cast<const ColumnNullable &>(src);
    const UInt8 is_null = src_concrete.isNullAt(position) ? src_concrete.getNullMapData()[position] : 0;
    if (is_null)
        materializeNullMap();
    nested_column->insertManyFrom(src_concrete.getNestedColumn(), position, length);
    if (hasNullMap())
    {
        auto & map = getNullMapData();
        map.resize_fill(map.size() + length, is_null);
    }
}

void ColumnNullable::insertFromNotNullable(const IColumn & src, size_t n)
{
    nested_column->insertFrom(src, n);
    if (hasNullMap())
        getNullMapData().push_back(false);
}

void ColumnNullable::insertRangeFromNotNullable(const IColumn & src, size_t start, size_t length)
{
    nested_column->insertRangeFrom(src, start, length);
    if (hasNullMap())
        getNullMapData().resize_fill(getNullMapData().size() + length);
}

void ColumnNullable::insertManyFromNotNullable(const IColumn & src, size_t position, size_t length)
{
    for (size_t i = 0; i < length; ++i)
        insertFromNotNullable(src, position);
}

void ColumnNullable::popBack(size_t n)
{
    nested_column->popBack(n);
    if (hasNullMap())
        getNullMapColumn().popBack(n);
}

ColumnCheckpointPtr ColumnNullable::getCheckpoint() const
{
    return std::make_shared<ColumnCheckpointWithNested>(size(), nested_column->getCheckpoint());
}

void ColumnNullable::updateCheckpoint(ColumnCheckpoint & checkpoint) const
{
    checkpoint.size = size();
    nested_column->updateCheckpoint(*assert_cast<ColumnCheckpointWithNested &>(checkpoint).nested);
}

void ColumnNullable::rollback(const ColumnCheckpoint & checkpoint)
{
    if (hasNullMap())
        getNullMapData().resize_assume_reserved(checkpoint.size);
    nested_column->rollback(*assert_cast<const ColumnCheckpointWithNested &>(checkpoint).nested);
}

ColumnPtr ColumnNullable::filter(const Filter & filt, ssize_t result_size_hint) const
{
    ColumnPtr filtered_data = nested_column->filter(filt, result_size_hint);
    const auto * map = tryGetNullMapColumn();
    if (!map)
        return ColumnNullable::create(filtered_data);
    ColumnPtr filtered_null_map = map->filter(filt, result_size_hint);
    return ColumnNullable::create(filtered_data, filtered_null_map);
}

void ColumnNullable::filter(const Filter & filt)
{
    nested_column->filter(filt);
    if (hasNullMap())
        getNullMapColumn().filter(filt);
}

void ColumnNullable::expand(const IColumn::Filter & mask, bool inverted)
{
    materializeNullMap();
    nested_column->expand(mask, inverted);
    /// Use 1 as default value so column will contain NULLs on rows where filter has 0.
    expandDataByMask<UInt8>(getNullMapData(), mask, inverted, 1);
}

ColumnPtr ColumnNullable::permute(const Permutation & perm, size_t limit) const
{
    ColumnPtr permuted_data = nested_column->permute(perm, limit);
    const auto * map = tryGetNullMapColumn();
    if (!map)
        return ColumnNullable::create(permuted_data);
    ColumnPtr permuted_null_map = map->permute(perm, limit);
    return ColumnNullable::create(permuted_data, permuted_null_map);
}

ColumnPtr ColumnNullable::index(const IColumn & indexes, size_t limit) const
{
    ColumnPtr indexed_data = nested_column->index(indexes, limit);
    const auto * map = tryGetNullMapColumn();
    if (!map)
        return ColumnNullable::create(indexed_data);
    ColumnPtr indexed_null_map = map->index(indexes, limit);
    return ColumnNullable::create(indexed_data, indexed_null_map);
}

#if USE_EMBEDDED_COMPILER

bool ColumnNullable::isComparatorCompilable() const
{
    return nested_column->isComparatorCompilable();
}

llvm::Value * ColumnNullable::compileComparator(llvm::IRBuilderBase & builder, llvm::Value * lhs, llvm::Value * rhs,
                                            llvm::Value * nan_direction_hint) const
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);
    auto * head = b.GetInsertBlock();

    llvm::Value * lhs_unwrapped_value = b.CreateExtractValue(lhs, {0});
    llvm::Value * lhs_is_null_value = b.CreateExtractValue(lhs, {1});

    llvm::Value * rhs_unwrapped_value = b.CreateExtractValue(rhs, {0});
    llvm::Value * rhs_is_null_value = b.CreateExtractValue(rhs, {1});

    llvm::Value * lhs_or_rhs_are_null = b.CreateOr(lhs_is_null_value, rhs_is_null_value);

    auto * lhs_or_rhs_are_null_block = llvm::BasicBlock::Create(head->getContext(), "lhs_or_rhs_are_null_block", head->getParent());
    auto * lhs_rhs_are_not_null_block = llvm::BasicBlock::Create(head->getContext(), "lhs_and_rhs_are_not_null_block", head->getParent());
    auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());

    b.CreateCondBr(lhs_or_rhs_are_null, lhs_or_rhs_are_null_block, lhs_rhs_are_not_null_block);

    b.SetInsertPoint(lhs_or_rhs_are_null_block);
    auto * lhs_equals_rhs_result = llvm::ConstantInt::getSigned(b.getInt8Ty(), 0);
    llvm::Value * lhs_and_rhs_are_null = b.CreateAnd(lhs_is_null_value, rhs_is_null_value);
    llvm::Value * lhs_is_null_result = b.CreateSelect(lhs_is_null_value, nan_direction_hint, b.CreateNeg(nan_direction_hint));
    llvm::Value * lhs_or_rhs_are_null_block_result = b.CreateSelect(lhs_and_rhs_are_null, lhs_equals_rhs_result, lhs_is_null_result);
    b.CreateBr(join_block);

    b.SetInsertPoint(lhs_rhs_are_not_null_block);
    llvm::Value * lhs_rhs_are_not_null_block_result
        = nested_column->compileComparator(builder, lhs_unwrapped_value, rhs_unwrapped_value, nan_direction_hint);
    b.CreateBr(join_block);

    b.SetInsertPoint(join_block);

    auto * result = b.CreatePHI(b.getInt8Ty(), 2);
    result->addIncoming(lhs_or_rhs_are_null_block_result, lhs_or_rhs_are_null_block);
    result->addIncoming(lhs_rhs_are_not_null_block_result, lhs_rhs_are_not_null_block);

    return result;
}

#endif

int ColumnNullable::compareAtImpl(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint, const Collator * collator) const
{
    /// NULL values share the properties of NaN values.
    /// Here the last parameter of compareAt is called null_direction_hint
    /// instead of the usual nan_direction_hint and is used to implement
    /// the ordering specified by either NULLS FIRST or NULLS LAST in the
    /// ORDER BY construction.

    const ColumnNullable & nullable_rhs = assert_cast<const ColumnNullable &>(rhs_);

    bool lval_is_null = isNullAt(n);
    bool rval_is_null = nullable_rhs.isNullAt(m);

    if (unlikely(lval_is_null || rval_is_null))
    {
        if (lval_is_null && rval_is_null)
            return 0;
        return lval_is_null ? null_direction_hint : -null_direction_hint;
    }

    const IColumn & nested_rhs = nullable_rhs.getNestedColumn();
    if (collator)
        return nested_column->compareAtWithCollation(n, m, nested_rhs, null_direction_hint, *collator);

    return nested_column->compareAt(n, m, nested_rhs, null_direction_hint);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
int ColumnNullable::compareAt(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint) const
#else
int ColumnNullable::doCompareAt(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint) const
#endif
{
    return compareAtImpl(n, m, rhs_, null_direction_hint);
}

int ColumnNullable::compareAtWithCollation(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint, const Collator & collator) const
{
    return compareAtImpl(n, m, rhs_, null_direction_hint, &collator);
}

size_t ColumnNullable::getEqualRangeEndAssumeSorted(size_t begin, size_t end, int nan_direction_hint) const
{
    if (begin >= end)
        return begin;

    const auto * map = tryGetNullMapData();
    if (!map)
        return nested_column->getEqualRangeEndAssumeSorted(begin, end, nan_direction_hint);

    /// In a sorted Nullable column the NULLs are grouped at one end, so null-ness forms a run within this
    /// range. Rows must be compared by null-ness, not by the raw null map byte: any non-zero byte means
    /// NULL, so the bytes within the NULL group may be arbitrary non-zero values in arbitrary order.
    const UInt8 * null_bytes = map->data();
    const bool ref_is_null = null_bytes[begin] != 0;

    /// A null-ness comparison is cheap, so use a longer linear probe (the default is 8).
    static constexpr size_t linear_probe = 16;
    size_t run_end = findEqualRangeEndAssumeSorted(
        begin, end, linear_probe, [&](size_t i) { return (null_bytes[i] != 0) == ref_is_null; });

    /// When the start row is non-NULL the run is additionally bounded by where the nested value changes.
    /// That nested scan stays within `[begin, run_end)`, which is entirely non-NULL, so it never reads NULL slots.
    if (!ref_is_null)
        run_end = nested_column->getEqualRangeEndAssumeSorted(begin, run_end, nan_direction_hint);

    return run_end;
}

void ColumnNullable::getPermutationImpl(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                    size_t limit, int null_direction_hint, Permutation & res, const Collator * collator) const
{
    /// Without NULLs the order is defined by the nested column alone.
    if (!hasNullMap())
    {
        if (collator)
            nested_column->getPermutationWithCollation(*collator, direction, stability, limit, null_direction_hint, res);
        else
            nested_column->getPermutation(direction, stability, limit, null_direction_hint, res);
        return;
    }

    /// Cannot pass limit because of unknown amount of NULLs.

    if (collator)
        nested_column->getPermutationWithCollation(*collator, direction, stability, 0, null_direction_hint, res);
    else
        nested_column->getPermutation(direction, stability, 0, null_direction_hint, res);

    bool reverse = direction == IColumn::PermutationSortDirection::Descending;
    const auto is_nulls_last = ((null_direction_hint > 0) != reverse);

    size_t res_size = res.size();

    if (!limit)
        limit = res_size;
    else
        limit = std::min(res_size, limit);

    /// For stable sort we must process all NULL values
    if (unlikely(stability == IColumn::PermutationSortStability::Stable))
        limit = res_size;

    if (is_nulls_last)
    {
        /// Shift all NULL values to the end.

        size_t read_idx = 0;
        size_t write_idx = 0;
        size_t end_idx = res_size;

        while (read_idx < limit && !isNullAt(res[read_idx]))
        {
            ++read_idx;
            ++write_idx;
        }

        ++read_idx;

        /// Invariants:
        ///  write_idx < read_idx
        ///  write_idx points to NULL
        ///  read_idx will be incremented to position of next not-NULL
        ///  there are range of NULLs between write_idx and read_idx - 1,
        /// We are moving elements from end to begin of this range,
        ///  so range will "bubble" towards the end.
        /// Relative order of NULL elements could be changed,
        ///  but relative order of non-NULLs is preserved.

        while (read_idx < end_idx && write_idx < limit)
        {
            if (!isNullAt(res[read_idx]))
            {
                std::swap(res[read_idx], res[write_idx]);
                ++write_idx;
            }
            ++read_idx;
        }

        if (unlikely(stability == IColumn::PermutationSortStability::Stable) && write_idx != res_size)
        {
            ::sort(res.begin() + write_idx, res.begin() + res_size);
        }
    }
    else
    {
        /// Shift all NULL values to the beginning.

        ssize_t read_idx = res.size() - 1;
        ssize_t write_idx = res.size() - 1;

        while (read_idx >= 0 && !isNullAt(res[read_idx]))
        {
            --read_idx;
            --write_idx;
        }

        --read_idx;

        while (read_idx >= 0 && write_idx >= 0)
        {
            if (!isNullAt(res[read_idx]))
            {
                std::swap(res[read_idx], res[write_idx]);
                --write_idx;
            }
            --read_idx;
        }

        if (unlikely(stability == IColumn::PermutationSortStability::Stable) && write_idx != 0)
        {
            ::sort(res.begin(), res.begin() + write_idx + 1);
        }
    }
}

void ColumnNullable::updatePermutationImpl(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                        size_t limit, int null_direction_hint, Permutation & res, EqualRanges & equal_ranges, const Collator * collator) const
{
    if (equal_ranges.empty())
        return;

    /// Without NULLs the order is defined by the nested column alone.
    if (!hasNullMap())
    {
        if (collator)
            nested_column->updatePermutationWithCollation(*collator, direction, stability, limit, null_direction_hint, res, equal_ranges);
        else
            nested_column->updatePermutation(direction, stability, limit, null_direction_hint, res, equal_ranges);
        return;
    }

    /// We will sort nested columns into `new_ranges` and call updatePermutation in next columns with `null_ranges`.
    EqualRanges new_ranges;
    EqualRanges null_ranges;

    bool reverse = direction == IColumn::PermutationSortDirection::Descending;
    const auto is_nulls_last = ((null_direction_hint > 0) != reverse);

    if (is_nulls_last)
    {
        /// Shift all NULL values to the end.
        for (const auto & [first, last] : equal_ranges)
        {
            /// Current interval is righter than limit.
            if (limit && first > limit)
                break;

            /// Consider a half interval [first, last)
            size_t read_idx = first;
            size_t write_idx = first;
            size_t end_idx = last;

            /// We can't check the limit here because the interval is not sorted by nested column.
            while (read_idx < end_idx && !isNullAt(res[read_idx]))
            {
                ++read_idx;
                ++write_idx;
            }

            ++read_idx;

            /// Invariants:
            ///  write_idx < read_idx
            ///  elements at [write_idx, min(read_idx, end_idx)) are all NULL
            /// We are moving elements from end to begin of this range,
            ///  so range will "bubble" towards the end.
            /// Relative order of NULL elements could be changed,
            ///  but relative order of non-NULLs is preserved.

            while (read_idx < end_idx && write_idx < end_idx)
            {
                if (!isNullAt(res[read_idx]))
                {
                    std::swap(res[read_idx], res[write_idx]);
                    ++write_idx;
                }
                ++read_idx;
            }

            /// We have a range [first, write_idx) of non-NULL values
            if (first + 1 < write_idx)
                new_ranges.emplace_back(first, write_idx);

            /// We have a range [write_idx, last) of NULL values
            if (write_idx + 1 < last)
                null_ranges.emplace_back(write_idx, last);
        }
    }
    else
    {
        /// Shift all NULL values to the beginning.
        /// This code is an exact mirror image of the is_nulls_last case above.
        for (const auto & [first, last] : equal_ranges)
        {
            /// Current interval is righter than limit.
            if (limit && first > limit)
                break;

            ssize_t read_idx = last - 1;
            ssize_t write_idx = last - 1;
            ssize_t begin_idx = static_cast<ssize_t>(first) - 1;

            while (read_idx > begin_idx && !isNullAt(res[read_idx]))
            {
                --read_idx;
                --write_idx;
            }

            --read_idx;

            while (read_idx > begin_idx && write_idx > begin_idx)
            {
                if (!isNullAt(res[read_idx]))
                {
                    std::swap(res[read_idx], res[write_idx]);
                    --write_idx;
                }
                --read_idx;
            }

            /// We have a range [write_idx+1, last) of non-NULL values.
            /// Only emit ranges with >= 2 elements (single-element ranges are already sorted).
            if (write_idx + 1 + 1 < static_cast<ssize_t>(last))
                new_ranges.emplace_back(write_idx + 1, last);

            /// We have a range [first, write_idx+1) of NULL values.
            if (static_cast<ssize_t>(first) + 1 < write_idx + 1)
                null_ranges.emplace_back(first, write_idx + 1);
        }
    }

    if (collator)
        nested_column->updatePermutationWithCollation(*collator, direction, stability, limit, null_direction_hint, res, new_ranges);
    else
        nested_column->updatePermutation(direction, stability, limit, null_direction_hint, res, new_ranges);

    if (unlikely(stability == PermutationSortStability::Stable))
    {
        for (auto & null_range : null_ranges)
            ::sort(std::ranges::next(res.begin(), null_range.from), std::ranges::next(res.begin(), null_range.to));
    }

    /// `equal_ranges` must stay sorted by `from` (downstream limit handling relies on it). Both
    /// `new_ranges` and `null_ranges` are individually sorted, but concatenating them is not, so
    /// merge the two sorted lists instead.
    EqualRanges merged;
    merged.reserve(new_ranges.size() + null_ranges.size());
    std::merge(
        std::make_move_iterator(new_ranges.begin()), std::make_move_iterator(new_ranges.end()),
        std::make_move_iterator(null_ranges.begin()), std::make_move_iterator(null_ranges.end()),
        std::back_inserter(merged),
        [](const EqualRange & lhs, const EqualRange & rhs) { return lhs.from < rhs.from; });
    equal_ranges = std::move(merged);
}

void ColumnNullable::getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                size_t limit, int null_direction_hint, Permutation & res) const
{
    getPermutationImpl(direction, stability, limit, null_direction_hint, res);
}

void ColumnNullable::updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                    size_t limit, int null_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const
{
    updatePermutationImpl(direction, stability, limit, null_direction_hint, res, equal_ranges);
}

void ColumnNullable::getPermutationWithCollation(const Collator & collator, IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                            size_t limit, int null_direction_hint, Permutation & res) const
{
    getPermutationImpl(direction, stability, limit, null_direction_hint, res, &collator);
}

void ColumnNullable::updatePermutationWithCollation(const Collator & collator, IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                            size_t limit, int null_direction_hint, Permutation & res, EqualRanges & equal_ranges) const
{
    updatePermutationImpl(direction, stability, limit, null_direction_hint, res, equal_ranges, &collator);
}


size_t ColumnNullable::estimateCardinalityInPermutedRange(const Permutation & permutation, const EqualRange & equal_range) const
{
    const size_t range_size = equal_range.size();
    if (range_size <= 1)
        return range_size;

    /// Some nested column types (e.g. Tuple, Object, Array of non-fixed elements) do not implement getDataAt.
    /// For them, return the upper-bound estimate from the base implementation - the same estimate
    /// such columns produce when they are not wrapped in Nullable.
    if (!nested_column->supportsGetDataAt())
        return IColumn::estimateCardinalityInPermutedRange(permutation, equal_range);

    /// TODO use sampling if the range is too large (e.g. 16k elements, but configurable)
    StringHashSet elements;
    bool has_null = false;
    bool inserted = false;
    for (size_t i = equal_range.from; i < equal_range.to; ++i)
    {
        size_t permuted_i = permutation[i];
        if (isNullAt(permuted_i))
        {
            has_null = true;
        }
        else
        {
            auto value = getDataAt(permuted_i);
            elements.emplace(value, inserted);
        }
    }
    return elements.size() + (has_null ? 1 : 0);
}

void ColumnNullable::reserve(size_t n)
{
    nested_column->reserve(n);
    if (hasNullMap())
        getNullMapData().reserve(n);
}

size_t ColumnNullable::capacity() const
{
    if (const auto * map = tryGetNullMapData())
        return map->capacity();
    return nested_column->capacity();
}

void ColumnNullable::prepareForSquashing(const VectorWithMemoryTracking<ColumnPtr> & source_columns, size_t factor)
{
    size_t new_size = size();
    VectorWithMemoryTracking<ColumnPtr> nested_source_columns;
    nested_source_columns.reserve(source_columns.size());
    for (const auto & source_column : source_columns)
    {
        const auto & source_nullable_column = assert_cast<const ColumnNullable &>(*source_column);
        new_size += source_nullable_column.size();
        nested_source_columns.push_back(source_nullable_column.getNestedColumnPtr());
    }

    nested_column->prepareForSquashing(nested_source_columns, factor);
    if (hasNullMap())
        getNullMapData().reserve(new_size * factor);
}

void ColumnNullable::shrinkToFit()
{
    nested_column->shrinkToFit();
    if (hasNullMap())
        getNullMapColumn().shrinkToFit();
}

void ColumnNullable::ensureOwnership()
{
    nested_column->ensureOwnership();
}

size_t ColumnNullable::byteSize() const
{
    const auto * map = tryGetNullMapColumn();
    return nested_column->byteSize() + (map ? map->byteSize() : 0);
}

size_t ColumnNullable::byteSizeAt(size_t n) const
{
    return sizeof(UInt8) + nested_column->byteSizeAt(n);
}

size_t ColumnNullable::allocatedBytes() const
{
    const auto * map = tryGetNullMapColumn();
    return nested_column->allocatedBytes() + (map ? map->allocatedBytes() : 0);
}

void ColumnNullable::protect()
{
    nested_column->protect();
    if (hasNullMap())
        getNullMapColumn().protect();
}

ColumnPtr ColumnNullable::compress(bool force_compression) const
{
    ColumnPtr nested_compressed = nested_column->compress(force_compression);
    const auto * map = tryGetNullMapColumn();
    ColumnPtr null_map_compressed = map ? map->compress(force_compression) : nullptr;

    size_t byte_size = nested_column->byteSize() + (map ? map->byteSize() : 0);
    /// Without a null map, the column can be decompressed only as a whole.
    Columns parts;
    if (null_map_compressed)
        parts = {nested_compressed, null_map_compressed};

    return ColumnCompressed::create(size(), byte_size,
        [my_nested_column = std::move(nested_compressed), my_null_map = std::move(null_map_compressed)]
        {
            return ColumnNullable::create(my_nested_column->decompress(), my_null_map ? my_null_map->decompress() : nullptr);
        },
        std::move(parts));
}


namespace
{

/// The following function implements a slightly more general version
/// of getExtremes() than the implementation from Not-Null IColumns.
/// It takes into account the possible presence of nullable values.
void getExtremesWithNulls(const IColumn & nested_column, const NullMap & null_array, Field & min, Field & max, size_t start, size_t end, bool null_last = false)
{
    size_t number_of_nulls = 0;
    size_t n = end - start;
    NullMap not_null_array(n);
    for (size_t i = 0; i < n; ++i)
    {
        if (null_array[start + i])
        {
            ++number_of_nulls;
            not_null_array[i] = 0;
        }
        else
        {
            not_null_array[i] = 1;
        }
    }
    if (number_of_nulls == 0)
    {
        nested_column.getExtremes(min, max, start, end);
    }
    else if (number_of_nulls == n)
    {
        min = POSITIVE_INFINITY;
        max = POSITIVE_INFINITY;
    }
    else
    {
        auto sub_column = nested_column.cut(start, n);
        auto filtered_column = sub_column->filter(not_null_array, -1);
        filtered_column->getExtremes(min, max, 0, filtered_column->size());
        if (null_last)
            max = POSITIVE_INFINITY;
    }
}
}


void ColumnNullable::getExtremes(Field & min, Field & max, size_t start, size_t end) const
{
    if (const auto * map = tryGetNullMapData())
        getExtremesWithNulls(*nested_column, *map, min, max, start, end);
    else
        nested_column->getExtremes(min, max, start, end);
}


void ColumnNullable::getExtremesNullLast(Field & min, Field & max, size_t start, size_t end) const
{
    if (const auto * map = tryGetNullMapData())
        getExtremesWithNulls(*nested_column, *map, min, max, start, end, true);
    else
        nested_column->getExtremes(min, max, start, end);
}


ColumnPtr ColumnNullable::replicate(const Offsets & offsets) const
{
    ColumnPtr replicated_data = nested_column->replicate(offsets);
    const auto * map = tryGetNullMapColumn();
    if (!map)
        return ColumnNullable::create(replicated_data);
    ColumnPtr replicated_null_map = map->replicate(offsets);
    return ColumnNullable::create(replicated_data, replicated_null_map);
}


template <bool negative>
void ColumnNullable::applyNullMapImpl(const NullMap & map, size_t offset)
{
    /// Applying a map without NULLs does not add NULLs.
    if (!negative && !hasNullMap() && offset + map.size() == size() && memoryIsZero(map.data(), 0, map.size()))
        return;

    NullMap & arr = getNullMapData();

    if (offset + map.size() != arr.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Null map of size {} at offset {} does not match ColumnNullable of size {}",
            map.size(), offset, arr.size());

    /// Any non-zero byte means NULL, so reduce the map before negating it: `negative ^ 2` would
    /// yield 3 and mark a row the map leaves present.
    for (size_t i = 0, size = map.size(); i < size; ++i)
        arr[offset + i] |= negative ^ !!map[i];
}

void ColumnNullable::applyNullMap(const NullMap & map)
{
    applyNullMapImpl<false>(map);
}

void ColumnNullable::applyNullMap(const ColumnUInt8 & map)
{
    applyNullMapImpl<false>(map.getData());
}

void ColumnNullable::applyNegatedNullMap(const NullMap & map, size_t offset)
{
    applyNullMapImpl<true>(map, offset);
}

void ColumnNullable::applyNegatedNullMap(const ColumnUInt8 & map)
{
    applyNullMapImpl<true>(map.getData());
}


void ColumnNullable::applyNullMap(const ColumnNullable & other)
{
    const auto * other_map = other.tryGetNullMapColumn();
    if (!other_map)
    {
        if (other.size() != size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Null map of size {} does not match ColumnNullable of size {}", other.size(), size());
        return;
    }
    applyNullMap(*other_map);
}

void ColumnNullable::checkConsistency() const
{
    const auto * map = tryGetNullMapColumn();
    if (map && map->size() != nested_column->size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Sizes of nested column and null map of Nullable column are not equal (null map size = {}, nested column size = {})",
            map->size(),
            nested_column->size());
}

ColumnPtr ColumnNullable::createWithOffsets(const IColumn::Offsets & offsets, const ColumnConst & column_with_default_value, size_t total_rows, size_t shift) const
{
    if (empty())
        return cloneEmpty();

    ColumnPtr new_values;
    ColumnPtr new_null_map;

    const ColumnNullable & nullable_column_with_default_value = assert_cast<const ColumnNullable &>(column_with_default_value.getDataColumn());
    if (nullable_column_with_default_value.isNullAt(0))
    {
        const auto & map = getNullMapColumnPtr();
        /// Value in main column, when null map is 1 is implementation defined. So, take any value.
        new_values = nested_column->createWithOffsets(offsets, *createColumnConstWithDefaultValue(nested_column), total_rows, shift);
        new_null_map = map->createWithOffsets(offsets, *createColumnConst(map, Field(1u)), total_rows, shift);
    }
    else
    {
        new_values = nested_column->createWithOffsets(offsets, *ColumnConst::create(nullable_column_with_default_value.getNestedColumnPtr(), 1), total_rows, shift);
        if (hasNullMap())
            new_null_map = null_map->createWithOffsets(offsets, *createColumnConst(null_map, Field(0u)), total_rows, shift);
    }

    return ColumnNullable::create(new_values, new_null_map);
}

void ColumnNullable::updateAt(const IColumn & src, size_t dst_pos, size_t src_pos)
{
    const auto & src_nullable = assert_cast<const ColumnNullable &>(src);
    const UInt8 is_null = src_nullable.isNullAt(src_pos) ? src_nullable.getNullMapData()[src_pos] : 0;
    if (is_null)
        materializeNullMap();
    nested_column->updateAt(src_nullable.getNestedColumn(), dst_pos, src_pos);
    if (hasNullMap())
        getNullMapData()[dst_pos] = is_null;
}

ColumnPtr ColumnNullable::getNestedColumnWithDefaultOnNull() const
{
    const auto * map = tryGetNullMapData();
    if (!map)
        return nested_column;

    auto res = nested_column->cloneEmpty();
    const auto & null_map_data = *map;
    size_t start = 0;
    size_t end = null_map_data.size();
    while (start < nested_column->size())
    {
        size_t next_null_index = start;
        while (next_null_index < end && !null_map_data[next_null_index])
            ++next_null_index;

        if (next_null_index != start)
            res->insertRangeFrom(*nested_column, start, next_null_index - start);

        size_t next_none_null_index = next_null_index;
        while (next_none_null_index < end && null_map_data[next_none_null_index])
            ++next_none_null_index;

        if (next_null_index != next_none_null_index)
            res->insertManyDefaults(next_none_null_index - next_null_index);

        start = next_none_null_index;
    }
    return res;
}

void ColumnNullable::chooseDynamicStructureForMerge(const VectorWithMemoryTracking<ColumnPtr> & source_columns, std::optional<size_t> max_dynamic_subcolumns)
{
    VectorWithMemoryTracking<ColumnPtr> nested_source_columns;
    nested_source_columns.reserve(source_columns.size());
    for (const auto & source_column : source_columns)
        nested_source_columns.push_back(assert_cast<const ColumnNullable &>(*source_column).getNestedColumnPtr());
    nested_column->chooseDynamicStructureForMerge(nested_source_columns, max_dynamic_subcolumns);
}

void ColumnNullable::takeExactDynamicStructureFrom(const IColumn & source)
{
    nested_column->takeExactDynamicStructureFrom(assert_cast<const ColumnNullable &>(source).getNestedColumn());
}

bool ColumnNullable::dynamicStructureEquals(const IColumn & rhs) const
{
    const auto & rhs_nested_column = assert_cast<const ColumnNullable &>(rhs).getNestedColumn();
    return nested_column->dynamicStructureEquals(rhs_nested_column);
}

void ColumnNullable::takeOrCalculateStatisticsFrom(const VectorWithMemoryTracking<ColumnPtr> & source_columns)
{
    VectorWithMemoryTracking<ColumnPtr> nested_source_columns;
    nested_source_columns.reserve(source_columns.size());
    for (const auto & source_column : source_columns)
        nested_source_columns.push_back(assert_cast<const ColumnNullable &>(*source_column).getNestedColumnPtr());
    nested_column->takeOrCalculateStatisticsFrom(nested_source_columns);
}

void ColumnNullable::fillFromRowStorePtrs(const DataTypePtr & type, const RowStorePointers & row_store_ptrs, size_t field_offset, size_t field_size, size_t begin, size_t count, PaddedPODArray<UInt8> *)
{
    materializeNullMap();
    nested_column->fillFromRowStorePtrs(removeNullable(type), row_store_ptrs, field_offset, field_size, begin, count, &getNullMapData());
}

/// A NULL row emits only the flag byte and never touches the nested column, so an
/// unsupported nested type (e.g. LowCardinality, Array) would slip through silently.
/// Probe row 0 once so every row rejects the same types a non-NULL row would, even
/// in an all-NULL block. Not on the hot path.
void ColumnNullable::validateNestedComparable() const
{
    String probe;
    nested_column->serializeAsComparable(0, probe);
}

/// Leading flag byte of a Nullable row's comparable encoding. `\x00` (non-NULL)
/// sorts before `\x01` (NULL), so NULLs compare greater, matching compareAt with
/// null_direction_hint = 1. A non-NULL row is `\x00` followed by the nested value's
/// encoding; a NULL row is just `\x01`.
void ColumnNullable::serializeAsComparable(size_t n, String & out) const
{
    if (isNullAt(n))
    {
        validateNestedComparable();
        out.push_back('\x01');
        return;
    }
    out.push_back('\x00');
    nested_column->serializeAsComparable(n, out);
}

void ColumnNullable::batchSerializeAsComparable(
    size_t num_rows,
    VectorWithMemoryTracking<String> & out,
    const Permutation * permutation,
    const UInt8 * outer_null_map) const
{
    if (num_rows == 0)
        return;

    validateNestedComparable();

    const auto * map = tryGetNullMapData();
    if (!map)
    {
        batchSerializeAsComparableImpl(
            num_rows, out, permutation, outer_null_map,
            [&](size_t, String & dst) { dst.push_back('\x00'); });
        nested_column->batchSerializeAsComparable(num_rows, out, permutation, outer_null_map);
        return;
    }

    /// Write the null flag for every row: NULL=0x01 (sorts after), non-NULL=0x00.
    const auto & null_map_data = *map;
    batchSerializeAsComparableImpl(
        num_rows, out, permutation, outer_null_map,
        [&](size_t src, String & dst) { dst.push_back(null_map_data[src] ? '\x01' : '\x00'); });

    /// Encode the nested values with one virtual dispatch; the merged null map
    /// lets the nested loop skip NULL rows.
    const UInt8 * effective_null_map = null_map_data.data();
    PaddedPODArray<UInt8> merged_null_map;
    if (outer_null_map)
    {
        merged_null_map.resize(null_map_data.size());
        for (size_t i = 0; i < null_map_data.size(); ++i)
            merged_null_map[i] = null_map_data[i] | outer_null_map[i];
        effective_null_map = merged_null_map.data();
    }
    nested_column->batchSerializeAsComparable(num_rows, out, permutation, effective_null_map);
}

ColumnPtr makeNullable(const ColumnPtr & column)
{
    if (isColumnNullable(*column))
        return column;

    if (isColumnConst(*column))
        return ColumnConst::create(makeNullable(assert_cast<const ColumnConst &>(*column).getDataColumnPtr()), column->size());

    return ColumnNullable::create(column);
}

ColumnPtr makeNullableOrLowCardinalityNullable(const ColumnPtr & column)
{
    if (isColumnNullableOrLowCardinalityNullable(*column))
        return column;

    if (isColumnConst(*column))
        return ColumnConst::create(makeNullable(assert_cast<const ColumnConst &>(*column).getDataColumnPtr()), column->size());

    if (column->lowCardinality())
        return assert_cast<const ColumnLowCardinality &>(*column).cloneNullable();

    return ColumnNullable::create(column);
}

ColumnConstPtr makeNullableSafe(const ColumnConstPtr & column)
{
    if (isColumnNullable(column->getDataColumn()))
        return column;

    return ColumnConst::create(makeNullableSafe(column->getDataColumnPtr()), column->size());
}

ColumnPtr makeNullableSafe(const ColumnPtr & column)
{
    if (isColumnNullable(*column))
        return column;

    if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
        return makeNullableSafe(column_const->getPtr());

    if (column->canBeInsideNullable())
        return makeNullable(column);

    return column;
}

ColumnPtr makeNullableOrLowCardinalityNullableSafe(const ColumnPtr & column)
{
    if (isColumnNullableOrLowCardinalityNullable(*column))
        return column;

    if (isColumnConst(*column))
        return ColumnConst::create(makeNullableOrLowCardinalityNullableSafe(assert_cast<const ColumnConst &>(*column).getDataColumnPtr()), column->size());

    if (column->lowCardinality())
        return assert_cast<const ColumnLowCardinality &>(*column).cloneNullable();

    if (column->canBeInsideNullable())
        return makeNullable(column);

    return column;
}

void materializeNullMapsRecursively(const IColumn & column)
{
    auto materialize = [](const IColumn & subcolumn)
    {
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(&subcolumn))
            nullable->materializeNullMap();
    };

    materialize(column);
    column.forEachSubcolumnRecursively(materialize);
}

ColumnPtr removeNullable(const ColumnPtr & column)
{
    if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(column.get()))
        return column_nullable->getNestedColumnPtr();
    return column;
}

ColumnPtr removeNullableOrLowCardinalityNullable(const ColumnPtr & column)
{
    if (const auto * column_low_cardinality = typeid_cast<const ColumnLowCardinality *>(column.get()))
    {
        if (!column_low_cardinality->nestedIsNullable())
            return column;
        return column_low_cardinality->cloneWithDefaultOnNull();
    }

    return removeNullable(column);
}

bool ColumnNullable::hasOnlyTypeDefaults() const
{
    const auto * data = tryGetNullMapData();
    if (!data)
        return empty();
    if (data->empty())
        return true;
    return memoryIsByte(data->data(), 0, data->size(), 1);
}

ColumnPlanes ColumnNullable::getPlanes() const
{
    /// The readers of the planes only read the null map, so a column without a null map exposes the shared zeros.
    ColumnPlanes planes(ColumnPlanes::Shape::Nullable, getNullMapRawDataOrZeros());
    planes.children = {&getNestedColumn()};
    return planes;
}
}
