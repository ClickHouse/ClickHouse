#include <Common/Arena.h>
#include <Common/SipHash.h>
#include <Common/NaNUtils.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/WeakHash.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnCompressed.h>
#include <Processors/Transforms/ColumnGathererTransform.h>

#if USE_EMBEDDED_COMPILER
#include <DataTypes/Native.h>
#include <llvm/IR/IRBuilder.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int SIZES_OF_NESTED_COLUMNS_ARE_INCONSISTENT;
}


ColumnNullable::ColumnNullable(MutableColumnPtr && nested_column_, MutableColumnPtr && null_map_)
    : nested_column(std::move(nested_column_)), null_map(std::move(null_map_))
{
    /// ColumnNullable cannot have constant nested column. But constant argument could be passed. Materialize it.
    nested_column = getNestedColumn().convertToFullColumnIfConst();

    if (!getNestedColumn().canBeInsideNullable())
        throw Exception{getNestedColumn().getName() + " cannot be inside Nullable column", ErrorCodes::ILLEGAL_COLUMN};

    if (isColumnConst(*null_map))
        throw Exception{"ColumnNullable cannot have constant null map", ErrorCodes::ILLEGAL_COLUMN};
}

void ColumnNullable::updateHashWithValue(size_t n, SipHash & hash) const
{
    const auto & arr = getNullMapData();
    hash.update(arr[n]);
    if (arr[n] == 0)
        getNestedColumn().updateHashWithValue(n, hash);
}

void ColumnNullable::updateWeakHash32(WeakHash32 & hash) const
{
    auto s = size();

    if (hash.getData().size() != s)
        throw Exception("Size of WeakHash32 does not match size of column: column size is " + std::to_string(s) +
                        ", hash size is " + std::to_string(hash.getData().size()), ErrorCodes::LOGICAL_ERROR);

    WeakHash32 old_hash = hash;
    nested_column->updateWeakHash32(hash);

    const auto & null_map_data = getNullMapData();
    auto & hash_data = hash.getData();
    auto & old_hash_data = old_hash.getData();

    /// Use old data for nulls.
    for (size_t row = 0; row < s; ++row)
        if (null_map_data[row])
            hash_data[row] = old_hash_data[row];
}

void ColumnNullable::updateHashFast(SipHash & hash) const
{
    null_map->updateHashFast(hash);
    nested_column->updateHashFast(hash);
}

MutableColumnPtr ColumnNullable::cloneResized(size_t new_size) const
{
    MutableColumnPtr new_nested_col = getNestedColumn().cloneResized(new_size);
    auto new_null_map = ColumnUInt8::create();

    if (new_size > 0)
    {
        new_null_map->getData().resize(new_size);

        size_t count = std::min(size(), new_size);
        memcpy(new_null_map->getData().data(), getNullMapData().data(), count * sizeof(getNullMapData()[0]));

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

void ColumnNullable::insertData(const char * pos, size_t length)
{
    if (pos == nullptr)
    {
        getNestedColumn().insertDefault();
        getNullMapData().push_back(1);
    }
    else
    {
        getNestedColumn().insertData(pos, length);
        getNullMapData().push_back(0);
    }
}

StringRef ColumnNullable::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    const auto & arr = getNullMapData();
    static constexpr auto s = sizeof(arr[0]);

    auto * pos = arena.allocContinue(s, begin);
    memcpy(pos, &arr[n], s);

    if (arr[n])
        return StringRef(pos, s);

    auto nested_ref = getNestedColumn().serializeValueIntoArena(n, arena, begin);

    /// serializeValueIntoArena may reallocate memory. Have to use ptr from nested_ref.data and move it back.
    return StringRef(nested_ref.data - s, nested_ref.size + s);
}

const char * ColumnNullable::deserializeAndInsertFromArena(const char * pos)
{
    UInt8 val = unalignedLoad<UInt8>(pos);
    pos += sizeof(val);

    getNullMapData().push_back(val);

    if (val == 0)
        pos = getNestedColumn().deserializeAndInsertFromArena(pos);
    else
        getNestedColumn().insertDefault();

    return pos;
}

const char * ColumnNullable::skipSerializedInArena(const char * pos) const
{
    UInt8 val = unalignedLoad<UInt8>(pos);
    pos += sizeof(val);

    if (val == 0)
        return getNestedColumn().skipSerializedInArena(pos);

    return pos;
}

void ColumnNullable::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const ColumnNullable & nullable_col = assert_cast<const ColumnNullable &>(src);
    getNullMapColumn().insertRangeFrom(*nullable_col.null_map, start, length);
    getNestedColumn().insertRangeFrom(*nullable_col.nested_column, start, length);
}

void ColumnNullable::insert(const Field & x)
{
    if (x.isNull())
    {
        getNestedColumn().insertDefault();
        getNullMapData().push_back(1);
    }
    else
    {
        getNestedColumn().insert(x);
        getNullMapData().push_back(0);
    }
}

void ColumnNullable::insertFrom(const IColumn & src, size_t n)
{
    const ColumnNullable & src_concrete = assert_cast<const ColumnNullable &>(src);
    getNestedColumn().insertFrom(src_concrete.getNestedColumn(), n);
    getNullMapData().push_back(src_concrete.getNullMapData()[n]);
}

void ColumnNullable::insertFromNotNullable(const IColumn & src, size_t n)
{
    getNestedColumn().insertFrom(src, n);
    getNullMapData().push_back(0);
}

void ColumnNullable::insertRangeFromNotNullable(const IColumn & src, size_t start, size_t length)
{
    getNestedColumn().insertRangeFrom(src, start, length);
    getNullMapData().resize_fill(getNullMapData().size() + length, 0);
}

void ColumnNullable::insertManyFromNotNullable(const IColumn & src, size_t position, size_t length)
{
    for (size_t i = 0; i < length; ++i)
        insertFromNotNullable(src, position);
}

void ColumnNullable::popBack(size_t n)
{
    getNestedColumn().popBack(n);
    getNullMapColumn().popBack(n);
}

ColumnPtr ColumnNullable::filter(const Filter & filt, ssize_t result_size_hint) const
{
    ColumnPtr filtered_data = getNestedColumn().filter(filt, result_size_hint);
    ColumnPtr filtered_null_map = getNullMapColumn().filter(filt, result_size_hint);
    return ColumnNullable::create(filtered_data, filtered_null_map);
}

void ColumnNullable::expand(const IColumn::Filter & mask, bool inverted)
{
    nested_column->expand(mask, inverted);
    null_map->expand(mask, inverted);
}

ColumnPtr ColumnNullable::permute(const Permutation & perm, size_t limit) const
{
    ColumnPtr permuted_data = getNestedColumn().permute(perm, limit);
    ColumnPtr permuted_null_map = getNullMapColumn().permute(perm, limit);
    return ColumnNullable::create(permuted_data, permuted_null_map);
}

ColumnPtr ColumnNullable::index(const IColumn & indexes, size_t limit) const
{
    ColumnPtr indexed_data = getNestedColumn().index(indexes, limit);
    ColumnPtr indexed_null_map = getNullMapColumn().index(indexes, limit);
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

    // if (unlikely(lval_is_null || rval_is_null))
    // {
    //     if (lval_is_null && rval_is_null)
    //         return 0;
    //     else
    //         return lval_is_null ? null_direction_hint : -null_direction_hint;
    // }

    b.SetInsertPoint(lhs_or_rhs_are_null_block);
    auto * lhs_equals_rhs_result = llvm::ConstantInt::getSigned(b.getInt8Ty(), 0);
    llvm::Value * lhs_and_rhs_are_null = b.CreateAnd(lhs_is_null_value, rhs_is_null_value);
    llvm::Value * lhs_is_null_result = b.CreateSelect(lhs_is_null_value, nan_direction_hint, b.CreateNeg(nan_direction_hint));
    llvm::Value * lhs_or_rhs_are_null_block_result = b.CreateSelect(lhs_and_rhs_are_null, lhs_equals_rhs_result, lhs_is_null_result);
    b.CreateBr(join_block);

    // getNestedColumn().compareAt(n, m, nested_rhs, null_direction_hint);

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
        else
            return lval_is_null ? null_direction_hint : -null_direction_hint;
    }

    const IColumn & nested_rhs = nullable_rhs.getNestedColumn();
    if (collator)
        return getNestedColumn().compareAtWithCollation(n, m, nested_rhs, null_direction_hint, *collator);

    return getNestedColumn().compareAt(n, m, nested_rhs, null_direction_hint);
}

int ColumnNullable::compareAt(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint) const
{
    return compareAtImpl(n, m, rhs_, null_direction_hint);
}

int ColumnNullable::compareAtWithCollation(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint, const Collator & collator) const
{
    return compareAtImpl(n, m, rhs_, null_direction_hint, &collator);
}

void ColumnNullable::compareColumn(const IColumn & rhs, size_t rhs_row_num,
                                   PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                                   int direction, int nan_direction_hint) const
{
    return doCompareColumn<ColumnNullable>(assert_cast<const ColumnNullable &>(rhs), rhs_row_num, row_indexes,
                                           compare_results, direction, nan_direction_hint);
}

bool ColumnNullable::hasEqualValues() const
{
    return hasEqualValuesImpl<ColumnNullable>();
}

void ColumnNullable::getPermutationImpl(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                    size_t limit, int null_direction_hint, Permutation & res, const Collator * collator) const
{
    /// Cannot pass limit because of unknown amount of NULLs.

    if (collator)
        getNestedColumn().getPermutationWithCollation(*collator, direction, stability, 0, null_direction_hint, res);
    else
        getNestedColumn().getPermutation(direction, stability, 0, null_direction_hint, res);

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

    /// We will sort nested columns into `new_ranges` and call updatePermutation in next columns with `null_ranges`.
    EqualRanges new_ranges, null_ranges;

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
            ///  write_idx points to NULL
            ///  read_idx will be incremented to position of next not-NULL
            ///  there are range of NULLs between write_idx and read_idx - 1,
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
            if (first != write_idx)
                new_ranges.emplace_back(first, write_idx);

            /// We have a range [write_idx, last) of NULL values
            if (write_idx != last)
                null_ranges.emplace_back(write_idx, last);
        }
    }
    else
    {
        /// Shift all NULL values to the beginning.
        for (const auto & [first, last] : equal_ranges)
        {
            /// Current interval is righter than limit.
            if (limit && first > limit)
                break;

            ssize_t read_idx = last - 1;
            ssize_t write_idx = last - 1;
            ssize_t begin_idx = first;

            while (read_idx >= begin_idx && !isNullAt(res[read_idx]))
            {
                --read_idx;
                --write_idx;
            }

            --read_idx;

            while (read_idx >= begin_idx && write_idx >= begin_idx)
            {
                if (!isNullAt(res[read_idx]))
                {
                    std::swap(res[read_idx], res[write_idx]);
                    --write_idx;
                }
                --read_idx;
            }

            /// We have a range [write_idx+1, last) of non-NULL values
            if (write_idx != static_cast<ssize_t>(last))
                new_ranges.emplace_back(write_idx + 1, last);

            /// We have a range [first, write_idx+1) of NULL values
            if (static_cast<ssize_t>(first) != write_idx)
                null_ranges.emplace_back(first, write_idx + 1);
        }
    }

    if (collator)
        getNestedColumn().updatePermutationWithCollation(*collator, direction, stability, limit, null_direction_hint, res, new_ranges);
    else
        getNestedColumn().updatePermutation(direction, stability, limit, null_direction_hint, res, new_ranges);

    equal_ranges = std::move(new_ranges);

    if (unlikely(stability == PermutationSortStability::Stable))
    {
        for (auto & null_range : null_ranges)
            ::sort(res.begin() + null_range.first, res.begin() + null_range.second);
    }

    std::move(null_ranges.begin(), null_ranges.end(), std::back_inserter(equal_ranges));
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

void ColumnNullable::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

void ColumnNullable::reserve(size_t n)
{
    getNestedColumn().reserve(n);
    getNullMapData().reserve(n);
}

void ColumnNullable::ensureOwnership()
{
    getNestedColumn().ensureOwnership();
}

size_t ColumnNullable::byteSize() const
{
    return getNestedColumn().byteSize() + getNullMapColumn().byteSize();
}

size_t ColumnNullable::byteSizeAt(size_t n) const
{
    return sizeof(getNullMapData()[0]) + getNestedColumn().byteSizeAt(n);
}

size_t ColumnNullable::allocatedBytes() const
{
    return getNestedColumn().allocatedBytes() + getNullMapColumn().allocatedBytes();
}

void ColumnNullable::protect()
{
    getNestedColumn().protect();
    getNullMapColumn().protect();
}

ColumnPtr ColumnNullable::compress() const
{
    ColumnPtr nested_compressed = nested_column->compress();
    ColumnPtr null_map_compressed = null_map->compress();

    size_t byte_size = nested_column->byteSize() + null_map->byteSize();

    return ColumnCompressed::create(size(), byte_size,
        [nested_column = std::move(nested_compressed), null_map = std::move(null_map_compressed)]
        {
            return ColumnNullable::create(nested_column->decompress(), null_map->decompress());
        });
}


namespace
{

/// The following function implements a slightly more general version
/// of getExtremes() than the implementation from Not-Null IColumns.
/// It takes into account the possible presence of nullable values.
void getExtremesWithNulls(const IColumn & nested_column, const NullMap & null_array, Field & min, Field & max, bool null_last = false)
{
    size_t number_of_nulls = 0;
    size_t n = null_array.size();
    NullMap not_null_array(n);
    for (auto i = 0ul; i < n; ++i)
    {
        if (null_array[i])
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
        nested_column.getExtremes(min, max);
    }
    else if (number_of_nulls == n)
    {
        min = POSITIVE_INFINITY;
        max = POSITIVE_INFINITY;
    }
    else
    {
        auto filtered_column = nested_column.filter(not_null_array, -1);
        filtered_column->getExtremes(min, max);
        if (null_last)
            max = POSITIVE_INFINITY;
    }
}
}


void ColumnNullable::getExtremes(Field & min, Field & max) const
{
    getExtremesWithNulls(getNestedColumn(), getNullMapData(), min, max);
}


void ColumnNullable::getExtremesNullLast(Field & min, Field & max) const
{
    getExtremesWithNulls(getNestedColumn(), getNullMapData(), min, max, true);
}


ColumnPtr ColumnNullable::replicate(const Offsets & offsets) const
{
    ColumnPtr replicated_data = getNestedColumn().replicate(offsets);
    ColumnPtr replicated_null_map = getNullMapColumn().replicate(offsets);
    return ColumnNullable::create(replicated_data, replicated_null_map);
}


template <bool negative>
void ColumnNullable::applyNullMapImpl(const NullMap & map)
{
    NullMap & arr = getNullMapData();

    if (arr.size() != map.size())
        throw Exception{"Inconsistent sizes of ColumnNullable objects", ErrorCodes::LOGICAL_ERROR};

    for (size_t i = 0, size = arr.size(); i < size; ++i)
        arr[i] |= negative ^ map[i];
}

void ColumnNullable::applyNullMap(const NullMap & map)
{
    applyNullMapImpl<false>(map);
}

void ColumnNullable::applyNullMap(const ColumnUInt8 & map)
{
    applyNullMapImpl<false>(map.getData());
}

void ColumnNullable::applyNegatedNullMap(const NullMap & map)
{
    applyNullMapImpl<true>(map);
}

void ColumnNullable::applyNegatedNullMap(const ColumnUInt8 & map)
{
    applyNullMapImpl<true>(map.getData());
}


void ColumnNullable::applyNullMap(const ColumnNullable & other)
{
    applyNullMap(other.getNullMapColumn());
}

void ColumnNullable::checkConsistency() const
{
    if (null_map->size() != getNestedColumn().size())
        throw Exception("Logical error: Sizes of nested column and null map of Nullable column are not equal",
            ErrorCodes::SIZES_OF_NESTED_COLUMNS_ARE_INCONSISTENT);
}

ColumnPtr ColumnNullable::createWithOffsets(const IColumn::Offsets & offsets, const Field & default_field, size_t total_rows, size_t shift) const
{
    ColumnPtr new_values;
    ColumnPtr new_null_map;

    if (default_field.getType() == Field::Types::Null)
    {
        auto default_column = nested_column->cloneEmpty();
        default_column->insertDefault();

        /// Value in main column, when null map is 1 is implementation defined. So, take any value.
        new_values = nested_column->createWithOffsets(offsets, (*default_column)[0], total_rows, shift);
        new_null_map = null_map->createWithOffsets(offsets, Field(1u), total_rows, shift);
    }
    else
    {
        new_values = nested_column->createWithOffsets(offsets, default_field, total_rows, shift);
        new_null_map = null_map->createWithOffsets(offsets, Field(0u), total_rows, shift);
    }

    return ColumnNullable::create(new_values, new_null_map);
}

ColumnPtr makeNullable(const ColumnPtr & column)
{
    if (isColumnNullable(*column))
        return column;

    if (isColumnConst(*column))
        return ColumnConst::create(makeNullable(assert_cast<const ColumnConst &>(*column).getDataColumnPtr()), column->size());

    return ColumnNullable::create(column, ColumnUInt8::create(column->size(), 0));
}

ColumnPtr makeNullableSafe(const ColumnPtr & column)
{
    if (isColumnNullable(*column))
        return column;

    if (isColumnConst(*column))
        return ColumnConst::create(makeNullableSafe(assert_cast<const ColumnConst &>(*column).getDataColumnPtr()), column->size());

    if (column->canBeInsideNullable())
        return makeNullable(column);

    return column;
}

}
