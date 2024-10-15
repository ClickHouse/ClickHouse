#include <Columns/IColumn.h>

#include <Columns/IColumnDummy.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObjectDeprecated.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnVector.h>
#include <Core/Field.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Transforms/ColumnGathererTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

String IColumn::dumpStructure() const
{
    WriteBufferFromOwnString res;
    res << getFamilyName() << "(size = " << size();

    forEachSubcolumn([&](const auto & subcolumn)
    {
        res << ", " << subcolumn->dumpStructure();
    });

    res << ")";
    return res.str();
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void IColumn::insertFrom(const IColumn & src, size_t n)
#else
void IColumn::doInsertFrom(const IColumn & src, size_t n)
#endif
{
    insert(src[n]);
}

ColumnPtr IColumn::createWithOffsets(const Offsets & offsets, const ColumnConst & column_with_default_value, size_t total_rows, size_t shift) const
{
    if (offsets.size() + shift != size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Incompatible sizes of offsets ({}), shift ({}) and size of column {}",
            offsets.size(),
            shift,
            size());

    auto res = cloneEmpty();
    res->reserve(total_rows);

    ssize_t current_offset = -1;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        ssize_t offsets_diff = static_cast<ssize_t>(offsets[i]) - current_offset;
        current_offset = offsets[i];

        if (offsets_diff > 1)
            res->insertManyFrom(column_with_default_value.getDataColumn(), 0, offsets_diff - 1);

        res->insertFrom(*this, i + shift);
    }

    ssize_t offsets_diff = static_cast<ssize_t>(total_rows) - current_offset;
    if (offsets_diff > 1)
        res->insertManyFrom(column_with_default_value.getDataColumn(), 0, offsets_diff - 1);

    return res;
}

size_t IColumn::estimateCardinalityInPermutedRange(const IColumn::Permutation & /*permutation*/, const EqualRange & equal_range) const
{
    return equal_range.size();
}

void IColumn::forEachSubcolumn(ColumnCallback callback) const
{
    const_cast<IColumn*>(this)->forEachSubcolumn([&callback](WrappedPtr & subcolumn)
    {
        callback(std::as_const(subcolumn));
    });
}

void IColumn::forEachSubcolumnRecursively(RecursiveColumnCallback callback) const
{
    const_cast<IColumn*>(this)->forEachSubcolumnRecursively([&callback](IColumn & subcolumn)
    {
        callback(std::as_const(subcolumn));
    });
}

bool isColumnNullable(const IColumn & column)
{
    return checkColumn<ColumnNullable>(column);
}

bool isColumnNullableOrLowCardinalityNullable(const IColumn & column)
{
    return isColumnNullable(column) || isColumnLowCardinalityNullable(column);
}

bool isColumnConst(const IColumn & column)
{
    return checkColumn<ColumnConst>(column);
}

template <typename Derived, typename Parent>
MutableColumns IColumnHelper<Derived, Parent>::scatter(IColumn::ColumnIndex num_columns, const IColumn::Selector & selector) const
{
    const auto & self = static_cast<const Derived &>(*this);
    size_t num_rows = self.size();

    if (num_rows != selector.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Size of selector: {} doesn't match size of column: {}", selector.size(), num_rows);

    MutableColumns columns(num_columns);
    for (auto & column : columns)
        column = self.cloneEmpty();

    {
        size_t reserve_size = static_cast<size_t>(num_rows * 1.1 / num_columns);    /// 1.1 is just a guess. Better to use n-sigma rule.

        if (reserve_size > 1)
            for (auto & column : columns)
                column->reserve(reserve_size);
    }

    for (size_t i = 0; i < num_rows; ++i)
        static_cast<Derived &>(*columns[selector[i]]).insertFrom(*this, i);

    return columns;
}

template <typename Derived, typename Parent>
void IColumnHelper<Derived, Parent>::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(static_cast<Derived &>(*this));
}

template <typename Derived, bool reversed>
void compareImpl(
    const Derived & lhs,
    const Derived & rhs,
    size_t rhs_row_num,
    PaddedPODArray<UInt64> * row_indexes [[maybe_unused]],
    PaddedPODArray<Int8> & compare_results,
    int nan_direction_hint)
{
    size_t num_rows = lhs.size();
    if (compare_results.empty())
        compare_results.resize(num_rows);
    else if (compare_results.size() != num_rows)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Size of compare_results: {} doesn't match rows_num: {}",
            compare_results.size(),
            num_rows);

    for (size_t row = 0; row < num_rows; ++row)
    {
        int res = lhs.compareAt(row, rhs_row_num, rhs, nan_direction_hint);
        assert(res == 1 || res == -1 || res == 0);
        compare_results[row] = static_cast<Int8>(res);

        if constexpr (reversed)
            compare_results[row] = -compare_results[row];
    }
}

template <typename Derived, bool reversed>
void compareWithIndexImpl(
    const Derived & lhs,
    const Derived & rhs,
    size_t rhs_row_num,
    PaddedPODArray<UInt64> * row_indexes [[maybe_unused]],
    PaddedPODArray<Int8> & compare_results,
    int nan_direction_hint)
{
    size_t num_rows = lhs.size();
    if (compare_results.empty())
        compare_results.resize(num_rows);
    else if (compare_results.size() != num_rows)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Size of compare_results: {} doesn't match rows_num: {}",
            compare_results.size(),
            num_rows);

    UInt64 * next_index = row_indexes->data();
    for (auto row : *row_indexes)
    {
        int res = lhs.compareAt(row, rhs_row_num, rhs, nan_direction_hint);
        assert(res == 1 || res == -1 || res == 0);
        compare_results[row] = static_cast<Int8>(res);

        if constexpr (reversed)
            compare_results[row] = -compare_results[row];

        if (compare_results[row] == 0)
        {
            *next_index = row;
            ++next_index;
        }
    }

    size_t equal_row_indexes_size = next_index - row_indexes->data();
    row_indexes->resize(equal_row_indexes_size);
}

template <typename Derived, typename Parent>
void IColumnHelper<Derived, Parent>::compareColumn(
    const IColumn & rhs_base,
    size_t rhs_row_num,
    PaddedPODArray<UInt64> * row_indexes,
    PaddedPODArray<Int8> & compare_results,
    int direction,
    int nan_direction_hint) const
{
    const auto & lhs = static_cast<const Derived &>(*this);
    const auto & rhs = static_cast<const Derived &>(rhs_base);
    if (direction < 0)
    {
        if (row_indexes)
            compareWithIndexImpl<Derived, true>(lhs, rhs, rhs_row_num, row_indexes, compare_results, nan_direction_hint);
        else
            compareImpl<Derived, true>(lhs, rhs, rhs_row_num, row_indexes, compare_results, nan_direction_hint);
    }
    else if (row_indexes)
    {
        compareWithIndexImpl<Derived, false>(lhs, rhs, rhs_row_num, row_indexes, compare_results, nan_direction_hint);
    }
    else
    {
        compareImpl<Derived, false>(lhs, rhs, rhs_row_num, row_indexes, compare_results, nan_direction_hint);
    }
}

template <typename Derived, typename Parent>
bool IColumnHelper<Derived, Parent>::hasEqualValues() const
{
    const auto & self = static_cast<const Derived &>(*this);
    size_t num_rows = self.size();
    for (size_t i = 1; i < num_rows; ++i)
    {
        if (self.compareAt(i, 0, self, 0) != 0)
            return false;
    }
    return true;
}

template <typename Derived, typename Parent>
double IColumnHelper<Derived, Parent>::getRatioOfDefaultRows(double sample_ratio) const
{
    if (sample_ratio <= 0.0 || sample_ratio > 1.0)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Value of 'sample_ratio' must be in interval (0.0; 1.0], but got: {}", sample_ratio);

    static constexpr auto max_number_of_rows_for_full_search = 1000;

    const auto & self = static_cast<const Derived &>(*this);
    size_t num_rows = self.size();
    size_t num_sampled_rows = std::min(static_cast<size_t>(num_rows * sample_ratio), num_rows);
    size_t num_checked_rows = 0;
    size_t res = 0;

    if (num_sampled_rows == num_rows || num_rows <= max_number_of_rows_for_full_search)
    {
        for (size_t i = 0; i < num_rows; ++i)
            res += self.isDefaultAt(i);
        num_checked_rows = num_rows;
    }
    else if (num_sampled_rows != 0)
    {
        for (size_t i = 0; i < num_rows; ++i)
        {
            if (num_checked_rows * num_rows <= i * num_sampled_rows)
            {
                res += self.isDefaultAt(i);
                ++num_checked_rows;
            }
        }
    }

    if (num_checked_rows == 0)
        return 0.0;

    return static_cast<double>(res) / num_checked_rows;
}

template <typename Derived, typename Parent>
UInt64 IColumnHelper<Derived, Parent>::getNumberOfDefaultRows() const
{
    const auto & self = static_cast<const Derived &>(*this);
    UInt64 res = 0;
    size_t num_rows = self.size();
    for (size_t i = 0; i < num_rows; ++i)
        res += self.isDefaultAt(i);
    return res;
}

template <typename Derived, typename Parent>
void IColumnHelper<Derived, Parent>::getIndicesOfNonDefaultRows(IColumn::Offsets & indices, size_t from, size_t limit) const
{
    const auto & self = static_cast<const Derived &>(*this);
    size_t to = limit && from + limit < self.size() ? from + limit : self.size();
    indices.reserve_exact(indices.size() + to - from);

    for (size_t i = from; i < to; ++i)
    {
        if (!self.isDefaultAt(i))
            indices.push_back(i);
    }
}

template <typename Derived, typename Parent>
StringRef
IColumnHelper<Derived, Parent>::serializeValueIntoArenaWithNull(size_t n, Arena & arena, char const *& begin, const UInt8 * is_null) const
{
    const auto & self = static_cast<const Derived &>(*this);
    if (is_null)
    {
        char * memory;
        if (is_null[n])
        {
            memory = arena.allocContinue(1, begin);
            *memory = 1;
            return {memory, 1};
        }

        size_t sz = self.byteSizeAt(n) + 1 /* null byte */;
        memory = arena.allocContinue(sz, begin);
        *memory = 0;
        self.serializeValueIntoMemory(n, memory + 1);
        return {memory, sz};
    }

    return self.serializeValueIntoArena(n, arena, begin);
}

template <typename Derived, typename Parent>
StringRef IColumnHelper<Derived, Parent>::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    if constexpr (!std::is_base_of_v<ColumnFixedSizeHelper, Derived>)
        return IColumn::serializeValueIntoArena(n, arena, begin);

    const auto & self = static_cast<const Derived &>(*this);
    size_t sz = self.byteSizeAt(n);
    char * memory = arena.allocContinue(sz, begin);
    self.serializeValueIntoMemory(n, memory);
    return {memory, sz};
}

template <typename Derived, typename Parent>
char * IColumnHelper<Derived, Parent>::serializeValueIntoMemoryWithNull(size_t n, char * memory, const UInt8 * is_null) const
{
    const auto & self = static_cast<const Derived &>(*this);
    if (is_null)
    {
        *memory = is_null[n];
        ++memory;
        if (is_null[n])
            return memory;
    }

    return self.serializeValueIntoMemory(n, memory);
}

template <typename Derived, typename Parent>
char * IColumnHelper<Derived, Parent>::serializeValueIntoMemory(size_t n, char * memory) const
{
    if constexpr (!std::is_base_of_v<ColumnFixedSizeHelper, Derived>)
        return IColumn::serializeValueIntoMemory(n, memory);

    const auto & self = static_cast<const Derived &>(*this);
    auto raw_data = self.getDataAt(n);
    memcpy(memory, raw_data.data, raw_data.size);
    return memory + raw_data.size;
}

template <typename Derived, typename Parent>
void IColumnHelper<Derived, Parent>::collectSerializedValueSizes(PaddedPODArray<UInt64> & sizes, const UInt8 * is_null) const
{
    if constexpr (!std::is_base_of_v<ColumnFixedSizeHelper, Derived>)
        return IColumn::collectSerializedValueSizes(sizes, is_null);

    const auto & self = static_cast<const Derived &>(*this);
    size_t rows = self.size();
    if (sizes.empty())
        sizes.resize_fill(rows);
    else if (sizes.size() != rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Size of sizes: {} doesn't match rows_num: {}. It is a bug", sizes.size(), rows);

    if (rows == 0)
        return;

    size_t element_size = self.byteSizeAt(0);
    if (is_null)
    {
        for (size_t i = 0; i < rows; ++i)
        {
            if (is_null[i])
                ++sizes[i];
            else
                sizes[i] += element_size + 1 /* null byte */;
        }
    }
    else
    {
        for (auto & sz : sizes)
            sz += element_size;
    }
}

template class IColumnHelper<ColumnVector<UInt8>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnVector<UInt16>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnVector<UInt32>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnVector<UInt64>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnVector<UInt128>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnVector<UInt256>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnVector<Int8>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnVector<Int16>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnVector<Int32>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnVector<Int64>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnVector<Int128>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnVector<Int256>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnVector<Float32>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnVector<Float64>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnVector<UUID>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnVector<IPv4>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnVector<IPv6>, ColumnFixedSizeHelper>;

template class IColumnHelper<ColumnDecimal<Decimal32>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnDecimal<Decimal64>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnDecimal<Decimal128>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnDecimal<Decimal256>, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnDecimal<DateTime64>, ColumnFixedSizeHelper>;

template class IColumnHelper<ColumnFixedString, ColumnFixedSizeHelper>;
template class IColumnHelper<ColumnString, IColumn>;

template class IColumnHelper<ColumnLowCardinality, IColumn>;
template class IColumnHelper<ColumnNullable, IColumn>;
template class IColumnHelper<ColumnConst, IColumn>;
template class IColumnHelper<ColumnArray, IColumn>;
template class IColumnHelper<ColumnTuple, IColumn>;
template class IColumnHelper<ColumnMap, IColumn>;
template class IColumnHelper<ColumnSparse, IColumn>;
template class IColumnHelper<ColumnObjectDeprecated, IColumn>;
template class IColumnHelper<ColumnAggregateFunction, IColumn>;
template class IColumnHelper<ColumnFunction, IColumn>;
template class IColumnHelper<ColumnCompressed, IColumn>;
template class IColumnHelper<ColumnVariant, IColumn>;
template class IColumnHelper<ColumnDynamic, IColumn>;
template class IColumnHelper<ColumnObject, IColumn>;

template class IColumnHelper<IColumnDummy, IColumn>;

}
