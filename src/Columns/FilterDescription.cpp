#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Columns/FilterDescription.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Common/TargetSpecific.h>
#include <Common/PODArray.h>
#include <Core/ColumnWithTypeAndName.h>

#include <algorithm>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}

MULTITARGET_FUNCTION_X86_V4(
MULTITARGET_FUNCTION_HEADER(
template <typename T>
void), convertColumnToBoolImpl, MULTITARGET_FUNCTION_BODY((const typename ColumnVector<T>::Container & data, IColumnFilter & res)
{
    for (size_t i = 0; i < res.size(); ++i)
        res[i] = static_cast<bool>(data[i]);
})
);

template <typename T>
ALWAYS_INLINE bool tryConvertColumnToBool(const IColumn & column, IColumnFilter & res)
{
    const auto * column_typed = checkAndGetColumn<ColumnVector<T>>(&column);
    if (!column_typed)
        return false;
    chassert(res.size() == column.size());

    auto & data = column_typed->getData();
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        convertColumnToBoolImpl_x86_64_v4<T>(data, res);
        return true;
    }
#endif
    convertColumnToBoolImpl<T>(data, res);

    return true;
}

bool tryConvertAnyColumnToBool(const IColumn & column, IColumnFilter & res)
{
    return tryConvertColumnToBool<Int8>(column, res) ||
        tryConvertColumnToBool<Int16>(column, res) ||
        tryConvertColumnToBool<Int32>(column, res) ||
        tryConvertColumnToBool<Int64>(column, res) ||
        tryConvertColumnToBool<UInt16>(column, res) ||
        tryConvertColumnToBool<UInt32>(column, res) ||
        tryConvertColumnToBool<UInt64>(column, res) ||
        tryConvertColumnToBool<Float32>(column, res) ||
        tryConvertColumnToBool<Float64>(column, res);
}

ConstantFilterDescription::ConstantFilterDescription(const IColumn & column)
{
    if (column.onlyNull())
    {
        always_false = true;
        return;
    }

    if (isColumnConst(column))
    {
        const ColumnConst & column_const = assert_cast<const ColumnConst &>(column);
        (column_const.getBool(0) ? always_true : always_false) = true;
    }
}

namespace
{

/// Bit `v` of `seen` is set when a row with truth value `v` (0 or 1) exists.
/// Returns false if `indexes_column` is not a `ColumnVector<IndexType>`.
/// A `UInt8` dictionary has at most 256 values plus one NULL key, so 257 covers it on the stack.
using TruthOfKeyTable = PODArrayWithStackMemory<UInt8, 257>;

template <typename IndexType>
bool collectTruthOfRows(const IColumn & indexes_column, const TruthOfKeyTable & truth_of_key, UInt8 & seen)
{
    const auto * indexes = typeid_cast<const ColumnVector<IndexType> *>(&indexes_column);
    if (!indexes)
        return false;

    const auto & data = indexes->getData();
    constexpr size_t block_size = 4096;

    for (size_t begin = 0; begin < data.size(); begin += block_size)
    {
        const size_t end = std::min(begin + block_size, data.size());
        for (size_t i = begin; i < end; ++i)
        {
            chassert(data[i] < truth_of_key.size());
            seen |= static_cast<UInt8>(1 << truth_of_key[data[i]]);
        }

        /// Both values are present, so the filter is not constant.
        if (seen == 3)
            break;
    }

    return true;
}

}

std::optional<bool> tryGetConstantTruthOfLowCardinalityFilter(const IColumn & column)
{
    const auto * column_lc = typeid_cast<const ColumnLowCardinality *>(&column);
    if (!column_lc || column_lc->empty())
        return std::nullopt;

    /// The dictionary holds the values of the keys: `UInt8`, or `Nullable(UInt8)` with NULL as a special key.
    const IColumn * keys = column_lc->getDictionary().getNestedColumn().get();
    const NullMap * key_null_map = nullptr;
    if (const auto * nullable_keys = typeid_cast<const ColumnNullable *>(keys))
    {
        keys = &nullable_keys->getNestedColumn();
        key_null_map = &nullable_keys->getNullMapData();
    }

    const auto * uint8_keys = typeid_cast<const ColumnUInt8 *>(keys);
    if (!uint8_keys)
        return std::nullopt;

    const auto & key_values = uint8_keys->getData();
    TruthOfKeyTable truth_of_key(key_values.size());
    UInt8 dict_seen = 0;
    for (size_t i = 0; i < key_values.size(); ++i)
    {
        truth_of_key[i] = key_values[i] && !(key_null_map && (*key_null_map)[i]);
        dict_seen |= static_cast<UInt8>(1 << truth_of_key[i]);
    }

    /// Every dictionary entry already agrees on the truth value (e.g. the typical `s IN ('z')`
    /// with `'z'` absent from the whole column, where the dictionary holds only `false`): the
    /// block is constant regardless of which entries its indexes actually reference, without
    /// scanning a single one of them.
    if (dict_seen == 1)
        return false;
    if (dict_seen == 2)
        return true;

    const IColumn & indexes = column_lc->getIndexes();
    UInt8 seen = 0;
    if (!collectTruthOfRows<UInt8>(indexes, truth_of_key, seen)
        && !collectTruthOfRows<UInt16>(indexes, truth_of_key, seen)
        && !collectTruthOfRows<UInt32>(indexes, truth_of_key, seen)
        && !collectTruthOfRows<UInt64>(indexes, truth_of_key, seen))
        return std::nullopt;

    if (seen == 1)
        return false;
    if (seen == 2)
        return true;
    return std::nullopt;
}

/// Extracts or converts filter data out of ColumnVector<some int or float>.
/// If we own the resulting filter, returns it and possibly resets `column` to nullptr.
/// If `column` is a shared ColumnUInt8, returns nullopt.
static std::optional<IColumnFilter> unpackOrConvertFilter(ColumnPtr & column)
{
    if (typeid_cast<const ColumnUInt8 *>(column.get()))
    {
        if (column->use_count() == 1)
        {
            /// Move the data out of the column so that the caller can mutate it without copying.
            auto mut_col = IColumn::mutate(std::move(column));
            auto filter = std::move(assert_cast<ColumnUInt8 &>(*mut_col).getData());
            column = nullptr;
            return std::make_optional(std::move(filter));
        }
        else
            return std::nullopt;
    }

    IColumnFilter res(column->size());
    if (!tryConvertAnyColumnToBool(*column, res))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
            "Illegal type {} of column for filter. Must be Number or Nullable(Number).", column->getName());
    return std::make_optional(std::move(res));
}

ColumnPtr FilterDescription::preprocessFilterColumn(ColumnPtr column)
{
    column = column->convertToFullIfWrapped()->convertToFullColumnIfLowCardinality();

    ColumnPtr null_map_column;
    if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(column.get()))
    {
        null_map_column = nullable_column->getNullMapColumnPtr();
        column = nullable_column->getNestedColumnPtr();
    }

    auto column_filter = unpackOrConvertFilter(column);

    if (null_map_column)
    {
        if (!column_filter)
        {
            /// If we don't own the filter yet, the copy will happen here.
            auto mut_col = IColumn::mutate(std::move(column));
            column_filter = std::move(assert_cast<ColumnUInt8 &>(*mut_col).getData());
        }

        const NullMap & null_map = assert_cast<const ColumnUInt8 &>(*null_map_column).getData();
        IColumn::Filter & res = *column_filter;

        const auto size = res.size();
        chassert(size == null_map.size());
        for (size_t i = 0; i < size; ++i)
        {
            auto has_val = static_cast<UInt8>(!!res[i]);
            auto not_null = static_cast<UInt8>(!null_map[i]);
            /// Instead of the logical AND operator(&&), the bitwise one(&) is utilized for the auto vectorization.
            res[i] = has_val & not_null;
        }
    }

    if (column_filter)
    {
        auto col = ColumnUInt8::create();
        col->getData() = std::move(*column_filter);
        column = std::move(col);
    }

    return column; // NOLINT(bugprone-use-after-move,hicpp-invalid-access-moved)
}

FilterDescription::FilterDescription(const IColumn & column_)
{
    data_holder = preprocessFilterColumn(column_.getPtr());
    data = &assert_cast<const ColumnUInt8 &>(*data_holder).getData();
}

ColumnPtr FilterDescription::filter(const IColumn & column, ssize_t result_size_hint) const
{
    return column.filter(*data, result_size_hint);
}

size_t FilterDescription::countBytesInFilter() const
{
    return DB::countBytesInFilter(*data);
}

ColumnPtr SparseFilterDescription::filter(const IColumn & column, ssize_t) const
{
    return column.index(*filter_indices, 0);
}

size_t SparseFilterDescription::countBytesInFilter() const
{
    return filter_indices->size();
}


SparseFilterDescription::SparseFilterDescription(const IColumn & column)
{
    auto throw_invalid_type = [&column]()
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
            "Illegal type {} of column for sparse filter. Must be Sparse(UInt8) or Sparse(Nullable(UInt8))",
            column.getName());
    };

    const auto * column_sparse = typeid_cast<const ColumnSparse *>(&column);
    if (!column_sparse)
        throw_invalid_type();

    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column_sparse->getValuesColumn()))
    {
        const auto * values = typeid_cast<const ColumnUInt8 *>(&nullable->getNestedColumn());
        if (!values)
            throw_invalid_type();

        const auto & offsets = column_sparse->getOffsetsData();
        ColumnUInt64::MutablePtr mutable_valid_offsets = ColumnUInt64::create();
        mutable_valid_offsets->reserve(offsets.size());
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            if (values->getBool(i + 1))
                mutable_valid_offsets->insertValue(offsets[i]);
        }

        valid_offsets = std::move(mutable_valid_offsets);
        filter_indices = &assert_cast<const ColumnUInt64 &>(*valid_offsets);
    }
    else
    {
        if (!typeid_cast<const ColumnUInt8 *>(&column_sparse->getValuesColumn()))
            throw_invalid_type();

        filter_indices = &assert_cast<const ColumnUInt64 &>(column_sparse->getOffsetsColumn());
    }
}

}
