#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Columns/FilterDescription.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Common/TargetSpecific.h>
#include <Core/ColumnWithTypeAndName.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}

MULTITARGET_FUNCTION_X86_V4_V3(
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
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        convertColumnToBoolImpl_x86_64_v3<T>(data, res);
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
    column = column->convertToFullIfNeeded();

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
        assert(size == null_map.size());
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
