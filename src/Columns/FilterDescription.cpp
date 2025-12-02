#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Columns/FilterDescription.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Core/ColumnWithTypeAndName.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}

template <typename T>
bool tryConvertColumnToBool(const IColumn & column, IColumnFilter & res)
{
    const auto * column_typed = checkAndGetColumn<ColumnVector<T>>(&column);
    if (!column_typed)
        return false;


    auto & data = column_typed->getData();
    size_t data_size = data.size();
    res.resize(data_size);
    for (size_t i = 0; i < data_size; ++i)
        res[i] = static_cast<bool>(data[i]);

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

/// Here we check for ColumnUInt8.
/// If the argument has a different type, convert it to ColumnUInt8.
/// If the argument is ColumnUInt8, check if we own it to avoid copying.
/// Fill the filter if we own the column and can modify the data later.
/// For ColumnUInt8 which is shared, return the fererence to existing filter.
static const IColumnFilter & unpackOrConvertFilter(ColumnPtr & column, std::optional<IColumnFilter> & filter)
{
    if (const auto * column_uint8 = typeid_cast<const ColumnUInt8 *>(column.get()))
    {
        if (column->use_count() == 1)
        {
            auto mut_col = IColumn::mutate(std::move(column));
            filter = std::move(assert_cast<ColumnUInt8 &>(*mut_col).getData());
            return *filter;
        }
        else
            return column_uint8->getData();
    }

    IColumnFilter res(column->size());
    if (!tryConvertAnyColumnToBool(*column, res))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
            "Illegal type {} of column for filter. Must be Number or Nullable(Number).", column->getName());
    filter = std::move(res);
    return *filter;
}

FilterDescription::FilterDescription(const IColumn & column_)
{
    ColumnPtr column = column_.getPtr();
    if (column_.isSparse())
        column = column_.convertToFullColumnIfSparse();

    if (column_.lowCardinality())
        column = column_.convertToFullColumnIfLowCardinality();

    ColumnPtr null_map_column;
    if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(column.get()))
    {
        null_map_column = nullable_column->getNullMapColumnPtr();
        column = nullable_column->getNestedColumnPtr();
    }

    std::optional<IColumnFilter> column_filter;
    /// We pass argument by reference, so for UInt8 or Nullable(UInt8) column we return the reference to existing filter.
    /// If the conversion or cast happened, column_filter will contain the newly-created data, so we avoid extra copying.
    data = &unpackOrConvertFilter(column, column_filter);

    if (null_map_column)
    {
        if (!column_filter)
        {
            /// If we don't own the filter yet, the copy will happen here.
            auto mut_col = IColumn::mutate(std::move(column));
            column_filter = std::move(assert_cast<ColumnUInt8 &>(*mut_col).getData());
            data = &*column_filter;
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
        data = &col->getData();
        data_holder = std::move(col);
    }
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
    const auto * column_sparse = typeid_cast<const ColumnSparse *>(&column);
    if (!column_sparse || !typeid_cast<const ColumnUInt8 *>(&column_sparse->getValuesColumn()))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
            "Illegal type {} of column for sparse filter. Must be Sparse(UInt8)", column.getName());

    filter_indices = &assert_cast<const ColumnUInt64 &>(column_sparse->getOffsetsColumn());
}

}
