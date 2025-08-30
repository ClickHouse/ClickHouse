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


FilterDescription::FilterDescription(const IColumn & column_)
{
    ColumnPtr holder;
    if (column_.isSparse())
        holder = recursiveRemoveSparse(column_.getPtr());

    if (column_.lowCardinality())
        holder = column_.convertToFullColumnIfLowCardinality();

    const auto * column = holder ? holder.get() : &column_;

    ColumnPtr null_map_column;
    if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(column))
    {
        column = &nullable_column->getNestedColumn();
        null_map_column = nullable_column->getNullMapColumnPtr();
    }

    const ColumnUInt8 * filter_column = typeid_cast<const ColumnUInt8 *>(column);
    if (!filter_column)
    {
        auto col = ColumnUInt8::create();
        col->getData().resize(column->size());
        if (!tryConvertAnyColumnToBool(*column, col->getData()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                "Illegal type {} of column for filter. Must be Number or Nullable(Number).", column->getName());
        filter_column = col.get();
        data_holder = std::move(col);
    }
    else
        data_holder = std::move(holder);

    data = &filter_column->getData();

    if (null_map_column)
    {
        ColumnPtr uint8_column = (data_holder && data_holder.get() == filter_column) ? std::move(data_holder) : filter_column->getPtr();
        MutableColumnPtr mutable_holder = IColumn::mutate(std::move(uint8_column));

        ColumnUInt8 * concrete_column = typeid_cast<ColumnUInt8 *>(mutable_holder.get());
        if (!concrete_column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                "Illegal type {} of column for filter. Must be UInt8 or Nullable(UInt8).", column_.getName());

        const NullMap & null_map = assert_cast<const ColumnUInt8 &>(*null_map_column).getData();
        IColumn::Filter & res = concrete_column->getData();

        const auto size = res.size();
        assert(size == null_map.size());
        for (size_t i = 0; i < size; ++i)
        {
            auto has_val = static_cast<UInt8>(!!res[i]);
            auto not_null = static_cast<UInt8>(!null_map[i]);
            /// Instead of the logical AND operator(&&), the bitwise one(&) is utilized for the auto vectorization.
            res[i] = has_val & not_null;
        }

        data = &res;
        data_holder = std::move(mutable_holder);
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
