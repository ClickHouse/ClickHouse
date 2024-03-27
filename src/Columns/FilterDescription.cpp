#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Columns/FilterDescription.h>
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
        ColumnPtr column_nested = column_const.getDataColumnPtr()->convertToFullColumnIfLowCardinality();

        if (!typeid_cast<const ColumnUInt8 *>(column_nested.get()))
        {
            const ColumnNullable * column_nested_nullable = checkAndGetColumn<ColumnNullable>(*column_nested);
            if (!column_nested_nullable || !typeid_cast<const ColumnUInt8 *>(&column_nested_nullable->getNestedColumn()))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                                "Illegal type {} of column for constant filter. Must be UInt8 or Nullable(UInt8).",
                                column_nested->getName());
            }
        }

        if (column_const.getValue<UInt64>())
            always_true = true;
        else
            always_false = true;
        return;
    }
}


FilterDescription::FilterDescription(const IColumn & column_)
{
    if (column_.isSparse())
        data_holder = recursiveRemoveSparse(column_.getPtr());

    if (column_.lowCardinality())
        data_holder = column_.convertToFullColumnIfLowCardinality();

    const auto & column = data_holder ? *data_holder : column_;

    if (const ColumnUInt8 * concrete_column = typeid_cast<const ColumnUInt8 *>(&column))
    {
        data = &concrete_column->getData();
    }
    else if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(column))
    {
        ColumnPtr nested_column = nullable_column->getNestedColumnPtr();
        MutableColumnPtr mutable_holder = IColumn::mutate(std::move(nested_column));

        ColumnUInt8 * data_column = typeid_cast<ColumnUInt8 *>(mutable_holder.get());
        if (!data_column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                "Illegal type {} of column for filter. Must be UInt8 or Nullable(UInt8).", column.getName());

        const NullMap & null_map = nullable_column->getNullMapData();
        IColumn::Filter & res = data_column->getData();

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
    else
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
            "Illegal type {} of column for filter. Must be UInt8 or Nullable(UInt8) or Const variants of them.",
            column.getName());
}

void FilterDescription::filterInPlace(IColumn & column) const
{
    if (!indexes_holder)
        lazyInitializeForFilterInPlace();

    column.filterInPlace(*indexes_holder, start);
}

void FilterDescription::lazyInitializeForFilterInPlace() const
{
    size_t rows = data->size();
    if (rows <= std::numeric_limits<UInt32>::max())
    {
        indexes_holder = ColumnUInt32::create();
        auto & indexes = assert_cast<ColumnUInt32 &>(indexes_holder->assumeMutableRef()).getData();
        start = filterToIndices(*data, indexes);
    }
    else
    {
        indexes_holder = ColumnUInt64::create();
        auto & indexes = assert_cast<ColumnUInt64 &>(indexes_holder->assumeMutableRef()).getData();
        start = filterToIndices(*data, indexes);
    }
}

/*
size_t FilterDescription::countBytesInFilter() const
{
    if (!indexes_holder)
        lazyInitializeForFilterInPlace();

    return start + indexes_holder->size();
}
*/

SparseFilterDescription::SparseFilterDescription(const IColumn & column)
{
    const auto * column_sparse = typeid_cast<const ColumnSparse *>(&column);
    if (!column_sparse || !typeid_cast<const ColumnUInt8 *>(&column_sparse->getValuesColumn()))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
            "Illegal type {} of column for sparse filter. Must be Sparse(UInt8)", column.getName());

    filter_indices = &assert_cast<const ColumnUInt64 &>(column_sparse->getOffsetsColumn());
}

}
