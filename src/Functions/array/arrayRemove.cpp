#include <Functions/array/arrayRemove.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

ColumnPtr ArrayRemoveImpl::execute(const ColumnArray & array, ColumnPtr mapped)
{
    const ColumnUInt8 * column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

    if (!column_filter)
    {
        const auto * column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

        if (!column_filter_const)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected type of filter column");

        if (column_filter_const->getValue<UInt8>())
            return array.clone();
        else
            return ColumnArray::create(
                array.getDataPtr()->cloneEmpty(),
                ColumnArray::ColumnOffsets::create(array.size(), 0));
    }

    const IColumn::Filter & filter = column_filter->getData();
    IColumn::Filter negated_filter(filter.size()); // Create a new filter with the negated values, handling NULLs separately.

    for (size_t i = 0; i < filter.size(); ++i)
    {
        negated_filter[i] = !filter[i];
    }

    ColumnPtr filtered = array.getData().filter(negated_filter, -1);

    const IColumn::Offsets & in_offsets = array.getOffsets();
    auto column_offsets = ColumnArray::ColumnOffsets::create(in_offsets.size());
    IColumn::Offsets & out_offsets = column_offsets->getData();

    size_t in_pos = 0;
    size_t out_pos = 0;
    for (size_t i = 0; i < in_offsets.size(); ++i)
    {
        for (; in_pos < in_offsets[i]; ++in_pos)
        {
            if (negated_filter[in_pos])
                ++out_pos;
        }
        out_offsets[i] = out_pos;
    }

    return ColumnArray::create(filtered, std::move(column_offsets));
}

template <typename T>
ColumnPtr ArrayRemoveImpl::execute(const ColumnArray &array, T element)
{

    auto filter_column = ColumnUInt8::create();

    const auto &data = array.getData();
    for (size_t i = 0; i < array.size(); ++i)
    {
        if (data[i] == element)
            filter_column->insert(UInt8(0));
        else
            filter_column->insert(UInt8(1));
    }

    ColumnPtr filtered = array.getData().filter(filter_column, -1);

    const IColumn::Offsets & in_offsets = array.getOffsets();
    auto column_offsets = ColumnArray::ColumnOffsets::create(in_offsets.size());
    IColumn::Offsets & out_offsets = column_offsets->getData();

    size_t in_pos = 0;
    size_t out_pos = 0;
    for (size_t i = 0; i < in_offsets.size(); ++i)
    {
        for (; in_pos < in_offsets[i]; ++in_pos)
        {
            if (filter_column[in_pos])
                ++out_pos;
        }
        out_offsets[i] = out_pos;
    }

    return ColumnArray::create(filtered, std::move(column_offsets));

}

REGISTER_FUNCTION(ArrayRemove)
{
    factory.registerFunction<FunctionArrayRemove>();
}

}
