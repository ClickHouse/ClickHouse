#include <Functions/array/arrayFilter.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

ColumnPtr ArrayFilterImpl::execute(const ColumnArray & array, ColumnPtr mapped)
{
    const ColumnUInt8 * column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

    if (!column_filter)
    {
        const auto * column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

        if (!column_filter_const)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected type of filter column: {}; The result of the lambda is expected to be a UInt8", mapped->getDataType());

        if (column_filter_const->getValue<UInt8>())
            return array.clone();
        return ColumnArray::create(array.getDataPtr()->cloneEmpty(), ColumnArray::ColumnOffsets::create(array.size(), 0));
    }

    const IColumn::Filter & filter = column_filter->getData();
    ColumnPtr filtered = array.getData().filter(filter, -1);

    const IColumn::Offsets & in_offsets = array.getOffsets();
    auto column_offsets = ColumnArray::ColumnOffsets::create(in_offsets.size());
    IColumn::Offsets & out_offsets = column_offsets->getData();

    size_t in_pos = 0;
    size_t out_pos = 0;
    for (size_t i = 0; i < in_offsets.size(); ++i)
    {
        for (; in_pos < in_offsets[i]; ++in_pos)
        {
            if (filter[in_pos])
                ++out_pos;
        }
        out_offsets[i] = out_pos;
    }

    return ColumnArray::create(filtered, std::move(column_offsets));
}

REGISTER_FUNCTION(ArrayFilter)
{
    factory.registerFunction<FunctionArrayFilter>();
}

}
