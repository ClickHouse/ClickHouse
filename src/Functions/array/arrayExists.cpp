#include <Functions/array/arrayExists.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

ColumnPtr ArrayExistsImpl::execute(const ColumnArray & array, ColumnPtr mapped)
{
    const ColumnUInt8 * column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

    if (!column_filter)
    {
        const auto * column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

        if (!column_filter_const)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected type of filter column: {}; The result of the lambda is expected to be a UInt8", mapped->getDataType());

        if (column_filter_const->getValue<UInt8>())
        {
            const IColumn::Offsets & offsets = array.getOffsets();
            auto out_column = ColumnUInt8::create(offsets.size());
            ColumnUInt8::Container & out_exists = out_column->getData();

            size_t pos = 0;
            for (size_t i = 0; i < offsets.size(); ++i)
            {
                out_exists[i] = offsets[i] - pos > 0;
                pos = offsets[i];
            }

            return out_column;
        }
        return DataTypeUInt8().createColumnConst(array.size(), 0u);
    }

    const IColumn::Filter & filter = column_filter->getData();
    const IColumn::Offsets & offsets = array.getOffsets();
    auto out_column = ColumnUInt8::create(offsets.size());
    ColumnUInt8::Container & out_exists = out_column->getData();

    size_t pos = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        UInt8 exists = 0;
        for (; pos < offsets[i]; ++pos)
        {
            if (filter[pos])
            {
                exists = 1;
                pos = offsets[i];
                break;
            }
        }
        out_exists[i] = exists;
    }

    return out_column;
}

REGISTER_FUNCTION(ArrayExists)
{
    factory.registerFunction<FunctionArrayExists>();
}

}
