#include <Functions/array/arrayAll.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

ColumnPtr ArrayAllImpl::execute(const ColumnArray & array, ColumnPtr mapped)
{
    const ColumnUInt8 * column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

    if (!column_filter)
    {
        const auto * column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

        if (!column_filter_const)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected type of filter column: {}; The result of the lambda is expected to be a UInt8", mapped->getDataType());

        if (column_filter_const->getValue<UInt8>())
            return DataTypeUInt8().createColumnConst(array.size(), 1u);

        const IColumn::Offsets & offsets = array.getOffsets();
        auto out_column = ColumnUInt8::create(offsets.size());
        ColumnUInt8::Container & out_all = out_column->getData();

        size_t pos = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            out_all[i] = offsets[i] == pos;
            pos = offsets[i];
        }

        return out_column;
    }

    const IColumn::Filter & filter = column_filter->getData();
    const IColumn::Offsets & offsets = array.getOffsets();
    auto out_column = ColumnUInt8::create(offsets.size());
    ColumnUInt8::Container & out_all = out_column->getData();

    size_t pos = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        UInt8 all = 1;
        for (; pos < offsets[i]; ++pos)
        {
            if (!filter[pos])
            {
                all = 0;
                pos = offsets[i];
                break;
            }
        }
        out_all[i] = all;
    }

    return out_column;
}

REGISTER_FUNCTION(ArrayAll)
{
    FunctionDocumentation::Description description = R"(
Returns `1` if lambda `func(x [, y1, y2, ... yN])` returns true for all elements. Otherwise, it returns `0`.
)";
    FunctionDocumentation::Syntax syntax = "arrayAll(func(x[, y1, ..., yN]), source_arr[, cond1_arr, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments = {
        {"func(x[, y1, ..., yN])", "A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`).", {"Lambda function"}},
        {"source_arr", "The source array to process.", {"Array(T)"}},
        {"cond1_arr, ...", "Optional. N condition arrays providing additional arguments to the lambda function.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if the lambda function returns true for all elements, `0` otherwise", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
        {"All elements match", "SELECT arrayAll(x, y -> x=y, [1, 2, 3], [1, 2, 3])", "1"},
        {"Not all elements match", "SELECT arrayAll(x, y -> x=y, [1, 2, 3], [1, 1, 1])", "0"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayAll>(documentation);
}

}
