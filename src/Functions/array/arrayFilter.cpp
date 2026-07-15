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
    FunctionDocumentation::Description description = "Returns an array containing only the elements in the source array for which a lambda function returns true.";
    FunctionDocumentation::Syntax syntax = "arrayFilter(func(x[, y1, ..., yN]), source_arr[, cond1_arr, ... , condN_arr])]";
    FunctionDocumentation::Arguments arguments = {
        {"func(x[, y1, ..., yN])", "A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`).", {"Lambda function"}},
        {"source_arr", "The source array to process.", {"Array(T)"}},
        {"[, cond1_arr, ... , condN_arr]", "Optional. N condition arrays providing additional arguments to the lambda function.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a subset of the source array", {"Array(T)"}};
    FunctionDocumentation::Examples examples = {
        {"Example 1", "SELECT arrayFilter(x -> x LIKE '%World%', ['Hello', 'abc World']) AS res", "['abc World']"},
        {"Example 2", R"(
SELECT
    arrayFilter(
        (i, x) -> x LIKE '%World%',
        arrayEnumerate(arr),
        ['Hello', 'abc World'] AS arr)
    AS res
)",
"[2]"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayFilter>(documentation);
}

}
