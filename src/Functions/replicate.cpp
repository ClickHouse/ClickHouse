#include <Functions/replicate.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFixedArrayAdaptor.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

DataTypePtr FunctionReplicate::getReturnTypeImpl(const DataTypes & arguments) const
{
    DataTypes converted_arguments = arguments;
    for (size_t i = 1; i < converted_arguments.size(); ++i)
        converted_arguments[i] = removeOuterFixedArray(converted_arguments[i]);
    return IFunction::getReturnTypeImpl(converted_arguments);
}

DataTypePtr FunctionReplicate::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    ColumnsWithTypeAndName converted_arguments = arguments;
    for (size_t i = 1; i < converted_arguments.size(); ++i)
        converted_arguments[i] = removeOuterFixedArray(converted_arguments[i]);
    return IFunction::getReturnTypeImpl(converted_arguments);
}

ColumnPtr FunctionReplicate::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const
{
    ColumnPtr first_column = arguments[0].column;
    ColumnPtr offsets;

    for (size_t i = 1; i < arguments.size(); ++i)
    {
        /// `FixedArray` provides the sizes in the same way as `Array`.
        const auto argument = removeOuterFixedArray(arguments[i]);
        const ColumnArray * array_column = checkAndGetColumn<ColumnArray>(argument.column.get());
        ColumnPtr temp_column;
        if (!array_column)
        {
            const auto * const_array_column = checkAndGetColumnConst<ColumnArray>(argument.column.get());
            if (!const_array_column)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected column for replicate");
            temp_column = const_array_column->convertToFullColumn();
            array_column = checkAndGetColumn<ColumnArray>(temp_column.get());
        }

        if (!offsets || offsets->empty())
            offsets = array_column->getOffsetsPtr();
    }

    const auto & offsets_data = assert_cast<const ColumnArray::ColumnOffsets &>(*offsets).getData(); /// NOLINT
    return ColumnArray::create(first_column->replicate(offsets_data)->convertToFullColumnIfConst(), offsets);
}

REGISTER_FUNCTION(Replicate)
{
    FunctionDocumentation::Description description = R"(
Creates an array with a single value.
)";
    FunctionDocumentation::Syntax syntax = "replicate(x, arr)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The value to fill the result array with.", {"Any"}},
        {"arr", "An array.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of the same length as `arr` filled with value `x`.", {"Array(T)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT replicate(1, ['a', 'b', 'c']);
        )",
        R"(
┌─replicate(1, ['a', 'b', 'c'])─┐
│ [1,1,1]                       │
└───────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionReplicate>(documentation);
}

}
