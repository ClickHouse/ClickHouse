#include <Functions/replicate.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}

DataTypePtr FunctionReplicate::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.size() < 2)
        throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                        "Function {} expect at least two arguments, got {}", getName(), arguments.size());

    for (size_t i = 1; i < arguments.size(); ++i)
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Argument {} for function {} must be array.",
                            i + 1, getName());
    }
    return std::make_shared<DataTypeArray>(arguments[0]);
}

ColumnPtr FunctionReplicate::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const
{
    ColumnPtr first_column = arguments[0].column;
    ColumnPtr offsets;

    for (size_t i = 1; i < arguments.size(); ++i)
    {
        const ColumnArray * array_column = checkAndGetColumn<ColumnArray>(arguments[i].column.get());
        ColumnPtr temp_column;
        if (!array_column)
        {
            const auto * const_array_column = checkAndGetColumnConst<ColumnArray>(arguments[i].column.get());
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
    FunctionDocumentation::Description description_replicate = R"(
Creates an array with a single value.
)";
    FunctionDocumentation::Syntax syntax_replicate = "replicate(x, arr)";
    FunctionDocumentation::Arguments arguments_replicate = {
        {"x", "The value to fill the result array with.", {"Any"}},
        {"arr", "An array.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_replicate = {"Returns an array of the same length as `arr` filled with value `x`.", {"Array(T)"}};
    FunctionDocumentation::Examples examples_replicate = {
    {
        "Usage example",
        R"(
SELECT replicate(1, ['a', 'b', 'c']);
        )",
        R"(
┌─replicate(1, ['a', 'b', 'c'])───┐
│ [1, 1, 1]                       │
└─────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_replicate = {1, 1};
    FunctionDocumentation::Category category_replicate = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_replicate = {description_replicate, syntax_replicate, arguments_replicate, returned_value_replicate, examples_replicate, introduced_in_replicate, category_replicate};

    factory.registerFunction<FunctionReplicate>(documentation_replicate);
}

}
