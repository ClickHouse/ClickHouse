#include <Functions/array/arrayConcat.h>
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/castColumn.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ColumnPtr FunctionArrayConcat::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    if (result_type->onlyNull())
        return result_type->createColumnConstWithDefaultValue(input_rows_count);

    size_t num_args = arguments.size();

    Columns preprocessed_columns(num_args);

    for (size_t i = 0; i < num_args; ++i)
    {
        const ColumnWithTypeAndName & arg = arguments[i];
        ColumnPtr preprocessed_column = arg.column;

        if (!arg.type->equals(*result_type))
            preprocessed_column = castColumn(arg, result_type);

        preprocessed_columns[i] = std::move(preprocessed_column);
    }

    VectorWithMemoryTracking<std::unique_ptr<GatherUtils::IArraySource>> sources;

    for (auto & argument_column : preprocessed_columns)
    {
        bool is_const = false;

        if (const auto * argument_column_const = typeid_cast<const ColumnConst *>(argument_column.get()))
        {
            is_const = true;
            argument_column = argument_column_const->getDataColumnPtr();
        }

        if (const auto * argument_column_array = typeid_cast<const ColumnArray *>(argument_column.get()))
            sources.emplace_back(GatherUtils::createArraySource(*argument_column_array, is_const, input_rows_count));
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Arguments for function {} must be arrays.", getName());
    }

    auto sink = GatherUtils::concat(sources);

    return sink;
}

REGISTER_FUNCTION(ArrayConcat)
{
    FunctionDocumentation::Description description = "Combines arrays passed as arguments.";
    FunctionDocumentation::Syntax syntax = "arrayConcat(arr1 [, arr2, ... , arrN])";
    FunctionDocumentation::Arguments arguments = {
        {"arr1 [, arr2, ... , arrN]", "N number of arrays to concatenate.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a single combined array from the provided array arguments.", {"Array(T)"}};
    FunctionDocumentation::Examples example = {{"Usage example", "SELECT arrayConcat([1, 2], [3, 4], [5, 6]) AS res", "[1, 2, 3, 4, 5, 6]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, example, introduced_in, category};

    factory.registerFunction<FunctionArrayConcat>(documentation);
}

}
