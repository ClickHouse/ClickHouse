#include <Functions/array/arrayConcat.h>
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/castColumn.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <base/range.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

DataTypePtr FunctionArrayConcat::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least one argument.", getName());

    DataTypes array_arguments;
    array_arguments.reserve(arguments.size());
    bool has_null_constant = false;

    for (auto i : collections::range(0, arguments.size()))
    {
        if (arguments[i]->onlyNull())
        {
            has_null_constant = true;
            continue;
        }

        const auto * array_type = typeid_cast<const DataTypeArray *>(removeNullable(arguments[i]).get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Argument {} for function {} must be an array but it has type {}.",
                            i, getName(), arguments[i]->getName());

        array_arguments.push_back(arguments[i]);
    }

    if (array_arguments.empty())
        return getLeastSupertype(arguments);

    auto result_type = getLeastSupertype(array_arguments);
    if (has_null_constant)
        return makeNullable(result_type);
    return result_type;
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
    ColumnUInt8::MutablePtr null_map;

    for (auto & argument_column : preprocessed_columns)
    {
        bool is_const = false;

        if (const auto * argument_column_const = typeid_cast<const ColumnConst *>(argument_column.get()))
        {
            is_const = true;
            argument_column = argument_column_const->getDataColumnPtr();
        }

        if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(argument_column.get()))
        {
            if (!null_map)
            {
                null_map = ColumnUInt8::create(nullable_column->getNullMapData().begin(), nullable_column->getNullMapData().end());
                if (is_const && null_map->size() == 1)
                    null_map->getData().resize_fill(input_rows_count, nullable_column->getNullMapData()[0]);
            }
            else
            {
                auto & null_map_data = null_map->getData();
                const auto & other_null_map = nullable_column->getNullMapData();
                if (is_const && other_null_map.size() == 1)
                {
                    for (auto & value : null_map_data)
                        value |= other_null_map[0];
                }
                else
                {
                    for (size_t row = 0, rows = null_map_data.size(); row < rows; ++row)
                        null_map_data[row] |= other_null_map[row];
                }
            }

            argument_column = nullable_column->getNestedColumnPtr();
        }

        if (const auto * argument_column_array = typeid_cast<const ColumnArray *>(argument_column.get()))
            sources.emplace_back(GatherUtils::createArraySource(*argument_column_array, is_const, input_rows_count));
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Arguments for function {} must be arrays.", getName());
    }

    auto sink = GatherUtils::concat(sources);

    if (result_type->isNullable())
    {
        if (!null_map)
            null_map = ColumnUInt8::create(input_rows_count, UInt8(0));
        return ColumnNullable::create(std::move(sink), std::move(null_map));
    }

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
