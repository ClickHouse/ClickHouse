#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/Context.h>
#include <Columns/ColumnConst.h>

#include <array>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}


class FunctionClamp final : public IFunction
{

public:
    static constexpr auto name = "clamp";

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionClamp>(); }

    DataTypePtr getReturnTypeImpl(const DataTypes & types) const override
    {
        if (types.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires 3 arguments", getName());

        return getLeastSupertype(types);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        struct ConvertedColumn
        {
            ColumnPtr column = nullptr;
            bool is_const = false;
        };

        std::array<ConvertedColumn, 3> converted_columns;
        bool has_const_column = false;
        for (size_t arg = 0; arg < arguments.size(); ++arg)
        {
            auto converted_col = castColumn(arguments[arg], result_type);
            if (const auto * const_column = checkAndGetColumn<ColumnConst>(converted_col.get()))
            {
                converted_columns[arg] = {const_column->getDataColumnPtr(), true};
                has_const_column = true;
            }
            else
            {
                converted_columns[arg] = {converted_col->convertToFullColumnIfConst(), false};
            }
        }

        auto result_column = result_type->createColumn();

        if (!has_const_column)
        {
            for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
            {
                if (converted_columns[1].column->compareAt(row_num, row_num, *converted_columns[2].column, 1) > 0)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The minimum value cannot be greater than the maximum value for function {}", getName());

                size_t best_arg = 0;
                if (converted_columns[1].column->compareAt(row_num, row_num, *converted_columns[best_arg].column, 1) > 0)
                    best_arg = 1;
                else if (converted_columns[2].column->compareAt(row_num, row_num, *converted_columns[best_arg].column, 1) < 0)
                    best_arg = 2;

                result_column->insertFrom(*converted_columns[best_arg].column, row_num);
            }

            return result_column;
        }

        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        {
            const size_t min_row = converted_columns[1].is_const ? 0 : row_num;
            const size_t max_row = converted_columns[2].is_const ? 0 : row_num;
            if (converted_columns[1].column->compareAt(min_row, max_row, *converted_columns[2].column, 1) > 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The minimum value cannot be greater than the maximum value for function {}", getName());

            size_t best_arg = 0;
            size_t best_row = converted_columns[0].is_const ? 0 : row_num;
            if (converted_columns[1].column->compareAt(min_row, best_row, *converted_columns[best_arg].column, 1) > 0)
            {
                best_arg = 1;
                best_row = min_row;
            }
            else if (converted_columns[2].column->compareAt(max_row, best_row, *converted_columns[best_arg].column, 1) < 0)
            {
                best_arg = 2;
                best_row = max_row;
            }

            result_column->insertFrom(*converted_columns[best_arg].column, best_row);
        }

        return result_column;
    }

};

REGISTER_FUNCTION(Clamp)
{
    FunctionDocumentation::Description description = R"(
Restricts a value to be within the specified minimum and maximum bounds.

If the value is less than the minimum, returns the minimum. If the value is greater than the maximum, returns the maximum. Otherwise, returns the value itself.

All arguments must be of comparable types. The result type is the largest compatible type among all arguments.
    )";
    FunctionDocumentation::Syntax syntax = "clamp(value, min, max)";
    FunctionDocumentation::Arguments arguments = {
        {"value", "The value to clamp."},
        {"min", "The minimum bound."},
        {"max", "The maximum bound."}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the value, restricted to the [min, max] range."};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", R"(
SELECT clamp(5, 1, 10) AS result;
        )",
        R"(
┌─result─┐
│      5 │
└────────┘
        )"},
        {"Value below minimum", R"(
SELECT clamp(-3, 0, 7) AS result;
        )",
        R"(
┌─result─┐
│      0 │
└────────┘
        )"},
        {"Value above maximum", R"(
SELECT clamp(15, 0, 7) AS result;
        )",
        R"(
┌─result─┐
│      7 │
└────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {24, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Conditional;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionClamp>(documentation);
}
}
