#include <Core/Settings.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_deprecated_error_prone_window_functions;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int DEPRECATED_FUNCTION;
}

namespace
{

// Implements function, giving value for column within range of given
// Example:
// | c1 |
// | 10 |
// | 20 |
// SELECT c1, neighbor(c1, 1) as c2:
// | c1 | c2 |
// | 10 | 20 |
// | 20 | 0  |
class FunctionNeighbor : public IFunction
{
public:
    static constexpr auto name = "neighbor";

    static FunctionPtr create(ContextPtr context)
    {
        if (!context->getSettingsRef()[Setting::allow_deprecated_error_prone_window_functions])
            throw Exception(
                ErrorCodes::DEPRECATED_FUNCTION,
                "Function {} is deprecated since its usage is error-prone (see docs)."
                "Please use proper window function or set `allow_deprecated_error_prone_window_functions` setting to enable it",
                name);

        return std::make_shared<FunctionNeighbor>();
    }

    /// Get the name of the function.
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

    bool isStateful() const override { return true; }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForConstants() const override { return false; }

    /// We do not use default implementation for LowCardinality because this is not a pure function.
    /// If used, optimization for LC may execute function only for dictionary, which gives wrong result.
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t number_of_arguments = arguments.size();

        if (number_of_arguments < 2 || number_of_arguments > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be from 2 to 3",
                getName(), number_of_arguments);

        // second argument must be an integer
        if (!isInteger(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {} - should be an integer", arguments[1]->getName(), getName());
        if (arguments[1]->isNullable())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {} - can not be Nullable",
                arguments[1]->getName(),
                getName());

        // check that default value column has supertype with first argument
        if (number_of_arguments == 3)
            return getLeastSupertype(DataTypes{arguments[0], arguments[2]});

        return arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & source_elem = arguments[0];
        const ColumnWithTypeAndName & offset_elem = arguments[1];
        bool has_defaults = arguments.size() == 3;

        ColumnPtr source_column_cast = castColumn(source_elem, result_type);
        ColumnPtr offset_column = offset_elem.column;

        ColumnPtr default_column_cast;
        if (has_defaults)
        {
            const ColumnWithTypeAndName & default_elem = arguments[2];
            default_column_cast = castColumn(default_elem, result_type);
        }

        bool source_is_constant = isColumnConst(*source_column_cast);
        bool offset_is_constant = isColumnConst(*offset_column);

        bool default_is_constant = false;
        if (has_defaults)
             default_is_constant = isColumnConst(*default_column_cast);

        if (source_is_constant)
            source_column_cast = assert_cast<const ColumnConst &>(*source_column_cast).getDataColumnPtr();
        if (offset_is_constant)
            offset_column = assert_cast<const ColumnConst &>(*offset_column).getDataColumnPtr();
        if (default_is_constant)
            default_column_cast = assert_cast<const ColumnConst &>(*default_column_cast).getDataColumnPtr();

        if (offset_is_constant)
        {
            /// Optimization for the case when we can copy many values at once.

            Int64 offset = offset_column->getInt(0);

            /// Protection from possible overflow.
            if (unlikely(offset > (1 << 30) || offset < -(1 << 30)))
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Too large offset: {} in function {}", offset, getName());

            auto result_column = result_type->createColumn();

            auto insert_range_from = [&](bool is_const, const ColumnPtr & src, Int64 begin, Int64 size)
            {
                /// Saturation of bounds.
                if (begin < 0)
                {
                    size += begin;
                    begin = 0;
                }
                if (size <= 0)
                    return;
                if (size > static_cast<Int64>(input_rows_count))
                    size = input_rows_count;

                if (!src)
                {
                    for (Int64 i = 0; i < size; ++i)
                        result_column->insertDefault();
                }
                else if (is_const)
                {
                    for (Int64 i = 0; i < size; ++i)
                        result_column->insertFrom(*src, 0);
                }
                else
                {
                    result_column->insertRangeFrom(*src, begin, size);
                }
            };

            if (offset == 0)
            {
                /// Degenerate case, just copy source column as is.
                return source_is_constant
                    ? ColumnConst::create(source_column_cast, input_rows_count)
                    : source_column_cast;
            }
            if (offset > 0)
            {
                insert_range_from(source_is_constant, source_column_cast, offset, static_cast<Int64>(input_rows_count) - offset);
                insert_range_from(default_is_constant, default_column_cast, static_cast<Int64>(input_rows_count) - offset, offset);
                return result_column;
            }

            insert_range_from(default_is_constant, default_column_cast, 0, -offset);
            insert_range_from(source_is_constant, source_column_cast, 0, static_cast<Int64>(input_rows_count) + offset);
            return result_column;
        }

        auto result_column = result_type->createColumn();

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            Int64 offset = offset_column->getInt(row);

            /// Protection from possible overflow.
            if (unlikely(offset > (1 << 30) || offset < -(1 << 30)))
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Too large offset: {} in function {}", offset, getName());

            Int64 src_idx = row + offset;

            if (src_idx >= 0 && src_idx < static_cast<Int64>(input_rows_count))
                result_column->insertFrom(*source_column_cast, source_is_constant ? 0 : src_idx);
            else if (has_defaults)
                result_column->insertFrom(*default_column_cast, default_is_constant ? 0 : row);
            else
                result_column->insertDefault();
        }

        return result_column;
    }
};

}

REGISTER_FUNCTION(Neighbor)
{
    FunctionDocumentation::Description description = R"(
Returns a value from a column at a specified offset from the current row.
This function is deprecated and error-prone because it operates on the physical order of data blocks which may not correspond to the logical order expected by users.
Consider using proper window functions instead.

The function can be enabled by setting `allow_deprecated_error_prone_window_functions = 1`.
)";
    FunctionDocumentation::Syntax syntax = "neighbor(column, offset[, default_value])";
    FunctionDocumentation::Arguments arguments = {
        {"column", "The source column.", {"Any"}},
        {"offset", "The offset from the current row. Positive values look forward, negative values look backward.", {"Integer"}},
        {"default_value", "Optional. The value to return if the offset goes beyond the data bounds. If not specified, uses the default value for the column type.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a value from the specified offset, or default if out of bounds.", {"Any"}};
    FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
SELECT number, neighbor(number, 2) FROM system.numbers LIMIT 10;
            )",
            R"(
┌─number─┬─neighbor(number, 2)─┐
│      0 │                   2 │
│      1 │                   3 │
│      2 │                   4 │
│      3 │                   5 │
│      4 │                   6 │
│      5 │                   7 │
│      6 │                   8 │
│      7 │                   9 │
│      8 │                   0 │
│      9 │                   0 │
└────────┴─────────────────────┘
            )"
        },
        {
            "With default value",
            R"(
SELECT number, neighbor(number, 2, 999) FROM system.numbers LIMIT 10;
            )",
            R"(
┌─number─┬─neighbor(number, 2, 999)─┐
│      0 │                        2 │
│      1 │                        3 │
│      2 │                        4 │
│      3 │                        5 │
│      4 │                        6 │
│      5 │                        7 │
│      6 │                        8 │
│      7 │                        9 │
│      8 │                      999 │
│      9 │                      999 │
└────────┴──────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionNeighbor>(documentation);
}

}
