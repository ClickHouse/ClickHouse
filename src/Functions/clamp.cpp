#include <Columns/ColumnVector.h>
#include <Common/NaNUtils.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/castTypeToEither.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/Context.h>

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
        size_t arg_size = arguments.size();
        Columns converted_columns(arg_size);
        for (size_t arg = 0; arg < arg_size; ++arg)
            converted_columns[arg] = castColumn(arguments[arg], result_type)->convertToFullColumnIfConst();

        if (ColumnPtr res = executeNumeric(converted_columns, result_type, input_rows_count))
            return res;

        auto result_column = result_type->createColumn();
        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        {
            if (converted_columns[1]->compareAt(row_num, row_num, *converted_columns[2], 1) > 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The minimum value cannot be greater than the maximum value for function {}", getName());

            size_t best_arg = 0;
            if (converted_columns[1]->compareAt(row_num, row_num, *converted_columns[best_arg], 1) > 0)
                best_arg = 1;
            else if (converted_columns[2]->compareAt(row_num, row_num, *converted_columns[best_arg], 1) < 0)
                best_arg = 2;

            result_column->insertFrom(*converted_columns[best_arg], row_num);
        }

        return result_column;
    }

private:
    /// Replicates `compareAt(...) > 0` with nan_direction_hint = 1: NaN is greater than any number.
    template <typename T>
    static bool greaterAt(T a, T b)
    {
        if constexpr (is_floating_point<T>)
        {
            if (isNaN(a))
                return !isNaN(b);
            if (isNaN(b))
                return false;
        }
        return a > b;
    }

    /// Vectorized clamp for numeric arguments. NULLs are handled by the default implementation,
    /// so only plain ColumnVector columns can appear here.
    /// Returns nullptr if the result type is not a plain number.
    static ColumnPtr executeNumeric(const Columns & columns, const DataTypePtr & result_type, size_t input_rows_count)
    {
        ColumnPtr res;
        castTypeToEither<
            DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64, DataTypeUInt128, DataTypeUInt256,
            DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64, DataTypeInt128, DataTypeInt256,
            DataTypeBFloat16, DataTypeFloat32, DataTypeFloat64>(
            result_type.get(),
            [&](const auto & type)
            {
                using T = typename std::decay_t<decltype(type)>::FieldType;
                res = executeNumericImpl<T>(columns, input_rows_count);
                return res != nullptr;
            });
        return res;
    }

    template <typename T>
    static ColumnPtr executeNumericImpl(const Columns & columns, size_t input_rows_count)
    {
        const auto * value_column = checkAndGetColumn<ColumnVector<T>>(columns[0].get());
        const auto * min_column = checkAndGetColumn<ColumnVector<T>>(columns[1].get());
        const auto * max_column = checkAndGetColumn<ColumnVector<T>>(columns[2].get());
        if (!value_column || !min_column || !max_column)
            return nullptr;

        const T * values = value_column->getData().data();
        const T * mins = min_column->getData().data();
        const T * maxs = max_column->getData().data();

        /// The bounds check is hoisted out of the main loop so the latter stays branch-free.
        bool have_invalid_bounds = false;
        for (size_t i = 0; i < input_rows_count; ++i)
            have_invalid_bounds |= greaterAt(mins[i], maxs[i]);
        if (have_invalid_bounds)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The minimum value cannot be greater than the maximum value for function {}", name);

        auto res = ColumnVector<T>::create(input_rows_count);
        T * out = res->getData().data();
        for (size_t i = 0; i < input_rows_count; ++i)
            out[i] = greaterAt(mins[i], values[i]) ? mins[i] : (greaterAt(values[i], maxs[i]) ? maxs[i] : values[i]);

        return res;
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
