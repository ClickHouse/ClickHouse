#include <Functions/FunctionExactDivide.h>
#include <Functions/FunctionFactory.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NumberTraits.h>
#include <Functions/castTypeToEither.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>

#include <algorithm>
#include <limits>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_DIVISION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

using IntegerTypes = TypeList<
    DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64, DataTypeUInt128, DataTypeUInt256,
    DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64, DataTypeInt128, DataTypeInt256>;

template <typename A, typename B>
auto divideIntegers(A a, B b)
{
    using Result = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;
    using Unsigned = typename NumberTraits::Construct<false, false, std::max({sizeof(A), sizeof(B), sizeof(UInt64)})>::Type;

    if (b == 0)
        throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Division by zero");

    const bool negative_a = is_signed_v<A> && a < 0;
    const bool negative_b = is_signed_v<B> && b < 0;
    const auto magnitude = [](auto value, bool negative) -> Unsigned
    {
        /// Unsigned subtraction also represents the magnitude of the minimum signed value.
        const auto unsigned_value = static_cast<Unsigned>(value);
        return negative ? Unsigned(0) - unsigned_value : unsigned_value;
    };

    const Unsigned quotient = magnitude(a, negative_a) / magnitude(b, negative_b);
    const Unsigned maximum = static_cast<Unsigned>(std::numeric_limits<Result>::max());
    if constexpr (is_signed_v<Result>)
    {
        if (negative_a != negative_b)
        {
            if (quotient > maximum + 1)
                throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Integer division result is out of range");
            if (quotient == maximum + 1)
                return std::numeric_limits<Result>::min();
            return static_cast<Result>(-static_cast<Result>(quotient));
        }
    }

    if (quotient > maximum)
        throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Integer division result is out of range");
    return static_cast<Result>(quotient);
}

template <typename A, typename B>
ColumnPtr executeDivision(const ColumnsWithTypeAndName & arguments, size_t rows)
{
    using Result = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;
    const auto * left_const = checkAndGetColumn<ColumnConst>(arguments[0].column.get());
    const auto * right_const = checkAndGetColumn<ColumnConst>(arguments[1].column.get());
    const auto & left = assert_cast<const ColumnVector<A> &>(left_const ? left_const->getDataColumn() : *arguments[0].column).getData();
    const auto & right = assert_cast<const ColumnVector<B> &>(right_const ? right_const->getDataColumn() : *arguments[1].column).getData();
    auto result = ColumnVector<Result>::create(rows);
    auto & data = result->getData();
    for (size_t i = 0; i < rows; ++i)
        data[i] = divideIntegers(left[left_const ? 0 : i], right[right_const ? 0 : i]);
    return result;
}

class FunctionExactDivide final : public IFunction
{
public:
    static constexpr auto name = "__divideIntegers";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionExactDivide>();
    }

    /// Keep canonical expression names distinct from floating-point `divide` in indexes and projections.
    [[nodiscard]] String getName() const override
    {
        return name;
    }
    [[nodiscard]] size_t getNumberOfArguments() const override
    {
        return 2;
    }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
    /// Skip unused dictionary entries and rows masked by either input's null map before dividing.
    [[nodiscard]] bool canBeExecutedOnDefaultArguments() const override
    {
        return false;
    }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override
    {
        return true;
    }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        DataTypePtr result;
        const bool matched = dispatch(arguments, [&]<typename A, typename B>()
        {
            using Result = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;
            result = std::make_shared<DataTypeNumber<Result>>();
        });
        if (!matched)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Exact integer division requires two integer arguments");
        return result;
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t rows) const override
    {
        ColumnPtr result;
        const bool matched = dispatch({arguments[0].type, arguments[1].type}, [&]<typename A, typename B>()
        {
            result = executeDivision<A, B>(arguments, rows);
        });
        if (!matched)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Exact integer division requires two integer arguments");
        return result;
    }

private:
    template <typename F>
    static bool dispatch(const DataTypes & arguments, F && function)
    {
        return castTypeToEither(IntegerTypes{}, arguments[0].get(), [&](const auto & left)
        {
            return castTypeToEither(IntegerTypes{}, arguments[1].get(), [&](const auto & right)
            {
                function.template operator()<typename std::decay_t<decltype(left)>::FieldType, typename std::decay_t<decltype(right)>::FieldType>();
                return true;
            });
        });
    }
};

}

FunctionPtr createFunctionExactDivide()
{
    return std::make_shared<FunctionExactDivide>();
}

REGISTER_FUNCTION(ExactDivide)
{
    FunctionDocumentation::Description description = R"(
Calculates the quotient of two integer arguments without conversion to floating point, truncating toward zero.
The result has the width of `x` and is signed if either argument is signed. Integer types through 256 bits are supported, including mixed signed and unsigned arguments.
Division by zero or a quotient outside the result type's range throws `ILLEGAL_DIVISION`. Nullable arguments propagate `NULL`.
The `sql_compatible_division_for_integers` setting does not affect this function.

This is the integer-only implementation used by `divideExact` for cases that require checked division.
Use `divideExact` for expressions that may also have floating-point or `Decimal` arguments and for the stable function name stored in metadata.
    )";
    FunctionDocumentation::Syntax syntax = "__divideIntegers(x, y)";
    FunctionDocumentation::Arguments arguments = {{"x", "Integer dividend."}, {"y", "Integer divisor."}};
    FunctionDocumentation::ReturnedValue returned_value = {"The integer quotient of `x` and `y`, truncated toward zero."};
    FunctionDocumentation::Examples examples =
    {
        {"Mixed signed and unsigned arguments", "SELECT __divideIntegers(toInt32(-5), toUInt64(2)) AS quotient, toTypeName(quotient)", "-2\tInt32"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Internal;
    factory.registerFunction<FunctionExactDivide>({description, syntax, arguments, {}, returned_value, examples, introduced_in, category});
}

}
