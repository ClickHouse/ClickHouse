#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <Common/Primality.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int BAD_ARGUMENTS;
}

namespace
{

constexpr unsigned default_rounds = 25;

class FunctionIsProbablePrime : public IFunction
{
public:
    static constexpr auto name = "isProbablePrime";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIsProbablePrime>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool isDeterministic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"n", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt), nullptr, "UInt8/UInt16/UInt32/UInt64/UInt128/UInt256"},
        };
        FunctionArgumentDescriptors optional_args{
            {"rounds",
             static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeUInt),
             &isColumnConst,
             "UInt8/UInt16/UInt32/UInt64 (constant)"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        unsigned rounds = default_rounds;
        if (arguments.size() == 2)
        {
            const UInt64 r = arguments[1].column->getUInt(0);
            if (r == 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second argument of function {} must be a positive integer constant", getName());
            rounds = static_cast<unsigned>(std::min<UInt64>(r, std::numeric_limits<unsigned>::max()));
        }

        const IColumn * column = arguments[0].column.get();
        auto result = ColumnUInt8::create(input_rows_count);
        auto & result_data = result->getData();

        if (!castTypeToEither<ColumnUInt8, ColumnUInt16, ColumnUInt32, ColumnUInt64, ColumnVector<UInt128>, ColumnVector<UInt256>>(
                column,
                [&](const auto & col)
                {
                    const auto & data = col.getData();
                    for (size_t i = 0; i < input_rows_count; ++i)
                        result_data[i] = Primality::isProbablePrime(data[i], rounds);
                    return true;
                }))
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(),
                getName());

        return result;
    }
};

}

REGISTER_FUNCTION(IsProbablePrime)
{
    FunctionDocumentation::Description description = R"(
Returns `1` if the argument is probably prime, `0` if it is definitely composite.

For `UInt8`, `UInt16`, `UInt32`, and `UInt64`, the result is exact and matches
[`isPrime`](/sql-reference/functions/math-functions#isPrime). The `rounds` argument is ignored.

For `UInt128` and `UInt256`, a return value of `1` is probabilistic. The optional `rounds` argument controls
how many [Miller-Rabin](https://en.wikipedia.org/wiki/Miller-Rabin_primality_test) rounds are used:
more rounds reduce the chance of a false positive and increase the running time. For any composite,
the false-positive rate is bounded by `4^(-rounds)`; the default of `25` keeps it below `10^-15`.

The function is deterministic: the same input and `rounds` value always produce the same result.
    )";
    FunctionDocumentation::Syntax syntax = "isProbablePrime(n[, rounds])";
    FunctionDocumentation::Arguments arguments
        = {{"n", "Unsigned integer to test for primality.", {"UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256"}},
           {"rounds",
            "Optional positive integer constant. Number of Miller-Rabin rounds for `UInt128`/`UInt256` (ignored for narrower types). "
            "Default `25`.",
            {"UInt8", "UInt16", "UInt32", "UInt64"}}};
    FunctionDocumentation::ReturnedValue returned_value
        = {"Returns `1` if `n` is probably prime, `0` if it is definitely composite.", {"UInt8"}};
    FunctionDocumentation::Examples examples
        = {{"Small prime", "SELECT isProbablePrime(17)", "1"},
           {"Small composite", "SELECT isProbablePrime(18)", "0"},
           {"Largest `UInt64` prime (exact result)", "SELECT isProbablePrime(18446744073709551557)", "1"},
           {"Mersenne prime `M_127` (`UInt128`)", "SELECT isProbablePrime(toUInt128('170141183460469231731687303715884105727'))", "1"},
           {"Curve25519 base field prime `2^255 - 19` (`UInt256`)",
            "SELECT isProbablePrime(toUInt256('57896044618658097711785492504343953926634992332820282019728792003956564819949'))",
            "1"},
           {"Faster, lower-confidence check: 5 rounds",
            "SELECT isProbablePrime(toUInt256('57896044618658097711785492504343953926634992332820282019728792003956564819949'), 5)",
            "1"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIsProbablePrime>(documentation);
}

}
