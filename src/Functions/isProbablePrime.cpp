#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
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

/// `4^-max_rounds` is below `10^-150` — well past the point of diminishing returns. The cap also
/// keeps the worst case bounded under the AST fuzzer (which can rewrite the `rounds` literal to
/// `std::numeric_limits<unsigned>::max()` and hang the stress test for half an hour, see #101354).
constexpr unsigned max_rounds = 256;

class FunctionIsProbablePrime : public IFunction
{
public:
    static constexpr auto name = "isProbablePrime";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionIsProbablePrime>(context); }

    explicit FunctionIsProbablePrime(ContextPtr context)
        : process_list_element(context ? context->getProcessListElement() : nullptr)
    {
    }

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
            if (r > max_rounds)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Second argument of function {} must not exceed {} (got {}); larger values offer no meaningful "
                    "improvement in confidence and can take too long to compute",
                    getName(), max_rounds, r);
            rounds = static_cast<unsigned>(r);
        }

        const QueryStatusPtr query_status = process_list_element;
        auto check_cancelled = [&query_status]
        {
            /// `checkTimeLimit` covers both `KILL QUERY` (via `is_killed`) and `max_execution_time`,
            /// throwing the appropriate exception when either fires.
            if (query_status)
                query_status->checkTimeLimit();
        };

        const IColumn * column = arguments[0].column.get();
        auto result = ColumnUInt8::create(input_rows_count);
        auto & result_data = result->getData();

        if (!castTypeToEither<ColumnUInt8, ColumnUInt16, ColumnUInt32, ColumnUInt64, ColumnVector<UInt128>, ColumnVector<UInt256>>(
                column,
                [&](const auto & col)
                {
                    const auto & data = col.getData();
                    using ValueT = std::decay_t<decltype(data[0])>;
                    for (size_t i = 0; i < input_rows_count; ++i)
                    {
                        if constexpr (Primality::is_big_uint<ValueT>)
                            result_data[i] = Primality::isProbablePrime(data[i], rounds, check_cancelled);
                        else
                            result_data[i] = Primality::isProbablePrime(data[i], rounds);
                    }
                    return true;
                }))
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(),
                getName());

        return result;
    }

private:
    QueryStatusPtr process_list_element;
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
more rounds reduce the chance of a false positive and increase the running time. With uniformly random
witnesses, the false-positive rate for a fixed composite is bounded by `4^(-rounds)`; the default of `25`
makes this bound smaller than `10^-15`. Values above `256` are rejected as `BAD_ARGUMENTS`, since they
offer no meaningful improvement and only waste time.

The function is deterministic: witnesses are derived from a fixed seed computed from `n`, so the same
`(n, rounds)` pair always produces the same result. As a consequence, a composite that happens to pass
this particular witness sequence will reproducibly return `1`, rather than failing the test independently
on each call. The `4^(-rounds)` bound should therefore be read as a guide to typical accuracy across
inputs, not as a per-call probability for a fixed input.
    )";
    FunctionDocumentation::Syntax syntax = "isProbablePrime(n[, rounds])";
    FunctionDocumentation::Arguments arguments
        = {{"n", "Unsigned integer to test for primality.", {"UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256"}},
           {"rounds",
            "Optional positive integer constant in `[1, 256]`. Number of Miller-Rabin rounds for `UInt128`/`UInt256` "
            "(ignored for narrower types). Default `25`.",
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
