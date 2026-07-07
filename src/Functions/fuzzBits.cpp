#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>
#include <base/arithmeticOverflow.h>

#include <memory>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int DECIMAL_OVERFLOW;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


namespace
{
    inline UInt8 getXorMask(UInt64 rand, double prob)
    {
        UInt8 res = 0;
        for (int i = 0; i < 8; ++i)
        {
            UInt8 rand8 = rand;
            rand >>= 8;
            res <<= 1;
            res |= (rand8 < prob * (1u << 8));
        }
        return res;
    }
    void fuzzBits(const char8_t * ptr_in, char8_t * ptr_out, size_t len, double prob)
    {
        pcg64_fast rng(randomSeed()); // TODO It is inefficient. We should use SIMD PRNG instead.

        for (size_t i = 0; i < len; ++i)
        {
            UInt64 rand = rng();
            auto mask = getXorMask(rand, prob);
            ptr_out[i] = ptr_in[i] ^ mask;
        }
    }


class FunctionFuzzBits : public IFunction
{
public:
    static constexpr auto name = "fuzzBits";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFuzzBits>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; } // indexing from 0

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must be String or FixedString",
                getName());

        if (!arguments[1].column || !isFloat(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument of function {} must be constant float", getName());

        return arguments[0].type;
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_in_untyped = arguments[0].column;

        if (input_rows_count == 0)
            return col_in_untyped;

        const double inverse_probability = assert_cast<const ColumnConst &>(*arguments[1].column).getValue<double>();

        if (inverse_probability < 0.0 || 1.0 < inverse_probability)
        {
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Second argument of function {} must be from `0.0` to `1.0`", getName());
        }

        if (const ColumnConst * col_in_untyped_const = checkAndGetColumnConstStringOrFixedString(col_in_untyped.get()))
        {
            col_in_untyped = col_in_untyped_const->getDataColumnPtr();
        }

        if (const ColumnString * col_in = checkAndGetColumn<ColumnString>(col_in_untyped.get()))
        {
            auto col_to = ColumnString::create();
            ColumnString::Chars & chars_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();

            chars_to.resize(col_in->getChars().size());
            const auto * ptr_in = col_in->getChars().data();
            auto * ptr_to = chars_to.data();
            fuzzBits(ptr_in, ptr_to, chars_to.size(), inverse_probability);
            offsets_to.assign(col_in->getOffsets().begin(), col_in->getOffsets().end());

            return col_to;
        }

        if (const ColumnFixedString * col_in_fixed = checkAndGetColumn<ColumnFixedString>(col_in_untyped.get()))
        {
            const auto n = col_in_fixed->getN();
            auto col_to = ColumnFixedString::create(n);
            ColumnFixedString::Chars & chars_to = col_to->getChars();

            size_t total_size;
            if (common::mulOverflow(input_rows_count, n, total_size))
                throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal math overflow");

            chars_to.resize(total_size);

            const auto * ptr_in = col_in_fixed->getChars().data();
            auto * ptr_to = chars_to.data();
            fuzzBits(ptr_in, ptr_to, chars_to.size(), inverse_probability);
            return col_to;
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
};

}

REGISTER_FUNCTION(FuzzBits)
{
    FunctionDocumentation::Description description = R"(
Flips the bits of the input string `s`, with probability `p` for each bit.
    )";
    FunctionDocumentation::Syntax syntax = "fuzzBits(s, p)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "String or FixedString to perform bit fuzzing on", {"String", "FixedString"}},
        {"p", "Probability of flipping each bit as a number between `0.0` and `1.0`", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a Fuzzed string with same type as `s`.", {"String", "FixedString"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT fuzzBits(materialize('abacaba'), 0.1)
FROM numbers(3)
        )",
        R"(
┌─fuzzBits(materialize('abacaba'), 0.1)─┐
│ abaaaja                               │
│ a*cjab+                               │
│ aeca2A                                │
└───────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionFuzzBits>(documentation);
}
}
