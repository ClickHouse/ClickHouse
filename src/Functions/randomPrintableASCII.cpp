#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>

#include <cstring>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_STRING_SIZE;
}

namespace
{

/** Generate random string of specified length with printable ASCII characters, almost uniformly distributed.
  * First argument is length, other optional arguments are ignored and used to prevent common subexpression elimination to get different values.
  */
class FunctionRandomPrintableASCII final : public IFunction
{
public:
    static constexpr auto name = "randomPrintableASCII";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionRandomPrintableASCII>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"length", &isNumber, nullptr, "(U)Int*"}
        };

        FunctionArgumentDescriptors optional_args{
            {"x", nullptr, nullptr, "Any"}
        };

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_to = ColumnString::create();
        ColumnString::Chars & data_to = col_to->getChars();
        ColumnString::Offsets & offsets_to = col_to->getOffsets();
        offsets_to.resize(input_rows_count);

        pcg64_fast rng(randomSeed());

        using Words = UInt16 __attribute__((vector_size(16)));
        using Wide = UInt32 __attribute__((vector_size(32)));
        using Bytes = UInt8 __attribute__((vector_size(8)));

        auto generate = [](UInt8 * out, UInt64 rand0, UInt64 rand1)
        {
            const UInt64 rand[2] = {rand0, rand1};
            Words words;
            memcpy(&words, rand, sizeof(words));
            /// Printable characters are from range [32; 126].
            /// https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
            const Words scaled = __builtin_convertvector((__builtin_convertvector(words, Wide) * 95u) >> 16, Words) + 32;
            const Bytes bytes = __builtin_convertvector(scaled, Bytes);
            memcpy(out, &bytes, sizeof(bytes));
        };

        const IColumn & length_column = *arguments[0].column;

        IColumn::Offset offset = 0;
        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        {
            size_t length = length_column.getUInt(row_num);
            if (length > (1 << 30))
                throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large string size in function {}", getName());

            IColumn::Offset next_offset = offset + length;
            data_to.resize(next_offset);
            offsets_to[row_num] = next_offset;

            auto * data_to_ptr = data_to.data();    /// avoid assert on array indexing after end
            size_t pos = offset;
            const size_t end = offset + length;
            /// Eight characters per iteration from two 64-bit random values, 16 bits each, scaled with a vector
            /// multiply-high and packed to bytes: one `pmulhuw` plus `packuswb` on x86. We have padding in column
            /// buffers that we can overwrite, so the tail of up to four characters is produced the same way.
            for (; pos + 4 < end; pos += 8)
                generate(data_to_ptr + pos, rng(), rng());
            if (pos < end)
                generate(data_to_ptr + pos, rng(), 0);

            offset = next_offset;
        }

        return col_to;
    }
};

}

REGISTER_FUNCTION(RandomPrintableASCII)
{
    FunctionDocumentation::Description description = R"(
Generates a random [ASCII](https://en.wikipedia.org/wiki/ASCII#Printable_characters) string with the specified number of characters.

If you pass `length < 0`, the behavior of the function is undefined.
    )";
    FunctionDocumentation::Syntax syntax = "randomPrintableASCII(length[, x])";
    FunctionDocumentation::Arguments arguments = {
        {"length", "String length in bytes.", {"(U)Int*"}},
        {"x", "Optional and ignored. The only purpose of the argument is to prevent [common subexpression elimination](/reference/functions/regular-functions/overview#common-subexpression-elimination) when the same function call is used multiple times in a query.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string with a random set of ASCII printable characters.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT number, randomPrintableASCII(30) AS str, length(str) FROM system.numbers LIMIT 3", R"(
┌─number─┬─str────────────────────────────┬─length(randomPrintableASCII(30))─┐
│      0 │ SuiCOSTvC0csfABSw=UcSzp2.`rv8x │                               30 │
│      1 │ 1Ag NlJ &RCN:*>HVPG;PE-nO"SUFD │                               30 │
│      2 │ /"+<"with:=LjJ Vm!c&hI*m#XTfzz │                               30 │
└────────┴────────────────────────────────┴──────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionRandomPrintableASCII>(documentation);
}

}
