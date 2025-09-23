#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_LARGE_STRING_SIZE;
}

namespace
{

/** Generate random string of specified length with printable ASCII characters, almost uniformly distributed.
  * First argument is length, other optional arguments are ignored and used to prevent common subexpression elimination to get different values.
  */
class FunctionRandomPrintableASCII : public IFunction
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires at least one argument: the size of resulting string", getName());

        if (arguments.size() > 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires at most two arguments: the size of resulting string and optional disambiguation tag", getName());

        const IDataType & length_type = *arguments[0];
        if (!isNumber(length_type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must have numeric type", getName());

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
            for (size_t pos = offset, end = offset + length; pos < end; pos += 4)    /// We have padding in column buffers that we can overwrite.
            {
                UInt64 rand = rng();

                UInt16 rand1 = rand;
                UInt16 rand2 = rand >> 16;
                UInt16 rand3 = rand >> 32;
                UInt16 rand4 = rand >> 48;

                /// Printable characters are from range [32; 126].
                /// https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/

                data_to_ptr[pos + 0] = 32 + ((rand1 * 95) >> 16);
                data_to_ptr[pos + 1] = 32 + ((rand2 * 95) >> 16);
                data_to_ptr[pos + 2] = 32 + ((rand3 * 95) >> 16);
                data_to_ptr[pos + 3] = 32 + ((rand4 * 95) >> 16);

                /// NOTE gcc failed to vectorize this code (aliasing of char?)
            }

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
        {"x", "Optional and ignored. The only purpose of the argument is to prevent [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) when the same function call is used multiple times in a query.", {"Any"}}
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
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionRandomPrintableASCII>(documentation);
}

}
