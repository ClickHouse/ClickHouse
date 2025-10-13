#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionsRandom.h>
#include <Functions/PerformanceAdaptors.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>
#include <base/unaligned.h>


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

/* Generate random string of specified length with fully random bytes (including zero). */
template <typename RandImpl>
class FunctionRandomStringImpl : public IFunction
{
public:
    static constexpr auto name = "randomString";

    String getName() const override { return name; }

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

        if (input_rows_count == 0)
            return col_to;

        /// Fill offsets.
        offsets_to.resize(input_rows_count);
        const IColumn & length_column = *arguments[0].column;

        IColumn::Offset offset = 0;
        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        {
            size_t length = length_column.getUInt(row_num);
            if (length > (1 << 30))
                throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large string size in function {}", getName());

            offset += length;
            offsets_to[row_num] = offset;
        }

        /// Fill random bytes.
        data_to.resize(offsets_to.back());
        RandImpl::execute(reinterpret_cast<char *>(data_to.data()), data_to.size());
        return col_to;
    }
};

class FunctionRandomString : public FunctionRandomStringImpl<TargetSpecific::Default::RandImpl>
{
public:
    explicit FunctionRandomString(ContextPtr context) : selector(context)
    {
        selector.registerImplementation<TargetArch::Default,
            FunctionRandomStringImpl<TargetSpecific::Default::RandImpl>>();

    #if USE_MULTITARGET_CODE
        selector.registerImplementation<TargetArch::AVX2,
            FunctionRandomStringImpl<TargetSpecific::AVX2::RandImpl>>();
    #endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionRandomString>(context);
    }

private:
    ImplementationSelector<IFunction> selector;
};

}

REGISTER_FUNCTION(RandomString)
{
    FunctionDocumentation::Description description = R"(
Generates a random string with the specified number of characters.
The returned characters are not necessarily ASCII characters, i.e. they may not be printable.
    )";
    FunctionDocumentation::Syntax syntax = "randomString(length[, x])";
    FunctionDocumentation::Arguments arguments = {
        {"length", "Length of the string in bytes.", {"(U)Int*"}},
        {"x", "Optional and ignored. The only purpose of the argument is to prevent [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) when the same function call is used multiple times in a query.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string filled with random bytes.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT randomString(5) AS str FROM numbers(2)", R"(
���
�v6B�
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionRandomString>(documentation);
}

}
