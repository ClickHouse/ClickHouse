#include "config.h"

#if USE_H3

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>

#include <h3api.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

class FunctionH3ToString : public IFunction
{
public:
    static constexpr auto name = "h3ToString";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3ToString>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isUInt64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arg->getName(), 1, getName());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * column = checkAndGetColumn<ColumnUInt64>(non_const_arguments[0].column.get());
        if (!column)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64.",
                arguments[0].type->getName(),
                1,
                getName());

        const auto & data = column->getData();

        auto col_res = ColumnString::create();
        auto & vec_res = col_res->getChars();
        auto & vec_offsets = col_res->getOffsets();

        vec_offsets.resize(input_rows_count);
        vec_res.resize(input_rows_count * 16);

        UInt8 * pos = vec_res.data();
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            UInt64 hindex = data[row];

            if (!isValidCell(hindex))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Invalid H3 index: {} in function {}", hindex, getName());

            bool started = false;
            for (size_t i = 0; i < 16; ++i)
            {
                UInt8 nibble = (hindex >> (4 * (15 - i))) & 0xf;
                if (nibble)
                    started = true;
                if (started)
                {
                    *pos = hexDigitLowercase(nibble);
                    ++pos;
                }
            }

            vec_offsets[row] = pos - vec_res.data();
        }

        vec_res.resize(vec_offsets.back());
        return col_res;
    }
};

}

REGISTER_FUNCTION(H3ToString)
{
    FunctionDocumentation::Description description = R"(
Converts the `H3Index` representation of the index to the string representation.
    )";
    FunctionDocumentation::Syntax syntax = "h3ToString(index)";
    FunctionDocumentation::Arguments arguments = {
        {"index", "H3 index number.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the string representation of the H3 index.",
        {"String"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Convert H3 index to string",
            "SELECT h3ToString(617420388352917503) AS h3_string",
            R"(
┌─h3_string───────┐
│ 89184926cdbffff │
└─────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 3};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionH3ToString>(documentation);
}

}

#endif
