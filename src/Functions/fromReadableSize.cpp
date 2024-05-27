#include <base/types.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/convert.hpp>
#include <boost/convert/strtol.hpp>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <limits>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

const std::unordered_map<std::string_view, UInt64> size_unit_to_bytes =
{
    {"b", 1L},
    // ISO/IEC 80000-13 binary units
    {"kib", 1024L},
    {"mib", 1024L * 1024L},
    {"gib", 1024L * 1024L * 1024L},
    {"tib", 1024L * 1024L * 1024L * 1024L},
    {"pib", 1024L * 1024L * 1024L * 1024L * 1024L},
    {"eib", 1024L * 1024L * 1024L * 1024L * 1024L * 1024L},

    // SI units
    {"kb", 1000L},
    {"mb", 1000L * 1000L},
    {"gb", 1000L * 1000L * 1000L},
    {"tb", 1000L * 1000L * 1000L * 1000L},
    {"pb", 1000L * 1000L * 1000L * 1000L * 1000L},
    {"eb", 1000L * 1000L * 1000L * 1000L * 1000L * 1000L},
};

class FunctionFromReadableSize : public IFunction
{
public:
    static constexpr auto name = "fromReadableSize";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFromReadableSize>(); }
    String getName() const override { return name; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args
        {
            {"readable_size", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
        };
        validateFunctionArgumentTypes(*this, arguments, args);

        return std::make_shared<DataTypeUInt64>();
    }

    

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_to = ColumnUInt64::create();
        auto & res_data = col_to->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {   
            std::string_view str = arguments[0].column->getDataAt(i).toView();
            ReadBufferFromString buf(str);
            // tryReadFloatText does seem to not raise any error when there is leading whitespace so we cehck for it explicitly
            skipWhitespaceIfAny(buf);
            if (buf.getPosition() > 0)
                throw_bad_arguments("Leading whitespace is not allowed", str);

            Float64 base = 0;
            if (!tryReadFloatText(base, buf))
                throw_bad_arguments("Unable to parse readable size numeric component", str);

            skipWhitespaceIfAny(buf);

            String unit;
            readStringUntilWhitespace(unit, buf);
            if (!buf.eof())
                throw_bad_arguments("Found trailing characters after readable size string", str);
            boost::algorithm::to_lower(unit);
            auto iter = size_unit_to_bytes.find(unit);
            if (iter == size_unit_to_bytes.end())
                throw_bad_arguments("Unknown readable size unit", unit);

            Float64 raw_num_bytes = base * iter->second;
            if (raw_num_bytes > std::numeric_limits<UInt64>::max())
                throw_bad_arguments("Result is too big for output type (UInt64)", raw_num_bytes);
            // As the input might be an arbitrary decimal number we might end up with a non-integer amount of bytes when parsing binary (eg MiB) units.
            // This doesn't make sense so we round up to indicate the byte size that can fit the passed size.
            UInt64 result = static_cast<UInt64>(std::ceil(raw_num_bytes));

            res_data.emplace_back(result);
        }

        return col_to;
    }


private:

    template <typename Arg>
    void throw_bad_arguments(const String & msg, Arg arg) const
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid expression for function {} - {} (\"{}\")", getName(), msg, arg);
    }

};
}

REGISTER_FUNCTION(FromReadableSize)
{
    factory.registerFunction<FunctionFromReadableSize>(FunctionDocumentation
        {
            .description=R"(
Given a string containing the readable representation of a byte size, this function returns the corresponding number of bytes:
[example:basic_binary]
[example:basic_decimal]

If the resulting number of bytes has a non-zero decimal part, the result is rounded up to indicate the number of bytes necessary to accommodate the provided size.
[example:round]

Accepts readable sizes up to the Exabyte (EB/EiB).

It always returns an UInt64 value.
)",
            .examples{
                {"basic_binary", "SELECT fromReadableSize('1 KiB')", "1024"},
                {"basic_decimal", "SELECT fromReadableSize('1.523 KB')", "1523"},
                {"round", "SELECT fromReadableSize('1.0001 KiB')", "1025"},
            },
            .categories{"OtherFunctions"}
        }
    );
}

}
