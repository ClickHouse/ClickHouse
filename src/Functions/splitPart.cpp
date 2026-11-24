#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/Exception.h>
#include <Common/FunctionDocumentation.h>

#include <limits>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{

/// The field number `position` of `string` split at the occurrences of `delimiter`, by the rules of
/// `split_part` of PostgreSQL. The fields are counted from 1, a negative position counts them from the end,
/// and a field that does not exist is an empty string. `position` is not zero.
std::string_view splitPart(std::string_view string, std::string_view delimiter, Int64 position)
{
    if (string.empty())
        return {};

    /// With an empty delimiter the whole string is the only field.
    if (delimiter.empty())
        return (position == 1 || position == -1) ? string : std::string_view{};

    if (position < 0)
    {
        /// The fields are the ones of a scan from the left also when they are counted from the end:
        /// `split_part('aaa', 'aa', -1)` is `a`, not an empty string.
        Int64 number_of_fields = 1;
        for (size_t pos = string.find(delimiter); pos != std::string_view::npos; pos = string.find(delimiter, pos + delimiter.size()))
            ++number_of_fields;

        position += number_of_fields + 1;
        if (position <= 0)
            return {};
    }

    size_t begin = 0;
    for (Int64 field = 1;; ++field)
    {
        const size_t end = string.find(delimiter, begin);
        if (field == position)
            return string.substr(begin, end == std::string_view::npos ? std::string_view::npos : end - begin);
        if (end == std::string_view::npos)
            return {};
        begin = end + delimiter.size();
    }
}

/// `split_part(string, delimiter, position)` - PostgreSQL compatibility function.
/// The native counterpart is `splitByString(delimiter, string)[position]`, which differs in what it does with
/// an empty delimiter (it splits the string into characters) and with a negative position.
class FunctionSplitPart final : public IFunction
{
public:
    static constexpr auto name = "split_part";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSplitPart>(); }

    [[nodiscard]] String getName() const override { return name; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 3; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    /// A zero position is not valid, and it is what a row behind a `NULL` holds.
    [[nodiscard]] bool canBeExecutedOnDefaultArguments() const override { return false; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"string", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
            {"delimiter", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
            {"position", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeInteger), nullptr, "Integer"},
        };
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeString>();
    }

    [[nodiscard]] DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn & string_column = *arguments[0].column;
        const IColumn & delimiter_column = *arguments[1].column;
        const IColumn & position_column = *arguments[2].column;
        const bool position_is_uint64 = WhichDataType(arguments[2].type).isUInt64();

        auto result = ColumnString::create();
        result->reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            /// A position that does not fit into `Int64` is beyond every field.
            if (position_is_uint64 && position_column.getUInt(i) > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            {
                result->insertDefault();
                continue;
            }

            const Int64 position = position_column.getInt(i);
            if (position == 0)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "The field position of function {} must not be zero", getName());

            const std::string_view part = splitPart(string_column.getDataAt(i), delimiter_column.getDataAt(i), position);
            if (part.empty())
                result->insertDefault();
            else
                result->insertData(part.data(), part.size());
        }

        return result;
    }
};

}

REGISTER_FUNCTION(SplitPart)
{
    FunctionDocumentation::Description description = R"(
PostgreSQL compatibility function. Splits a string at the occurrences of a delimiter and returns one of the fields.

The fields are counted from 1, and a negative `position` counts them from the end: `-1` is the last field.
A field that does not exist is an empty string, and a `position` of zero is an error.
If the delimiter is empty, the whole string is the only field.

The native counterpart is `splitByString(delimiter, s)[position]`, which splits the string into characters if the delimiter is empty.
)";
    FunctionDocumentation::Syntax syntax = "split_part(s, delimiter, position)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "The string to split.", {"String"}},
        {"delimiter", "The delimiter.", {"String"}},
        {"position", "The number of the field, counted from 1, or from the end if it is negative.", {"(U)Int*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the field, or an empty string if there is no such field.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT split_part('a,b,c', ',', 2)", "b"},
        {"From the end", "SELECT split_part('a,b,c', ',', -1)", "c"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSplitting;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSplitPart>(documentation, FunctionFactory::Case::Insensitive);
}

}
