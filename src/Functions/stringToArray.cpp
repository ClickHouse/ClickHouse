#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/Exception.h>
#include <Common/FunctionDocumentation.h>
#include <Common/UTF8Helpers.h>

#include <algorithm>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// `string_to_array(string, delimiter[, null_string])` - PostgreSQL compatibility function.
/// The native counterpart is `splitByString(delimiter, string)`. The rules of PostgreSQL that differ from it:
/// - an empty string gives an empty array, not an array with one empty string;
/// - with an empty delimiter the whole string is the only element, it is not split into characters;
/// - a `NULL` delimiter splits the string into characters;
/// - an element equal to `null_string` is replaced by `NULL`, if `null_string` is given and is not `NULL`.
/// A `NULL` delimiter and a `NULL` `null_string` are values with a meaning of their own, which is why the
/// function handles `NULL` arguments itself. The one rule it cannot follow is that a `NULL` string gives
/// `NULL`: an array cannot be `NULL`, so the result is an empty array, as for other functions that return
/// an array.
class FunctionStringToArray final : public IFunction
{
public:
    static constexpr auto name = "string_to_array";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionStringToArray>(); }

    [[nodiscard]] String getName() const override { return name; }
    [[nodiscard]] bool isVariadic() const override { return true; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 0; }
    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, expected 2 or 3",
                getName(), arguments.size());

        for (const auto & argument : arguments)
        {
            const DataTypePtr type = removeNullable(argument);
            if (!isString(type) && !isNothing(type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of an argument of function {}: expected String",
                    argument->getName(), getName());
        }

        /// Only the form with `null_string` can produce a `NULL` element.
        DataTypePtr element_type = std::make_shared<DataTypeString>();
        if (arguments.size() == 3)
            element_type = std::make_shared<DataTypeNullable>(element_type);
        return std::make_shared<DataTypeArray>(element_type);
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn & string_column = *arguments[0].column;
        const IColumn & delimiter_column = *arguments[1].column;
        const IColumn * null_string_column = arguments.size() == 3 ? arguments[2].column.get() : nullptr;

        auto elements = ColumnString::create();
        auto elements_null_map = ColumnUInt8::create();
        auto offsets = ColumnArray::ColumnOffsets::create();
        auto & elements_null_map_data = elements_null_map->getData();
        auto & offsets_data = offsets->getData();
        offsets_data.reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const bool has_null_string = null_string_column && !null_string_column->isNullAt(i);
            const std::string_view null_string = has_null_string ? null_string_column->getDataAt(i) : std::string_view{};

            const auto add_element = [&](std::string_view element)
            {
                const bool is_null = has_null_string && element == null_string;
                if (is_null || element.empty())
                    elements->insertDefault();
                else
                    elements->insertData(element.data(), element.size());

                if (null_string_column)
                    elements_null_map_data.push_back(is_null);
            };

            /// A `NULL` string and an empty string have no elements.
            const std::string_view string = string_column.isNullAt(i) ? std::string_view{} : string_column.getDataAt(i);
            if (!string.empty())
            {
                if (delimiter_column.isNullAt(i))
                {
                    /// Every character is an element.
                    size_t pos = 0;
                    while (pos < string.size())
                    {
                        const size_t length = std::min(UTF8::seqLength(static_cast<UInt8>(string[pos])), string.size() - pos);
                        add_element(string.substr(pos, length));
                        pos += length;
                    }
                }
                else
                {
                    const std::string_view delimiter = delimiter_column.getDataAt(i);

                    /// With an empty delimiter the loop ends at once, and the whole string is the only element.
                    size_t begin = 0;
                    size_t end = delimiter.empty() ? std::string_view::npos : string.find(delimiter, begin);
                    while (end != std::string_view::npos)
                    {
                        add_element(string.substr(begin, end - begin));
                        begin = end + delimiter.size();
                        end = string.find(delimiter, begin);
                    }
                    add_element(string.substr(begin));
                }
            }

            offsets_data.push_back(elements->size());
        }

        if (null_string_column)
            return ColumnArray::create(ColumnNullable::create(std::move(elements), std::move(elements_null_map)), std::move(offsets));
        return ColumnArray::create(std::move(elements), std::move(offsets));
    }
};

}

REGISTER_FUNCTION(StringToArray)
{
    FunctionDocumentation::Description description = R"(
PostgreSQL compatibility function. Splits a string at the occurrences of a delimiter into an array of substrings.

The native counterpart is [`splitByString`](/reference/functions/regular-functions/splitting-merging-functions#splitByString)`(delimiter, s)`, which takes the arguments in the other order. The function follows the rules of PostgreSQL where they differ:
- An empty string gives an empty array.
- If the delimiter is empty, the whole string is the only element of the array.
- If the delimiter is `NULL`, every character of the string is an element of the array.
- If `null_string` is given and is not `NULL`, the elements equal to it are replaced by `NULL`.

A `NULL` string gives an empty array, not `NULL`, because an array cannot be `NULL`.
)";
    FunctionDocumentation::Syntax syntax = "string_to_array(s, delimiter[, null_string])";
    FunctionDocumentation::Arguments arguments = {
        {"s", "The string to split.", {"String"}},
        {"delimiter", "The delimiter, or `NULL` to split the string into characters.", {"String"}},
        {"null_string", "Optional. The elements equal to this string are replaced by `NULL`.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of the substrings of `s`. With `null_string`, the elements of the array are `Nullable`.", {"Array(String)", "Array(Nullable(String))"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT string_to_array('a,b,c', ',')", "['a','b','c']"},
        {"With null_string", "SELECT string_to_array('a,*,c', ',', '*')", "['a',NULL,'c']"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSplitting;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionStringToArray>(documentation, FunctionFactory::Case::Insensitive);
}

}
