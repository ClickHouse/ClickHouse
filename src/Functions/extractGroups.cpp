#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/Regexps.h>

#include <memory>
#include <string>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/** Match all groups of given input string with given re, return array of arrays of matches.
 *
 *  SELECT extractGroups('hello abc=111 world', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')
 * should produce:
 *   ['abc', '111']
 */
class FunctionExtractGroups : public IFunction
{
public:
    static constexpr auto name = "extractGroups";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionExtractGroups>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"haystack", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "const String or const FixedString"},
            {"needle", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), isColumnConst, "const String or const FixedString"},
        };
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr column_haystack = arguments[0].column;
        const ColumnPtr column_needle = arguments[1].column;

        const auto needle = typeid_cast<const ColumnConst &>(*column_needle).getValue<String>();

        if (needle.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} length of 'needle' argument must be greater than 0.", getName());

        const OptimizedRegularExpression regexp = Regexps::createRegexp<false, false, false>(needle);
        const auto & re2 = regexp.getRE2();

        if (!re2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "There are no groups in regexp: {}", needle);

        const size_t groups_count = re2->NumberOfCapturingGroups();

        if (!groups_count)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "There are no groups in regexp: {}", needle);

        // Including 0-group, which is the whole regexp.
        PODArrayWithStackMemory<std::string_view, 128> matched_groups(groups_count + 1);

        ColumnArray::ColumnOffsets::MutablePtr offsets_col = ColumnArray::ColumnOffsets::create();
        ColumnString::MutablePtr data_col = ColumnString::create();

        auto & offsets_data = offsets_col->getData();

        offsets_data.resize(input_rows_count);
        ColumnArray::Offset current_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view current_row = column_haystack->getDataAt(i);

            if (re2->Match({current_row.data(), current_row.size()},
                0, current_row.size(), re2::RE2::UNANCHORED, matched_groups.data(),
                static_cast<int>(matched_groups.size())))
            {
                // 1 is to exclude group #0 which is whole re match.
                for (size_t group = 1; group <= groups_count; ++group)
                    data_col->insertData(matched_groups[group].data(), matched_groups[group].size());

                current_offset += groups_count;
            }

            offsets_data[i] = current_offset;
        }

        return ColumnArray::create(std::move(data_col), std::move(offsets_col));
    }
};

}

REGISTER_FUNCTION(ExtractGroups)
{
    FunctionDocumentation::Description description = R"(
Extracts all groups from non-overlapping substrings matched by a regular expression.
    )";
    FunctionDocumentation::Syntax syntax = "extractGroups(s, regexp)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "Input string to extract from.", {"String", "FixedString"}},
        {"regexp", "Regular expression. Constant.", {"const String", "const FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"If the function finds at least one matching group, it returns Array(Array(String)) column, clustered by group_id (`1` to `N`, where `N` is number of capturing groups in regexp). If there is no matching group, it returns an empty array.", {"Array(Array(String))"}};
    FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
WITH '< Server: nginx
< Date: Tue, 22 Jan 2019 00:26:14 GMT
< Content-Type: text/html; charset=UTF-8
< Connection: keep-alive
' AS s
SELECT extractGroups(s, '< ([\\w\\-]+): ([^\\r\\n]+)');
)",
            R"(
[['Server','nginx'],['Date','Tue, 22 Jan 2019 00:26:14 GMT'],['Content-Type','text/html; charset=UTF-8'],['Connection','keep-alive']]
    )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionExtractGroups>(documentation);
}

}

