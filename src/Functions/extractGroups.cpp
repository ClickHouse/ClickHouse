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
            std::string_view current_row = column_haystack->getDataAt(i).toView();

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
    factory.registerFunction<FunctionExtractGroups>();
}

}

