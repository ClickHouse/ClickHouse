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
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_COMPILE_REGEXP;
}


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
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionExtractGroups>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"haystack", isStringOrFixedString, nullptr, "const String or const FixedString"},
            {"needle", isStringOrFixedString, isColumnConst, "const String or const FixedString"},
        };
        validateFunctionArgumentTypes(*this, arguments, args);

        /// Two-dimensional array of strings, each `row` of top array represents matching groups.
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const ColumnPtr column_haystack = block.getByPosition(arguments[0]).column;
        const ColumnPtr column_needle = block.getByPosition(arguments[1]).column;

        const auto needle = typeid_cast<const ColumnConst &>(*column_needle).getValue<String>();

        if (needle.empty())
            throw Exception(getName() + " length of 'needle' argument must be greater than 0.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        const auto regexp = Regexps::get<false, false>(needle);
        const size_t groups_count = regexp->getNumberOfSubpatterns();

        // Including 0-group, which is the whole regexp.
        OptimizedRegularExpression::MatchVec matched_groups(groups_count + 1);

        ColumnArray::ColumnOffsets::MutablePtr offsets_col = ColumnArray::ColumnOffsets::create();
        ColumnString::MutablePtr data_col = ColumnString::create();

        auto & offsets_data = offsets_col->getData();

        offsets_data.resize(input_rows_count);
        ColumnArray::Offset current_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto & current_row = column_haystack->getDataAt(i);

            if (regexp->match(current_row.data, current_row.size, matched_groups, groups_count + 1))
            {
                // 1 is to exclude group #0 which is whole re match.
                for (size_t group = 1; group <= groups_count; ++group)
                    data_col->insertData(current_row.data + matched_groups[group].offset, matched_groups[group].length);

                current_offset += groups_count;
            }

            offsets_data[i] = current_offset;
        }

        block.getByPosition(result).column = ColumnArray::create(std::move(data_col), std::move(offsets_col));
    }
};

void registerFunctionExtractGroups(FunctionFactory & factory)
{
    factory.registerFunction<FunctionExtractGroups>();
}

}

