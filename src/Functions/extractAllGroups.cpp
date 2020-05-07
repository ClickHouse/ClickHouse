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
}


/** Match all groups of given input string with given re, return array of arrays of matches.
 *
 *  SELECT extractAllGroups('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')
 * should produce:
 *   [['abc', '111'], ['def', '222'], ['ghi', '333']]
 */
class FunctionExtractAllGroups : public IFunction
{
public:
    static constexpr auto name = "extractAllGroups";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionExtractAllGroups>(); }

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
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()));
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const ColumnPtr column_haystack = block.getByPosition(arguments[0]).column;
        const ColumnPtr column_needle = block.getByPosition(arguments[1]).column;

        const auto needle = typeid_cast<const ColumnConst &>(*column_needle).getValue<String>();

        if (needle.empty())
            throw Exception(getName() + " length of 'needle' argument must be greater than 0.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        const auto regexp = Regexps::get<false, false>(needle);
        const auto & re2 = regexp->getRE2();
        const size_t groups_count = re2->NumberOfCapturingGroups();

        // Including 0-group, which is the whole regexp.
        PODArrayWithStackMemory<re2_st::StringPiece, 128> matched_groups(groups_count + 1);

        ColumnArray::ColumnOffsets::MutablePtr root_offsets_col = ColumnArray::ColumnOffsets::create();
        ColumnArray::ColumnOffsets::MutablePtr nested_offsets_col = ColumnArray::ColumnOffsets::create();
        ColumnString::MutablePtr data_col = ColumnString::create();

        auto & root_offsets_data = root_offsets_col->getData();
        auto & nested_offsets_data = nested_offsets_col->getData();

        root_offsets_data.resize(input_rows_count);
        ColumnArray::Offset current_root_offset = 0;
        ColumnArray::Offset current_nested_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            StringRef current_row = column_haystack->getDataAt(i);

            // Extract all non-intersecting matches from haystack except group #0.
            auto pos = current_row.data;
            auto end = pos + current_row.size;
            while (pos < end
                && re2->Match(re2_st::StringPiece(pos, end - pos),
                    0, end - pos, re2_st::RE2::UNANCHORED, matched_groups.data(), matched_groups.size()))
            {
                // 1 is to exclude group #0 which is whole re match.
                for (size_t group = 1; group <= groups_count; ++group)
                    data_col->insertData(matched_groups[group].data(), matched_groups[group].size());

                pos = matched_groups[0].data() + matched_groups[0].size();

                current_nested_offset += groups_count;
                nested_offsets_data.push_back(current_nested_offset);

                ++current_root_offset;
            }

            root_offsets_data[i] = current_root_offset;
        }
        ColumnArray::MutablePtr nested_array_col = ColumnArray::create(std::move(data_col), std::move(nested_offsets_col));
        ColumnArray::MutablePtr root_array_col = ColumnArray::create(std::move(nested_array_col), std::move(root_offsets_col));
        block.getByPosition(result).column = std::move(root_array_col);
    }
};

void registerFunctionExtractAllGroups(FunctionFactory & factory)
{
    factory.registerFunction<FunctionExtractAllGroups>();
}

}
