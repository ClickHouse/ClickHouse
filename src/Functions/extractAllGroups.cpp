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
 *  SELECT extractAllGroups('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')
 * should produce:
 *   [['abc','def','ghi'], ['111','333','333']]
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

        // twodimensional array of strings, each `row` of root array represents a group match.
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()));
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const ColumnPtr column_haystack = block.getByPosition(arguments[0]).column;
        const ColumnPtr column_needle = block.getByPosition(arguments[1]).column;

        const auto needle = typeid_cast<const ColumnConst &>(*column_needle).getDataAt(0);

        if (needle.size == 0)
            throw Exception(getName() + " length of 'needle' argument must be greater than 0.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        re2_st::RE2 re{re2_st::StringPiece(needle.data, needle.size)};
        if (!re.ok())
            throw Exception(getName() + " invalid regular expression: " + re.error(), ErrorCodes::CANNOT_COMPILE_REGEXP);

        const size_t groups_count = re.NumberOfCapturingGroups();
        std::vector<re2_st::StringPiece> all_matches;
        // number of times RE matched on each row of haystack column.
        std::vector<size_t> number_of_matches_per_row;

        // we expect RE to match multiple times on each row, `* 8` is arbitrary to reduce number of re-allocations.
        all_matches.reserve(input_rows_count * groups_count * 8);
        number_of_matches_per_row.reserve(input_rows_count);

        // including 0-group, which is the whole RE
        std::vector<re2_st::StringPiece> matched_groups(groups_count + 1);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t matches_per_row = 0;
            const auto & current_row = column_haystack->getDataAt(i);
            const auto haystack = re2_st::StringPiece(current_row.data, current_row.size);

            // Extract all non-intersecting matches from haystack except group #0.
            size_t start_pos = 0;
            while (start_pos < haystack.size() && re.Match(haystack,
                    start_pos, haystack.size(),
                    re2_st::RE2::UNANCHORED,
                    matched_groups.data(), matched_groups.size()))
            {
                // +1 is to exclude group #0 which is whole re match.
                all_matches.insert(all_matches.end(), matched_groups.begin() + 1, matched_groups.end());
                start_pos = (matched_groups[0].begin() - haystack.begin()) + matched_groups[0].size();

                ++matches_per_row;
            }

            number_of_matches_per_row.push_back(matches_per_row);
        }

        ColumnString::MutablePtr data_col = ColumnString::create();
        {
            size_t total_matched_groups_string_len = 0;
            for (const auto & m : all_matches)
                total_matched_groups_string_len += m.length();

            data_col->reserve(total_matched_groups_string_len);
        }

        ColumnArray::ColumnOffsets::MutablePtr nested_offsets_col = ColumnArray::ColumnOffsets::create();
        ColumnArray::ColumnOffsets::MutablePtr root_offsets_col = ColumnArray::ColumnOffsets::create();
        nested_offsets_col->reserve(matched_groups.size());
        root_offsets_col->reserve(groups_count);

        // Re-arrange `all_matches` from:
        // [
        //      "ROW 0: 1st group 1st match",
        //      "ROW 0: 2nd group 1st match",
        //      ...,
        //      "ROW 0: 1st group 2nd match",
        //      "ROW 0: 2nd group 2nd match",
        //      ...,
        //      "ROW 1: 1st group 1st match",
        //      ...
        // ]
        //
        // into column of 2D arrays:
        // [
        //      /* all matchig groups from ROW 0 of haystack column */
        //      ["ROW 0: 1st group 1st match", "ROW 0: 1st group 2nd match", ...],
        //      ["ROW 0: 2nd group 1st match", "ROW 0: 2nd group 2nd match", ...],
        //      ...
        // ],
        // [
        //      /* all matchig groups from row 1 of haystack column */
        //      ["ROW 1: 1st group 1st match", ...],
        //      ...
        // ]

        size_t row_offset = 0;
        for (const auto matches_per_row : number_of_matches_per_row)
        {
            const size_t next_row_offset = row_offset + matches_per_row * groups_count;
            for (size_t group_id = 0; group_id < groups_count; ++group_id)
            {
                for (size_t i = row_offset + group_id; i < next_row_offset && i < all_matches.size(); i += groups_count)
                {
                    const auto & match = all_matches[i];
                    data_col->insertData(match.begin(), match.length());
                }
                nested_offsets_col->insertValue(data_col->size());
            }
            root_offsets_col->insertValue(nested_offsets_col->size());
            row_offset = next_row_offset;
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
