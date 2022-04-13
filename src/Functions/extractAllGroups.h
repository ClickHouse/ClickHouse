#pragma once
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/Regexps.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>

#include <memory>
#include <string>
#include <vector>

#include <Core/iostream_debug_helpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int TOO_LARGE_ARRAY_SIZE;
}


enum class ExtractAllGroupsResultKind
{
    VERTICAL,
    HORIZONTAL
};


/** Match all groups of given input string with given re, return array of arrays of matches.
 *
 * Depending on `Impl::Kind`, result is either grouped by group id (Horizontal) or in order of appearance (Vertical):
 *
 *  SELECT extractAllGroupsVertical('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')
 * =>
 *   [['abc', '111'], ['def', '222'], ['ghi', '333']]
 *
 *  SELECT extractAllGroupsHorizontal('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')
 * =>
 *   [['abc', 'def', 'ghi'], ['111', '222', '333']
*/
template <typename Impl>
class FunctionExtractAllGroups : public IFunction
{
    ContextPtr context;

public:
    static constexpr auto Kind = Impl::Kind;
    static constexpr auto name = Impl::Name;

    explicit FunctionExtractAllGroups(ContextPtr context_)
        : context(context_)
    {}

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionExtractAllGroups>(context); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"haystack", &isStringOrFixedString<IDataType>, nullptr, "const String or const FixedString"},
            {"needle", &isStringOrFixedString<IDataType>, isColumnConst, "const String or const FixedString"},
        };
        validateFunctionArgumentTypes(*this, arguments, args);

        /// Two-dimensional array of strings, each `row` of top array represents matching groups.
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        static const auto MAX_GROUPS_COUNT = 128;

        const ColumnPtr column_haystack = arguments[0].column;
        const ColumnPtr column_needle = arguments[1].column;

        const auto needle = typeid_cast<const ColumnConst &>(*column_needle).getValue<String>();

        if (needle.empty())
            throw Exception("Length of 'needle' argument must be greater than 0.", ErrorCodes::BAD_ARGUMENTS);

        using StringPiece = typename Regexps::Regexp::StringPieceType;
        auto holder = Regexps::get<false, false>(needle);
        const auto & regexp = holder->getRE2();

        if (!regexp)
            throw Exception("There are no groups in regexp: " + needle, ErrorCodes::BAD_ARGUMENTS);

        const size_t groups_count = regexp->NumberOfCapturingGroups();

        if (!groups_count)
            throw Exception("There are no groups in regexp: " + needle, ErrorCodes::BAD_ARGUMENTS);

        if (groups_count > MAX_GROUPS_COUNT - 1)
            throw Exception("Too many groups in regexp: " + std::to_string(groups_count)
                            + ", max: " + std::to_string(MAX_GROUPS_COUNT - 1),
                            ErrorCodes::BAD_ARGUMENTS);

        // Including 0-group, which is the whole regexp.
        PODArrayWithStackMemory<StringPiece, MAX_GROUPS_COUNT> matched_groups(groups_count + 1);

        ColumnArray::ColumnOffsets::MutablePtr root_offsets_col = ColumnArray::ColumnOffsets::create();
        ColumnArray::ColumnOffsets::MutablePtr nested_offsets_col = ColumnArray::ColumnOffsets::create();
        ColumnString::MutablePtr data_col = ColumnString::create();

        auto & root_offsets_data = root_offsets_col->getData();
        auto & nested_offsets_data = nested_offsets_col->getData();

        ColumnArray::Offset current_root_offset = 0;
        ColumnArray::Offset current_nested_offset = 0;

        if constexpr (Kind == ExtractAllGroupsResultKind::VERTICAL)
        {
            root_offsets_data.resize(input_rows_count);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                StringRef current_row = column_haystack->getDataAt(i);

                // Extract all non-intersecting matches from haystack except group #0.
                const auto * pos = current_row.data;
                const auto * end = pos + current_row.size;
                while (pos < end
                    && regexp->Match({pos, static_cast<size_t>(end - pos)},
                        0, end - pos, regexp->UNANCHORED, matched_groups.data(), matched_groups.size()))
                {
                    // 1 is to exclude group #0 which is whole re match.
                    for (size_t group = 1; group <= groups_count; ++group)
                        data_col->insertData(matched_groups[group].data(), matched_groups[group].size());

                    /// If match is empty - it's technically Ok but we have to shift one character nevertheless
                    /// to avoid infinite loop.
                    pos = matched_groups[0].data() + std::max<size_t>(1, matched_groups[0].size());

                    current_nested_offset += groups_count;
                    nested_offsets_data.push_back(current_nested_offset);

                    ++current_root_offset;
                }

                root_offsets_data[i] = current_root_offset;
            }
        }
        else
        {
            /// Additional limit to fail fast on supposedly incorrect usage.
            const auto max_matches_per_row = context->getSettingsRef().regexp_max_matches_per_row;

            PODArray<StringPiece, 0> all_matches;
            /// Number of times RE matched on each row of haystack column.
            PODArray<size_t, 0> number_of_matches_per_row;

            /// We expect RE to match multiple times on each row, `* 8` is arbitrary to reduce number of re-allocations.
            all_matches.reserve(input_rows_count * groups_count * 8);
            number_of_matches_per_row.reserve(input_rows_count);

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                size_t matches_per_row = 0;

                const auto & current_row = column_haystack->getDataAt(i);

                // Extract all non-intersecting matches from haystack except group #0.
                const auto * pos = current_row.data;
                const auto * end = pos + current_row.size;
                while (pos < end
                    && regexp->Match({pos, static_cast<size_t>(end - pos)},
                        0, end - pos, regexp->UNANCHORED, matched_groups.data(), matched_groups.size()))
                {
                    // 1 is to exclude group #0 which is whole re match.
                    for (size_t group = 1; group <= groups_count; ++group)
                        all_matches.push_back(matched_groups[group]);

                    ++matches_per_row;
                    if (matches_per_row > max_matches_per_row)
                        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                                "Too many matches per row (> {}) in the result of function {}",
                                max_matches_per_row, getName());

                    pos = matched_groups[0].data() + std::max<size_t>(1, matched_groups[0].size());
                }

                number_of_matches_per_row.push_back(matches_per_row);
            }

            {
                size_t total_matched_groups_string_len = 0;
                for (const auto & m : all_matches)
                    total_matched_groups_string_len += m.length();

                data_col->reserve(total_matched_groups_string_len);
            }

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
        }

        ColumnArray::MutablePtr nested_array_col = ColumnArray::create(std::move(data_col), std::move(nested_offsets_col));
        ColumnArray::MutablePtr root_array_col = ColumnArray::create(std::move(nested_array_col), std::move(root_offsets_col));
        return root_array_col;
    }
};

}
