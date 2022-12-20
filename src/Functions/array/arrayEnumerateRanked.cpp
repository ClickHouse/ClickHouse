#include <algorithm>
#include <Columns/ColumnConst.h>
#include <Common/assert_cast.h>
#include "arrayEnumerateRanked.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ArraysDepths getArraysDepths(const ColumnsWithTypeAndName & arguments)
{
    const size_t num_arguments = arguments.size();

    DepthType clear_depth = 1;
    DepthTypes depths;

    /// function signature is the following:
    /// f(c0, arr1, c1, arr2, c2, ...)
    ///
    /// c0 is something called "clear_depth" here.
    /// cN... - how deep to look into the corresponding arrN, (called "depths" here)
    ///   may be omitted - then it means "look at the full depth".

    size_t array_num = 0;
    DepthType prev_array_depth = 0;
    for (size_t i = 0; i < num_arguments; ++i)
    {
        const DataTypePtr & type = arguments[i].type;
        const DataTypeArray * type_array = typeid_cast<const DataTypeArray *>(type.get());

        if (type_array)
        {
            if (depths.size() < array_num && prev_array_depth)
                depths.emplace_back(prev_array_depth);

            prev_array_depth = type_array->getNumberOfDimensions();
            ++array_num;
        }
        else
        {
            const auto & depth_column = arguments[i].column;

            if (depth_column && isColumnConst(*depth_column))
            {
                UInt64 value = assert_cast<const ColumnConst &>(*depth_column).getValue<UInt64>();
                if (!value)
                    throw Exception("Incorrect arguments for function arrayEnumerateUniqRanked or arrayEnumerateDenseRanked: depth ("
                        + std::to_string(value) + ") cannot be less or equal 0.",
                        ErrorCodes::BAD_ARGUMENTS);

                if (i == 0)
                {
                    clear_depth = value;
                }
                else
                {
                    if (depths.size() >= array_num)
                        throw Exception("Incorrect arguments for function arrayEnumerateUniqRanked or arrayEnumerateDenseRanked: depth ("
                            + std::to_string(value) + ") for missing array.",
                            ErrorCodes::BAD_ARGUMENTS);
                    if (value > prev_array_depth)
                        throw Exception(
                            "Arguments for function arrayEnumerateUniqRanked/arrayEnumerateDenseRanked incorrect: depth="
                                + std::to_string(value) + " for array with depth=" + std::to_string(prev_array_depth) + ".",
                            ErrorCodes::BAD_ARGUMENTS);

                    depths.emplace_back(value);
                }
            }
        }
    }

    if (depths.size() < array_num)
        depths.emplace_back(prev_array_depth);

    if (depths.empty())
        throw Exception("Incorrect arguments for function arrayEnumerateUniqRanked or arrayEnumerateDenseRanked: at least one array should be passed.",
            ErrorCodes::BAD_ARGUMENTS);

    DepthType max_array_depth = 0;
    for (auto depth : depths)
        max_array_depth = std::max(depth, max_array_depth);

    if (clear_depth > max_array_depth)
        throw Exception("Incorrect arguments for function arrayEnumerateUniqRanked or arrayEnumerateDenseRanked: clear_depth ("
            + std::to_string(clear_depth) + ") can't be larger than max_array_depth (" + std::to_string(max_array_depth) + ").",
            ErrorCodes::BAD_ARGUMENTS);

    return {clear_depth, depths, max_array_depth};
}

}
