#include "arrayEnumerateRanked.h"
//#include <Common/typeid_cast.h>

#include <Core/iostream_debug_helpers.h>

namespace DB
{
std::tuple<DepthType, DepthTypes, DepthType> getDepths(const ColumnsWithTypeAndName & arguments)
{
    const size_t num_arguments = arguments.size();
    DepthType clear_depth = 1;
    DepthType max_array_depth = 0;
    DepthTypes depths;

    size_t array_num = 0;
    DepthType last_array_depth = 0;
    for (size_t i = 0; i < num_arguments; ++i)
    {

DUMP(arguments[i].type);
DUMP(arguments[i].type.get());
        const auto type = arguments[i].type;

        DUMP(isArray(type));
        if (isArray(type)){
            if (depths.size() < array_num && last_array_depth) {
                depths.emplace_back(last_array_depth);
                last_array_depth = 0;
            }

            DepthType depth = 0;
            //auto sub_type_array = typeid_cast<const DataTypeArray *>(type.get());
            auto sub_type = type;
            do {
                //sub_type_array = typeid_cast<const DataTypeArray *>(sub_type_array->getNestedType().get());
                auto sub_type_array = typeid_cast<const DataTypeArray *>(sub_type.get());
DUMP(sub_type_array);
                if (!sub_type_array)
                    break;
                //sub_type_array = typeid_cast<const DataTypeArray *>(sub_type.get())
                sub_type = sub_type_array->getNestedType();
                ++depth;
            } while (isArray(sub_type));
DUMP(array_num, last_array_depth, depth);
            last_array_depth = depth;
            ++array_num;
        }

        if (!arguments[i].column)
            continue;
                    //throw Exception(                        "Missing column " + std::to_string(i),                        ErrorCodes::LOGICAL_ERROR);

        const auto non_const = arguments[i].column->convertToFullColumnIfConst();
        const auto array = typeid_cast<const ColumnArray *>(non_const.get());
/*

        if (array)
        {
DUMP(i, depths.size(), array_num);
            if (depths.size() < array_num && last_array_depth) {
                depths.emplace_back(last_array_depth);
                last_array_depth = 0;
            }
DUMP(i, depths.size(), array_num);

            // Find array max depth, will use if function call without user-defined depth for this array
            DepthType depth = 1;
            Columns array_holders;
            const DB::ColumnArray * sub_array = typeid_cast<const ColumnArray *>(&array->getData());
            do
            {
                if (!sub_array)
                    break;
                const auto sub_non_const = sub_array->convertToFullColumnIfConst();
                array_holders.emplace_back(sub_non_const); // Think about in-column cache of Full Column
                sub_array = typeid_cast<const ColumnArray *>(sub_non_const.get());
                if (!sub_array)
                    break;
                sub_array = typeid_cast<const ColumnArray *>(&sub_array->getData()); 
                ++depth;
            } while (sub_array);

            last_array_depth = depth;
            ++array_num;
        }
        else 
*/

DUMP("finddepth", i, arguments[i]);



        if (!array)
        {
DUMP(i, array_num);
            const auto & depth_column = arguments[i].column;

            if (depth_column && depth_column->isColumnConst())
            {
                auto value = depth_column->getUInt(0);
                if (!value)
                    throw Exception(
                        "Arguments for function arrayEnumerateUniqRanked/arrayEnumerateDenseRanked incorrect: depth ("
                            + std::to_string(value) + ") cant be 0.",
                        ErrorCodes::BAD_ARGUMENTS);

                if (i == 0)
                {
                    clear_depth = value;
                }
                else
                {
DUMP(i, array_num, depths.size(), value);
                    //if (depths.size() >= array_num + (!array_num ? 1 : 0))  {
                    if (depths.size() >= array_num)  {
                        throw Exception(
                        "Arguments for function arrayEnumerateUniqRanked/arrayEnumerateDenseRanked incorrect: depth ("
                            + std::to_string(value) + ") for missing array.",
                        ErrorCodes::BAD_ARGUMENTS);
                    }
                    depths.emplace_back(value);
                }
            }
        }
    }
DUMP( array_num, depths, last_array_depth);
            if (depths.size() < array_num) {
                depths.emplace_back(last_array_depth);
            }

DUMP( depths, max_array_depth);

            for (auto & depth : depths) {
DUMP( depth, max_array_depth);
                    if (max_array_depth < depth)
                        max_array_depth = depth;
            }


DUMP( array_num, clear_depth, max_array_depth, depths);


    if (depths.empty())
        throw Exception("Arguments for function arrayEnumerateUniqRanked/arrayEnumerateDenseRanked incorrect: At least one array should be passed.", ErrorCodes::BAD_ARGUMENTS);

    if (clear_depth > max_array_depth)
        throw Exception(
            "Arguments for function arrayEnumerateUniqRanked/arrayEnumerateDenseRanked incorrect: clear_depth ("
                + std::to_string(clear_depth) + ") cant be larger than max_array_depth (" + std::to_string(max_array_depth) + ").",
            ErrorCodes::BAD_ARGUMENTS);

    return std::make_tuple(clear_depth, depths, max_array_depth);
}

}
