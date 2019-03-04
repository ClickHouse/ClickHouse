#include "arrayEnumerateRanked.h"

namespace DB
{
ArraysDepths getArraysDepths(const ColumnsWithTypeAndName & arguments)
{
    const size_t num_arguments = arguments.size();
    DepthType clear_depth = 1;
    DepthType max_array_depth = 0;
    DepthTypes depths;

    size_t array_num = 0;
    DepthType last_array_depth = 0;
    for (size_t i = 0; i < num_arguments; ++i)
    {
        const auto type = arguments[i].type;

        if (isArray(type))
        {
            if (depths.size() < array_num && last_array_depth)
            {
                depths.emplace_back(last_array_depth);
                last_array_depth = 0;
            }

            DepthType depth = 0;
            auto sub_type = type;
            do
            {
                auto sub_type_array = typeid_cast<const DataTypeArray *>(sub_type.get());
                if (!sub_type_array)
                    break;
                sub_type = sub_type_array->getNestedType();
                ++depth;
            } while (isArray(sub_type));
            last_array_depth = depth;
            ++array_num;
        }

        if (!arguments[i].column)
            continue;

        const IColumn * non_const = nullptr;
        if (auto const_array_column = typeid_cast<const ColumnConst *>(arguments[i].column.get()))
            non_const = const_array_column->getDataColumnPtr().get();
        const auto array = typeid_cast<const ColumnArray *>(non_const ? non_const : arguments[i].column.get());

        if (!array)
        {
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
                    if (depths.size() >= array_num)
                    {
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
    if (depths.size() < array_num)
    {
        depths.emplace_back(last_array_depth);
    }


    for (auto & depth : depths)
    {
        if (max_array_depth < depth)
            max_array_depth = depth;
    }

    if (depths.empty())
        throw Exception(
            "Arguments for function arrayEnumerateUniqRanked/arrayEnumerateDenseRanked incorrect: At least one array should be passed.",
            ErrorCodes::BAD_ARGUMENTS);

    if (clear_depth > max_array_depth)
        throw Exception(
            "Arguments for function arrayEnumerateUniqRanked/arrayEnumerateDenseRanked incorrect: clear_depth ("
                + std::to_string(clear_depth) + ") cant be larger than max_array_depth (" + std::to_string(max_array_depth) + ").",
            ErrorCodes::BAD_ARGUMENTS);

    return {clear_depth, depths, max_array_depth};
}

}
