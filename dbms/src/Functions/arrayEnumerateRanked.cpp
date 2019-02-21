#include "arrayEnumerateRanked.h"

namespace DB
{

std::tuple<DepthType, DepthTypes, DepthType> getDepths(const ColumnsWithTypeAndName & arguments)
{
    const size_t num_arguments = arguments.size();
    DepthType clear_depth = 1;
    DepthType max_array_depth = 1;
    DepthTypes depths;

    for (size_t i = 0; i < num_arguments; ++i)
    {
        if (!arguments[i].column)
            continue;
        const auto non_const = arguments[i].column->convertToFullColumnIfConst();
        const auto array = typeid_cast<const ColumnArray *>(non_const.get());

        if (!array)
        {
            const auto & depth_column = arguments[i].column;

            if (depth_column                && depth_column->isColumnConst()            )
            {
                auto value = depth_column->getUInt(0);
                if (i == 0)
                {
                    clear_depth = value;
                }
                else
                {
                    depths.emplace_back(value);
                    if (max_array_depth < value)
                        max_array_depth = value;
                }
            }
        }
    }
    if (clear_depth > max_array_depth)
        throw Exception( "Arguments for function arrayEnumerateUniqRanked incorrect: clear_depth=" + std::to_string(clear_depth)+ " cant be larger than max_array_depth=" + std::to_string(max_array_depth) + ".", ErrorCodes::BAD_ARGUMENTS);

    return std::make_tuple(clear_depth, depths, max_array_depth);
}

}
