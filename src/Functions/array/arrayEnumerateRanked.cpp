#include <Columns/ColumnConst.h>
#include <Functions/array/arrayEnumerateRanked.h>
#include <Common/assert_cast.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ArraysDepths getArraysDepths(const ColumnsWithTypeAndName & arguments, const char * function_name)
{
    const size_t num_arguments = arguments.size();
    if (!num_arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing arguments for function arrayEnumerateUniqRanked");

    DepthType clear_depth = 1;
    size_t i = 0;
    if (const DataTypeArray * type_array = typeid_cast<const DataTypeArray *>(arguments[0].type.get()); !type_array)
    {
        /// If the first argument is not an array, it must be a const positive and non zero number
        const auto & depth_column = arguments[i].column;
        if (!depth_column || !isColumnConst(*depth_column))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "First argument of {} must be Const(UInt64)", function_name);
        Field f = assert_cast<const ColumnConst &>(*depth_column).getField();
        if (f.getType() != Field::Types::UInt64 || f.safeGet<UInt64>() == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "First argument of {} must be a positive integer", function_name);

        clear_depth = static_cast<DepthType>(f.safeGet<UInt64>());
        i++;
    }


    /// The rest of the arguments must be in the shape: arr1, c1, arr2, c2, ...
    /// cN... - how deep to look into the corresponding arrN, (called "depths" here)
    /// may be omitted - then it means "look at the full depth"
    DepthTypes depths;
    for (; i < num_arguments; i++)
    {
        const DataTypePtr & type = arguments[i].type;
        const DataTypeArray * current_type_array = typeid_cast<const DataTypeArray *>(type.get());
        if (!current_type_array)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Incorrect argument {} type of function {}. Expected an Array, got {}",
                i + 1,
                function_name,
                type->getName());

        if (i == num_arguments - 1)
        {
            depths.emplace_back(current_type_array->getNumberOfDimensions());
        }
        else
        {
            const DataTypeArray * next_argument_array = typeid_cast<const DataTypeArray *>(arguments[i + 1].type.get());
            if (next_argument_array)
            {
                depths.emplace_back(current_type_array->getNumberOfDimensions());
            }
            else
            {
                i++;
                /// The following argument is not array, so it must be a const positive integer with the depth
                const auto & depth_column = arguments[i].column;
                if (!depth_column || !isColumnConst(*depth_column))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Incorrect argument {} type of function {}. Expected an Array or Const(UInt64), got {}",
                        i + 1,
                        function_name,
                        arguments[i].type->getName());
                Field f = assert_cast<const ColumnConst &>(*depth_column).getField();
                if (f.getType() != Field::Types::UInt64 || f.safeGet<UInt64>() == 0)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Incorrect argument {} of function {}. Expected a positive integer",
                        i + 1,
                        function_name);
                UInt64 value = f.safeGet<UInt64>();
                UInt64 prev_array_depth = current_type_array->getNumberOfDimensions();
                if (value > prev_array_depth)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Incorrect argument {} of function {}. Required depth '{}' is larger than the array depth ({})",
                        i + 1,
                        function_name,
                        value,
                        prev_array_depth);
                depths.emplace_back(value);
            }
        }
    }

    if (depths.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Incorrect arguments for function {}: At least one array should be passed", function_name);

    DepthType max_array_depth = 0;
    for (auto depth : depths)
        max_array_depth = std::max(depth, max_array_depth);

    if (clear_depth > max_array_depth)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Incorrect arguments for function {}: clear_depth ({}) can't be larger than max_array_depth ({})",
            function_name,
            clear_depth,
            max_array_depth);

    return {clear_depth, depths, max_array_depth};
}

}
