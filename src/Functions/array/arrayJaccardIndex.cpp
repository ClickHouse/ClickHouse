#include <Columns/ColumnArray.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeNothing.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
}

namespace
{
template <bool apply_distinct>
constexpr String getFunctionArrayJaccardIndexName()
{
    if constexpr (apply_distinct)
        return "arrayJaccard";
    else
        return "arrayJaccardIndex";
}
}

template <bool apply_distinct = false>
class FunctionArrayJaccardIndex : public IFunction
{
private:
    using ResultType = Float64;

    struct LeftAndRightSizes
    {
        size_t left_size;
        size_t right_size;
    };

    template <bool left_is_const, bool right_is_const>
    static LeftAndRightSizes getArraySizes(const ColumnArray::Offsets & left_offsets, const ColumnArray::Offsets & right_offsets, size_t i)
    {
        size_t left_size;
        size_t right_size;

        if constexpr (left_is_const)
            left_size = left_offsets[0];
        else
            left_size = left_offsets[i] - left_offsets[i - 1];

        if constexpr (right_is_const)
            right_size = right_offsets[0];
        else
            right_size = right_offsets[i] - right_offsets[i - 1];

        return {left_size, right_size};
    }

public:
    static constexpr String name{getFunctionArrayJaccardIndexName<apply_distinct>()};
    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionArrayJaccardIndex>(context_); }
    explicit FunctionArrayJaccardIndex(ContextPtr context_) : context(context_) {}
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"array_1", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
            {"array_2", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
        };
        validateFunctionArguments(*this, arguments, args);
        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto cast_to_array = [&](const IColumn & col) -> std::pair<const ColumnArray *, bool>
        {
            if (const ColumnConst * col_const = typeid_cast<const ColumnConst *>(&col))
            {
                const ColumnArray & col_const_array = checkAndGetColumn<ColumnArray>(*col_const->getDataColumnPtr());
                return {&col_const_array, true};
            }
            if (const ColumnArray * col_non_const_array = checkAndGetColumn<ColumnArray>(&col))
                return {col_non_const_array, false};
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Argument for function {} must be array but it has type {}.", col.getName(), getName());
        };

        auto intersect_array = FunctionFactory::instance().get("arrayIntersect", context)->build(arguments);

        ColumnWithTypeAndName intersect_column;
        intersect_column.type = intersect_array->getResultType();
        intersect_column.column = intersect_array->execute(arguments, intersect_column.type, input_rows_count, /* dry_run = */ false);

        const auto * intersect_column_type = checkAndGetDataType<DataTypeArray>(intersect_column.type.get());
        if (!intersect_column_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected return type for function arrayIntersect");

        auto col_res = ColumnVector<ResultType>::create();
        typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
        vec_res.resize(input_rows_count);

        bool intersection_empty = typeid_cast<const DataTypeNothing *>(intersect_column_type->getNestedType().get());

        // `arrayJaccardIndex` throws an exception here that's why we should check apply_distinct here
        if (intersection_empty && apply_distinct)
        {
            for (double & vec_re : vec_res)
                vec_re = 0;

            return col_res;
        }

        auto execute_vector = [&]<bool left_is_const, bool right_is_const>(const ColumnArray * left_array, const ColumnArray * right_array)
        {
            const auto & left_offsets = left_array->getOffsets();
            const auto & right_offsets = right_array->getOffsets();

            if (intersection_empty && !apply_distinct)
            {
                for (size_t i = 0; i < vec_res.size(); ++i)
                {
                    LeftAndRightSizes sizes = getArraySizes<left_is_const, right_is_const>(left_offsets, right_offsets, i);
                    if (sizes.left_size == 0 && sizes.right_size == 0)
                        throw Exception(
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "array aggregate functions cannot be performed on two empty arrays");
                    vec_res[i] = 0;
                }
            }
            else
            {
                const ColumnArray & intersect_column_array = checkAndGetColumn<ColumnArray>(*intersect_column.column);
                const auto & intersect_offsets = intersect_column_array.getOffsets();

                for (size_t i = 0; i < vec_res.size(); ++i)
                {
                    LeftAndRightSizes sizes = getArraySizes<left_is_const, right_is_const>(left_offsets, right_offsets, i);
                    size_t intersect_size = intersect_offsets[i] - intersect_offsets[i - 1];
                    vec_res[i] = static_cast<ResultType>(intersect_size) / (sizes.left_size + sizes.right_size - intersect_size);
                }
            }
        };

        auto execute_vector_dispatch
            = [&](const ColumnArray * left_array, bool left_is_const, const ColumnArray * right_array, bool right_is_const)
        {
            if (left_is_const && right_is_const)
                return execute_vector.template operator()<true, true>(left_array, right_array);
            else if (!left_is_const && right_is_const)
                return execute_vector.template operator()<false, true>(left_array, right_array);
            else if (left_is_const && !right_is_const)
                return execute_vector.template operator()<true, false>(left_array, right_array);
            else
                return execute_vector.template operator()<false, false>(left_array, right_array);
        };

        if constexpr (apply_distinct)
        {
            auto distinct_function_resolver = FunctionFactory::instance().get("arrayDistinct", context);

            auto left_distinct_array = distinct_function_resolver->build({arguments[0]});
            auto left_column = left_distinct_array->execute(
                {arguments[0]}, left_distinct_array->getResultType(), input_rows_count, /* dry_run = */ false);

            auto right_distinct_array = distinct_function_resolver->build({arguments[1]});
            auto right_column = right_distinct_array->execute(
                {arguments[1]}, right_distinct_array->getResultType(), input_rows_count, /* dry_run = */ false);

            const auto [left_array, left_is_const] = cast_to_array(*left_column);
            const auto [right_array, right_is_const] = cast_to_array(*right_column);

            execute_vector_dispatch(left_array, left_is_const, right_array, right_is_const);
        }
        else
        {
            const auto [left_array, left_is_const] = cast_to_array(*arguments[0].column);
            const auto [right_array, right_is_const] = cast_to_array(*arguments[1].column);

            execute_vector_dispatch(left_array, left_is_const, right_array, right_is_const);
        }

        return col_res;
    }

private:
    ContextPtr context;
};

REGISTER_FUNCTION(ArrayJaccardIndex)
{
    {
        FunctionDocumentation::Description description
            = "Returns the [Jaccard index](https://en.wikipedia.org/wiki/Jaccard_index) of two arrays.";
        FunctionDocumentation::Syntax syntax = "arrayJaccardIndex(arr_x, arr_y)";
        FunctionDocumentation::Arguments arguments = {
            {"arr_x", "First array. [`Array(T)`](/sql-reference/data-types/array)."},
            {"arr_y", "Second array. [`Array(T)`](/sql-reference/data-types/array)."},
        };
        FunctionDocumentation::ReturnedValue returned_value
            = "Returns the Jaccard index of `arr_x` and `arr_y`.[Float64](/sql-reference/data-types/float)";
        FunctionDocumentation::Examples examples
            = {{"Usage example", "SELECT arrayJaccardIndex([1, 2], [2, 3]) AS res", "0.3333333333333333"}};
        FunctionDocumentation::IntroducedIn introduced_in = {23, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionArrayJaccardIndex<false>>(documentation);
    }

    {
        FunctionDocumentation::Description description = R"(
Returns the [Jaccard index](https://en.wikipedia.org/wiki/Jaccard_index) of two arrays.

::note
Unlike `arrayJaccardIndex` behaves exactly as `stringJaccardIndex`.
In other words this function returns 0 if both arrays are empty and the same result as `arrayJaccardIndex(arrayDistinct(arr_x), arrayDistinct(arr_y))` for other cases.
:::
        )";
        FunctionDocumentation::Syntax syntax = "arrayJaccard(arr_x, arr_y)";
        FunctionDocumentation::Arguments arguments = {
            {"arr_x", "First array. [`Array(T)`](/sql-reference/data-types/array)."},
            {"arr_y", "Second array. [`Array(T)`](/sql-reference/data-types/array)."},
        };
        FunctionDocumentation::ReturnedValue returned_value
            = "Returns the Jaccard index of `arr_x` and `arr_y`.[Float64](/sql-reference/data-types/float)";
        FunctionDocumentation::Examples examples
            = {{"Usage example", "SELECT arrayJaccard([1, 2, 1, 2], [2, 3, 3, 3, 3]) AS res", "0.3333333333333333"}};
        FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionArrayJaccardIndex<true>>(documentation);
    }
}
}
