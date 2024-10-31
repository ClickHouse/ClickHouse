#include <Columns/ColumnArray.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeNothing.h>
#include <Core/ColumnsWithTypeAndName.h>
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

    template <bool left_is_const, bool right_is_const>
    static void vector(const ColumnArray::Offsets & intersect_offsets, const ColumnArray::Offsets & left_offsets, const ColumnArray::Offsets & right_offsets, PaddedPODArray<ResultType> & res)
    {
        for (size_t i = 0; i < res.size(); ++i)
        {
            LeftAndRightSizes sizes = getArraySizes<left_is_const, right_is_const>(left_offsets, right_offsets, i);
            size_t intersect_size = intersect_offsets[i] - intersect_offsets[i - 1];
            res[i] = static_cast<ResultType>(intersect_size) / (sizes.left_size + sizes.right_size - intersect_size);
        }
    }

    template <bool left_is_const, bool right_is_const>
    static void vectorWithEmptyIntersect(const ColumnArray::Offsets & left_offsets, const ColumnArray::Offsets & right_offsets, PaddedPODArray<ResultType> & res)
    {
        for (size_t i = 0; i < res.size(); ++i)
        {
            LeftAndRightSizes sizes = getArraySizes<left_is_const, right_is_const>(left_offsets, right_offsets, i);
            if (sizes.left_size == 0 && sizes.right_size == 0)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "array aggregate functions cannot be performed on two empty arrays");
            res[i] = 0;
        }
    }

public:
    static constexpr auto name = "arrayJaccardIndex";
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
        auto cast_to_array = [&](const ColumnWithTypeAndName & col) -> std::pair<const ColumnArray *, bool>
        {
            if (const ColumnConst * col_const = typeid_cast<const ColumnConst *>(col.column.get()))
            {
                const ColumnArray & col_const_array = checkAndGetColumn<ColumnArray>(*col_const->getDataColumnPtr());
                return {&col_const_array, true};
            }
            if (const ColumnArray * col_non_const_array = checkAndGetColumn<ColumnArray>(col.column.get()))
                return {col_non_const_array, false};
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Argument for function {} must be array but it has type {}.", col.column->getName(), getName());
        };

        const auto & [left_array, left_is_const] = cast_to_array(arguments[0]);
        const auto & [right_array, right_is_const] = cast_to_array(arguments[1]);

        auto intersect_array = FunctionFactory::instance().get("arrayIntersect", context)->build(arguments);

        ColumnWithTypeAndName intersect_column;
        intersect_column.type = intersect_array->getResultType();
        intersect_column.column = intersect_array->execute(arguments, intersect_column.type, input_rows_count);

        const auto * intersect_column_type = checkAndGetDataType<DataTypeArray>(intersect_column.type.get());
        if (!intersect_column_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected return type for function arrayIntersect");

        auto col_res = ColumnVector<ResultType>::create();
        typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
        vec_res.resize(input_rows_count);

#define EXECUTE_VECTOR(left_is_const, right_is_const) \
    if (typeid_cast<const DataTypeNothing *>(intersect_column_type->getNestedType().get())) \
        vectorWithEmptyIntersect<left_is_const, right_is_const>(left_array->getOffsets(), right_array->getOffsets(), vec_res); \
    else \
    { \
        const ColumnArray & intersect_column_array = checkAndGetColumn<ColumnArray>(*intersect_column.column); \
        vector<left_is_const, right_is_const>(intersect_column_array.getOffsets(), left_array->getOffsets(), right_array->getOffsets(), vec_res); \
    }

        if (!left_is_const && !right_is_const)
            EXECUTE_VECTOR(false, false)
        else if (!left_is_const && right_is_const)
            EXECUTE_VECTOR(false, true)
        else if (left_is_const && !right_is_const)
            EXECUTE_VECTOR(true, false)
        else
            EXECUTE_VECTOR(true, true)

#undef EXECUTE_VECTOR

        return col_res;
    }

private:
    ContextPtr context;
};

REGISTER_FUNCTION(ArrayJaccardIndex)
{
    factory.registerFunction<FunctionArrayJaccardIndex>();
}

}
