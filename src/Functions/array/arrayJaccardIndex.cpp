#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/getMostSubtype.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>

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
public:
    using ResultType = Float64;
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
            // XXX
            {"array_1", [](const IDataType & type) { return isArray(type.getPtr()); }, nullptr, "Array"},
            {"array_2", [](const IDataType & type) { return isArray(type.getPtr()); }, nullptr, "Array"},
        };
        validateFunctionArgumentTypes(*this, arguments, args);
        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    template <bool is_const_left, bool is_const_right>
    static void getArraySize(const ColumnArray::Offsets & left_offsets, const ColumnArray::Offsets & right_offsets, size_t & left_size, size_t & right_size, const size_t & i)
    {
        if constexpr (is_const_left)
            left_size = left_offsets[0];
        else
            left_size = left_offsets[i] - left_offsets[i - 1];
        if constexpr (is_const_right)
            right_size = right_offsets[0];
        else
            right_size = right_offsets[i] - right_offsets[i - 1];
    }

    template <bool is_const_left, bool is_const_right>
    static void vector(const ColumnArray::Offsets & intersect_offsets, const ColumnArray::Offsets & left_offsets, const ColumnArray::Offsets & right_offsets, PaddedPODArray<ResultType> & res)
    {
        size_t left_size;
        size_t right_size;
        for (size_t i = 0; i < res.size(); ++i)
        {
            getArraySize<is_const_left, is_const_right>(left_offsets, right_offsets, left_size, right_size, i);
            size_t intersect_size = intersect_offsets[i] - intersect_offsets[i - 1];
            res[i] = static_cast<ResultType>(intersect_size) / (left_size + right_size - intersect_size);
        }
    }

    template <bool is_const_left, bool is_const_right>
    static void vectorWithEmptyIntersect(const ColumnArray::Offsets & left_offsets, const ColumnArray::Offsets & right_offsets, PaddedPODArray<ResultType> & res)
    {
        size_t left_size;
        size_t right_size;
        for (size_t i = 0; i < res.size(); ++i)
        {
            getArraySize<is_const_left, is_const_right>(left_offsets, right_offsets, left_size, right_size, i);
            if ((!left_size && !right_size))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "array aggregate functions cannot be performed on two empty arrays");
            res[i] = 0;
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto cast_array = [&](const ColumnWithTypeAndName & col) -> std::pair<const ColumnArray *, bool>
        {
            const ColumnArray * res;
            bool is_const = false;
            if (const ColumnConst * col_const = typeid_cast<const ColumnConst *>(col.column.get()))
            {
                res = checkAndGetColumn<ColumnArray>(col_const->getDataColumnPtr().get());
                is_const = true;
            }
            else if (!(res = checkAndGetColumn<ColumnArray>(col.column.get())))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument for function {} must be array but it has type {}.",
                        col.column->getName(), getName());
            return {res, is_const};
        };

        const auto & [left_array, is_const_left] = cast_array(arguments[0]);
        const auto & [right_array, is_const_right] = cast_array(arguments[1]);

        auto intersect_array = FunctionFactory::instance().get("arrayIntersect", context)->build(arguments);
        ColumnWithTypeAndName intersect_column;
        intersect_column.type = intersect_array->getResultType();
        intersect_column.column = intersect_array->execute(arguments, intersect_column.type, input_rows_count);
        const auto * return_type_intersect = checkAndGetDataType<DataTypeArray>(intersect_column.type.get());
        if (!return_type_intersect)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected return type for function arrayIntersect");

        auto col_res = ColumnVector<ResultType>::create();
        typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
        vec_res.resize(input_rows_count);

#define EXECUTE_VECTOR(is_const_left, is_const_right) \
    if (typeid_cast<const DataTypeNothing *>(return_type_intersect->getNestedType().get())) \
        vectorWithEmptyIntersect<is_const_left, is_const_right>(left_array->getOffsets(), right_array->getOffsets(), vec_res); \
    else \
    { \
        const ColumnArray * intersect_column_array = checkAndGetColumn<ColumnArray>(intersect_column.column.get()); \
        vector<is_const_left, is_const_right>(intersect_column_array->getOffsets(), left_array->getOffsets(), right_array->getOffsets(), vec_res); \
    }

        if (!is_const_left && !is_const_right)
            EXECUTE_VECTOR(false, false)
        else if (!is_const_left && is_const_right)
            EXECUTE_VECTOR(false, true)
        else if (is_const_left && !is_const_right)
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
