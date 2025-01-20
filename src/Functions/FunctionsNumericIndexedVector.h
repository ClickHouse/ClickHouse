#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <iterator>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsExternalDictionaries.h>
#include <Functions/IFunction.h>
#include <Interpreters/castColumn.h>
#include <base/types.h>
#include "Common/ErrorCodes.h"
#include <Common/FieldVisitorToString.h>
#include <Common/PODArray_fwd.h>
#include <Common/typeid_cast.h>
#include "AggregateFunctions/IAggregateFunction.h"
#include "FunctionHelpers.h"
#include "Functions/countMatches.h"
#include "IO/ReadBuffer.h"

#include <absl/strings/str_split.h>
#include <fmt/format.h>

#include <AggregateFunctions/AggregateFunctionGroupNumericIndexedVectorData.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}


template <template <typename, typename, template <typename, typename> class> class FuncImpl>
class FunctionNumericIndexedVector : public IFunction
{
public:
    static constexpr auto name = FuncImpl<UInt8, Float64, BSINumericIndexedVector>::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionNumericIndexedVector>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * type0 = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(type0 && type0->getFunctionName() == "groupNumericIndexedVector"))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be a NumericIndexedVector but it has type {}",
                getName(), arguments[0]->getName());

        const auto * type1 = typeid_cast<const DataTypeAggregateFunction *>(arguments[1].get());
        if (!(type1 && type1->getFunctionName() == "groupNumericIndexedVector")
            && type0->getArgumentsDataTypes()[1].get()->getTypeId() != arguments[1].get()->getTypeId())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function {} must be a NumericIndexedVector or ValueType of NumericIndexedVector but it has type {}",
                getName(), arguments[1]->getName());

        if (type1
            && (type0->getArgumentsDataTypes()[0]->getTypeId() != type1->getArgumentsDataTypes()[0]->getTypeId()
                || type0->getArgumentsDataTypes()[1]->getTypeId() != type1->getArgumentsDataTypes()[1]->getTypeId()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The nested types in NumericIndexedVector must be the same, but one is ({}, {}), and the other is ({}, {}).",
                type0->getArgumentsDataTypes()[0]->getName(), type0->getArgumentsDataTypes()[1]->getName(),
                type1->getArgumentsDataTypes()[0]->getName(), type1->getArgumentsDataTypes()[1]->getName());

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return executeImplBridgeFirst(arguments, input_rows_count);
    }

private:
    ColumnPtr executeImplBridgeFirst(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        const IDataType * from_type = arguments[0].type.get();
        const auto * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        WhichDataType which(aggr_type->getArgumentsDataTypes()[0]);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return executeImplBridgeSecond<TYPE>(aggr_type->getArgumentsDataTypes()[1], arguments, input_rows_count);
        FOR_NUMERIC_INDEXED_VECTOR_INDEX_TYPES(DISPATCH)
#undef DISPATCH
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unexpected index type {} of argument of function {}", from_type->getName(), getName());
    }

    template <typename IndexType>
    ColumnPtr
    executeImplBridgeSecond(const DataTypePtr second_type, const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        WhichDataType which(second_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return executeImplBridgeThird<IndexType, TYPE>(arguments, input_rows_count);
        FOR_NUMERIC_INDEXED_VECTOR_VALUE_TYPES(DISPATCH)
#undef DISPATCH
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unexpected value type of argument of function {}", getName());
    }

    template <typename IndexType, typename ValueType>
    ColumnPtr executeImplBridgeThird(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        const auto * type0 = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].type.get());
        Array parameters = type0->getParameters();
        if (parameters.empty() || applyVisitor(FieldVisitorToString(), parameters[0]) != "BSI")
        {
            return executeNumericIndexedVectorDataDispatch<IndexType, ValueType, BSINumericIndexedVector>(arguments, input_rows_count);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported parameters");
        }
    }

    template <typename IndexType, typename ValueType, template <typename, typename> class VectorImpl>
    ColumnPtr executeNumericIndexedVectorDataDispatch(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        bool is_column_const[2];
        for (int i = 0; i < 2; ++i)
            is_column_const[i] = isColumnConst(*arguments[i].column);

        // First argument
        const ColumnAggregateFunction * first_column_ptr = nullptr;
        if (is_column_const[0])
            first_column_ptr = typeid_cast<const ColumnAggregateFunction *>(
                typeid_cast<const ColumnConst *>(arguments[0].column.get())->getDataColumnPtr().get());
        else
            first_column_ptr = typeid_cast<const ColumnAggregateFunction *>(arguments[0].column.get());

        // Second argument
        bool is_second_column_agg = WhichDataType(arguments[1].column->getDataType()).isAggregateFunction();
        if (is_second_column_agg)
        {
            const ColumnAggregateFunction * second_column_ptr = nullptr;
            if (is_column_const[1])
                second_column_ptr = typeid_cast<const ColumnAggregateFunction *>(
                    typeid_cast<const ColumnConst *>(arguments[1].column.get())->getDataColumnPtr().get());
            else
                second_column_ptr = typeid_cast<const ColumnAggregateFunction *>(arguments[1].column.get());
            return executeNumericIndexedVectorData<IndexType, ValueType, VectorImpl>(
                first_column_ptr, is_column_const[0], second_column_ptr, is_column_const[1], input_rows_count);
        }
        else
        {
            const PaddedPODArray<ValueType> * second_column_ptr = nullptr;
            if (is_column_const[1])
                second_column_ptr = &typeid_cast<const ColumnVector<ValueType> *>(
                                         typeid_cast<const ColumnConst *>(arguments[1].column.get())->getDataColumnPtr().get())
                                         ->getData();
            else
                second_column_ptr = &typeid_cast<const ColumnVector<ValueType> *>(arguments[1].column.get())->getData();

            return executeNumericIndexedVectorDataWithScalar<IndexType, ValueType, VectorImpl>(
                first_column_ptr, is_column_const[0], second_column_ptr, is_column_const[1], input_rows_count);
        }
    }

    template <typename IndexType, typename ValueType, template <typename, typename> class VectorImpl>
    ColumnPtr executeNumericIndexedVectorData(
        const ColumnAggregateFunction * first_column_ptr,
        bool is_first_column_const,
        const ColumnAggregateFunction * second_column_ptr,
        bool is_second_column_const,
        size_t input_rows_count) const
    {
        auto col_to = ColumnAggregateFunction::create(first_column_ptr->getAggregateFunction());
        col_to->reserve(input_rows_count);

        const PaddedPODArray<AggregateDataPtr> & first_container = first_column_ptr->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            AggregateDataPtr first_data_ptr = is_first_column_const ? first_container[0] : first_container[i];
            auto lhs = reinterpret_cast<const AggregateFunctionGroupNumericIndexedVectorData<IndexType, ValueType, VectorImpl> *>(
                first_data_ptr);

            AggregateDataPtr second_data_ptr = is_second_column_const ? second_column_ptr->getData()[0] : second_column_ptr->getData()[i];
            auto rhs = reinterpret_cast<const AggregateFunctionGroupNumericIndexedVectorData<IndexType, ValueType, VectorImpl> *>(
                second_data_ptr);

            // check the name of operation (bitmapAnd) and check if it is the situation mentioned above
            col_to->insertDefault();
            auto res = reinterpret_cast<AggregateFunctionGroupNumericIndexedVectorData<IndexType, ValueType, VectorImpl> *>(
                col_to->getData()[i]);
            res->init = true;
            FuncImpl<IndexType, ValueType, VectorImpl>::apply(*lhs, *rhs, *res);
        }
        return col_to;
    }

    template <typename IndexType, typename ValueType, template <typename, typename> class VectorImpl>
    ColumnPtr executeNumericIndexedVectorDataWithScalar(
        const ColumnAggregateFunction * first_column_ptr,
        bool is_first_column_const,
        const PaddedPODArray<ValueType> * second_column_ptr,
        bool is_second_column_const,
        size_t input_rows_count) const
    {
        auto col_to = ColumnAggregateFunction::create(first_column_ptr->getAggregateFunction());
        col_to->reserve(input_rows_count);

        const PaddedPODArray<AggregateDataPtr> & first_container = first_column_ptr->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            AggregateDataPtr first_data_ptr = is_first_column_const ? first_container[0] : first_container[i];
            auto lhs = reinterpret_cast<const AggregateFunctionGroupNumericIndexedVectorData<IndexType, ValueType, VectorImpl> *>(
                first_data_ptr);

            ValueType value = is_second_column_const ? (*second_column_ptr)[0] : (*second_column_ptr)[i];

            col_to->insertDefault();
            auto res = reinterpret_cast<AggregateFunctionGroupNumericIndexedVectorData<IndexType, ValueType, VectorImpl> *>(
                col_to->getData()[i]);
            res->init = true;
            FuncImpl<IndexType, ValueType, VectorImpl>::apply(*lhs, value, *res);
        }
        return col_to;
    }
};


template <typename IndexType, typename ValueType, template <typename, typename> class VectorImpl>
struct NumericIndexedVectorPointwiseAddImpl
{
    static constexpr auto name = "numericIndexedVectorPointwiseAdd";

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<IndexType, ValueType, VectorImpl> & lhs,
        const ValueType & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<IndexType, ValueType, VectorImpl> & res)
    {
        lhs.vector.pointwiseAdd(rhs, res.vector);
    }

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<IndexType, ValueType, VectorImpl> & lhs,
        const AggregateFunctionGroupNumericIndexedVectorData<IndexType, ValueType, VectorImpl> & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<IndexType, ValueType, VectorImpl> & res)
    {
        lhs.vector.pointwiseAdd(rhs.vector, res.vector);
    }
};

using FunctionNumericIndexedVectorAdd = FunctionNumericIndexedVector<NumericIndexedVectorPointwiseAddImpl>;


template <typename ToType, typename FuncImpl>
class FunctionNumericIndexedVectorToNumberImpl : public IFunction
{
public:
    static constexpr auto name = FuncImpl::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionNumericIndexedVectorToNumberImpl<ToType, FuncImpl>>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * bitmap_type = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bitmap_type && bitmap_type->getFunctionName() == "groupNumericIndexedVector"))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be a vectorBitmap but it has type {}",
                getName(), arguments[0]->getName());
        return std::make_shared<DataTypeNumber<ToType>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return executeImplBridgeFirst(arguments, input_rows_count);
    }

private:
    ColumnPtr executeImplBridgeFirst(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        const IDataType * from_type = arguments[0].type.get();
        const auto * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        WhichDataType which(aggr_type->getArgumentsDataTypes()[0]);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return executeImplBridgeSecond<TYPE>(aggr_type->getArgumentsDataTypes()[1], arguments, input_rows_count);
        FOR_NUMERIC_INDEXED_VECTOR_INDEX_TYPES(DISPATCH)
#undef DISPATCH
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Unexpected index type {} of argument of function {}", from_type->getName(), getName());
    }

    template <typename IndexType>
    ColumnPtr
    executeImplBridgeSecond(const DataTypePtr second_type, const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        WhichDataType which(second_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return executeImplBridgeThird<IndexType, TYPE>(arguments, input_rows_count);
        FOR_NUMERIC_INDEXED_VECTOR_VALUE_TYPES(DISPATCH)
#undef DISPATCH
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unexpected value type of argument of function {}", getName());
    }

    template <typename IndexType, typename ValueType>
    ColumnPtr executeImplBridgeThird(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        const auto * type0 = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].type.get());
        Array parameters = type0->getParameters();
        if (parameters.empty() || applyVisitor(FieldVisitorToString(), parameters[0]) != "BSI")
        {
            return executeNumericIndexedVector<IndexType, ValueType, BSINumericIndexedVector>(arguments, input_rows_count);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported parameters");
        }
    }

    template <typename IndexType, typename ValueType, template <typename, typename> class VectorImpl>
    ColumnPtr executeNumericIndexedVector(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        bool is_column_const = isColumnConst(*arguments[0].column);

        // First argument
        const ColumnAggregateFunction * column_ptr = nullptr;
        if (is_column_const)
            column_ptr = typeid_cast<const ColumnAggregateFunction *>(
                typeid_cast<const ColumnConst *>(arguments[0].column.get())->getDataColumnPtr().get());
        else
            column_ptr = typeid_cast<const ColumnAggregateFunction *>(arguments[0].column.get());
        const PaddedPODArray<AggregateDataPtr> & container = column_ptr->getData();

        auto col_to = ColumnVector<ToType>::create(input_rows_count);
        typename ColumnVector<ToType>::Container & vec_to = col_to->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            AggregateDataPtr data_ptr = is_column_const ? container[0] : container[i];
            auto lhs = reinterpret_cast<const AggregateFunctionGroupNumericIndexedVectorData<IndexType, ValueType, VectorImpl> *>(data_ptr);
            vec_to[i] = FuncImpl::apply(*lhs);
        }
        return col_to;
    }
};

template <typename ToType>
struct NumericIndexedVectorCardinalityImpl
{
public:
    static constexpr auto name = "numericIndexedVectorCardinality";
    template <typename IndexType, typename ValueType, template <typename, typename> class VectorImpl>
    static ToType apply(const AggregateFunctionGroupNumericIndexedVectorData<IndexType, ValueType, VectorImpl> & lhs)
    {
        return lhs.vector.getCardinality();
    }
};

template <typename ToType>
struct NumericIndexedVectorAllValueSumImpl
{
public:
    static constexpr auto name = "numericIndexedVectorAllValueSum";
    template <typename IndexType, typename ValueType, template <typename, typename> class VectorImpl>
    static ToType apply(const AggregateFunctionGroupNumericIndexedVectorData<IndexType, ValueType, VectorImpl> & lhs)
    {
        return lhs.vector.getAllValueSum();
    }
};

using FunctionNumericIndexedVectorCardinality
    = FunctionNumericIndexedVectorToNumberImpl<UInt64, NumericIndexedVectorCardinalityImpl<UInt64>>;
using FunctionNumericIndexedVectorAllValueSum
    = FunctionNumericIndexedVectorToNumberImpl<Float64, NumericIndexedVectorAllValueSumImpl<Float64>>;

template <typename FuncImpl>
class FunctionNumericIndexedVectorToStringImpl : public IFunction
{
public:
    static constexpr auto name = FuncImpl::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionNumericIndexedVectorToStringImpl<FuncImpl>>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * bitmap_type = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bitmap_type && bitmap_type->getFunctionName() == "groupNumericIndexedVector"))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be a vectorBitmap but it has type {}",
                getName(), arguments[0]->getName());
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return executeImplBridgeFirst(arguments, input_rows_count);
    }

private:
    ColumnPtr executeImplBridgeFirst(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        const IDataType * from_type = arguments[0].type.get();
        const auto * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        WhichDataType which(aggr_type->getArgumentsDataTypes()[0]);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return executeImplBridgeSecond<TYPE>(aggr_type->getArgumentsDataTypes()[1], arguments, input_rows_count);
        FOR_NUMERIC_INDEXED_VECTOR_INDEX_TYPES(DISPATCH)
#undef DISPATCH
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Unexpected index type {} of argument of function {}", from_type->getName(), getName());
    }

    template <typename IndexType>
    ColumnPtr
    executeImplBridgeSecond(const DataTypePtr second_type, const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        WhichDataType which(second_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return executeImplBridgeThird<IndexType, TYPE>(arguments, input_rows_count);
        FOR_NUMERIC_INDEXED_VECTOR_VALUE_TYPES(DISPATCH)
#undef DISPATCH
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unexpected value type of argument of function {}", getName());
    }

    template <typename IndexType, typename ValueType>
    ColumnPtr executeImplBridgeThird(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        const auto * type0 = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].type.get());
        Array parameters = type0->getParameters();
        if (parameters.empty() || applyVisitor(FieldVisitorToString(), parameters[0]) != "BSI")
        {
            return executeNumericIndexedVector<IndexType, ValueType, BSINumericIndexedVector>(arguments, input_rows_count);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported parameters");
        }
    }

    template <typename IndexType, typename ValueType, template <typename, typename> class VectorImpl>
    ColumnPtr executeNumericIndexedVector(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        bool is_column_const = isColumnConst(*arguments[0].column);

        // First argument
        const ColumnAggregateFunction * column_ptr = nullptr;
        if (is_column_const)
            column_ptr = typeid_cast<const ColumnAggregateFunction *>(
                typeid_cast<const ColumnConst *>(arguments[0].column.get())->getDataColumnPtr().get());
        else
            column_ptr = typeid_cast<const ColumnAggregateFunction *>(arguments[0].column.get());
        const PaddedPODArray<AggregateDataPtr> & container = column_ptr->getData();

        auto col_to = ColumnString::create();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            AggregateDataPtr data_ptr = is_column_const ? container[0] : container[i];
            auto lhs = reinterpret_cast<const AggregateFunctionGroupNumericIndexedVectorData<IndexType, ValueType, VectorImpl> *>(data_ptr);
            String lhs_str = FuncImpl::apply(*lhs);
            col_to->insertData(lhs_str.c_str(), lhs_str.length());
        }
        return col_to;
    }
};

struct NumericIndexedVectorShortDebugStringImpl
{
public:
    static constexpr auto name = "numericIndexedVectorShortDebugString";
    template <typename IndexType, typename ValueType, template <typename, typename> class VectorImpl>
    static String apply(const AggregateFunctionGroupNumericIndexedVectorData<IndexType, ValueType, VectorImpl> & lhs)
    {
        return lhs.vector.shortDebugString();
    }
};

using FunctionNumericIndexedVectorShortDebugString = FunctionNumericIndexedVectorToStringImpl<NumericIndexedVectorShortDebugStringImpl>;

template <typename Name>
class FunctionNumericIndexedVectorToMapImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionNumericIndexedVectorToMapImpl>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeAggregateFunction * type = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(type && type->getFunctionName() == "groupNumericIndexedVector"))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be a vectorBitmap but it has type {}.",
                getName(), arguments[0]->getName());

        DataTypes tmp;
        tmp.push_back(type->getArgumentsDataTypes()[0]);
        tmp.push_back(type->getArgumentsDataTypes()[1]);

        return std::make_shared<DataTypeMap>(tmp);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return executeImplBridgeFirst(arguments, result_type, input_rows_count);
    }

private:
    ColumnPtr
    executeImplBridgeFirst(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        const IDataType * from_type = arguments[0].type.get();
        const auto * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        WhichDataType which(aggr_type->getArgumentsDataTypes()[0]);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return executeImplBridgeSecond<TYPE>(aggr_type->getArgumentsDataTypes()[1], arguments, result_type, input_rows_count);
        FOR_NUMERIC_INDEXED_VECTOR_INDEX_TYPES(DISPATCH)
#undef DISPATCH
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unexpected index type {} of argument of function {}",
            from_type->getName(), getName());
    }

    template <typename IndexType>
    ColumnPtr executeImplBridgeSecond(
        const DataTypePtr second_type,
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        size_t input_rows_count) const
    {
        WhichDataType which(second_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return executeImplBridgeThird<IndexType, TYPE>(arguments, result_type, input_rows_count);
        FOR_NUMERIC_INDEXED_VECTOR_VALUE_TYPES(DISPATCH)
#undef DISPATCH
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unexpected value type of argument of function {}", getName());
    }

    template <typename IndexType, typename ValueType>
    ColumnPtr
    executeImplBridgeThird(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        const auto * type0 = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].type.get());
        Array parameters = type0->getParameters();
        if (parameters.empty() || applyVisitor(FieldVisitorToString(), parameters[0]) != "BSI")
        {
            return executeNumericIndexedVector<IndexType, ValueType, BSINumericIndexedVector>(arguments, result_type, input_rows_count);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported parameters");
        }
    }

    template <typename IndexType, typename ValueType, template <typename, typename> class VectorImpl>
    ColumnPtr
    executeNumericIndexedVector(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        bool is_column_const = isColumnConst(*arguments[0].column);

        // First argument
        const ColumnAggregateFunction * column_ptr = nullptr;
        if (is_column_const)
            column_ptr = typeid_cast<const ColumnAggregateFunction *>(
                typeid_cast<const ColumnConst *>(arguments[0].column.get())->getDataColumnPtr().get());
        else
            column_ptr = typeid_cast<const ColumnAggregateFunction *>(arguments[0].column.get());
        const PaddedPODArray<AggregateDataPtr> & container = column_ptr->getData();

        const auto & result_type_map = static_cast<const DataTypeMap &>(*result_type);
        const DataTypePtr & key_type = result_type_map.getKeyType();
        const DataTypePtr & value_type = result_type_map.getValueType();

        MutableColumnPtr keys_data = key_type->createColumn();
        MutableColumnPtr values_data = value_type->createColumn();
        MutableColumnPtr offsets = DataTypeNumber<IColumn::Offset>().createColumn();

        PaddedPODArray<IndexType> & keys_pod = typeid_cast<ColumnVector<IndexType> &>(*keys_data).getData();
        PaddedPODArray<ValueType> & values_pod = typeid_cast<ColumnVector<ValueType> &>(*values_data).getData();
        offsets->reserve(input_rows_count);
        ColumnArray::Offset res_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            AggregateDataPtr data_ptr = is_column_const ? container[0] : container[i];
            auto lhs = reinterpret_cast<const AggregateFunctionGroupNumericIndexedVectorData<IndexType, ValueType, VectorImpl> *>(data_ptr);
            UInt64 count = lhs->vector.toIndexValueMap(keys_pod, values_pod);
            res_offset += count;
            offsets->insert(res_offset);
        }
        auto nested_column
            = ColumnArray::create(ColumnTuple::create(Columns{std::move(keys_data), std::move(values_data)}), std::move(offsets));
        return ColumnMap::create(nested_column);
    }
};

struct NameNumericIndexedVectorToMap
{
    static constexpr auto name = "numericIndexedVectorToMap";
};

using FunctionNumericIndexedVectorToMap = FunctionNumericIndexedVectorToMapImpl<NameNumericIndexedVectorToMap>;

}
