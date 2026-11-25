#pragma once

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/castColumn.h>
#include <Common/FieldVisitorConvertToNumber.h>

#include <Functions/FunctionsNumericIndexedVectorHelper.h>

/// Include this last — see the reason inside
#include <AggregateFunctions/AggregateFunctionGroupNumericIndexedVectorData.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
}

template <typename Name>
class FunctionNumericIndexedVectorBuildImpl : public IFunction,
                                              public FunctionNumericIndexedVectorHelper<FunctionNumericIndexedVectorBuildImpl<Name>>
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionNumericIndexedVectorBuildImpl>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = checkAndGetDataType<DataTypeMap>(arguments[0].get());
        if (!arg)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The argument for function {} must be Map type", getName());
        DataTypes argument_types = {arg->getKeyType(), arg->getValueType()};
        Array params_row;
        AggregateFunctionProperties properties;
        AggregateFunctionPtr numeric_indexed_vector_function;
        auto action = NullsAction::EMPTY;
        numeric_indexed_vector_function = AggregateFunctionFactory::instance().get(
            NameAggregateFunctionGroupNumericIndexedVector::name, action, argument_types, params_row, properties);
        return std::make_shared<DataTypeAggregateFunction>(numeric_indexed_vector_function, argument_types, params_row);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        const auto * map_type = typeid_cast<const DataTypeMap *>(from_type);
        DataTypePtr index_type = map_type->getKeyType();
        DataTypePtr value_type = map_type->getValueType();
        const auto & result_type_agg = typeid_cast<const DataTypeAggregateFunction &>(*result_type);
        Array parameters = result_type_agg.getParameters();
        return this->executeHelper(index_type, value_type, parameters, arguments, result_type, input_rows_count);
    }

    template <typename VectorImpl>
    ColumnPtr executeBSI(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        bool is_column_const = isColumnConst(*arguments[0].column);
        const auto * map_column = is_column_const ? checkAndGetColumnConstData<ColumnMap>(arguments[0].column.get())
                                                  : checkAndGetColumn<ColumnMap>(arguments[0].column.get());
        if (!map_column)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Argument for function {} must be map, got {} instead",
                getName(),
                arguments[0].column->getName());
        const auto & nested_column = map_column->getNestedColumn();
        const auto & keys_data = map_column->getNestedData().getColumn(0);
        const auto & values_data = map_column->getNestedData().getColumn(1);
        const auto & offsets = nested_column.getOffsets();

        const auto & result_type_agg = static_cast<const DataTypeAggregateFunction &>(*result_type);
        auto col_to = ColumnAggregateFunction::create(result_type_agg.getFunction());
        col_to->reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t from = is_column_const ? 0 : (i == 0 ? 0 : offsets[i - 1]);
            size_t to = is_column_const ? offsets[0] : offsets[i];
            col_to->insertDefault();
            auto res = reinterpret_cast<AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> *>(col_to->getData()[i]);
            res->init = true;
            for (size_t j = from; j < to; ++j)
            {
                res->vector.addValue(
                    applyVisitor(FieldVisitorConvertToNumber<typename VectorImpl::IndexType>(), keys_data[j]),
                    applyVisitor(FieldVisitorConvertToNumber<typename VectorImpl::ValueType>(), values_data[j]));
            }
        }
        return col_to;
    }
};

struct NameNumericIndexedVectorBuild
{
    static constexpr auto name = "numericIndexedVectorBuild";
};

using FunctionNumericIndexedVectorBuild = FunctionNumericIndexedVectorBuildImpl<NameNumericIndexedVectorBuild>;

template <template <class> class FuncImpl>
class FunctionNumericIndexedVector : public IFunction, public FunctionNumericIndexedVectorHelper<FunctionNumericIndexedVector<FuncImpl>>
{
public:
    /// The template parameters in BSINumericIndexedVector are randomly filled.
    static constexpr auto name = FuncImpl<BSINumericIndexedVector<UInt8, Float64>>::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionNumericIndexedVector>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * type0 = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(type0 && type0->getFunctionName() == NameAggregateFunctionGroupNumericIndexedVector::name))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be a NumericIndexedVector but it has type {}",
                getName(),
                arguments[0]->getName());

        const auto * type1 = typeid_cast<const DataTypeAggregateFunction *>(arguments[1].get());

        if (type1 && type1->getFunctionName() != NameAggregateFunctionGroupNumericIndexedVector::name)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function {} must be a NumericIndexedVector or Numeric Value",
                getName());


        if (type1
            && (type0->getArgumentsDataTypes()[0]->getTypeId() != type1->getArgumentsDataTypes()[0]->getTypeId()
                || type0->getArgumentsDataTypes()[1]->getTypeId() != type1->getArgumentsDataTypes()[1]->getTypeId()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The nested types in NumericIndexedVector must be the same, but one is ({}, {}), and the other is ({}, {}).",
                type0->getArgumentsDataTypes()[0]->getName(),
                type0->getArgumentsDataTypes()[1]->getName(),
                type1->getArgumentsDataTypes()[0]->getName(),
                type1->getArgumentsDataTypes()[1]->getName());

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        const auto * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        DataTypePtr index_type = aggr_type->getArgumentsDataTypes()[0];
        DataTypePtr value_type = aggr_type->getArgumentsDataTypes()[1];
        Array parameters = aggr_type->getParameters();
        return this->executeHelper(index_type, value_type, parameters, arguments, result_type, input_rows_count);
    }

    template <typename VectorImpl>
    ColumnPtr executeBSI(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
    {
        bool is_column_const[2];
        for (int i = 0; i < 2; ++i)
            is_column_const[i] = isColumnConst(*arguments[i].column);

        /// First argument
        const ColumnAggregateFunction * first_column_ptr = nullptr;
        if (is_column_const[0])
            first_column_ptr = typeid_cast<const ColumnAggregateFunction *>(
                typeid_cast<const ColumnConst *>(arguments[0].column.get())->getDataColumnPtr().get());
        else
            first_column_ptr = typeid_cast<const ColumnAggregateFunction *>(arguments[0].column.get());

        /// Second argument
        bool is_second_column_agg = WhichDataType(arguments[1].column->getDataType()).isAggregateFunction();
        if (is_second_column_agg)
        {
            const ColumnAggregateFunction * second_column_ptr = nullptr;
            if (is_column_const[1])
                second_column_ptr = typeid_cast<const ColumnAggregateFunction *>(
                    typeid_cast<const ColumnConst *>(arguments[1].column.get())->getDataColumnPtr().get());
            else
                second_column_ptr = typeid_cast<const ColumnAggregateFunction *>(arguments[1].column.get());
            return executeBSIData<VectorImpl>(
                first_column_ptr, is_column_const[0], second_column_ptr, is_column_const[1], input_rows_count);
        }
        else
        {
            auto value_column = castColumn(arguments[1], std::make_shared<DataTypeNumber<typename VectorImpl::ValueType>>());
            const PaddedPODArray<typename VectorImpl::ValueType> * second_column_ptr;
            if (is_column_const[1])
                second_column_ptr = &typeid_cast<const ColumnVector<typename VectorImpl::ValueType> &>(
                                         typeid_cast<const ColumnConst &>(*value_column.get()).getDataColumn())
                                         .getData();
            else
                second_column_ptr = &typeid_cast<const ColumnVector<typename VectorImpl::ValueType> &>(*value_column.get()).getData();

            return executeBSIDataWithScalar<VectorImpl>(
                first_column_ptr, is_column_const[0], second_column_ptr, is_column_const[1], input_rows_count);
        }
    }

private:
    template <typename VectorImpl>
    ColumnPtr executeBSIData(
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
            auto lhs = reinterpret_cast<const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> *>(first_data_ptr);

            AggregateDataPtr second_data_ptr = is_second_column_const ? second_column_ptr->getData()[0] : second_column_ptr->getData()[i];
            auto rhs = reinterpret_cast<const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> *>(second_data_ptr);

            col_to->insertDefault();
            auto res = reinterpret_cast<AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> *>(col_to->getData()[i]);
            res->init = true;
            FuncImpl<VectorImpl>::apply(*lhs, *rhs, *res);
        }
        return col_to;
    }

    template <typename VectorImpl>
    ColumnPtr executeBSIDataWithScalar(
        const ColumnAggregateFunction * first_column_ptr,
        bool is_first_column_const,
        const PaddedPODArray<typename VectorImpl::ValueType> * second_column_ptr,
        bool is_second_column_const,
        size_t input_rows_count) const
    {
        auto col_to = ColumnAggregateFunction::create(first_column_ptr->getAggregateFunction());
        col_to->reserve(input_rows_count);

        const PaddedPODArray<AggregateDataPtr> & first_container = first_column_ptr->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            AggregateDataPtr first_data_ptr = is_first_column_const ? first_container[0] : first_container[i];
            auto lhs = reinterpret_cast<const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> *>(first_data_ptr);

            typename VectorImpl::ValueType value = is_second_column_const ? (*second_column_ptr)[0] : (*second_column_ptr)[i];

            col_to->insertDefault();
            auto res = reinterpret_cast<AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> *>(col_to->getData()[i]);
            res->init = true;
            FuncImpl<VectorImpl>::apply(*lhs, value, *res);
        }
        return col_to;
    }
};


template <typename VectorImpl>
struct NumericIndexedVectorPointwiseAddImpl
{
    static constexpr auto name = "numericIndexedVectorPointwiseAdd";

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const typename VectorImpl::ValueType & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseAdd(lhs.vector, rhs, res.vector);
    }

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseAdd(lhs.vector, rhs.vector, res.vector);
    }
};

template <typename VectorImpl>
struct NumericIndexedVectorPointwiseSubtractImpl
{
    static constexpr auto name = "numericIndexedVectorPointwiseSubtract";

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const typename VectorImpl::ValueType & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseSubtract(lhs.vector, rhs, res.vector);
    }

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseSubtract(lhs.vector, rhs.vector, res.vector);
    }
};

template <typename VectorImpl>
struct NumericIndexedVectorPointwiseMultiplyImpl
{
    static constexpr auto name = "numericIndexedVectorPointwiseMultiply";

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const typename VectorImpl::ValueType & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseMultiply(lhs.vector, rhs, res.vector);
    }

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseMultiply(lhs.vector, rhs.vector, res.vector);
    }
};

template <typename VectorImpl>
struct NumericIndexedVectorPointwiseDivideImpl
{
    static constexpr auto name = "numericIndexedVectorPointwiseDivide";

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const typename VectorImpl::ValueType & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseDivide(lhs.vector, rhs, res.vector);
    }

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseDivide(lhs.vector, rhs.vector, res.vector);
    }
};

template <typename VectorImpl>
struct NumericIndexedVectorPointwiseEqualImpl
{
    static constexpr auto name = "numericIndexedVectorPointwiseEqual";

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const typename VectorImpl::ValueType & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseEqual(lhs.vector, rhs, res.vector);
    }

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseEqual(lhs.vector, rhs.vector, res.vector);
    }
};

template <typename VectorImpl>
struct NumericIndexedVectorPointwiseNotEqualImpl
{
    static constexpr auto name = "numericIndexedVectorPointwiseNotEqual";

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const typename VectorImpl::ValueType & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseNotEqual(lhs.vector, rhs, res.vector);
    }

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseNotEqual(lhs.vector, rhs.vector, res.vector);
    }
};

template <typename VectorImpl>
struct NumericIndexedVectorPointwiseLessImpl
{
    static constexpr auto name = "numericIndexedVectorPointwiseLess";

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const typename VectorImpl::ValueType & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseLess(lhs.vector, rhs, res.vector);
    }

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseLess(lhs.vector, rhs.vector, res.vector);
    }
};

template <typename VectorImpl>
struct NumericIndexedVectorPointwiseLessEqualImpl
{
    static constexpr auto name = "numericIndexedVectorPointwiseLessEqual";

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const typename VectorImpl::ValueType & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseLessEqual(lhs.vector, rhs, res.vector);
    }

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseLessEqual(lhs.vector, rhs.vector, res.vector);
    }
};

template <typename VectorImpl>
struct NumericIndexedVectorPointwiseGreaterImpl
{
    static constexpr auto name = "numericIndexedVectorPointwiseGreater";

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const typename VectorImpl::ValueType & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseGreater(lhs.vector, rhs, res.vector);
    }

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseGreater(lhs.vector, rhs.vector, res.vector);
    }
};
template <typename VectorImpl>
struct NumericIndexedVectorPointwiseGreaterEqualImpl
{
    static constexpr auto name = "numericIndexedVectorPointwiseGreaterEqual";

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const typename VectorImpl::ValueType & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseGreaterEqual(lhs.vector, rhs, res.vector);
    }

    static void apply(
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs,
        const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & rhs,
        AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & res)
    {
        NumericIndexedVector<VectorImpl>::pointwiseGreaterEqual(lhs.vector, rhs.vector, res.vector);
    }
};

using FunctionNumericIndexedVectorPointwiseAdd = FunctionNumericIndexedVector<NumericIndexedVectorPointwiseAddImpl>;
using FunctionNumericIndexedVectorPointwiseSubtract = FunctionNumericIndexedVector<NumericIndexedVectorPointwiseSubtractImpl>;
using FunctionNumericIndexedVectorPointwiseMultiply = FunctionNumericIndexedVector<NumericIndexedVectorPointwiseMultiplyImpl>;
using FunctionNumericIndexedVectorPointwiseDivide = FunctionNumericIndexedVector<NumericIndexedVectorPointwiseDivideImpl>;
using FunctionNumericIndexedVectorPointwiseEqual = FunctionNumericIndexedVector<NumericIndexedVectorPointwiseEqualImpl>;
using FunctionNumericIndexedVectorPointwiseNotEqual = FunctionNumericIndexedVector<NumericIndexedVectorPointwiseNotEqualImpl>;
using FunctionNumericIndexedVectorPointwiseLess = FunctionNumericIndexedVector<NumericIndexedVectorPointwiseLessImpl>;
using FunctionNumericIndexedVectorPointwiseLessEqual = FunctionNumericIndexedVector<NumericIndexedVectorPointwiseLessEqualImpl>;
using FunctionNumericIndexedVectorPointwiseGreater = FunctionNumericIndexedVector<NumericIndexedVectorPointwiseGreaterImpl>;
using FunctionNumericIndexedVectorPointwiseGreaterEqual = FunctionNumericIndexedVector<NumericIndexedVectorPointwiseGreaterEqualImpl>;

template <typename ToType, typename FuncImpl>
class FunctionNumericIndexedVectorToNumberImpl
    : public IFunction,
      public FunctionNumericIndexedVectorHelper<FunctionNumericIndexedVectorToNumberImpl<ToType, FuncImpl>>
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
        const auto * agg_type = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(agg_type && agg_type->getFunctionName() == NameAggregateFunctionGroupNumericIndexedVector::name))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be a NumericIndexedVector but it has type {}",
                getName(),
                arguments[0]->getName());
        return std::make_shared<DataTypeNumber<ToType>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        const auto * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        DataTypePtr index_type = aggr_type->getArgumentsDataTypes()[0];
        DataTypePtr value_type = aggr_type->getArgumentsDataTypes()[1];
        Array parameters = aggr_type->getParameters();
        return this->executeHelper(index_type, value_type, parameters, arguments, result_type, input_rows_count);
    }

    template <typename VectorImpl>
    ColumnPtr executeBSI(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
    {
        bool is_column_const = isColumnConst(*arguments[0].column);

        /// First argument
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
            auto lhs = reinterpret_cast<const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> *>(data_ptr);
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
    template <typename VectorImpl>
    static ToType apply(const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs)
    {
        return lhs.vector.getCardinality();
    }
};

template <typename ToType>
struct NumericIndexedVectorAllValueSumImpl
{
public:
    static constexpr auto name = "numericIndexedVectorAllValueSum";
    template <typename VectorImpl>
    static ToType apply(const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs)
    {
        return lhs.vector.getAllValueSum();
    }
};

using FunctionNumericIndexedVectorCardinality
    = FunctionNumericIndexedVectorToNumberImpl<UInt64, NumericIndexedVectorCardinalityImpl<UInt64>>;
using FunctionNumericIndexedVectorAllValueSum
    = FunctionNumericIndexedVectorToNumberImpl<Float64, NumericIndexedVectorAllValueSumImpl<Float64>>;

class FunctionNumericIndexedVectorGetValueImpl : public IFunction,
                                                 public FunctionNumericIndexedVectorHelper<FunctionNumericIndexedVectorGetValueImpl>
{
public:
    static constexpr auto name = "numericIndexedVectorGetValue";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionNumericIndexedVectorGetValueImpl>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * type0 = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(type0 && type0->getFunctionName() == NameAggregateFunctionGroupNumericIndexedVector::name))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be a NumericIndexedVector but it has type {}",
                getName(),
                arguments[0]->getName());
        WhichDataType which(type0->getArgumentsDataTypes()[1]->getTypeId());
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return std::make_shared<DataTypeNumber<TYPE>>(); /// NOLINT
        FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Unexpected value type {} of argument of function {}",
            type0->getArgumentsDataTypes()[1]->getName(),
            getName());
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        const auto * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        DataTypePtr index_type = aggr_type->getArgumentsDataTypes()[0];
        DataTypePtr value_type = aggr_type->getArgumentsDataTypes()[1];
        Array parameters = aggr_type->getParameters();
        return this->executeHelper(index_type, value_type, parameters, arguments, result_type, input_rows_count);
    }

    template <typename VectorImpl>
    ColumnPtr executeBSI(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
    {
        bool is_column_const[2];
        for (int i = 0; i < 2; ++i)
            is_column_const[i] = isColumnConst(*arguments[i].column);

        /// First argument
        const ColumnAggregateFunction * column_ptr = nullptr;
        if (is_column_const[0])
            column_ptr = typeid_cast<const ColumnAggregateFunction *>(
                typeid_cast<const ColumnConst *>(arguments[0].column.get())->getDataColumnPtr().get());
        else
            column_ptr = typeid_cast<const ColumnAggregateFunction *>(arguments[0].column.get());
        const PaddedPODArray<AggregateDataPtr> & container = column_ptr->getData();

        /// Second argument
        auto uint64_column = castColumn(arguments[1], std::make_shared<DataTypeUInt64>());
        const IColumn * second_column_ptr = uint64_column.get();

        const PaddedPODArray<UInt64> * container1;
        if (is_column_const[1])
            container1 = &typeid_cast<const ColumnUInt64 &>(typeid_cast<const ColumnConst &>(*second_column_ptr).getDataColumn()).getData();
        else
            container1 = &typeid_cast<const ColumnUInt64 &>(*second_column_ptr).getData();

        auto col_to = ColumnVector<typename VectorImpl::ValueType>::create(input_rows_count);
        typename ColumnVector<typename VectorImpl::ValueType>::Container & vec_to = col_to->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            AggregateDataPtr data_ptr = is_column_const[0] ? container[0] : container[i];
            auto lhs = reinterpret_cast<const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> *>(data_ptr);

            UInt64 index = is_column_const[1] ? (*container1)[0] : (*container1)[i];

            vec_to[i] = lhs->vector.getValue(static_cast<typename VectorImpl::IndexType>(index));
        }
        return col_to;
    }
};


template <typename FuncImpl>
class FunctionNumericIndexedVectorToStringImpl
    : public IFunction,
      public FunctionNumericIndexedVectorHelper<FunctionNumericIndexedVectorToStringImpl<FuncImpl>>
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
        const auto * agg_type = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(agg_type && agg_type->getFunctionName() == NameAggregateFunctionGroupNumericIndexedVector::name))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be a NumericIndexedVector but it has type {}",
                getName(),
                arguments[0]->getName());
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        const auto * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        DataTypePtr index_type = aggr_type->getArgumentsDataTypes()[0];
        DataTypePtr value_type = aggr_type->getArgumentsDataTypes()[1];
        Array parameters = aggr_type->getParameters();
        return this->executeHelper(index_type, value_type, parameters, arguments, result_type, input_rows_count);
    }

    template <typename VectorImpl>
    ColumnPtr executeBSI(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
    {
        bool is_column_const = isColumnConst(*arguments[0].column);

        /// First argument
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
            auto lhs = reinterpret_cast<const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> *>(data_ptr);
            String lhs_str = FuncImpl::apply(*lhs);
            col_to->insertData(lhs_str.data(), lhs_str.length());
        }
        return col_to;
    }
};

struct NumericIndexedVectorShortDebugStringImpl
{
public:
    static constexpr auto name = "numericIndexedVectorShortDebugString";
    template <typename VectorImpl>
    static String apply(const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs)
    {
        return lhs.vector.shortDebugString();
    }
};

using FunctionNumericIndexedVectorShortDebugString = FunctionNumericIndexedVectorToStringImpl<NumericIndexedVectorShortDebugStringImpl>;

template <typename Name>
class FunctionNumericIndexedVectorToMapImpl : public IFunction,
                                              public FunctionNumericIndexedVectorHelper<FunctionNumericIndexedVectorToMapImpl<Name>>
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
        if (!(type && type->getFunctionName() == NameAggregateFunctionGroupNumericIndexedVector::name))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be a NumericIndexedVector but it has type {}.",
                getName(),
                arguments[0]->getName());

        DataTypes tmp;
        tmp.push_back(type->getArgumentsDataTypes()[0]);
        tmp.push_back(type->getArgumentsDataTypes()[1]);

        return std::make_shared<DataTypeMap>(tmp);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        const auto * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        DataTypePtr index_type = aggr_type->getArgumentsDataTypes()[0];
        DataTypePtr value_type = aggr_type->getArgumentsDataTypes()[1];
        Array parameters = aggr_type->getParameters();
        return this->executeHelper(index_type, value_type, parameters, arguments, result_type, input_rows_count);
    }

    template <typename VectorImpl>
    ColumnPtr executeBSI(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        bool is_column_const = isColumnConst(*arguments[0].column);

        /// First argument
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

        PaddedPODArray<typename VectorImpl::IndexType> & keys_pod
            = typeid_cast<ColumnVector<typename VectorImpl::IndexType> &>(*keys_data).getData();
        PaddedPODArray<typename VectorImpl::ValueType> & values_pod
            = typeid_cast<ColumnVector<typename VectorImpl::ValueType> &>(*values_data).getData();
        offsets->reserve(input_rows_count);
        ColumnArray::Offset res_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            AggregateDataPtr data_ptr = is_column_const ? container[0] : container[i];
            auto lhs = reinterpret_cast<const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> *>(data_ptr);
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
