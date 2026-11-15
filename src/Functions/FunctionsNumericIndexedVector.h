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
#include <Common/assert_cast.h>

#include <algorithm>
#include <type_traits>

#include <Functions/FunctionsNumericIndexedVectorHelper.h>

/// Include this last — see the reason inside
#include <AggregateFunctions/AggregateFunctionGroupNumericIndexedVectorData.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int LOGICAL_ERROR;
}

static const DataTypeAggregateFunction * checkAndGetNumericIndexedVectorType(const DataTypePtr & arg, const String & func_name)
{
    const auto * type = typeid_cast<const DataTypeAggregateFunction *>(arg.get());
    if (!(type && type->getFunctionName() == NameAggregateFunctionGroupNumericIndexedVector::name))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument for function {} must be a NumericIndexedVector, but it is of type {}",
            func_name,
            arg->getName());
    return type;
}

template <typename ColumnAggregateFunction>
static const ColumnAggregateFunction * getColumnData(const ColumnPtr & column, bool & is_const)
{
    is_const = isColumnConst(*column);
    if (is_const)
        return assert_cast<const ColumnAggregateFunction *>(
            assert_cast<const ColumnConst *>(column.get())->getDataColumnPtr().get());
    return assert_cast<const ColumnAggregateFunction *>(column.get());
}

template <typename Name>
class FunctionNumericIndexedVectorBuildImpl final : public IFunction,
                                              public FunctionNumericIndexedVectorHelper<FunctionNumericIndexedVectorBuildImpl<Name>>
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionNumericIndexedVectorBuildImpl>(); }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 1; }

    String getSignatureString() const override
    {
        return "(Map(K, V)) -> AggregateFunction('groupNumericIndexedVector', K, V)";
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        const auto * map_type = assert_cast<const DataTypeMap *>(from_type);
        DataTypePtr index_type = map_type->getKeyType();
        DataTypePtr value_type = map_type->getValueType();
        const auto & result_type_agg = assert_cast<const DataTypeAggregateFunction &>(*result_type);
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

/// The pointwise functions differ only in the operation applied to each row, so they share one implementation.
class FunctionNumericIndexedVectorPointwise : public IFunction, public FunctionNumericIndexedVectorHelper<FunctionNumericIndexedVectorPointwise>
{
public:
    enum class Operation : UInt8
    {
        Add,
        Subtract,
        Multiply,
        Divide,
        Equal,
        NotEqual,
        Less,
        LessEqual,
        Greater,
        GreaterEqual,
        Max,
        Min,
    };

    explicit FunctionNumericIndexedVectorPointwise(Operation operation_) : operation(operation_) {}

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    /// Binary operations on `NumericIndexedVector` sketches, scalars, or a `Bitmap`.
    /// Both sketches must share inner index/value types, which the DSL does not
    /// model, so the signature is documentation-only.
    String getSignatureString() const override
    {
        String signature
            = "(A : AggregateFunction('groupNumericIndexedVector', Any, Any), AggregateFunction('groupNumericIndexedVector', Any, Any)) -> A"
              " OR (A : AggregateFunction('groupNumericIndexedVector', Any, Any), AggregateFunction('groupBitmap', Any)) -> A";
        if (operation != Operation::Max && operation != Operation::Min)
            signature += " OR (A : AggregateFunction('groupNumericIndexedVector', Any, Any), Number) -> A";
        return signature;
    }

    /// Keep the `getReturnTypeImpl` overload for `DataTypes` authoritative: `executeImpl` selects
    /// the implementation from the first argument and reinterprets the second state as the same
    /// specialization, so mismatched inner types must be rejected here.
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;
        return getReturnTypeImpl(data_types);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * type0 = checkAndGetNumericIndexedVectorType(arguments[0], getName());

        const auto * type1 = typeid_cast<const DataTypeAggregateFunction *>(arguments[1].get());
        if (type1)
        {
            if (type1->getFunctionName() == NameAggregateFunctionGroupNumericIndexedVector::name)
            {
                if (type0->getArgumentsDataTypes()[0]->getTypeId() != type1->getArgumentsDataTypes()[0]->getTypeId()
                    || type0->getArgumentsDataTypes()[1]->getTypeId() != type1->getArgumentsDataTypes()[1]->getTypeId())
                {
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "The nested types in NumericIndexedVector must be the same, but one is ({}, {}), and the other is ({}, {}).",
                        type0->getArgumentsDataTypes()[0]->getName(),
                        type0->getArgumentsDataTypes()[1]->getName(),
                        type1->getArgumentsDataTypes()[0]->getName(),
                        type1->getArgumentsDataTypes()[1]->getName());
                }
            }
            else if (type1->getFunctionName() == AggregateFunctionGroupBitmapData<UInt8>::name())
            {
                if (type0->getArgumentsDataTypes()[0]->getTypeId() != type1->getArgumentsDataTypes()[0]->getTypeId())
                {
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Bitmap element type and NumericIndexedVector index type must match for function {}: "
                        "got Bitmap({}) vs NumericIndexedVector({})",
                        getName(),
                        type1->getArgumentsDataTypes()[0]->getName(),
                        type0->getArgumentsDataTypes()[0]->getName());
                }
            }
            else
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Second argument for function {} must be a NumericIndexedVector, (u)Int*, Float*, or Bitmap",
                    getName());
            }
        }
        else if (operation == Operation::Max || operation == Operation::Min)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function {} must be a NumericIndexedVector or Bitmap, got {}",
                getName(),
                arguments[1]->getName());
        }

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        const auto * aggr_type = assert_cast<const DataTypeAggregateFunction *>(from_type);
        DataTypePtr index_type = aggr_type->getArgumentsDataTypes()[0];
        DataTypePtr value_type = aggr_type->getArgumentsDataTypes()[1];
        Array parameters = aggr_type->getParameters();
        return this->executeHelper(index_type, value_type, parameters, arguments, result_type, input_rows_count);
    }

    template <typename VectorImpl>
    ColumnPtr executeBSI(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
    {
        bool is_column_const[2];

        /// First argument
        const auto * first_column_ptr = getColumnData<ColumnAggregateFunction>(arguments[0].column, is_column_const[0]);

        /// Second argument
        bool is_second_column_agg = WhichDataType(arguments[1].column->getDataType()).isAggregateFunction();
        if (is_second_column_agg)
        {
            const auto * second_column_ptr = getColumnData<ColumnAggregateFunction>(arguments[1].column, is_column_const[1]);
            return executeBSIData<VectorImpl>(
                first_column_ptr, is_column_const[0], second_column_ptr, is_column_const[1], input_rows_count);
        }
        else
        {
            auto value_column = castColumn(arguments[1], std::make_shared<DataTypeNumber<typename VectorImpl::ValueType>>());
            const auto * scalar_column
                = getColumnData<ColumnVector<typename VectorImpl::ValueType>>(value_column, is_column_const[1]);
            const PaddedPODArray<typename VectorImpl::ValueType> * second_column_ptr = &scalar_column->getData();

            return executeBSIDataWithScalar<VectorImpl>(
                first_column_ptr, is_column_const[0], second_column_ptr, is_column_const[1], input_rows_count);
        }
    }

private:
    const Operation operation;

    template <typename VectorImpl, typename Rhs>
    void apply(const NumericIndexedVector<VectorImpl> & lhs, const Rhs & rhs, NumericIndexedVector<VectorImpl> & res) const
    {
        using Vector = NumericIndexedVector<VectorImpl>;
        switch (operation)
        {
            case Operation::Add: Vector::pointwiseAdd(lhs, rhs, res); break;
            case Operation::Subtract: Vector::pointwiseSubtract(lhs, rhs, res); break;
            case Operation::Multiply: Vector::pointwiseMultiply(lhs, rhs, res); break;
            case Operation::Divide: Vector::pointwiseDivide(lhs, rhs, res); break;
            case Operation::Equal: Vector::pointwiseEqual(lhs, rhs, res); break;
            case Operation::NotEqual: Vector::pointwiseNotEqual(lhs, rhs, res); break;
            case Operation::Less: Vector::pointwiseLess(lhs, rhs, res); break;
            case Operation::LessEqual: Vector::pointwiseLessEqual(lhs, rhs, res); break;
            case Operation::Greater: Vector::pointwiseGreater(lhs, rhs, res); break;
            case Operation::GreaterEqual: Vector::pointwiseGreaterEqual(lhs, rhs, res); break;
            case Operation::Max:
            case Operation::Min:
            {
                if constexpr (std::is_same_v<Rhs, Vector>)
                {
                    if (operation == Operation::Max)
                        Vector::pointwiseMax(lhs, rhs, res);
                    else
                        Vector::pointwiseMin(lhs, rhs, res);
                }
                else
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Scalar argument for function {} should have been rejected in getReturnTypeImpl",
                        getName());
                }
                break;
            }
        }
    }

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

        const String second_func_name = second_column_ptr->getAggregateFunction()->getName();
        const bool is_numeric_indexed_vector = (second_func_name == NameAggregateFunctionGroupNumericIndexedVector::name);
        const bool is_bitmap = (second_func_name == AggregateFunctionGroupBitmapData<UInt8>::name());

        if (!is_numeric_indexed_vector && !is_bitmap)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Second argument for function {} has unexpected type (should have been rejected in getReturnTypeImpl)",
                getName());

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            AggregateDataPtr first_data_ptr = is_first_column_const ? first_container[0] : first_container[i];
            auto lhs = reinterpret_cast<const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> *>(first_data_ptr);

            AggregateDataPtr second_data_ptr = is_second_column_const ? second_column_ptr->getData()[0] : second_column_ptr->getData()[i];
            col_to->insertDefault();
            auto res = reinterpret_cast<AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> *>(col_to->getData()[i]);
            res->init = true;

            if (is_numeric_indexed_vector)
            {
                auto rhs = reinterpret_cast<const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> *>(second_data_ptr);
                apply(lhs->vector, rhs->vector, res->vector);
            }
            else
            {
                auto rhs = reinterpret_cast<const AggregateFunctionGroupBitmapData<typename VectorImpl::IndexType> *>(second_data_ptr);
                AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> rhs_vector;
                rhs_vector.init = true;
                rhs_vector.vector.initializeFromVectorAndBitmap(lhs->vector, *rhs);
                apply(lhs->vector, rhs_vector.vector, res->vector);
            }
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
            apply(lhs->vector, value, res->vector);
        }
        return col_to;
    }
};

template <typename Name, FunctionNumericIndexedVectorPointwise::Operation op>
class FunctionNumericIndexedVector final : public FunctionNumericIndexedVectorPointwise
{
public:
    static constexpr auto name = Name::name;

    FunctionNumericIndexedVector() : FunctionNumericIndexedVectorPointwise(op) {}

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionNumericIndexedVector>(); }

    String getName() const override { return name; }
};

struct NameNumericIndexedVectorPointwiseAdd { static constexpr auto name = "numericIndexedVectorPointwiseAdd"; };
struct NameNumericIndexedVectorPointwiseSubtract { static constexpr auto name = "numericIndexedVectorPointwiseSubtract"; };
struct NameNumericIndexedVectorPointwiseMultiply { static constexpr auto name = "numericIndexedVectorPointwiseMultiply"; };
struct NameNumericIndexedVectorPointwiseDivide { static constexpr auto name = "numericIndexedVectorPointwiseDivide"; };
struct NameNumericIndexedVectorPointwiseEqual { static constexpr auto name = "numericIndexedVectorPointwiseEqual"; };
struct NameNumericIndexedVectorPointwiseNotEqual { static constexpr auto name = "numericIndexedVectorPointwiseNotEqual"; };
struct NameNumericIndexedVectorPointwiseLess { static constexpr auto name = "numericIndexedVectorPointwiseLess"; };
struct NameNumericIndexedVectorPointwiseLessEqual { static constexpr auto name = "numericIndexedVectorPointwiseLessEqual"; };
struct NameNumericIndexedVectorPointwiseGreater { static constexpr auto name = "numericIndexedVectorPointwiseGreater"; };
struct NameNumericIndexedVectorPointwiseGreaterEqual { static constexpr auto name = "numericIndexedVectorPointwiseGreaterEqual"; };
struct NameNumericIndexedVectorPointwiseMax
{
    static constexpr auto name = "numericIndexedVectorPointwiseMax";
};
struct NameNumericIndexedVectorPointwiseMin
{
    static constexpr auto name = "numericIndexedVectorPointwiseMin";
};

using FunctionNumericIndexedVectorPointwiseAdd
    = FunctionNumericIndexedVector<NameNumericIndexedVectorPointwiseAdd, FunctionNumericIndexedVectorPointwise::Operation::Add>;
using FunctionNumericIndexedVectorPointwiseSubtract
    = FunctionNumericIndexedVector<NameNumericIndexedVectorPointwiseSubtract, FunctionNumericIndexedVectorPointwise::Operation::Subtract>;
using FunctionNumericIndexedVectorPointwiseMultiply
    = FunctionNumericIndexedVector<NameNumericIndexedVectorPointwiseMultiply, FunctionNumericIndexedVectorPointwise::Operation::Multiply>;
using FunctionNumericIndexedVectorPointwiseDivide
    = FunctionNumericIndexedVector<NameNumericIndexedVectorPointwiseDivide, FunctionNumericIndexedVectorPointwise::Operation::Divide>;
using FunctionNumericIndexedVectorPointwiseEqual
    = FunctionNumericIndexedVector<NameNumericIndexedVectorPointwiseEqual, FunctionNumericIndexedVectorPointwise::Operation::Equal>;
using FunctionNumericIndexedVectorPointwiseNotEqual
    = FunctionNumericIndexedVector<NameNumericIndexedVectorPointwiseNotEqual, FunctionNumericIndexedVectorPointwise::Operation::NotEqual>;
using FunctionNumericIndexedVectorPointwiseLess
    = FunctionNumericIndexedVector<NameNumericIndexedVectorPointwiseLess, FunctionNumericIndexedVectorPointwise::Operation::Less>;
using FunctionNumericIndexedVectorPointwiseLessEqual
    = FunctionNumericIndexedVector<NameNumericIndexedVectorPointwiseLessEqual, FunctionNumericIndexedVectorPointwise::Operation::LessEqual>;
using FunctionNumericIndexedVectorPointwiseGreater
    = FunctionNumericIndexedVector<NameNumericIndexedVectorPointwiseGreater, FunctionNumericIndexedVectorPointwise::Operation::Greater>;
using FunctionNumericIndexedVectorPointwiseGreaterEqual
    = FunctionNumericIndexedVector<NameNumericIndexedVectorPointwiseGreaterEqual, FunctionNumericIndexedVectorPointwise::Operation::GreaterEqual>;
using FunctionNumericIndexedVectorPointwiseMax
    = FunctionNumericIndexedVector<NameNumericIndexedVectorPointwiseMax, FunctionNumericIndexedVectorPointwise::Operation::Max>;
using FunctionNumericIndexedVectorPointwiseMin
    = FunctionNumericIndexedVector<NameNumericIndexedVectorPointwiseMin, FunctionNumericIndexedVectorPointwise::Operation::Min>;

template <typename ToType, typename FuncImpl>
class FunctionNumericIndexedVectorToNumberImpl
    : public IFunction,
      public FunctionNumericIndexedVectorHelper<FunctionNumericIndexedVectorToNumberImpl<ToType, FuncImpl>>
{
public:
    static constexpr auto name = FuncImpl::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionNumericIndexedVectorToNumberImpl<ToType, FuncImpl>>(); }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 1; }

    String getSignatureString() const override
    {
        return "(AggregateFunction('groupNumericIndexedVector', Any, Any)) -> " + DataTypeNumber<ToType>{}.getName();
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        checkAndGetNumericIndexedVectorType(arguments[0], getName());
        return std::make_shared<DataTypeNumber<ToType>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        const auto * aggr_type = assert_cast<const DataTypeAggregateFunction *>(from_type);
        DataTypePtr index_type = aggr_type->getArgumentsDataTypes()[0];
        DataTypePtr value_type = aggr_type->getArgumentsDataTypes()[1];
        Array parameters = aggr_type->getParameters();
        return this->executeHelper(index_type, value_type, parameters, arguments, result_type, input_rows_count);
    }

    template <typename VectorImpl>
    ColumnPtr executeBSI(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
    {
        bool is_column_const = false;
        /// First argument
        const auto * column_ptr = getColumnData<ColumnAggregateFunction>(arguments[0].column, is_column_const);
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

class FunctionNumericIndexedVectorGetValueImpl final : public IFunction,
                                                 public FunctionNumericIndexedVectorHelper<FunctionNumericIndexedVectorGetValueImpl>
{
public:
    static constexpr auto name = "numericIndexedVectorGetValue";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionNumericIndexedVectorGetValueImpl>(); }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    /// The result type is the sketch's value type V (the second inner argument
    /// of `groupNumericIndexedVector`). The DSL captures it directly inside
    /// the matcher, so the signature is authoritative for this function.
    String getSignatureString() const override
    {
        return "(AggregateFunction('groupNumericIndexedVector', Any, V), Any) -> V";
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * type0 = checkAndGetNumericIndexedVectorType(arguments[0], getName());
        WhichDataType which(type0->getArgumentsDataTypes()[1]->getTypeId());
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return std::make_shared<DataTypeNumber<TYPE>>(); /// NOLINT(bugprone-macro-parentheses)
        FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Unexpected value type {} of NumericIndexedVector for function {}",
            type0->getArgumentsDataTypes()[1]->getName(),
            getName());
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        const auto * aggr_type = assert_cast<const DataTypeAggregateFunction *>(from_type);
        DataTypePtr index_type = aggr_type->getArgumentsDataTypes()[0];
        DataTypePtr value_type = aggr_type->getArgumentsDataTypes()[1];
        Array parameters = aggr_type->getParameters();
        return this->executeHelper(index_type, value_type, parameters, arguments, result_type, input_rows_count);
    }

    template <typename VectorImpl>
    ColumnPtr executeBSI(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
    {
        bool is_column_const[2];

        /// First argument
        const auto * column_ptr = getColumnData<ColumnAggregateFunction>(arguments[0].column, is_column_const[0]);
        const PaddedPODArray<AggregateDataPtr> & container = column_ptr->getData();

        /// Second argument
        auto uint64_column = castColumn(arguments[1], std::make_shared<DataTypeUInt64>());
        const auto * index_column = getColumnData<ColumnUInt64>(uint64_column, is_column_const[1]);
        const PaddedPODArray<UInt64> * container1 = &index_column->getData();

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

template <template <class> class FuncImpl>
class FunctionNumericIndexedVectorToSameValueTypeImpl
    : public IFunction,
      public FunctionNumericIndexedVectorHelper<FunctionNumericIndexedVectorToSameValueTypeImpl<FuncImpl>>
{
public:
    static constexpr auto name = FuncImpl<BSINumericIndexedVector<UInt8, Float64>>::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionNumericIndexedVectorToSameValueTypeImpl<FuncImpl>>(); }
    String getName() const override { return name; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * type0 = checkAndGetNumericIndexedVectorType(arguments[0], getName());

        WhichDataType which(type0->getArgumentsDataTypes()[1]->getTypeId());
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return std::make_shared<DataTypeNumber<TYPE>>(); /// NOLINT(bugprone-macro-parentheses)
        FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Unexpected value type {} of NumericIndexedVector for function {}",
            type0->getArgumentsDataTypes()[1]->getName(),
            getName());
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        const auto * aggr_type = assert_cast<const DataTypeAggregateFunction *>(from_type);
        DataTypePtr index_type = aggr_type->getArgumentsDataTypes()[0];
        DataTypePtr value_type = aggr_type->getArgumentsDataTypes()[1];
        Array parameters = aggr_type->getParameters();
        return this->executeHelper(index_type, value_type, parameters, arguments, result_type, input_rows_count);
    }

    template <typename VectorImpl>
    ColumnPtr executeBSI(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
    {
        bool is_column_const = false;
        /// First argument
        const auto * column_ptr = getColumnData<ColumnAggregateFunction>(arguments[0].column, is_column_const);
        const PaddedPODArray<AggregateDataPtr> & container = column_ptr->getData();

        using RetT = typename VectorImpl::ValueType;
        auto out = ColumnVector<RetT>::create(input_rows_count);
        auto & vec = out->getData();

        if (is_column_const)
        {
            const auto * lhs = reinterpret_cast<const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> *>(container[0]);
            RetT value = FuncImpl<VectorImpl>::apply(*lhs);
            std::fill(vec.begin(), vec.end(), value);
        }
        else
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const auto * lhs = reinterpret_cast<const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> *>(container[i]);
                vec[i] = FuncImpl<VectorImpl>::apply(*lhs);
            }
        }
        return out;
    }
};

template <typename VectorImpl>
struct NumericIndexedVectorGetMaxValueImpl
{
    static constexpr auto name = "numericIndexedVectorGetMaxValue";
    static typename VectorImpl::ValueType apply(const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs)
    {
        return lhs.vector.getMaxValue();
    }
};

template <typename VectorImpl>
struct NumericIndexedVectorGetMinValueImpl
{
    static constexpr auto name = "numericIndexedVectorGetMinValue";
    static typename VectorImpl::ValueType apply(const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs)
    {
        return lhs.vector.getMinValue();
    }
};

using FunctionNumericIndexedVectorGetMaxValue = FunctionNumericIndexedVectorToSameValueTypeImpl<NumericIndexedVectorGetMaxValueImpl>;
using FunctionNumericIndexedVectorGetMinValue = FunctionNumericIndexedVectorToSameValueTypeImpl<NumericIndexedVectorGetMinValueImpl>;


template <typename FuncImpl>
class FunctionNumericIndexedVectorToStringImpl
    : public IFunction,
      public FunctionNumericIndexedVectorHelper<FunctionNumericIndexedVectorToStringImpl<FuncImpl>>
{
public:
    static constexpr auto name = FuncImpl::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionNumericIndexedVectorToStringImpl<FuncImpl>>(); }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 1; }

    String getSignatureString() const override
    {
        return "(AggregateFunction('groupNumericIndexedVector', Any, Any)) -> String";
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        checkAndGetNumericIndexedVectorType(arguments[0], getName());
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        const auto * aggr_type = assert_cast<const DataTypeAggregateFunction *>(from_type);
        DataTypePtr index_type = aggr_type->getArgumentsDataTypes()[0];
        DataTypePtr value_type = aggr_type->getArgumentsDataTypes()[1];
        Array parameters = aggr_type->getParameters();
        return this->executeHelper(index_type, value_type, parameters, arguments, result_type, input_rows_count);
    }

    template <typename VectorImpl>
    ColumnPtr executeBSI(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
    {
        bool is_column_const = false;
        /// First argument
        const auto * column_ptr = getColumnData<ColumnAggregateFunction>(arguments[0].column, is_column_const);
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
    static constexpr auto name = "numericIndexedVectorShortDebugString";
    template <typename VectorImpl>
    static String apply(const AggregateFunctionGroupNumericIndexedVectorData<VectorImpl> & lhs)
    {
        return lhs.vector.shortDebugString();
    }
};

using FunctionNumericIndexedVectorShortDebugString = FunctionNumericIndexedVectorToStringImpl<NumericIndexedVectorShortDebugStringImpl>;

template <typename Name>
class FunctionNumericIndexedVectorToMapImpl final : public IFunction,
                                              public FunctionNumericIndexedVectorHelper<FunctionNumericIndexedVectorToMapImpl<Name>>
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionNumericIndexedVectorToMapImpl>(); }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 1; }

    String getSignatureString() const override
    {
        return "(AggregateFunction('groupNumericIndexedVector', K, V)) -> Map(K, V)";
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * type = checkAndGetNumericIndexedVectorType(arguments[0], getName());

        DataTypes tmp;
        tmp.push_back(type->getArgumentsDataTypes()[0]);
        tmp.push_back(type->getArgumentsDataTypes()[1]);

        return std::make_shared<DataTypeMap>(tmp);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        const auto * aggr_type = assert_cast<const DataTypeAggregateFunction *>(from_type);
        DataTypePtr index_type = aggr_type->getArgumentsDataTypes()[0];
        DataTypePtr value_type = aggr_type->getArgumentsDataTypes()[1];
        Array parameters = aggr_type->getParameters();
        return this->executeHelper(index_type, value_type, parameters, arguments, result_type, input_rows_count);
    }

    template <typename VectorImpl>
    ColumnPtr executeBSI(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        bool is_column_const = false;
        /// First argument
        const auto * column_ptr = getColumnData<ColumnAggregateFunction>(arguments[0].column, is_column_const);
        const PaddedPODArray<AggregateDataPtr> & container = column_ptr->getData();

        const auto & result_type_map = static_cast<const DataTypeMap &>(*result_type);
        const DataTypePtr & key_type = result_type_map.getKeyType();
        const DataTypePtr & value_type = result_type_map.getValueType();

        MutableColumnPtr keys_data = key_type->createColumn();
        MutableColumnPtr values_data = value_type->createColumn();
        MutableColumnPtr offsets = DataTypeNumber<IColumn::Offset>().createColumn();

        PaddedPODArray<typename VectorImpl::IndexType> & keys_pod
            = assert_cast<ColumnVector<typename VectorImpl::IndexType> &>(*keys_data).getData();
        PaddedPODArray<typename VectorImpl::ValueType> & values_pod
            = assert_cast<ColumnVector<typename VectorImpl::ValueType> &>(*values_data).getData();
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
