#pragma once

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** Bitmap functions.
  * Build a bitmap from integer array:
  * bitmapBuild: integer[] -> bitmap
  *
  * Convert bitmap to integer array:
  * bitmapToArray:	bitmap -> integer[]
  *
  * Return subset in specified range (not include the range_end):
  * bitmapSubsetInRange:    bitmap,integer,integer -> bitmap
  *
  * Two bitmap and calculation:
  * bitmapAnd:	bitmap,bitmap -> bitmap
  *
  * Two bitmap or calculation:
  * bitmapOr:	bitmap,bitmap -> bitmap
  *
  * Two bitmap xor calculation:
  * bitmapXor:	bitmap,bitmap -> bitmap
  *
  * Two bitmap andnot calculation:
  * bitmapAndnot:	bitmap,bitmap -> bitmap
  *
  * Retrun bitmap cardinality:
  * bitmapCardinality:	bitmap -> integer
  *
  * Two bitmap and calculation, return cardinality:
  * bitmapAndCardinality:	bitmap,bitmap -> integer
  *
  * Two bitmap or calculation, return cardinality:
  * bitmapOrCardinality:	bitmap,bitmap -> integer
  *
  * Two bitmap xor calculation, return cardinality:
  * bitmapXorCardinality:	bitmap,bitmap -> integer
  *
  * Two bitmap andnot calculation, return cardinality:
  * bitmapAndnotCardinality: bitmap,bitmap -> integer
  *
  * Determine if a bitmap contains the given integer:
  * bitmapContains: bitmap,integer -> bool
  *
  * Judge if a bitmap is superset of the another one:
  * bitmapHasAll: bitmap,bitmap -> bool
  *
  * Judge if the intersection of two bitmap is nonempty:
  * bitmapHasAny: bitmap,bitmap -> bool
  */

template <typename Name>
class FunctionBitmapBuildImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionBitmapBuildImpl>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->onlyNull())
            return arguments[0];

        auto array_type = typeid_cast<const DataTypeArray *>(arguments[0].get());
        if (!array_type)
            throw Exception(
                "First argument for function " + getName() + " must be an array but it has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto nested_type = array_type->getNestedType();
        DataTypes argument_types = {nested_type};
        Array params_row;
        AggregateFunctionPtr bitmap_function
            = AggregateFunctionFactory::instance().get(AggregateFunctionGroupBitmapData<UInt32>::name(), argument_types, params_row);

        return std::make_shared<DataTypeAggregateFunction>(bitmap_function, argument_types, params_row);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /* input_rows_count */) override
    {
        const IDataType * from_type = block.getByPosition(arguments[0]).type.get();
        auto array_type = typeid_cast<const DataTypeArray *>(from_type);
        auto nested_type = array_type->getNestedType();

        DataTypes argument_types = {nested_type};

        WhichDataType which(nested_type);
        if (which.isUInt8())
            executeBitmapData<UInt8>(block, argument_types, arguments, result);
        else if (which.isUInt16())
            executeBitmapData<UInt16>(block, argument_types, arguments, result);
        else if (which.isUInt32())
            executeBitmapData<UInt32>(block, argument_types, arguments, result);
        else if (which.isUInt64())
            executeBitmapData<UInt64>(block, argument_types, arguments, result);
        else
            throw Exception(
                "Unexpected type " + from_type->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    template <typename T>
    void executeBitmapData(Block & block, DataTypes & argument_types, const ColumnNumbers & arguments, size_t result)
    {
        // input data
        const ColumnArray * array = typeid_cast<const ColumnArray *>(block.getByPosition(arguments[0]).column.get());
        ColumnPtr mapped = array->getDataPtr();
        const ColumnArray::Offsets & offsets = array->getOffsets();
        const ColumnVector<T> * column = checkAndGetColumn<ColumnVector<T>>(&*mapped);
        const typename ColumnVector<T>::Container & input_data = column->getData();

        // output data
        Array params_row;
        AggregateFunctionPtr bitmap_function
            = AggregateFunctionFactory::instance().get(AggregateFunctionGroupBitmapData<UInt32>::name(), argument_types, params_row);
        auto col_to = ColumnAggregateFunction::create(bitmap_function);
        col_to->reserve(offsets.size());

        size_t pos = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            col_to->insertDefault();
            AggregateFunctionGroupBitmapData<T> & bitmap_data
                = *reinterpret_cast<AggregateFunctionGroupBitmapData<T> *>(col_to->getData()[i]);
            for (; pos < offsets[i]; ++pos)
            {
                bitmap_data.rbs.add(input_data[pos]);
            }
        }
        block.getByPosition(result).column = std::move(col_to);
    }
};

template <typename Name>
class FunctionBitmapToArrayImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionBitmapToArrayImpl>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeAggregateFunction * bitmap_type = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bitmap_type && bitmap_type->getFunctionName() == AggregateFunctionGroupBitmapData<UInt32>::name()))
            throw Exception(
                "First argument for function " + getName() + " must be an bitmap but it has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const DataTypePtr data_type = bitmap_type->getArgumentsDataTypes()[0];

        return std::make_shared<DataTypeArray>(data_type);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        // input data
        const auto & return_type = block.getByPosition(result).type;
        auto res_ptr = return_type->createColumn();
        ColumnArray & res = assert_cast<ColumnArray &>(*res_ptr);

        IColumn & res_data = res.getData();
        ColumnArray::Offsets & res_offsets = res.getOffsets();

        const IDataType * from_type = block.getByPosition(arguments[0]).type.get();
        const DataTypeAggregateFunction * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        WhichDataType which(aggr_type->getArgumentsDataTypes()[0]);
        if (which.isUInt8())
            executeIntType<UInt8>(block, arguments, input_rows_count, res_data, res_offsets);
        else if (which.isUInt16())
            executeIntType<UInt16>(block, arguments, input_rows_count, res_data, res_offsets);
        else if (which.isUInt32())
            executeIntType<UInt32>(block, arguments, input_rows_count, res_data, res_offsets);
        else if (which.isUInt64())
            executeIntType<UInt64>(block, arguments, input_rows_count, res_data, res_offsets);
        else
            throw Exception(
                "Unexpected type " + from_type->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        block.getByPosition(result).column = std::move(res_ptr);
    }

private:
    using ToType = UInt64;

    template <typename T>
    void executeIntType(
        Block & block, const ColumnNumbers & arguments, size_t input_rows_count, IColumn & res_data_col, ColumnArray::Offsets & res_offsets)
        const
    {
        const ColumnAggregateFunction * column
            = typeid_cast<const ColumnAggregateFunction *>(block.getByPosition(arguments[0]).column.get());

        PaddedPODArray<T> & res_data = typeid_cast<ColumnVector<T> &>(res_data_col).getData();
        ColumnArray::Offset res_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const AggregateFunctionGroupBitmapData<T> & bd1
                = *reinterpret_cast<const AggregateFunctionGroupBitmapData<T> *>(column->getData()[i]);
            UInt64 count = bd1.rbs.rb_to_array(res_data);
            res_offset += count;
            res_offsets.emplace_back(res_offset);
        }
    }
};

class FunctionBitmapSubsetInRange : public IFunction
{
public:
    static constexpr auto name = "bitmapSubsetInRange";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionBitmapSubsetInRange>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 3; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeAggregateFunction * bitmap_type = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bitmap_type && bitmap_type->getFunctionName() == AggregateFunctionGroupBitmapData<UInt32>::name()))
            throw Exception(
                "First argument for function " + getName() + " must be an bitmap but it has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto arg_type1 = typeid_cast<const DataTypeNumber<UInt32> *>(arguments[1].get());
        if (!(arg_type1))
            throw Exception(
                "Second argument for function " + getName() + " must be UInt32 but it has type " + arguments[1]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto arg_type2 = typeid_cast<const DataTypeNumber<UInt32> *>(arguments[1].get());
        if (!(arg_type2))
            throw Exception(
                "Third argument for function " + getName() + " must be UInt32 but it has type " + arguments[2]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const IDataType * from_type = block.getByPosition(arguments[0]).type.get();
        const DataTypeAggregateFunction * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        WhichDataType which(aggr_type->getArgumentsDataTypes()[0]);
        if (which.isUInt8())
            executeIntType<UInt8>(block, arguments, result, input_rows_count);
        else if (which.isUInt16())
            executeIntType<UInt16>(block, arguments, result, input_rows_count);
        else if (which.isUInt32())
            executeIntType<UInt32>(block, arguments, result, input_rows_count);
        else if (which.isUInt64())
            executeIntType<UInt64>(block, arguments, result, input_rows_count);
        else
            throw Exception(
                "Unexpected type " + from_type->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    using ToType = UInt64;

    template <typename T>
    void executeIntType(
        Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
        const
    {
        const IColumn * columns[3];
        bool is_column_const[3];
        const ColumnAggregateFunction * colAggFunc;
        const PaddedPODArray<AggregateDataPtr> * container0;
        const PaddedPODArray<UInt32> * container1, * container2;

        for (size_t i = 0; i < 3; ++i)
        {
            columns[i] = block.getByPosition(arguments[i]).column.get();
            is_column_const[i] = isColumnConst(*columns[i]);
        }
        if (is_column_const[0])
        {
            colAggFunc = typeid_cast<const ColumnAggregateFunction*>(typeid_cast<const ColumnConst*>(columns[0])->getDataColumnPtr().get());
        }
        else
        {
            colAggFunc = typeid_cast<const ColumnAggregateFunction*>(columns[0]);
        }
        container0 = &colAggFunc->getData();
        if (is_column_const[1])
            container1 = &typeid_cast<const ColumnUInt32*>(typeid_cast<const ColumnConst*>(columns[1])->getDataColumnPtr().get())->getData();
        else
            container1 = &typeid_cast<const ColumnUInt32*>(columns[1])->getData();
        if (is_column_const[2])
            container2 = &typeid_cast<const ColumnUInt32*>(typeid_cast<const ColumnConst*>(columns[2])->getDataColumnPtr().get())->getData();
        else
            container2 = &typeid_cast<const ColumnUInt32*>(columns[2])->getData();

        auto col_to = ColumnAggregateFunction::create(colAggFunc->getAggregateFunction());
        col_to->reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const AggregateDataPtr dataPtr0 = is_column_const[0] ? (*container0)[0] : (*container0)[i];
            const AggregateFunctionGroupBitmapData<T>& bd0
                = *reinterpret_cast<const AggregateFunctionGroupBitmapData<T>*>(dataPtr0);
            const UInt32 range_start = is_column_const[1] ? (*container1)[0] : (*container1)[i];
            const UInt32 range_end = is_column_const[2] ? (*container2)[0] : (*container2)[i];

            col_to->insertDefault();
            AggregateFunctionGroupBitmapData<T> & bd2
                = *reinterpret_cast<AggregateFunctionGroupBitmapData<T> *>(col_to->getData()[i]);
            bd0.rbs.rb_range(range_start, range_end, bd2.rbs);
        }
        block.getByPosition(result).column = std::move(col_to);
    }
};

template <typename Name>
class FunctionBitmapSelfCardinalityImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionBitmapSelfCardinalityImpl>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        auto bitmap_type = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bitmap_type && bitmap_type->getFunctionName() == AggregateFunctionGroupBitmapData<UInt32>::name()))
            throw Exception(
                "First argument for function " + getName() + " must be an bitmap but it has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeNumber<ToType>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        auto col_to = ColumnVector<ToType>::create(input_rows_count);
        typename ColumnVector<ToType>::Container & vec_to = col_to->getData();
        const IDataType * from_type = block.getByPosition(arguments[0]).type.get();

        const DataTypeAggregateFunction * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        WhichDataType which(aggr_type->getArgumentsDataTypes()[0]);
        if (which.isUInt8())
            executeIntType<UInt8>(block, arguments, input_rows_count, vec_to);
        else if (which.isUInt16())
            executeIntType<UInt16>(block, arguments, input_rows_count, vec_to);
        else if (which.isUInt32())
            executeIntType<UInt32>(block, arguments, input_rows_count, vec_to);
        else if (which.isUInt64())
            executeIntType<UInt64>(block, arguments, input_rows_count, vec_to);
        else
            throw Exception(
                "Unexpected type " + from_type->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        block.getByPosition(result).column = std::move(col_to);
    }

private:
    using ToType = UInt64;

    template <typename T>
    void executeIntType(
        Block & block, const ColumnNumbers & arguments, size_t input_rows_count, typename ColumnVector<ToType>::Container & vec_to)
    {
        const ColumnAggregateFunction * column
            = typeid_cast<const ColumnAggregateFunction *>(block.getByPosition(arguments[0]).column.get());
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const AggregateFunctionGroupBitmapData<T> & bd1
                = *reinterpret_cast<const AggregateFunctionGroupBitmapData<T> *>(column->getData()[i]);
            vec_to[i] = bd1.rbs.size();
        }
    }
};

template <typename T>
struct BitmapAndCardinalityImpl
{
    using ReturnType = UInt64;
    static UInt64 apply(const AggregateFunctionGroupBitmapData<T> & bd1, const AggregateFunctionGroupBitmapData<T> & bd2)
    {
        // roaring_bitmap_and_cardinality( rb1, rb2 );
        return bd1.rbs.rb_and_cardinality(bd2.rbs);
    }
};


template <typename T>
struct BitmapOrCardinalityImpl
{
    using ReturnType = UInt64;
    static UInt64 apply(const AggregateFunctionGroupBitmapData<T> & bd1, const AggregateFunctionGroupBitmapData<T> & bd2)
    {
        // return roaring_bitmap_or_cardinality( rb1, rb2 );
        return bd1.rbs.rb_or_cardinality(bd2.rbs);
    }
};

template <typename T>
struct BitmapXorCardinalityImpl
{
    using ReturnType = UInt64;
    static UInt64 apply(const AggregateFunctionGroupBitmapData<T> & bd1, const AggregateFunctionGroupBitmapData<T> & bd2)
    {
        // return roaring_bitmap_xor_cardinality( rb1, rb2 );
        return bd1.rbs.rb_xor_cardinality(bd2.rbs);
    }
};

template <typename T>
struct BitmapAndnotCardinalityImpl
{
    using ReturnType = UInt64;
    static UInt64 apply(const AggregateFunctionGroupBitmapData<T> & bd1, const AggregateFunctionGroupBitmapData<T> & bd2)
    {
        // roaring_bitmap_andnot_cardinality( rb1, rb2 );
        return bd1.rbs.rb_andnot_cardinality(bd2.rbs);
    }
};

template <typename T>
struct BitmapHasAllImpl
{
    using ReturnType = UInt8;
    static UInt8 apply(const AggregateFunctionGroupBitmapData<T> & bd1, const AggregateFunctionGroupBitmapData<T> & bd2)
    {
        return bd1.rbs.rb_is_subset(bd2.rbs);
    }
};

template <typename T>
struct BitmapHasAnyImpl
{
    using ReturnType = UInt8;
    static UInt8 apply(const AggregateFunctionGroupBitmapData<T> & bd1, const AggregateFunctionGroupBitmapData<T> & bd2)
    {
        return bd1.rbs.rb_intersect(bd2.rbs);
    }
};

class FunctionBitmapContains : public IFunction
{
public:
    static constexpr auto name = "bitmapContains";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionBitmapContains>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        auto bitmap_type0 = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bitmap_type0 && bitmap_type0->getFunctionName() == AggregateFunctionGroupBitmapData<UInt32>::name()))
            throw Exception(
                "First argument for function " + getName() + " must be an bitmap but it has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        auto arg_type1 = typeid_cast<const DataTypeNumber<UInt32> *>(arguments[1].get());
        if (!(arg_type1))
            throw Exception(
                "Second argument for function " + getName() + " must be UInt32 but it has type " + arguments[1]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<UInt8>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        auto col_to = ColumnVector<UInt8>::create(input_rows_count);
        typename ColumnVector<UInt8>::Container & vec_to = col_to->getData();
        const IDataType * from_type = block.getByPosition(arguments[0]).type.get();

        const DataTypeAggregateFunction * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        WhichDataType which(aggr_type->getArgumentsDataTypes()[0]);
        if (which.isUInt8())
            executeIntType<UInt8>(block, arguments, input_rows_count, vec_to);
        else if (which.isUInt16())
            executeIntType<UInt16>(block, arguments, input_rows_count, vec_to);
        else if (which.isUInt32())
            executeIntType<UInt32>(block, arguments, input_rows_count, vec_to);
        else if (which.isUInt64())
            executeIntType<UInt64>(block, arguments, input_rows_count, vec_to);
        else
            throw Exception(
                "Unexpected type " + from_type->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        block.getByPosition(result).column = std::move(col_to);
    }

private:
    template <typename T>
    void executeIntType(
        Block & block, const ColumnNumbers & arguments, size_t input_rows_count, typename ColumnVector<UInt8>::Container & vec_to)
    {
        const IColumn * columns[2];
        bool is_column_const[2];
        const PaddedPODArray<AggregateDataPtr> * container0;
        const PaddedPODArray<UInt32> * container1;

        for (size_t i = 0; i < 2; ++i)
        {
            columns[i] = block.getByPosition(arguments[i]).column.get();
            is_column_const[i] = isColumnConst(*columns[i]);
        }
        if (is_column_const[0])
            container0 = &typeid_cast<const ColumnAggregateFunction*>(typeid_cast<const ColumnConst*>(columns[0])->getDataColumnPtr().get())->getData();
        else
            container0 = &typeid_cast<const ColumnAggregateFunction*>(columns[0])->getData();
        if (is_column_const[1])
            container1 = &typeid_cast<const ColumnUInt32*>(typeid_cast<const ColumnConst*>(columns[1])->getDataColumnPtr().get())->getData();
        else
            container1 = &typeid_cast<const ColumnUInt32*>(columns[1])->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const AggregateDataPtr dataPtr0 = is_column_const[0] ? (*container0)[0] : (*container0)[i];
            const UInt32 data1 = is_column_const[1] ? (*container1)[0] : (*container1)[i];
            const AggregateFunctionGroupBitmapData<T>& bd0
                = *reinterpret_cast<const AggregateFunctionGroupBitmapData<T>*>(dataPtr0);
            vec_to[i] = bd0.rbs.rb_contains(data1);
        }
    }
};

template <template <typename> class Impl, typename Name, typename ToType>
class FunctionBitmapCardinality : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionBitmapCardinality>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        auto bitmap_type0 = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bitmap_type0 && bitmap_type0->getFunctionName() == AggregateFunctionGroupBitmapData<UInt32>::name()))
            throw Exception(
                "First argument for function " + getName() + " must be an bitmap but it has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto bitmap_type1 = typeid_cast<const DataTypeAggregateFunction *>(arguments[1].get());
        if (!(bitmap_type1 && bitmap_type1->getFunctionName() == AggregateFunctionGroupBitmapData<UInt32>::name()))
            throw Exception(
                "Second argument for function " + getName() + " must be an bitmap but it has type " + arguments[1]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (bitmap_type0->getArgumentsDataTypes()[0]->getTypeId() != bitmap_type1->getArgumentsDataTypes()[0]->getTypeId())
            throw Exception(
                "The nested type in bitmaps must be the same, but one is " + bitmap_type0->getArgumentsDataTypes()[0]->getName()
                    + ", and the other is " + bitmap_type1->getArgumentsDataTypes()[0]->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<ToType>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        auto col_to = ColumnVector<ToType>::create(input_rows_count);
        typename ColumnVector<ToType>::Container & vec_to = col_to->getData();
        const IDataType * from_type = block.getByPosition(arguments[0]).type.get();

        const DataTypeAggregateFunction * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        WhichDataType which(aggr_type->getArgumentsDataTypes()[0]);
        if (which.isUInt8())
            executeIntType<UInt8>(block, arguments, input_rows_count, vec_to);
        else if (which.isUInt16())
            executeIntType<UInt16>(block, arguments, input_rows_count, vec_to);
        else if (which.isUInt32())
            executeIntType<UInt32>(block, arguments, input_rows_count, vec_to);
        else if (which.isUInt64())
            executeIntType<UInt64>(block, arguments, input_rows_count, vec_to);
        else
            throw Exception(
                "Unexpected type " + from_type->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        block.getByPosition(result).column = std::move(col_to);
    }

private:
    template <typename T>
    void executeIntType(
        Block & block, const ColumnNumbers & arguments, size_t input_rows_count, typename ColumnVector<ToType>::Container & vec_to)
    {
        const ColumnAggregateFunction * columns[2];
        bool is_column_const[2];
        for (size_t i = 0; i < 2; ++i)
        {
            if (auto argument_column_const = checkAndGetColumn<ColumnConst>(block.getByPosition(arguments[i]).column.get()))
            {
                columns[i] = typeid_cast<const ColumnAggregateFunction*>(argument_column_const->getDataColumnPtr().get());
                is_column_const[i] = true;
            }
            else
            {
                columns[i] = typeid_cast<const ColumnAggregateFunction*>(block.getByPosition(arguments[i]).column.get());
                is_column_const[i] = false;
            }
        }

        const PaddedPODArray<AggregateDataPtr> & container0 = columns[0]->getData();
        const PaddedPODArray<AggregateDataPtr> & container1 = columns[1]->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const AggregateDataPtr dataPtr0 = is_column_const[0] ? container0[0] : container0[i];
            const AggregateDataPtr dataPtr1 = is_column_const[1] ? container1[0] : container1[i];
            const AggregateFunctionGroupBitmapData<T> & bd1
                = *reinterpret_cast<const AggregateFunctionGroupBitmapData<T>*>(dataPtr0);
            const AggregateFunctionGroupBitmapData<T> & bd2
                = *reinterpret_cast<const AggregateFunctionGroupBitmapData<T>*>(dataPtr1);
            vec_to[i] = Impl<T>::apply(bd1, bd2);
        }
    }
};

template <typename T>
struct BitmapAndImpl
{
    static void apply(AggregateFunctionGroupBitmapData<T> & toBd, const AggregateFunctionGroupBitmapData<T> & bd2)
    {
        toBd.rbs.rb_and(bd2.rbs);
    }
};

template <typename T>
struct BitmapOrImpl
{
    static void apply(AggregateFunctionGroupBitmapData<T> & toBd, const AggregateFunctionGroupBitmapData<T> & bd2)
    {
        toBd.rbs.rb_or(bd2.rbs);
    }
};

template <typename T>
struct BitmapXorImpl
{
    static void apply(AggregateFunctionGroupBitmapData<T> & toBd, const AggregateFunctionGroupBitmapData<T> & bd2)
    {
        toBd.rbs.rb_xor(bd2.rbs);
    }
};

template <typename T>
struct BitmapAndnotImpl
{
    static void apply(AggregateFunctionGroupBitmapData<T> & toBd, const AggregateFunctionGroupBitmapData<T> & bd2)
    {
        toBd.rbs.rb_andnot(bd2.rbs);
    }
};

template <template <typename> class Impl, typename Name>
class FunctionBitmap : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionBitmap>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        auto bitmap_type0 = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bitmap_type0 && bitmap_type0->getFunctionName() == AggregateFunctionGroupBitmapData<UInt32>::name()))
            throw Exception(
                "First argument for function " + getName() + " must be an bitmap but it has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto bitmap_type1 = typeid_cast<const DataTypeAggregateFunction *>(arguments[1].get());
        if (!(bitmap_type1 && bitmap_type1->getFunctionName() == AggregateFunctionGroupBitmapData<UInt32>::name()))
            throw Exception(
                "Second argument for function " + getName() + " must be an bitmap but it has type " + arguments[1]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (bitmap_type0->getArgumentsDataTypes()[0]->getTypeId() != bitmap_type1->getArgumentsDataTypes()[0]->getTypeId())
            throw Exception(
                "The nested type in bitmaps must be the same, but one is " + bitmap_type0->getArgumentsDataTypes()[0]->getName()
                    + ", and the other is " + bitmap_type1->getArgumentsDataTypes()[0]->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const IDataType * from_type = block.getByPosition(arguments[0]).type.get();
        const DataTypeAggregateFunction * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        WhichDataType which(aggr_type->getArgumentsDataTypes()[0]);
        if (which.isUInt8())
            executeBitmapData<UInt8>(block, arguments, result, input_rows_count);
        else if (which.isUInt16())
            executeBitmapData<UInt16>(block, arguments, result, input_rows_count);
        else if (which.isUInt32())
            executeBitmapData<UInt32>(block, arguments, result, input_rows_count);
        else if (which.isUInt64())
            executeBitmapData<UInt64>(block, arguments, result, input_rows_count);
        else
            throw Exception(
                "Unexpected type " + from_type->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    template <typename T>
    void executeBitmapData(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
    {
        const ColumnAggregateFunction * columns[2];
        for (size_t i = 0; i < 2; ++i)
        {
            if (auto argument_column_const = typeid_cast<const ColumnConst *>(block.getByPosition(arguments[i]).column.get()))
                columns[i] = typeid_cast<const ColumnAggregateFunction *>(argument_column_const->getDataColumnPtr().get());
            else
                columns[i] = typeid_cast<const ColumnAggregateFunction *>(block.getByPosition(arguments[i]).column.get());
        }

        auto col_to = ColumnAggregateFunction::create(columns[0]->getAggregateFunction());

        col_to->reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            col_to->insertFrom(columns[0]->getData()[i]);
            AggregateFunctionGroupBitmapData<T> & toBd = *reinterpret_cast<AggregateFunctionGroupBitmapData<T> *>(col_to->getData()[i]);
            const AggregateFunctionGroupBitmapData<T> & bd2
                = *reinterpret_cast<const AggregateFunctionGroupBitmapData<T> *>(columns[1]->getData()[i]);
            Impl<T>::apply(toBd, bd2);
        }
        block.getByPosition(result).column = std::move(col_to);
    }
};

struct NameBitmapBuild
{
    static constexpr auto name = "bitmapBuild";
};
using FunctionBitmapBuild = FunctionBitmapBuildImpl<NameBitmapBuild>;

struct NameBitmapToArray
{
    static constexpr auto name = "bitmapToArray";
};
using FunctionBitmapToArray = FunctionBitmapToArrayImpl<NameBitmapToArray>;

struct NameBitmapCardinality
{
    static constexpr auto name = "bitmapCardinality";
};
struct NameBitmapAndCardinality
{
    static constexpr auto name = "bitmapAndCardinality";
};
struct NameBitmapOrCardinality
{
    static constexpr auto name = "bitmapOrCardinality";
};
struct NameBitmapXorCardinality
{
    static constexpr auto name = "bitmapXorCardinality";
};
struct NameBitmapAndnotCardinality
{
    static constexpr auto name = "bitmapAndnotCardinality";
};
struct NameBitmapHasAll
{
    static constexpr auto name = "bitmapHasAll";
};
struct NameBitmapHasAny
{
    static constexpr auto name = "bitmapHasAny";
};

using FunctionBitmapSelfCardinality = FunctionBitmapSelfCardinalityImpl<NameBitmapCardinality>;
using FunctionBitmapAndCardinality = FunctionBitmapCardinality<BitmapAndCardinalityImpl, NameBitmapAndCardinality, UInt64>;
using FunctionBitmapOrCardinality = FunctionBitmapCardinality<BitmapOrCardinalityImpl, NameBitmapOrCardinality, UInt64>;
using FunctionBitmapXorCardinality = FunctionBitmapCardinality<BitmapXorCardinalityImpl, NameBitmapXorCardinality, UInt64>;
using FunctionBitmapAndnotCardinality = FunctionBitmapCardinality<BitmapAndnotCardinalityImpl, NameBitmapAndnotCardinality, UInt64>;
using FunctionBitmapHasAll = FunctionBitmapCardinality<BitmapHasAllImpl, NameBitmapHasAll, UInt8>;
using FunctionBitmapHasAny = FunctionBitmapCardinality<BitmapHasAnyImpl, NameBitmapHasAny, UInt8>;

struct NameBitmapAnd
{
    static constexpr auto name = "bitmapAnd";
};
struct NameBitmapOr
{
    static constexpr auto name = "bitmapOr";
};
struct NameBitmapXor
{
    static constexpr auto name = "bitmapXor";
};
struct NameBitmapAndnot
{
    static constexpr auto name = "bitmapAndnot";
};
using FunctionBitmapAnd = FunctionBitmap<BitmapAndImpl, NameBitmapAnd>;
using FunctionBitmapOr = FunctionBitmap<BitmapOrImpl, NameBitmapOr>;
using FunctionBitmapXor = FunctionBitmap<BitmapXorImpl, NameBitmapXor>;
using FunctionBitmapAndnot = FunctionBitmap<BitmapAndnotImpl, NameBitmapAndnot>;


}
