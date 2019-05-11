#pragma once

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>


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
        ColumnArray & res = static_cast<ColumnArray &>(*res_ptr);

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

template <template <typename> class Impl, typename Name>
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
        const ColumnAggregateFunction * columns[2];
        for (size_t i = 0; i < 2; ++i)
        {
            if (auto argument_column_const = typeid_cast<const ColumnConst *>(block.getByPosition(arguments[i]).column.get()))
                columns[i] = typeid_cast<const ColumnAggregateFunction *>(argument_column_const->getDataColumnPtr().get());
            else
                columns[i] = typeid_cast<const ColumnAggregateFunction *>(block.getByPosition(arguments[i]).column.get());
        }

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const AggregateFunctionGroupBitmapData<T> & bd1
                = *reinterpret_cast<const AggregateFunctionGroupBitmapData<T> *>(columns[0]->getData()[i]);
            const AggregateFunctionGroupBitmapData<T> & bd2
                = *reinterpret_cast<const AggregateFunctionGroupBitmapData<T> *>(columns[1]->getData()[i]);
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

using FunctionBitmapSelfCardinality = FunctionBitmapSelfCardinalityImpl<NameBitmapCardinality>;
using FunctionBitmapAndCardinality = FunctionBitmapCardinality<BitmapAndCardinalityImpl, NameBitmapAndCardinality>;
using FunctionBitmapOrCardinality = FunctionBitmapCardinality<BitmapOrCardinalityImpl, NameBitmapOrCardinality>;
using FunctionBitmapXorCardinality = FunctionBitmapCardinality<BitmapXorCardinalityImpl, NameBitmapXorCardinality>;
using FunctionBitmapAndnotCardinality = FunctionBitmapCardinality<BitmapAndnotCardinalityImpl, NameBitmapAndnotCardinality>;

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
