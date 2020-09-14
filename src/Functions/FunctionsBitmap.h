#pragma once

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

// TODO include this last because of a broken roaring header. See the comment
// inside.
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
}

/** Bitmap functions.
  * Build a bitmap from integer array:
  * bitmapBuild: integer[] -> bitmap
  *
  * Convert bitmap to integer array:
  * bitmapToArray:    bitmap -> integer[]
  *
  * Retrun the smallest value in the set:
  * bitmapMin:    bitmap -> integer
  *
  * Retrun the greatest value in the set:
  * bitmapMax:    bitmap -> integer
  *
  * Return subset in specified range (not include the range_end):
  * bitmapSubsetInRange:    bitmap,integer,integer -> bitmap
  *
  * Return subset of the smallest `limit` values in set which is no smaller than `range_start`.
  * bitmapSubsetLimit:    bitmap,integer,integer -> bitmap
  *
  * Transform an array of values in a bitmap to another array of values, the result is a new bitmap.
  * bitmapTransform:    bitmap,integer[],integer[] -> bitmap
  *
  * Two bitmap and calculation:
  * bitmapAnd:    bitmap,bitmap -> bitmap
  *
  * Two bitmap or calculation:
  * bitmapOr:    bitmap,bitmap -> bitmap
  *
  * Two bitmap xor calculation:
  * bitmapXor:    bitmap,bitmap -> bitmap
  *
  * Two bitmap andnot calculation:
  * bitmapAndnot:    bitmap,bitmap -> bitmap
  *
  * Retrun bitmap cardinality:
  * bitmapCardinality:    bitmap -> integer
  *
  * Two bitmap and calculation, return cardinality:
  * bitmapAndCardinality:    bitmap,bitmap -> integer
  *
  * Two bitmap or calculation, return cardinality:
  * bitmapOrCardinality:    bitmap,bitmap -> integer
  *
  * Two bitmap xor calculation, return cardinality:
  * bitmapXorCardinality:    bitmap,bitmap -> integer
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
        AggregateFunctionProperties properties;
        AggregateFunctionPtr bitmap_function = AggregateFunctionFactory::instance().get(
            AggregateFunctionGroupBitmapData<UInt32>::name(), argument_types, params_row, properties);

        return std::make_shared<DataTypeAggregateFunction>(bitmap_function, argument_types, params_row);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /* input_rows_count */) const override
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
    void executeBitmapData(Block & block, DataTypes & argument_types, const ColumnNumbers & arguments, size_t result) const
    {
        // input data
        const ColumnArray * array = typeid_cast<const ColumnArray *>(block.getByPosition(arguments[0]).column.get());
        ColumnPtr mapped = array->getDataPtr();
        const ColumnArray::Offsets & offsets = array->getOffsets();
        const ColumnVector<T> * column = checkAndGetColumn<ColumnVector<T>>(&*mapped);
        const typename ColumnVector<T>::Container & input_data = column->getData();

        // output data
        Array params_row;
        AggregateFunctionProperties properties;
        AggregateFunctionPtr bitmap_function = AggregateFunctionFactory::instance().get(
            AggregateFunctionGroupBitmapData<UInt32>::name(), argument_types, params_row, properties);
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
                "First argument for function " + getName() + " must be a bitmap but it has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const DataTypePtr data_type = bitmap_type->getArgumentsDataTypes()[0];

        return std::make_shared<DataTypeArray>(data_type);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
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
            const AggregateFunctionGroupBitmapData<T> & bitmap_data_1
                = *reinterpret_cast<const AggregateFunctionGroupBitmapData<T> *>(column->getData()[i]);
            UInt64 count = bitmap_data_1.rbs.rb_to_array(res_data);
            res_offset += count;
            res_offsets.emplace_back(res_offset);
        }
    }
};

template <typename Impl>
class FunctionBitmapSubset : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionBitmapSubset<Impl>>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 3; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeAggregateFunction * bitmap_type = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bitmap_type && bitmap_type->getFunctionName() == AggregateFunctionGroupBitmapData<UInt32>::name()))
            throw Exception(
                "First argument for function " + getName() + " must be a bitmap but it has type " + arguments[0]->getName() + ".",
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

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
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
        const ColumnAggregateFunction * col_agg_func;
        const PaddedPODArray<AggregateDataPtr> * container0;
        const PaddedPODArray<UInt32> * container1, * container2;

        for (size_t i = 0; i < 3; ++i)
        {
            columns[i] = block.getByPosition(arguments[i]).column.get();
            is_column_const[i] = isColumnConst(*columns[i]);
        }
        if (is_column_const[0])
            col_agg_func = typeid_cast<const ColumnAggregateFunction*>(typeid_cast<const ColumnConst*>(columns[0])->getDataColumnPtr().get());
        else
            col_agg_func = typeid_cast<const ColumnAggregateFunction*>(columns[0]);

        container0 = &col_agg_func->getData();
        if (is_column_const[1])
            container1 = &typeid_cast<const ColumnUInt32*>(typeid_cast<const ColumnConst*>(columns[1])->getDataColumnPtr().get())->getData();
        else
            container1 = &typeid_cast<const ColumnUInt32*>(columns[1])->getData();
        if (is_column_const[2])
            container2 = &typeid_cast<const ColumnUInt32*>(typeid_cast<const ColumnConst*>(columns[2])->getDataColumnPtr().get())->getData();
        else
            container2 = &typeid_cast<const ColumnUInt32*>(columns[2])->getData();

        auto col_to = ColumnAggregateFunction::create(col_agg_func->getAggregateFunction());
        col_to->reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const AggregateDataPtr data_ptr_0 = is_column_const[0] ? (*container0)[0] : (*container0)[i];
            const AggregateFunctionGroupBitmapData<T> & bitmap_data_0
                = *reinterpret_cast<const AggregateFunctionGroupBitmapData<T>*>(data_ptr_0);
            const UInt32 range_start = is_column_const[1] ? (*container1)[0] : (*container1)[i];
            const UInt32 range_end = is_column_const[2] ? (*container2)[0] : (*container2)[i];

            col_to->insertDefault();
            AggregateFunctionGroupBitmapData<T> & bitmap_data_2
                = *reinterpret_cast<AggregateFunctionGroupBitmapData<T> *>(col_to->getData()[i]);
            Impl::apply(bitmap_data_0, range_start, range_end, bitmap_data_2);
        }
        block.getByPosition(result).column = std::move(col_to);
    }
};

struct BitmapSubsetInRangeImpl
{
public:
    static constexpr auto name = "bitmapSubsetInRange";
    template <typename T>
    static void apply(const AggregateFunctionGroupBitmapData<T> & bitmap_data_0, UInt32 range_start, UInt32 range_end, AggregateFunctionGroupBitmapData<T> & bitmap_data_2)
    {
        bitmap_data_0.rbs.rb_range(range_start, range_end, bitmap_data_2.rbs);
    }
};

struct BitmapSubsetLimitImpl
{
public:
    static constexpr auto name = "bitmapSubsetLimit";
    template <typename T>
    static void apply(const AggregateFunctionGroupBitmapData<T> & bitmap_data_0, UInt32 range_start, UInt32 range_end, AggregateFunctionGroupBitmapData<T> & bitmap_data_2)
    {
        bitmap_data_0.rbs.rb_limit(range_start, range_end, bitmap_data_2.rbs);
    }
};

using FunctionBitmapSubsetInRange = FunctionBitmapSubset<BitmapSubsetInRangeImpl>;
using FunctionBitmapSubsetLimit = FunctionBitmapSubset<BitmapSubsetLimitImpl>;


class FunctionBitmapTransform : public IFunction
{
public:
    static constexpr auto name = "bitmapTransform";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionBitmapTransform>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 3; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeAggregateFunction * bitmap_type = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bitmap_type && bitmap_type->getFunctionName() == AggregateFunctionGroupBitmapData<UInt32>::name()))
            throw Exception(
                "First argument for function " + getName() + " must be a bitmap but it has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        for (size_t i = 0; i < 2; ++i)
        {
            auto array_type = typeid_cast<const DataTypeArray *>(arguments[i + 1].get());
            String msg(i == 0 ? "Second" : "Third");
            msg += " argument for function " + getName() + " must be an UInt32 array but it has type " + arguments[i + 1]->getName() + ".";
            if (!array_type)
                throw Exception(msg, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            auto nested_type = array_type->getNestedType();
            WhichDataType which(nested_type);
            if (!which.isUInt32())
                throw Exception(msg, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
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
        Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const
    {
        const IColumn * columns[3];
        bool is_column_const[3];
        const ColumnAggregateFunction * col_agg_func;
        const PaddedPODArray<AggregateDataPtr> * container0;
        const ColumnArray * array;

        for (size_t i = 0; i < 3; ++i)
        {
            columns[i] = block.getByPosition(arguments[i]).column.get();
            is_column_const[i] = isColumnConst(*columns[i]);
        }
        if (is_column_const[0])
        {
            col_agg_func = typeid_cast<const ColumnAggregateFunction*>(typeid_cast<const ColumnConst*>(columns[0])->getDataColumnPtr().get());
        }
        else
        {
            col_agg_func = typeid_cast<const ColumnAggregateFunction*>(columns[0]);
        }
        container0 = &col_agg_func->getData();

        if (is_column_const[1])
            array = typeid_cast<const ColumnArray*>(typeid_cast<const ColumnConst*>(columns[1])->getDataColumnPtr().get());
        else
        {
            array = typeid_cast<const ColumnArray *>(block.getByPosition(arguments[1]).column.get());
        }
        const ColumnArray::Offsets & from_offsets = array->getOffsets();
        const ColumnVector<UInt32>::Container & from_container = typeid_cast<const ColumnVector<UInt32> *>(&array->getData())->getData();

        if (is_column_const[2])
            array = typeid_cast<const ColumnArray*>(typeid_cast<const ColumnConst*>(columns[2])->getDataColumnPtr().get());
        else
            array = typeid_cast<const ColumnArray *>(block.getByPosition(arguments[2]).column.get());

        const ColumnArray::Offsets & to_offsets = array->getOffsets();
        const ColumnVector<UInt32>::Container & to_container = typeid_cast<const ColumnVector<UInt32> *>(&array->getData())->getData();
        auto col_to = ColumnAggregateFunction::create(col_agg_func->getAggregateFunction());
        col_to->reserve(input_rows_count);

        size_t from_start;
        size_t from_end;
        size_t to_start;
        size_t to_end;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const AggregateDataPtr data_ptr_0 = is_column_const[0] ? (*container0)[0] : (*container0)[i];
            const AggregateFunctionGroupBitmapData<T> & bitmap_data_0
                = *reinterpret_cast<const AggregateFunctionGroupBitmapData<T> *>(data_ptr_0);
            if (is_column_const[1])
            {
                from_start = 0;
                from_end = from_container.size();
            }
            else
            {
                from_start = i == 0 ? 0 : from_offsets[i - 1];
                from_end = from_offsets[i];
            }
            if (is_column_const[2])
            {
                to_start = 0;
                to_end = to_container.size();
            }
            else
            {
                to_start = i == 0 ? 0 : to_offsets[i - 1];
                to_end = to_offsets[i];
            }
            if (from_end - from_start != to_end - to_start)
                throw Exception("From array size and to array size mismatch", ErrorCodes::LOGICAL_ERROR);

            col_to->insertDefault();
            AggregateFunctionGroupBitmapData<T> & bitmap_data_2
                = *reinterpret_cast<AggregateFunctionGroupBitmapData<T> *>(col_to->getData()[i]);
            bitmap_data_2.rbs.merge(bitmap_data_0.rbs);
            bitmap_data_2.rbs.rb_replace(&from_container[from_start], &to_container[to_start], from_end - from_start);
        }
        block.getByPosition(result).column = std::move(col_to);
    }
};

template <typename Impl>
class FunctionBitmapSelfCardinalityImpl : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionBitmapSelfCardinalityImpl<Impl>>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        auto bitmap_type = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bitmap_type && bitmap_type->getFunctionName() == AggregateFunctionGroupBitmapData<UInt32>::name()))
            throw Exception(
                "First argument for function " + getName() + " must be a bitmap but it has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeNumber<ToType>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
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
        Block & block, const ColumnNumbers & arguments, size_t input_rows_count, typename ColumnVector<ToType>::Container & vec_to) const
    {
        const ColumnAggregateFunction * column
            = typeid_cast<const ColumnAggregateFunction *>(block.getByPosition(arguments[0]).column.get());
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const AggregateFunctionGroupBitmapData<T> & bitmap_data
                = *reinterpret_cast<const AggregateFunctionGroupBitmapData<T> *>(column->getData()[i]);
            vec_to[i] = Impl::apply(bitmap_data);
        }
    }
};

struct BitmapCardinalityImpl
{
public:
    static constexpr auto name = "bitmapCardinality";
    template <typename T>
    static UInt64 apply(const AggregateFunctionGroupBitmapData<T> & bitmap_data)
    {
        return bitmap_data.rbs.size();
    }
};

struct BitmapMinImpl
{
public:
    static constexpr auto name = "bitmapMin";
    template <typename T>
    static UInt64 apply(const AggregateFunctionGroupBitmapData<T> & bitmap_data)
    {
        return bitmap_data.rbs.rb_min();
    }
};

struct BitmapMaxImpl
{
public:
    static constexpr auto name = "bitmapMax";
    template <typename T>
    static UInt64 apply(const AggregateFunctionGroupBitmapData<T> & bitmap_data)
    {
        return bitmap_data.rbs.rb_max();
    }
};

template <typename T>
struct BitmapAndCardinalityImpl
{
    using ReturnType = UInt64;
    static UInt64 apply(const AggregateFunctionGroupBitmapData<T> & bitmap_data_1, const AggregateFunctionGroupBitmapData<T> & bitmap_data_2)
    {
        // roaring_bitmap_and_cardinality( rb1, rb2 );
        return bitmap_data_1.rbs.rb_and_cardinality(bitmap_data_2.rbs);
    }
};


template <typename T>
struct BitmapOrCardinalityImpl
{
    using ReturnType = UInt64;
    static UInt64 apply(const AggregateFunctionGroupBitmapData<T> & bitmap_data_1, const AggregateFunctionGroupBitmapData<T> & bitmap_data_2)
    {
        // return roaring_bitmap_or_cardinality( rb1, rb2 );
        return bitmap_data_1.rbs.rb_or_cardinality(bitmap_data_2.rbs);
    }
};

template <typename T>
struct BitmapXorCardinalityImpl
{
    using ReturnType = UInt64;
    static UInt64 apply(const AggregateFunctionGroupBitmapData<T> & bitmap_data_1, const AggregateFunctionGroupBitmapData<T> & bitmap_data_2)
    {
        // return roaring_bitmap_xor_cardinality( rb1, rb2 );
        return bitmap_data_1.rbs.rb_xor_cardinality(bitmap_data_2.rbs);
    }
};

template <typename T>
struct BitmapAndnotCardinalityImpl
{
    using ReturnType = UInt64;
    static UInt64 apply(const AggregateFunctionGroupBitmapData<T> & bitmap_data_1, const AggregateFunctionGroupBitmapData<T> & bitmap_data_2)
    {
        // roaring_bitmap_andnot_cardinality( rb1, rb2 );
        return bitmap_data_1.rbs.rb_andnot_cardinality(bitmap_data_2.rbs);
    }
};

template <typename T>
struct BitmapHasAllImpl
{
    using ReturnType = UInt8;
    static UInt8 apply(const AggregateFunctionGroupBitmapData<T> & bitmap_data_1, const AggregateFunctionGroupBitmapData<T> & bitmap_data_2)
    {
        return bitmap_data_1.rbs.rb_is_subset(bitmap_data_2.rbs);
    }
};

template <typename T>
struct BitmapHasAnyImpl
{
    using ReturnType = UInt8;
    static UInt8 apply(const AggregateFunctionGroupBitmapData<T> & bitmap_data_1, const AggregateFunctionGroupBitmapData<T> & bitmap_data_2)
    {
        return bitmap_data_1.rbs.rb_intersect(bitmap_data_2.rbs);
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
                "First argument for function " + getName() + " must be a bitmap but it has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        auto arg_type1 = typeid_cast<const DataTypeNumber<UInt32> *>(arguments[1].get());
        if (!(arg_type1))
            throw Exception(
                "Second argument for function " + getName() + " must be UInt32 but it has type " + arguments[1]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<UInt8>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
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
        Block & block, const ColumnNumbers & arguments, size_t input_rows_count, typename ColumnVector<UInt8>::Container & vec_to) const
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
            const AggregateDataPtr data_ptr_0 = is_column_const[0] ? (*container0)[0] : (*container0)[i];
            const UInt32 data1 = is_column_const[1] ? (*container1)[0] : (*container1)[i];
            const AggregateFunctionGroupBitmapData<T> & bitmap_data_0
                = *reinterpret_cast<const AggregateFunctionGroupBitmapData<T> *>(data_ptr_0);
            vec_to[i] = bitmap_data_0.rbs.rb_contains(data1);
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
                "First argument for function " + getName() + " must be a bitmap but it has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto bitmap_type1 = typeid_cast<const DataTypeAggregateFunction *>(arguments[1].get());
        if (!(bitmap_type1 && bitmap_type1->getFunctionName() == AggregateFunctionGroupBitmapData<UInt32>::name()))
            throw Exception(
                "Second argument for function " + getName() + " must be a bitmap but it has type " + arguments[1]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (bitmap_type0->getArgumentsDataTypes()[0]->getTypeId() != bitmap_type1->getArgumentsDataTypes()[0]->getTypeId())
            throw Exception(
                "The nested type in bitmaps must be the same, but one is " + bitmap_type0->getArgumentsDataTypes()[0]->getName()
                    + ", and the other is " + bitmap_type1->getArgumentsDataTypes()[0]->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<ToType>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
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
        Block & block, const ColumnNumbers & arguments, size_t input_rows_count, typename ColumnVector<ToType>::Container & vec_to) const
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
            const AggregateDataPtr data_ptr_0 = is_column_const[0] ? container0[0] : container0[i];
            const AggregateDataPtr data_ptr_1 = is_column_const[1] ? container1[0] : container1[i];
            const AggregateFunctionGroupBitmapData<T> & bitmap_data_1
                = *reinterpret_cast<const AggregateFunctionGroupBitmapData<T> *>(data_ptr_0);
            const AggregateFunctionGroupBitmapData<T> & bitmap_data_2
                = *reinterpret_cast<const AggregateFunctionGroupBitmapData<T> *>(data_ptr_1);
            vec_to[i] = Impl<T>::apply(bitmap_data_1, bitmap_data_2);
        }
    }
};

template <typename T>
struct BitmapAndImpl
{
    static void apply(AggregateFunctionGroupBitmapData<T> & bitmap_data_1, const AggregateFunctionGroupBitmapData<T> & bitmap_data_2)
    {
        bitmap_data_1.rbs.rb_and(bitmap_data_2.rbs);
    }
};

template <typename T>
struct BitmapOrImpl
{
    static void apply(AggregateFunctionGroupBitmapData<T> & bitmap_data_1, const AggregateFunctionGroupBitmapData<T> & bitmap_data_2)
    {
        bitmap_data_1.rbs.rb_or(bitmap_data_2.rbs);
    }
};

template <typename T>
struct BitmapXorImpl
{
    static void apply(AggregateFunctionGroupBitmapData<T> & bitmap_data_1, const AggregateFunctionGroupBitmapData<T> & bitmap_data_2)
    {
        bitmap_data_1.rbs.rb_xor(bitmap_data_2.rbs);
    }
};

template <typename T>
struct BitmapAndnotImpl
{
    static void apply(AggregateFunctionGroupBitmapData<T> & bitmap_data_1, const AggregateFunctionGroupBitmapData<T> & bitmap_data_2)
    {
        bitmap_data_1.rbs.rb_andnot(bitmap_data_2.rbs);
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
                "First argument for function " + getName() + " must be a bitmap but it has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto bitmap_type1 = typeid_cast<const DataTypeAggregateFunction *>(arguments[1].get());
        if (!(bitmap_type1 && bitmap_type1->getFunctionName() == AggregateFunctionGroupBitmapData<UInt32>::name()))
            throw Exception(
                "Second argument for function " + getName() + " must be a bitmap but it has type " + arguments[1]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (bitmap_type0->getArgumentsDataTypes()[0]->getTypeId() != bitmap_type1->getArgumentsDataTypes()[0]->getTypeId())
            throw Exception(
                "The nested type in bitmaps must be the same, but one is " + bitmap_type0->getArgumentsDataTypes()[0]->getName()
                    + ", and the other is " + bitmap_type1->getArgumentsDataTypes()[0]->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
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
    void executeBitmapData(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const
    {
        const ColumnAggregateFunction * columns[2];
        bool is_column_const[2];
        for (size_t i = 0; i < 2; ++i)
        {
            if (auto argument_column_const = typeid_cast<const ColumnConst *>(block.getByPosition(arguments[i]).column.get()))
            {
                columns[i] = typeid_cast<const ColumnAggregateFunction *>(argument_column_const->getDataColumnPtr().get());
                is_column_const[i] = true;
            }
            else
            {
                columns[i] = typeid_cast<const ColumnAggregateFunction *>(block.getByPosition(arguments[i]).column.get());
                is_column_const[i] = false;
            }
        }

        auto col_to = ColumnAggregateFunction::create(columns[0]->getAggregateFunction());

        col_to->reserve(input_rows_count);

        const PaddedPODArray<AggregateDataPtr> & container0 = columns[0]->getData();
        const PaddedPODArray<AggregateDataPtr> & container1 = columns[1]->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const AggregateDataPtr data_ptr_0 = is_column_const[0] ? container0[0] : container0[i];
            const AggregateDataPtr data_ptr_1 = is_column_const[1] ? container1[0] : container1[i];

            col_to->insertFrom(data_ptr_0);
            AggregateFunctionGroupBitmapData<T> & bitmap_data_1 = *reinterpret_cast<AggregateFunctionGroupBitmapData<T> *>(col_to->getData()[i]);
            const AggregateFunctionGroupBitmapData<T> & bitmap_data_2
                = *reinterpret_cast<const AggregateFunctionGroupBitmapData<T> *>(data_ptr_1);
            Impl<T>::apply(bitmap_data_1, bitmap_data_2);
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

using FunctionBitmapSelfCardinality = FunctionBitmapSelfCardinalityImpl<BitmapCardinalityImpl>;
using FunctionBitmapMin = FunctionBitmapSelfCardinalityImpl<BitmapMinImpl>;
using FunctionBitmapMax = FunctionBitmapSelfCardinalityImpl<BitmapMaxImpl>;
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
