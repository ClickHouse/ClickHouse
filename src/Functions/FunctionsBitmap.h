#pragma once

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/castColumn.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
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
  * Return the smallest value in the set:
  * bitmapMin:    bitmap -> integer
  *
  * Return the greatest value in the set:
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
  * Return bitmap cardinality:
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

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBitmapBuildImpl>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->onlyNull())
            return arguments[0];

        const auto * array_type = typeid_cast<const DataTypeArray *>(arguments[0].get());
        if (!array_type)
            throw Exception(
                "First argument for function " + getName() + " must be an array but it has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto nested_type = array_type->getNestedType();
        DataTypes argument_types = {nested_type};
        Array params_row;
        AggregateFunctionProperties properties;
        AggregateFunctionPtr bitmap_function;
        WhichDataType which(nested_type);
        if (which.isUInt8())
            bitmap_function = AggregateFunctionFactory::instance().get(
                AggregateFunctionGroupBitmapData<UInt8>::name(), argument_types, params_row, properties);
        else if (which.isUInt16())
            bitmap_function = AggregateFunctionFactory::instance().get(
                AggregateFunctionGroupBitmapData<UInt16>::name(), argument_types, params_row, properties);
        else if (which.isUInt32())
            bitmap_function = AggregateFunctionFactory::instance().get(
                AggregateFunctionGroupBitmapData<UInt32>::name(), argument_types, params_row, properties);
        else if (which.isUInt64())
            bitmap_function = AggregateFunctionFactory::instance().get(
                AggregateFunctionGroupBitmapData<UInt64>::name(), argument_types, params_row, properties);
        else if (which.isInt8())
            bitmap_function = AggregateFunctionFactory::instance().get(
                AggregateFunctionGroupBitmapData<Int8>::name(), argument_types, params_row, properties);
        else if (which.isInt16())
            bitmap_function = AggregateFunctionFactory::instance().get(
                AggregateFunctionGroupBitmapData<Int16>::name(), argument_types, params_row, properties);
        else if (which.isInt32())
            bitmap_function = AggregateFunctionFactory::instance().get(
                AggregateFunctionGroupBitmapData<Int32>::name(), argument_types, params_row, properties);
        else if (which.isInt64())
            bitmap_function = AggregateFunctionFactory::instance().get(
                AggregateFunctionGroupBitmapData<Int64>::name(), argument_types, params_row, properties);
        else
            throw Exception(
                "Unexpected type " + array_type->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeAggregateFunction>(bitmap_function, argument_types, params_row);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /* input_rows_count */) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        const auto * array_type = typeid_cast<const DataTypeArray *>(from_type);
        const auto & nested_type = array_type->getNestedType();

        DataTypes argument_types = {nested_type};

        WhichDataType which(nested_type);
        if (which.isUInt8())
            return executeBitmapData<UInt8>(argument_types, arguments);
        else if (which.isUInt16())
            return executeBitmapData<UInt16>(argument_types, arguments);
        else if (which.isUInt32())
            return executeBitmapData<UInt32>(argument_types, arguments);
        else if (which.isUInt64())
            return executeBitmapData<UInt64>(argument_types, arguments);
        else if (which.isInt8())
            return executeBitmapData<Int8>(argument_types, arguments);
        else if (which.isInt16())
            return executeBitmapData<Int16>(argument_types, arguments);
        else if (which.isInt32())
            return executeBitmapData<Int32>(argument_types, arguments);
        else if (which.isInt64())
            return executeBitmapData<Int64>(argument_types, arguments);
        else
            throw Exception(
                "Unexpected type " + from_type->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    template <typename T>
    ColumnPtr executeBitmapData(DataTypes & argument_types, const ColumnsWithTypeAndName & arguments) const
    {
        // input data
        const ColumnArray * array = typeid_cast<const ColumnArray *>(arguments[0].column.get());
        const ColumnPtr & mapped = array->getDataPtr();
        const ColumnArray::Offsets & offsets = array->getOffsets();
        const ColumnVector<T> * column = checkAndGetColumn<ColumnVector<T>>(&*mapped);
        const typename ColumnVector<T>::Container & input_data = column->getData();

        // output data
        Array params_row;
        AggregateFunctionProperties properties;
        AggregateFunctionPtr bitmap_function = AggregateFunctionFactory::instance().get(
            AggregateFunctionGroupBitmapData<T>::name(), argument_types, params_row, properties);
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
        return col_to;
    }
};

template <typename Name>
class FunctionBitmapToArrayImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBitmapToArrayImpl>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeAggregateFunction * bitmap_type = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bitmap_type && bitmap_type->getFunctionName() =="groupBitmap"))
            throw Exception(
                "First argument for function " + getName() + " must be a bitmap but it has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const DataTypePtr data_type = bitmap_type->getArgumentsDataTypes()[0];

        return std::make_shared<DataTypeArray>(data_type);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        // input data
        const auto & return_type = result_type;
        auto res_ptr = return_type->createColumn();
        ColumnArray & res = assert_cast<ColumnArray &>(*res_ptr);

        IColumn & res_data = res.getData();
        ColumnArray::Offsets & res_offsets = res.getOffsets();

        const IDataType * from_type = arguments[0].type.get();
        const DataTypeAggregateFunction * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        WhichDataType which(aggr_type->getArgumentsDataTypes()[0]);
        if (which.isUInt8())
            executeIntType<UInt8>(arguments, input_rows_count, res_data, res_offsets);
        else if (which.isUInt16())
            executeIntType<UInt16>(arguments, input_rows_count, res_data, res_offsets);
        else if (which.isUInt32())
            executeIntType<UInt32>(arguments, input_rows_count, res_data, res_offsets);
        else if (which.isUInt64())
            executeIntType<UInt64>(arguments, input_rows_count, res_data, res_offsets);
        else if (which.isInt8())
            executeIntType<Int8>(arguments, input_rows_count, res_data, res_offsets);
        else if (which.isInt16())
            executeIntType<Int16>(arguments, input_rows_count, res_data, res_offsets);
        else if (which.isInt32())
            executeIntType<Int32>(arguments, input_rows_count, res_data, res_offsets);
        else if (which.isInt64())
            executeIntType<Int64>(arguments, input_rows_count, res_data, res_offsets);
        else
            throw Exception(
                "Unexpected type " + from_type->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return res_ptr;
    }

private:
    using ToType = UInt64;

    template <typename T>
    void executeIntType(
            const ColumnsWithTypeAndName & arguments, size_t input_rows_count, IColumn & res_data_col, ColumnArray::Offsets & res_offsets)
        const
    {
        const ColumnAggregateFunction * column
            = typeid_cast<const ColumnAggregateFunction *>(arguments[0].column.get());

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

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBitmapSubset<Impl>>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 3; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeAggregateFunction * bitmap_type = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bitmap_type && bitmap_type->getFunctionName() == "groupBitmap"))
            throw Exception(
                "First argument for function " + getName() + " must be a bitmap but it has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        for (size_t i = 1; i < 3; ++i)
        {
            WhichDataType which(arguments[i].get());
            if (!(which.isUInt8() || which.isUInt16() || which.isUInt32() || which.isUInt64()))
            {
                throw Exception(
                    "The second and third arguments for function " + getName() + " must be one of [UInt8, UInt16, UInt32, UInt64] but one of them has type " + arguments[1]->getName() + ".",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }
        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        const DataTypeAggregateFunction * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        WhichDataType which(aggr_type->getArgumentsDataTypes()[0]);
        if (which.isUInt8())
            return executeIntType<UInt8>(arguments, input_rows_count);
        else if (which.isUInt16())
            return executeIntType<UInt16>(arguments, input_rows_count);
        else if (which.isUInt32())
            return executeIntType<UInt32>(arguments, input_rows_count);
        else if (which.isUInt64())
            return executeIntType<UInt64>(arguments, input_rows_count);
        else if (which.isInt8())
            return executeIntType<Int8>(arguments, input_rows_count);
        else if (which.isInt16())
            return executeIntType<Int16>(arguments, input_rows_count);
        else if (which.isInt32())
            return executeIntType<Int32>(arguments, input_rows_count);
        else if (which.isInt64())
            return executeIntType<Int64>(arguments, input_rows_count);
        else
            throw Exception(
                "Unexpected type " + from_type->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    using ToType = UInt64;

    template <typename T>
    ColumnPtr executeIntType(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        const IColumn * column_ptrs[3];
        bool is_column_const[3];
        const ColumnAggregateFunction * col_agg_func;
        const PaddedPODArray<AggregateDataPtr> * container0;
        const PaddedPODArray<UInt64> * container1, * container2;

        ColumnPtr column_holder[2];
        for (size_t i = 0; i < 3; ++i)
        {
            if (i > 0)
            {
                column_holder[i - 1] = castColumn(arguments[i], std::make_shared<DataTypeUInt64>());
                column_ptrs[i] = column_holder[i-1].get();
            }
            else
            {
                column_ptrs[i] = arguments[i].column.get();
            }
            is_column_const[i] = isColumnConst(*column_ptrs[i]);
        }

        if (is_column_const[0])
            col_agg_func = typeid_cast<const ColumnAggregateFunction*>(typeid_cast<const ColumnConst*>(column_ptrs[0])->getDataColumnPtr().get());
        else
            col_agg_func = typeid_cast<const ColumnAggregateFunction*>(column_ptrs[0]);

        container0 = &col_agg_func->getData();
        if (is_column_const[1])
            container1 = &typeid_cast<const ColumnUInt64*>(typeid_cast<const ColumnConst*>(column_ptrs[1])->getDataColumnPtr().get())->getData();
        else
            container1 = &typeid_cast<const ColumnUInt64*>(column_ptrs[1])->getData();
        if (is_column_const[2])
            container2 = &typeid_cast<const ColumnUInt64*>(typeid_cast<const ColumnConst*>(column_ptrs[2])->getDataColumnPtr().get())->getData();
        else
            container2 = &typeid_cast<const ColumnUInt64*>(column_ptrs[2])->getData();

        auto col_to = ColumnAggregateFunction::create(col_agg_func->getAggregateFunction());
        col_to->reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const AggregateDataPtr data_ptr_0 = is_column_const[0] ? (*container0)[0] : (*container0)[i];
            const AggregateFunctionGroupBitmapData<T> & bitmap_data_0
                = *reinterpret_cast<const AggregateFunctionGroupBitmapData<T>*>(data_ptr_0);
            const UInt64 range_start = is_column_const[1] ? (*container1)[0] : (*container1)[i];
            const UInt64 range_end = is_column_const[2] ? (*container2)[0] : (*container2)[i];

            col_to->insertDefault();
            AggregateFunctionGroupBitmapData<T> & bitmap_data_2
                = *reinterpret_cast<AggregateFunctionGroupBitmapData<T> *>(col_to->getData()[i]);
            Impl::apply(bitmap_data_0, range_start, range_end, bitmap_data_2);
        }
        return col_to;
    }
};

struct BitmapSubsetInRangeImpl
{
public:
    static constexpr auto name = "bitmapSubsetInRange";
    template <typename T>
    static void apply(
        const AggregateFunctionGroupBitmapData<T> & bitmap_data_0,
        UInt64 range_start,
        UInt64 range_end,
        AggregateFunctionGroupBitmapData<T> & bitmap_data_2)
    {
        bitmap_data_0.rbs.rb_range(range_start, range_end, bitmap_data_2.rbs);
    }
};

struct BitmapSubsetLimitImpl
{
public:
    static constexpr auto name = "bitmapSubsetLimit";
    template <typename T>
    static void apply(
        const AggregateFunctionGroupBitmapData<T> & bitmap_data_0,
        UInt64 range_start,
        UInt64 range_end,
        AggregateFunctionGroupBitmapData<T> & bitmap_data_2)
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

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBitmapTransform>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 3; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeAggregateFunction * bitmap_type = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bitmap_type && bitmap_type->getFunctionName() == "groupBitmap"))
            throw Exception(
                "First argument for function " + getName() + " must be a bitmap but it has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        for (size_t i = 0; i < 2; ++i)
        {
            const auto * array_type = typeid_cast<const DataTypeArray *>(arguments[i + 1].get());
            String msg = "The second and third arguments for function " + getName() + " must be an one of [Array(UInt8), Array(UInt16), Array(UInt32), Array(UInt64)] but one of them has type " + arguments[i + 1]->getName() + ".";

            if (!array_type)
                throw Exception(msg, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            auto nested_type = array_type->getNestedType();
            WhichDataType which(nested_type);
            if (!(which.isUInt8() || which.isUInt16() || which.isUInt32() || which.isUInt64()))
                throw Exception(msg, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        const DataTypeAggregateFunction * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        WhichDataType which(aggr_type->getArgumentsDataTypes()[0]);
        if (which.isUInt8())
            return executeIntType<UInt8>(arguments, input_rows_count);
        else if (which.isUInt16())
            return executeIntType<UInt16>(arguments, input_rows_count);
        else if (which.isUInt32())
            return executeIntType<UInt32>(arguments, input_rows_count);
        else if (which.isUInt64())
            return executeIntType<UInt64>(arguments, input_rows_count);
        else if (which.isInt8())
            return executeIntType<Int8>(arguments, input_rows_count);
        else if (which.isInt16())
            return executeIntType<Int16>(arguments, input_rows_count);
        else if (which.isInt32())
            return executeIntType<Int32>(arguments, input_rows_count);
        else if (which.isInt64())
            return executeIntType<Int64>(arguments, input_rows_count);
        else
            throw Exception(
                "Unexpected type " + from_type->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    using ToType = UInt64;

    template <typename T>
    ColumnPtr executeIntType(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        const IColumn * column_ptrs[3];
        bool is_column_const[3];
        const ColumnAggregateFunction * col_agg_func;
        const PaddedPODArray<AggregateDataPtr> * container0;

        const ColumnArray * array1;
        const ColumnArray * array2;

        ColumnPtr column_holder[2];
        for (size_t i = 0; i < 3; ++i)
        {
            if (i > 0)
            {
                auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
                column_holder[i - 1] = castColumn(arguments[i], array_type);
                column_ptrs[i] = column_holder[i-1].get();
            }
            else
            {
                column_ptrs[i] = arguments[i].column.get();
            }
            is_column_const[i] = isColumnConst(*column_ptrs[i]);
        }

        if (is_column_const[0])
        {
            col_agg_func = typeid_cast<const ColumnAggregateFunction*>(typeid_cast<const ColumnConst*>(column_ptrs[0])->getDataColumnPtr().get());
        }
        else
        {
            col_agg_func = typeid_cast<const ColumnAggregateFunction*>(column_ptrs[0]);
        }
        container0 = &col_agg_func->getData();

        if (is_column_const[1])
            array1 = typeid_cast<const ColumnArray*>(typeid_cast<const ColumnConst*>(column_ptrs[1])->getDataColumnPtr().get());
        else
            array1 = typeid_cast<const ColumnArray *>(column_ptrs[1]);

        const ColumnArray::Offsets & from_offsets = array1->getOffsets();
        const ColumnVector<UInt64>::Container & from_container = typeid_cast<const ColumnVector<UInt64> *>(&array1->getData())->getData();

        if (is_column_const[2])
            array2 = typeid_cast<const ColumnArray*>(typeid_cast<const ColumnConst*>(column_ptrs[2])->getDataColumnPtr().get());
        else
            array2 = typeid_cast<const ColumnArray *>(column_ptrs[2]);

        const ColumnArray::Offsets & to_offsets = array2->getOffsets();
        const ColumnVector<UInt64>::Container & to_container = typeid_cast<const ColumnVector<UInt64> *>(&array2->getData())->getData();
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
        return col_to;
    }
};

template <typename Impl>
class FunctionBitmapSelfCardinalityImpl : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBitmapSelfCardinalityImpl<Impl>>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * bitmap_type = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bitmap_type && bitmap_type->getFunctionName() == "groupBitmap"))
            throw Exception(
                "First argument for function " + getName() + " must be a bitmap but it has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeNumber<ToType>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_to = ColumnVector<ToType>::create(input_rows_count);
        typename ColumnVector<ToType>::Container & vec_to = col_to->getData();
        const IDataType * from_type = arguments[0].type.get();

        const DataTypeAggregateFunction * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        WhichDataType which(aggr_type->getArgumentsDataTypes()[0]);
        if (which.isUInt8())
            executeIntType<UInt8>(arguments, input_rows_count, vec_to);
        else if (which.isUInt16())
            executeIntType<UInt16>(arguments, input_rows_count, vec_to);
        else if (which.isUInt32())
            executeIntType<UInt32>(arguments, input_rows_count, vec_to);
        else if (which.isUInt64())
            executeIntType<UInt64>(arguments, input_rows_count, vec_to);
        else if (which.isInt8())
            executeIntType<Int8>(arguments, input_rows_count, vec_to);
        else if (which.isInt16())
            executeIntType<Int16>(arguments, input_rows_count, vec_to);
        else if (which.isInt32())
            executeIntType<Int32>(arguments, input_rows_count, vec_to);
        else if (which.isInt64())
            executeIntType<Int64>(arguments, input_rows_count, vec_to);
        else
            throw Exception(
                "Unexpected type " + from_type->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return col_to;
    }

private:
    using ToType = UInt64;

    template <typename T>
    void executeIntType(
            const ColumnsWithTypeAndName & arguments, size_t input_rows_count, typename ColumnVector<ToType>::Container & vec_to) const
    {
        const ColumnAggregateFunction * column
            = typeid_cast<const ColumnAggregateFunction *>(arguments[0].column.get());
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

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBitmapContains>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * bitmap_type0 = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bitmap_type0 && bitmap_type0->getFunctionName() == "groupBitmap"))
            throw Exception(
                "First argument for function " + getName() + " must be a bitmap but it has type " + arguments[0]->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        WhichDataType which(arguments[1].get());
        if (!which.isNativeInt() && !which.isNativeUInt())
            throw Exception(
                "Second argument for function " + getName() + " must be an native integer type but it has type " + arguments[1]->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<UInt8>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_to = ColumnVector<UInt8>::create(input_rows_count);
        typename ColumnVector<UInt8>::Container & vec_to = col_to->getData();
        const IDataType * from_type = arguments[0].type.get();

        const DataTypeAggregateFunction * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        WhichDataType which(aggr_type->getArgumentsDataTypes()[0]);
        if (which.isUInt8())
            executeIntType<UInt8>(arguments, input_rows_count, vec_to);
        else if (which.isUInt16())
            executeIntType<UInt16>(arguments, input_rows_count, vec_to);
        else if (which.isUInt32())
            executeIntType<UInt32>(arguments, input_rows_count, vec_to);
        else if (which.isUInt64())
            executeIntType<UInt64>(arguments, input_rows_count, vec_to);
        else if (which.isInt8())
            executeIntType<Int8>(arguments, input_rows_count, vec_to);
        else if (which.isInt16())
            executeIntType<Int16>(arguments, input_rows_count, vec_to);
        else if (which.isInt32())
            executeIntType<Int32>(arguments, input_rows_count, vec_to);
        else if (which.isInt64())
            executeIntType<Int64>(arguments, input_rows_count, vec_to);
        else
            throw Exception(
                "Unexpected type " + from_type->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return col_to;
    }

private:
    template <typename T>
    void executeIntType(
            const ColumnsWithTypeAndName & arguments, size_t input_rows_count, typename ColumnVector<UInt8>::Container & vec_to) const
    {
        const IColumn * column_ptrs[2];
        bool is_column_const[2];

        const PaddedPODArray<AggregateDataPtr> * container0;
        const PaddedPODArray<UInt64> * container1;

        column_ptrs[0] = arguments[0].column.get();
        is_column_const[0] = isColumnConst(*column_ptrs[0]);

        if (is_column_const[0])
            container0 = &typeid_cast<const ColumnAggregateFunction*>(typeid_cast<const ColumnConst*>(column_ptrs[0])->getDataColumnPtr().get())->getData();
        else
            container0 = &typeid_cast<const ColumnAggregateFunction*>(column_ptrs[0])->getData();

        // we can always cast the second column to ColumnUInt64
        auto uint64_column = castColumn(arguments[1], std::make_shared<DataTypeUInt64>());
        column_ptrs[1] = uint64_column.get();
        is_column_const[1] = isColumnConst(*column_ptrs[1]);

        if (is_column_const[1])
            container1 = &typeid_cast<const ColumnUInt64*>(typeid_cast<const ColumnConst*>(column_ptrs[1])->getDataColumnPtr().get())->getData();
        else
            container1 = &typeid_cast<const ColumnUInt64*>(column_ptrs[1])->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const AggregateDataPtr data_ptr_0 = is_column_const[0] ? (*container0)[0] : (*container0)[i];
            const UInt64 data1 = is_column_const[1] ? (*container1)[0] : (*container1)[i];
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

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBitmapCardinality>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * bitmap_type0 = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bitmap_type0 && bitmap_type0->getFunctionName() == "groupBitmap"))
            throw Exception(
                "First argument for function " + getName() + " must be a bitmap but it has type " + arguments[0]->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const auto * bitmap_type1 = typeid_cast<const DataTypeAggregateFunction *>(arguments[1].get());
        if (!(bitmap_type1 && bitmap_type1->getFunctionName() == "groupBitmap"))
            throw Exception(
                "Second argument for function " + getName() + " must be a bitmap but it has type " + arguments[1]->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (bitmap_type0->getArgumentsDataTypes()[0]->getTypeId() != bitmap_type1->getArgumentsDataTypes()[0]->getTypeId())
            throw Exception(
                "The nested type in bitmaps must be the same, but one is " + bitmap_type0->getArgumentsDataTypes()[0]->getName()
                    + ", and the other is " + bitmap_type1->getArgumentsDataTypes()[0]->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<ToType>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_to = ColumnVector<ToType>::create(input_rows_count);
        typename ColumnVector<ToType>::Container & vec_to = col_to->getData();
        const IDataType * from_type = arguments[0].type.get();

        const DataTypeAggregateFunction * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        WhichDataType which(aggr_type->getArgumentsDataTypes()[0]);
        if (which.isUInt8())
            executeIntType<UInt8>(arguments, input_rows_count, vec_to);
        else if (which.isUInt16())
            executeIntType<UInt16>(arguments, input_rows_count, vec_to);
        else if (which.isUInt32())
            executeIntType<UInt32>(arguments, input_rows_count, vec_to);
        else if (which.isUInt64())
            executeIntType<UInt64>(arguments, input_rows_count, vec_to);
        else if (which.isInt8())
            executeIntType<Int8>(arguments, input_rows_count, vec_to);
        else if (which.isInt16())
            executeIntType<Int16>(arguments, input_rows_count, vec_to);
        else if (which.isInt32())
            executeIntType<Int32>(arguments, input_rows_count, vec_to);
        else if (which.isInt64())
            executeIntType<Int64>(arguments, input_rows_count, vec_to);
        else
            throw Exception(
                "Unexpected type " + from_type->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return col_to;
    }

private:
    template <typename T>
    void executeIntType(
            const ColumnsWithTypeAndName & arguments, size_t input_rows_count, typename ColumnVector<ToType>::Container & vec_to) const
    {
        const ColumnAggregateFunction * column_ptrs[2];
        bool is_column_const[2];
        for (size_t i = 0; i < 2; ++i)
        {
            if (const auto * argument_column_const = checkAndGetColumn<ColumnConst>(arguments[i].column.get()))
            {
                column_ptrs[i] = typeid_cast<const ColumnAggregateFunction*>(argument_column_const->getDataColumnPtr().get());
                is_column_const[i] = true;
            }
            else
            {
                column_ptrs[i] = typeid_cast<const ColumnAggregateFunction*>(arguments[i].column.get());
                is_column_const[i] = false;
            }
        }

        const PaddedPODArray<AggregateDataPtr> & container0 = column_ptrs[0]->getData();
        const PaddedPODArray<AggregateDataPtr> & container1 = column_ptrs[1]->getData();

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

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBitmap>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * bitmap_type0 = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bitmap_type0 && bitmap_type0->getFunctionName() == "groupBitmap"))
            throw Exception(
                "First argument for function " + getName() + " must be a bitmap but it has type " + arguments[0]->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const auto * bitmap_type1 = typeid_cast<const DataTypeAggregateFunction *>(arguments[1].get());
        if (!(bitmap_type1 && bitmap_type1->getFunctionName() == "groupBitmap"))
            throw Exception(
                "Second argument for function " + getName() + " must be a bitmap but it has type " + arguments[1]->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (bitmap_type0->getArgumentsDataTypes()[0]->getTypeId() != bitmap_type1->getArgumentsDataTypes()[0]->getTypeId())
            throw Exception(
                "The nested type in bitmaps must be the same, but one is " + bitmap_type0->getArgumentsDataTypes()[0]->getName()
                    + ", and the other is " + bitmap_type1->getArgumentsDataTypes()[0]->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        const DataTypeAggregateFunction * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        WhichDataType which(aggr_type->getArgumentsDataTypes()[0]);
        if (which.isUInt8())
            return executeBitmapData<UInt8>(arguments, input_rows_count);
        else if (which.isUInt16())
            return executeBitmapData<UInt16>(arguments, input_rows_count);
        else if (which.isUInt32())
            return executeBitmapData<UInt32>(arguments, input_rows_count);
        else if (which.isUInt64())
            return executeBitmapData<UInt64>(arguments, input_rows_count);
        else if (which.isInt8())
            return executeBitmapData<Int8>(arguments, input_rows_count);
        else if (which.isInt16())
            return executeBitmapData<Int16>(arguments, input_rows_count);
        else if (which.isInt32())
            return executeBitmapData<Int32>(arguments, input_rows_count);
        else if (which.isInt64())
            return executeBitmapData<Int64>(arguments, input_rows_count);
        else
            throw Exception(
                "Unexpected type " + from_type->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    template <typename T>
    ColumnPtr executeBitmapData(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        const ColumnAggregateFunction * column_ptrs[2];
        bool is_column_const[2];
        for (size_t i = 0; i < 2; ++i)
        {
            if (const auto * argument_column_const = typeid_cast<const ColumnConst *>(arguments[i].column.get()))
            {
                column_ptrs[i] = typeid_cast<const ColumnAggregateFunction *>(argument_column_const->getDataColumnPtr().get());
                is_column_const[i] = true;
            }
            else
            {
                column_ptrs[i] = typeid_cast<const ColumnAggregateFunction *>(arguments[i].column.get());
                is_column_const[i] = false;
            }
        }

        auto col_to = ColumnAggregateFunction::create(column_ptrs[0]->getAggregateFunction());

        col_to->reserve(input_rows_count);

        const PaddedPODArray<AggregateDataPtr> & container0 = column_ptrs[0]->getData();
        const PaddedPODArray<AggregateDataPtr> & container1 = column_ptrs[1]->getData();

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
        return col_to;
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
