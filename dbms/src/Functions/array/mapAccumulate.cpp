#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <numeric>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename T> struct floatOrIntValue
{
    static inline T getValue(const IColumn &, size_t) { throw "not implemented"; }
};
template <> struct floatOrIntValue<Int64>
{
    static inline Int64 getValue(const IColumn &col, size_t i) { return col.getInt(i); }
};
template <> struct floatOrIntValue<UInt64>
{
    static inline UInt64 getValue(const IColumn &col, size_t i) { return col.getUInt(i); }
};
template <> struct floatOrIntValue<Float64>
{
    static inline Float64 getValue(const IColumn &col, size_t i) { return col.getFloat64(i); }
};

class FunctionMapAccumulate : public IFunction
{
public:
    static constexpr auto name = "mapAccumulate";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMapAccumulate>(); }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception{getName() + " accepts at least two arrays for key and value", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        if (arguments.size() > 3)
            throw Exception{"too many arguments in " + getName() + " call", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        const DataTypeArray * keys_array = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        const DataTypeArray * values_array = checkAndGetDataType<DataTypeArray>(arguments[1].get());

        if (!keys_array || !values_array)
            throw Exception{getName() + " accepts two arrays for key and value", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        WhichDataType whichKey(keys_array->getNestedType());
        DataTypePtr key_type, value_type;

        if (whichKey.isNativeUInt())
            key_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
        else if (whichKey.isNativeInt())
            key_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>());
        else
            throw Exception("Keys for " + getName() + " should be of integer type (signed or unsigned)", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        WhichDataType whichVal(values_array->getNestedType());
        if (whichVal.isNativeUInt())
            value_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
        else if (whichVal.isNativeInt())
            value_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>());
        else if (whichVal.isFloat())
            value_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
        else
            throw Exception(getName() + " cannot add values of type " + values_array->getNestedType()->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeTuple>(DataTypes{key_type, value_type});
    }

    template <typename KeyType, typename ValType>
    bool executeInternal(Block & block, const ColumnArray *keys_array, const ColumnArray *values_array, bool keys_array_is_const, bool values_array_is_const,
                         ColumnPtr & max_key_column_ptr, const size_t result)
    {
        bool max_key_is_provided = max_key_column_ptr != nullptr;
        bool max_key_column_is_const = false;
        KeyType	max_key = 0;

        if (max_key_is_provided && isColumnConst(*max_key_column_ptr))
        {
            auto *column_const = static_cast<const ColumnConst *>(&*max_key_column_ptr);
            max_key = column_const->template getValue<KeyType>();
            max_key_column_is_const = true;
        }

        const IColumn::Offsets & keys_offset = keys_array->getOffsets();
        const IColumn::Offsets & offsetValues = values_array->getOffsets();

        auto keys_vector = ColumnVector<KeyType>::create();
        typename ColumnVector<KeyType>::Container & keys = keys_vector->getData();

        auto values_vector = ColumnVector<ValType>::create();
        typename ColumnVector<ValType>::Container & values = values_vector->getData();

        auto offsets_vector = ColumnArray::ColumnOffsets::create(keys_array->size());
        typename ColumnVector<IColumn::Offset>::Container & offsets = offsets_vector->getData();

        size_t rows = keys_array_is_const? values_array->size() : keys_array->size();

        /* Prepare some space for resulting arrays trying to avoid extra allocations */
        offsets.resize(rows);
        if (max_key_is_provided)
        {
            KeyType sz = max_key;
            if (!max_key_column_is_const)
                sz = floatOrIntValue<KeyType>::getValue(*max_key_column_ptr, 0);

            keys.reserve(rows * sz);
            values.reserve(rows * sz);
        }
        else
        {
            keys.reserve(rows * keys_offset[0]);
            values.reserve(rows * keys_offset[0]);
        }

        IColumn::Offset offset{0}, prev_keys_offset{0}, prev_values_offset{0};

        /*
         * Iterate through two arrays and fill result values. It is possible that one or
         * both arrays are const, so we make sure we don't increase row index for them.
         */
        for (size_t row = 0, row1 = 0, row2 = 0; row < rows; ++row)
        {
            ValType sum = 0;
            KeyType	prev_key = 0;

            /* update the current max key if it's not constant */
            if (max_key_is_provided && !max_key_column_is_const)
                max_key = floatOrIntValue<KeyType>::getValue(*max_key_column_ptr, row);

            if (keys_offset[row1] - prev_keys_offset != offsetValues[row2] - prev_values_offset)
                throw Exception{"Arrays for " + getName() + " should have equal size of elements", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

            bool done = false;
            for (size_t i = 0; !done && i < keys_offset[row1] - prev_keys_offset; ++i)
            {
                auto key = static_cast<KeyType>(keys_array->getData().getInt(prev_keys_offset + i));

                /*
                 * if there is gap, fill by generated keys and last sum,
                 * prevKey at first is not initialized, so start when i > 0
                 */
                for (; i > 0 && !done && prev_key < key; ++prev_key)
                {
                    keys.push_back(prev_key);
                    values.push_back(sum);
                    ++offset;

                    done = max_key_is_provided && prev_key >= max_key;
                }

                if (!done)
                {
                    sum += floatOrIntValue<ValType>::getValue(values_array->getData(), prev_values_offset + i);
                    keys.push_back(key);
                    values.push_back(sum);
                    prev_key = key + 1;
                    ++offset;

                    done = max_key_is_provided && key >= max_key;
                }
            }

            /* if max key if provided, try to extend the current arrays */
            if (max_key_is_provided)
            {
                for (; prev_key <= max_key; ++prev_key, ++offset)
                {
                    keys.push_back(prev_key);
                    values.push_back(sum);
                }
            }

            if (!keys_array_is_const)
            {
                prev_keys_offset = keys_offset[row1];
                ++row1;
            }
            if (!values_array_is_const)
            {
                prev_values_offset = offsetValues[row2];
                ++row2;
            }

            offsets[row] = offset;
        }

        auto result_keys_array = ColumnArray::create(std::move(keys_vector), std::move(offsets_vector->clone()));
        auto result_values_array = ColumnArray::create(std::move(values_vector), std::move(offsets_vector));

        ColumnPtr res = ColumnTuple::create(Columns{std::move(result_keys_array), std::move(result_values_array)});
        if (keys_array_is_const && values_array_is_const)
            res = ColumnConst::create(res, 1);

        block.getByPosition(result).column = res;
        return true;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
    {
        auto col1 = block.safeGetByPosition(arguments[0]),
             col2 = block.safeGetByPosition(arguments[1]);

        auto keys_array = checkAndGetColumn<ColumnArray>(col1.column.get()),
             values_array = checkAndGetColumn<ColumnArray>(col2.column.get());

        bool keys_array_is_const = false, values_array_is_const = false;

        /* Determine const arrays */
        if (!keys_array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(col1.column.get());
            if (!const_array)
                throw Exception("Expected array column, found " + col1.column->getName(), ErrorCodes::ILLEGAL_COLUMN);

            keys_array = checkAndGetColumn<ColumnArray>(const_array->getDataColumnPtr().get());
            keys_array_is_const = true;
        }

        if (!values_array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(col2.column.get());
            if (!const_array)
                throw Exception("Expected array column, found " + col2.column->getName(), ErrorCodes::ILLEGAL_COLUMN);

            values_array = checkAndGetColumn<ColumnArray>(const_array->getDataColumnPtr().get());
            values_array_is_const = true;
        }

        if (!keys_array || !values_array)
            /* something went wrong */
            throw Exception{"Illegal columns in arguments of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};

        /* Arrays could have different sizes if one of them is constant (therefore it has size 1) */
        if (!keys_array_is_const && !values_array_is_const && (keys_array->size() != values_array->size()))
            throw Exception{"Arrays for " + getName() + " should have equal size of rows", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        const DataTypeArray * k = checkAndGetDataType<DataTypeArray>(col1.type.get());
        const DataTypeArray * v = checkAndGetDataType<DataTypeArray>(col2.type.get());
        WhichDataType whichKey(k->getNestedType());
        WhichDataType whichVal(v->getNestedType());

        ColumnPtr max_key_column_ptr = nullptr;

        if (arguments.size() == 3)
            /* max key provided */
            max_key_column_ptr = block.getByPosition(arguments[2]).column;

        /* Determine array types and call according functions */
        bool res = false;
        if (whichKey.isNativeInt() && whichVal.isNativeInt())
            res = executeInternal<Int64, Int64>(block, keys_array, values_array, keys_array_is_const, values_array_is_const, max_key_column_ptr, result);
        else if (whichKey.isNativeInt() && whichVal.isNativeUInt())
            res = executeInternal<Int64, UInt64>(block, keys_array, values_array, keys_array_is_const, values_array_is_const, max_key_column_ptr, result);
        else if (whichKey.isNativeUInt() && whichVal.isNativeInt())
            res = executeInternal<UInt64, Int64>(block, keys_array, values_array, keys_array_is_const, values_array_is_const, max_key_column_ptr, result);
        else if (whichKey.isNativeUInt() && whichVal.isNativeUInt())
            res = executeInternal<UInt64, UInt64>(block, keys_array, values_array, keys_array_is_const, values_array_is_const, max_key_column_ptr, result);
        else if (whichKey.isNativeInt() && whichVal.isFloat())
            res = executeInternal<Int64, Float64>(block, keys_array, values_array, keys_array_is_const, values_array_is_const, max_key_column_ptr, result);
        else if (whichKey.isNativeUInt() && whichVal.isFloat())
            res = executeInternal<UInt64, Float64>(block, keys_array, values_array, keys_array_is_const, values_array_is_const, max_key_column_ptr, result);

        if (!res)
            throw Exception{"Illegal columns in arguments of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
    }
};


void registerFunctionMapAccumulate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMapAccumulate>();
}

}
