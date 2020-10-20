#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include "Core/ColumnWithTypeAndName.h"
#include "DataTypes/IDataType.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionMapPopulateSeries : public IFunction
{
public:
    static constexpr auto name = "mapPopulateSeries";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMapPopulateSeries>(); }

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

        const DataTypeArray * key_array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        const DataTypeArray * val_array_type = checkAndGetDataType<DataTypeArray>(arguments[1].get());

        if (!key_array_type || !val_array_type)
            throw Exception{getName() + " accepts two arrays for key and value", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        DataTypePtr keys_type = key_array_type->getNestedType();
        WhichDataType which_key(keys_type);
        if (!(which_key.isNativeInt() || which_key.isNativeUInt()))
        {
            throw Exception(
                "Keys for " + getName() + " should be of native integer type (signed or unsigned)", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        if (arguments.size() == 3)
        {
            DataTypePtr max_key_type = arguments[2];
            WhichDataType which_max_key(max_key_type);

            if (which_max_key.isNullable())
                throw Exception(
                    "Max key argument in arguments of function " + getName() + " can not be Nullable",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            if (keys_type->getTypeId() != max_key_type->getTypeId())
                throw Exception("Max key type in " + getName() + " should be same as keys type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeTuple>(DataTypes{arguments[0], arguments[1]});
    }

    template <typename KeyType, typename ValType>
    void execute2(
            ColumnsWithTypeAndName & columns, size_t result, ColumnPtr key_column, ColumnPtr val_column, ColumnPtr max_key_column, const DataTypeTuple & res_type)
        const
    {
        MutableColumnPtr res_tuple = res_type.createColumn();

        auto * to_tuple = assert_cast<ColumnTuple *>(res_tuple.get());
        auto & to_keys_arr = assert_cast<ColumnArray &>(to_tuple->getColumn(0));
        auto & to_keys_data = to_keys_arr.getData();
        auto & to_keys_offsets = to_keys_arr.getOffsets();

        auto & to_vals_arr = assert_cast<ColumnArray &>(to_tuple->getColumn(1));
        auto & to_values_data = to_vals_arr.getData();

        bool max_key_is_const = false, key_is_const = false, val_is_const = false;

        const auto * keys_array = checkAndGetColumn<ColumnArray>(key_column.get());
        if (!keys_array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(key_column.get());
            if (!const_array)
                throw Exception("Expected array column, found " + key_column->getName(), ErrorCodes::ILLEGAL_COLUMN);

            keys_array = checkAndGetColumn<ColumnArray>(const_array->getDataColumnPtr().get());
            key_is_const = true;
        }

        const auto * values_array = checkAndGetColumn<ColumnArray>(val_column.get());
        if (!values_array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(val_column.get());
            if (!const_array)
                throw Exception("Expected array column, found " + val_column->getName(), ErrorCodes::ILLEGAL_COLUMN);

            values_array = checkAndGetColumn<ColumnArray>(const_array->getDataColumnPtr().get());
            val_is_const = true;
        }

        if (!keys_array || !values_array)
            /* something went wrong */
            throw Exception{"Illegal columns in arguments of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};

        KeyType max_key_const{0};

        if (max_key_column && isColumnConst(*max_key_column))
        {
            const auto * column_const = static_cast<const ColumnConst *>(&*max_key_column);
            max_key_const = column_const->template getValue<KeyType>();
            max_key_is_const = true;
        }

        auto & keys_data = assert_cast<const ColumnVector<KeyType> &>(keys_array->getData()).getData();
        auto & values_data = assert_cast<const ColumnVector<ValType> &>(values_array->getData()).getData();

        // Original offsets
        const IColumn::Offsets & key_offsets = keys_array->getOffsets();
        const IColumn::Offsets & val_offsets = values_array->getOffsets();

        IColumn::Offset offset{0};
        size_t row_count = key_is_const ? values_array->size() : keys_array->size();

        std::map<KeyType, ValType> res_map;

        //Iterate through two arrays and fill result values.
        for (size_t row = 0; row < row_count; ++row)
        {
            size_t key_offset = 0, val_offset = 0, array_size = key_offsets[0], val_array_size = val_offsets[0];

            res_map.clear();

            if (!key_is_const)
            {
                key_offset = row > 0 ? key_offsets[row - 1] : 0;
                array_size = key_offsets[row] - key_offset;
            }

            if (!val_is_const)
            {
                val_offset = row > 0 ? val_offsets[row - 1] : 0;
                val_array_size = val_offsets[row] - val_offset;
            }

            if (array_size != val_array_size)
                throw Exception("Key and value array should have same amount of elements", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            if (array_size == 0)
            {
                to_keys_offsets.push_back(offset);
                continue;
            }

            for (size_t i = 0; i < array_size; ++i)
            {
                res_map.insert({keys_data[key_offset + i], values_data[val_offset + i]});
            }

            auto min_key = res_map.begin()->first;
            auto max_key = res_map.rbegin()->first;

            if (max_key_column)
            {
                /* update the current max key if it's not constant */
                if (max_key_is_const)
                {
                    max_key = max_key_const;
                }
                else
                {
                    max_key = (static_cast<const ColumnVector<KeyType> *>(max_key_column.get()))->getData()[row];
                }

                /* no need to add anything, max key is less that first key */
                if (max_key < min_key)
                {
                    to_keys_offsets.push_back(offset);
                    continue;
                }
            }

            /* fill the result arrays */
            KeyType key;
            for (key = min_key; key <= max_key; ++key)
            {
                to_keys_data.insert(key);

                auto it = res_map.find(key);
                if (it != res_map.end())
                {
                    to_values_data.insert(it->second);
                }
                else
                {
                    to_values_data.insertDefault();
                }

                ++offset;
            }

            to_keys_offsets.push_back(offset);
        }

        to_vals_arr.getOffsets().insert(to_keys_offsets.begin(), to_keys_offsets.end());
        columns[result].column = std::move(res_tuple);
    }

    template <typename KeyType>
    void execute1(
            ColumnsWithTypeAndName & columns, size_t result, ColumnPtr key_column, ColumnPtr val_column, ColumnPtr max_key_column, const DataTypeTuple & res_type)
        const
    {
        const auto & val_type = (assert_cast<const DataTypeArray *>(res_type.getElements()[1].get()))->getNestedType();
        switch (val_type->getTypeId())
        {
            case TypeIndex::Int8:
                execute2<KeyType, Int8>(columns, result, key_column, val_column, max_key_column, res_type);
                break;
            case TypeIndex::Int16:
                execute2<KeyType, Int16>(columns, result, key_column, val_column, max_key_column, res_type);
                break;
            case TypeIndex::Int32:
                execute2<KeyType, Int32>(columns, result, key_column, val_column, max_key_column, res_type);
                break;
            case TypeIndex::Int64:
                execute2<KeyType, Int64>(columns, result, key_column, val_column, max_key_column, res_type);
                break;
            case TypeIndex::UInt8:
                execute2<KeyType, UInt8>(columns, result, key_column, val_column, max_key_column, res_type);
                break;
            case TypeIndex::UInt16:
                execute2<KeyType, UInt16>(columns, result, key_column, val_column, max_key_column, res_type);
                break;
            case TypeIndex::UInt32:
                execute2<KeyType, UInt32>(columns, result, key_column, val_column, max_key_column, res_type);
                break;
            case TypeIndex::UInt64:
                execute2<KeyType, UInt64>(columns, result, key_column, val_column, max_key_column, res_type);
                break;
            default:
                throw Exception{"Illegal columns in arguments of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
        }
    }

    void executeImpl(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t) const override
    {
        auto col1 = columns[arguments[0]];
        auto col2 = columns[arguments[1]];

        const auto * k = assert_cast<const DataTypeArray *>(col1.type.get());
        const auto * v = assert_cast<const DataTypeArray *>(col2.type.get());

        /* determine output type */
        const DataTypeTuple & res_type = DataTypeTuple(
            DataTypes{std::make_shared<DataTypeArray>(k->getNestedType()), std::make_shared<DataTypeArray>(v->getNestedType())});

        ColumnPtr max_key_column = nullptr;

        if (arguments.size() == 3)
        {
            /* max key provided */
            max_key_column = columns[arguments[2]].column;
        }

        switch (k->getNestedType()->getTypeId())
        {
            case TypeIndex::Int8:
                execute1<Int8>(columns, result, col1.column, col2.column, max_key_column, res_type);
                break;
            case TypeIndex::Int16:
                execute1<Int16>(columns, result, col1.column, col2.column, max_key_column, res_type);
                break;
            case TypeIndex::Int32:
                execute1<Int32>(columns, result, col1.column, col2.column, max_key_column, res_type);
                break;
            case TypeIndex::Int64:
                execute1<Int64>(columns, result, col1.column, col2.column, max_key_column, res_type);
                break;
            case TypeIndex::UInt8:
                execute1<UInt8>(columns, result, col1.column, col2.column, max_key_column, res_type);
                break;
            case TypeIndex::UInt16:
                execute1<UInt16>(columns, result, col1.column, col2.column, max_key_column, res_type);
                break;
            case TypeIndex::UInt32:
                execute1<UInt32>(columns, result, col1.column, col2.column, max_key_column, res_type);
                break;
            case TypeIndex::UInt64:
                execute1<UInt64>(columns, result, col1.column, col2.column, max_key_column, res_type);
                break;
            default:
                throw Exception{"Illegal columns in arguments of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
        }
    }
};

void registerFunctionMapPopulateSeries(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMapPopulateSeries>();
}

}
