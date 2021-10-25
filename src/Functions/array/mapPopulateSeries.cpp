#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include "Core/ColumnWithTypeAndName.h"
#include "DataTypes/DataTypeMap.h"
#include "DataTypes/IDataType.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

class FunctionMapPopulateSeries : public IFunction
{
public:
    static constexpr auto name = "mapPopulateSeries";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapPopulateSeries>(); }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    void checkTypes(const DataTypePtr & key_type, const DataTypePtr max_key_type) const
    {
        WhichDataType which_key(key_type);
        if (!(which_key.isInt() || which_key.isUInt()))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Keys for {} function should be of integer type (signed or unsigned)", getName());
        }

        if (max_key_type)
        {
            WhichDataType which_max_key(max_key_type);

            if (which_max_key.isNullable())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Max key argument in arguments of function " + getName() + " can not be Nullable");

            if (key_type->getTypeId() != max_key_type->getTypeId())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Max key type in {} should be same as keys type", getName());
        }
    }

    DataTypePtr getReturnTypeForTuple(const DataTypes & arguments) const
    {
        if (arguments.size() < 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} accepts at least two arrays for key and value", getName());

        if (arguments.size() > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Too many arguments in {} call", getName());

        const DataTypeArray * key_array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        const DataTypeArray * val_array_type = checkAndGetDataType<DataTypeArray>(arguments[1].get());

        if (!key_array_type || !val_array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} accepts two arrays for key and value", getName());

        const auto & key_type = key_array_type->getNestedType();

        if (arguments.size() == 3)
            this->checkTypes(key_type, arguments[2]);
        else
            this->checkTypes(key_type, nullptr);

        return std::make_shared<DataTypeTuple>(DataTypes{arguments[0], arguments[1]});
    }

    DataTypePtr getReturnTypeForMap(const DataTypes & arguments) const
    {
        const auto * map = assert_cast<const DataTypeMap *>(arguments[0].get());
        if (arguments.size() == 1)
            this->checkTypes(map->getKeyType(), nullptr);
        else if (arguments.size() == 2)
            this->checkTypes(map->getKeyType(), arguments[1]);
        else
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Too many arguments in {} call", getName());

        return std::make_shared<DataTypeMap>(map->getKeyType(), map->getValueType());
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, getName() + " accepts at least one map or two arrays");

        if (arguments[0]->getTypeId() == TypeIndex::Array)
            return getReturnTypeForTuple(arguments);
        else if (arguments[0]->getTypeId() == TypeIndex::Map)
            return getReturnTypeForMap(arguments);
        else
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} only accepts one map or arrays, but got {}",
                getName(),
                arguments[0]->getName());
    }

    // Struct holds input and output columns references,
    // Both arrays and maps have similar columns to work with but extracted differently
    template <typename KeyType, typename ValType>
    struct ColumnsInOut
    {
        // inputs
        const PaddedPODArray<KeyType> & in_keys_data;
        const PaddedPODArray<ValType> & in_vals_data;
        const IColumn::Offsets & in_key_offsets;
        const IColumn::Offsets & in_val_offsets;
        size_t row_count;
        bool key_is_const;
        bool val_is_const;

        // outputs
        PaddedPODArray<KeyType> & out_keys_data;
        PaddedPODArray<ValType> & out_vals_data;

        IColumn::Offsets & out_keys_offsets;
        // with map argument this field will not be used
        IColumn::Offsets * out_vals_offsets;
    };

    template <typename KeyType, typename ValType>
    ColumnsInOut<KeyType, ValType> getInOutDataFromArrays(MutableColumnPtr & res_column, ColumnPtr * arg_columns) const
    {
        auto * out_tuple = assert_cast<ColumnTuple *>(res_column.get());
        auto & out_keys_array = assert_cast<ColumnArray &>(out_tuple->getColumn(0));
        auto & out_vals_array = assert_cast<ColumnArray &>(out_tuple->getColumn(1));

        const auto * key_column = arg_columns[0].get();
        const auto * in_keys_array = checkAndGetColumn<ColumnArray>(key_column);

        bool key_is_const = false, val_is_const = false;

        if (!in_keys_array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(key_column);
            if (!const_array)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN, "Expected array column in function {}, found {}", getName(), key_column->getName());

            in_keys_array = checkAndGetColumn<ColumnArray>(const_array->getDataColumnPtr().get());
            key_is_const = true;
        }

        const auto * val_column = arg_columns[1].get();
        const auto * in_values_array = checkAndGetColumn<ColumnArray>(val_column);
        if (!in_values_array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(val_column);
            if (!const_array)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN, "Expected array column in function {}, found {}", getName(), val_column->getName());

            in_values_array = checkAndGetColumn<ColumnArray>(const_array->getDataColumnPtr().get());
            val_is_const = true;
        }

        if (!in_keys_array || !in_values_array)
            /* something went wrong */
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal columns in arguments of function " + getName());

        const auto & in_keys_data = assert_cast<const ColumnVector<KeyType> &>(in_keys_array->getData()).getData();
        const auto & in_values_data = assert_cast<const ColumnVector<ValType> &>(in_values_array->getData()).getData();
        const auto & in_keys_offsets = in_keys_array->getOffsets();
        const auto & in_vals_offsets = in_values_array->getOffsets();

        auto & out_keys_data = assert_cast<ColumnVector<KeyType> &>(out_keys_array.getData()).getData();
        auto & out_vals_data = assert_cast<ColumnVector<ValType> &>(out_vals_array.getData()).getData();
        auto & out_keys_offsets = out_keys_array.getOffsets();

        size_t row_count = key_is_const ? in_values_array->size() : in_keys_array->size();
        IColumn::Offsets * out_vals_offsets = &out_vals_array.getOffsets();

        return {
            in_keys_data,
            in_values_data,
            in_keys_offsets,
            in_vals_offsets,
            row_count,
            key_is_const,
            val_is_const,
            out_keys_data,
            out_vals_data,
            out_keys_offsets,
            out_vals_offsets};
    }

    template <typename KeyType, typename ValType>
    ColumnsInOut<KeyType, ValType> getInOutDataFromMap(MutableColumnPtr & res_column, ColumnPtr * arg_columns) const
    {
        const auto * in_map = assert_cast<const ColumnMap *>(arg_columns[0].get());
        const auto & in_nested_array = in_map->getNestedColumn();
        const auto & in_nested_tuple = in_map->getNestedData();
        const auto & in_keys_data = assert_cast<const ColumnVector<KeyType> &>(in_nested_tuple.getColumn(0)).getData();
        const auto & in_vals_data = assert_cast<const ColumnVector<ValType> &>(in_nested_tuple.getColumn(1)).getData();
        const auto & in_keys_offsets = in_nested_array.getOffsets();

        auto * out_map = assert_cast<ColumnMap *>(res_column.get());
        auto & out_nested_array = out_map->getNestedColumn();
        auto & out_nested_tuple = out_map->getNestedData();
        auto & out_keys_data = assert_cast<ColumnVector<KeyType> &>(out_nested_tuple.getColumn(0)).getData();
        auto & out_vals_data = assert_cast<ColumnVector<ValType> &>(out_nested_tuple.getColumn(1)).getData();
        auto & out_keys_offsets = out_nested_array.getOffsets();

        return {
            in_keys_data,
            in_vals_data,
            in_keys_offsets,
            in_keys_offsets,
            in_nested_array.size(),
            false,
            false,
            out_keys_data,
            out_vals_data,
            out_keys_offsets,
            nullptr};
    }

    template <typename KeyType, typename ValType>
    ColumnPtr execute2(ColumnPtr * arg_columns, ColumnPtr max_key_column, const DataTypePtr & res_type) const
    {
        MutableColumnPtr res_column = res_type->createColumn();
        bool max_key_is_const = false;
        auto columns = res_column->getDataType() == TypeIndex::Tuple ? getInOutDataFromArrays<KeyType, ValType>(res_column, arg_columns)
                                                                     : getInOutDataFromMap<KeyType, ValType>(res_column, arg_columns);

        KeyType max_key_const{0};

        if (max_key_column && isColumnConst(*max_key_column))
        {
            const auto * column_const = static_cast<const ColumnConst *>(&*max_key_column);
            max_key_const = column_const->template getValue<KeyType>();
            max_key_is_const = true;
        }

        IColumn::Offset offset{0};
        std::map<KeyType, ValType> res_map;

        //Iterate through two arrays and fill result values.
        for (size_t row = 0; row < columns.row_count; ++row)
        {
            size_t key_offset = 0, val_offset = 0, items_count = columns.in_key_offsets[0], val_array_size = columns.in_val_offsets[0];

            res_map.clear();

            if (!columns.key_is_const)
            {
                key_offset = row > 0 ? columns.in_key_offsets[row - 1] : 0;
                items_count = columns.in_key_offsets[row] - key_offset;
            }

            if (!columns.val_is_const)
            {
                val_offset = row > 0 ? columns.in_val_offsets[row - 1] : 0;
                val_array_size = columns.in_val_offsets[row] - val_offset;
            }

            if (items_count != val_array_size)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Key and value array should have same amount of elements in function {}",
                    getName());

            if (items_count == 0)
            {
                columns.out_keys_offsets.push_back(offset);
                continue;
            }

            for (size_t i = 0; i < items_count; ++i)
            {
                res_map.insert({columns.in_keys_data[key_offset + i], columns.in_vals_data[val_offset + i]});
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
                    columns.out_keys_offsets.push_back(offset);
                    continue;
                }
            }

            static constexpr size_t MAX_ARRAY_SIZE = 1ULL << 30;
            if (static_cast<size_t>(max_key) - static_cast<size_t>(min_key) > MAX_ARRAY_SIZE)
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size in the result of function {}", getName());

            /* fill the result arrays */
            KeyType key;
            for (key = min_key;; ++key)
            {
                columns.out_keys_data.push_back(key);

                auto it = res_map.find(key);
                if (it != res_map.end())
                {
                    columns.out_vals_data.push_back(it->second);
                }
                else
                {
                    columns.out_vals_data.push_back(0);
                }

                ++offset;
                if (key == max_key)
                    break;
            }

            columns.out_keys_offsets.push_back(offset);
        }

        if (columns.out_vals_offsets)
            columns.out_vals_offsets->insert(columns.out_keys_offsets.begin(), columns.out_keys_offsets.end());

        return res_column;
    }

    template <typename KeyType>
    ColumnPtr execute1(ColumnPtr * arg_columns, ColumnPtr max_key_column, const DataTypePtr & res_type, const DataTypePtr & val_type) const
    {
        switch (val_type->getTypeId())
        {
            case TypeIndex::Int8:
                return execute2<KeyType, Int8>(arg_columns, max_key_column, res_type);
            case TypeIndex::Int16:
                return execute2<KeyType, Int16>(arg_columns, max_key_column, res_type);
            case TypeIndex::Int32:
                return execute2<KeyType, Int32>(arg_columns, max_key_column, res_type);
            case TypeIndex::Int64:
                return execute2<KeyType, Int64>(arg_columns, max_key_column, res_type);
            case TypeIndex::Int128:
                return execute2<KeyType, Int128>(arg_columns, max_key_column, res_type);
            case TypeIndex::Int256:
                return execute2<KeyType, Int256>(arg_columns, max_key_column, res_type);
            case TypeIndex::UInt8:
                return execute2<KeyType, UInt8>(arg_columns, max_key_column, res_type);
            case TypeIndex::UInt16:
                return execute2<KeyType, UInt16>(arg_columns, max_key_column, res_type);
            case TypeIndex::UInt32:
                return execute2<KeyType, UInt32>(arg_columns, max_key_column, res_type);
            case TypeIndex::UInt64:
                return execute2<KeyType, UInt64>(arg_columns, max_key_column, res_type);
            case TypeIndex::UInt128:
                return execute2<KeyType, UInt128>(arg_columns, max_key_column, res_type);
            case TypeIndex::UInt256:
                return execute2<KeyType, UInt256>(arg_columns, max_key_column, res_type);
            default:
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal columns in arguments of function " + getName());
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        DataTypePtr res_type, key_type, val_type;
        ColumnPtr max_key_column = nullptr;
        ColumnPtr arg_columns[] = {arguments[0].column, nullptr};

        if (arguments[0].type->getTypeId() == TypeIndex::Array)
        {
            key_type = assert_cast<const DataTypeArray *>(arguments[0].type.get())->getNestedType();
            val_type = assert_cast<const DataTypeArray *>(arguments[1].type.get())->getNestedType();
            res_type = getReturnTypeImpl(DataTypes{arguments[0].type, arguments[1].type});

            arg_columns[1] = arguments[1].column;
            if (arguments.size() == 3)
            {
                /* max key provided */
                max_key_column = arguments[2].column;
            }
        }
        else
        {
            assert(arguments[0].type->getTypeId() == TypeIndex::Map);

            const auto * map_type = assert_cast<const DataTypeMap *>(arguments[0].type.get());
            res_type = getReturnTypeImpl(DataTypes{arguments[0].type});
            key_type = map_type->getKeyType();
            val_type = map_type->getValueType();

            if (arguments.size() == 2)
            {
                /* max key provided */
                max_key_column = arguments[1].column;
            }
        }

        switch (key_type->getTypeId())
        {
            case TypeIndex::Int8:
                return execute1<Int8>(arg_columns, max_key_column, res_type, val_type);
            case TypeIndex::Int16:
                return execute1<Int16>(arg_columns, max_key_column, res_type, val_type);
            case TypeIndex::Int32:
                return execute1<Int32>(arg_columns, max_key_column, res_type, val_type);
            case TypeIndex::Int64:
                return execute1<Int64>(arg_columns, max_key_column, res_type, val_type);
            case TypeIndex::Int128:
                return execute1<Int128>(arg_columns, max_key_column, res_type, val_type);
            case TypeIndex::Int256:
                return execute1<Int256>(arg_columns, max_key_column, res_type, val_type);
            case TypeIndex::UInt8:
                return execute1<UInt8>(arg_columns, max_key_column, res_type, val_type);
            case TypeIndex::UInt16:
                return execute1<UInt16>(arg_columns, max_key_column, res_type, val_type);
            case TypeIndex::UInt32:
                return execute1<UInt32>(arg_columns, max_key_column, res_type, val_type);
            case TypeIndex::UInt64:
                return execute1<UInt64>(arg_columns, max_key_column, res_type, val_type);
            case TypeIndex::UInt128:
                return execute1<UInt128>(arg_columns, max_key_column, res_type, val_type);
            case TypeIndex::UInt256:
                return execute1<UInt256>(arg_columns, max_key_column, res_type, val_type);
            default:
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal columns in arguments of function " + getName());
        }
    }
};

void registerFunctionMapPopulateSeries(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMapPopulateSeries>();
}
}
