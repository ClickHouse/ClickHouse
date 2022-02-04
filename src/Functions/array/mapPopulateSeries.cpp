#include <Core/ColumnWithTypeAndName.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include "DataTypes/IDataType.h"
#include "DataTypes/DataTypeMap.h"
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/castColumn.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
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
    bool useDefaultImplementationForConstants() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    void checkTypes(const DataTypePtr & key_type, const DataTypePtr & value_type, const DataTypePtr & max_key_type) const
    {
        WhichDataType key_data_type(key_type);
        WhichDataType value_data_type(value_type);

        if (!(key_data_type.isInt() || key_data_type.isUInt()))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} key argument should be of signed or unsigned integer type. Actual type {}",
                getName(),
                key_type->getName());
        }

        if (!(value_data_type.isInt() || value_data_type.isUInt()))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} key argument should be of signed or unsigned integer type. Actual type {}",
                getName(),
                key_type->getName());
        }

        if (!max_key_type)
            return;

        WhichDataType max_key_data_type(max_key_type);

        if (max_key_data_type.isNullable())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} max key argument can not be Nullable. Actual type {}",
                getName(),
                max_key_type->getName());

        if (!(max_key_data_type.isInt() || max_key_data_type.isUInt()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} max key should be of signed or unsigned integer type. Actual type {}.",
                getName(),
                key_type->getName(),
                max_key_type->getName());
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Function {} accepts at least one map or two arrays arguments, and optional max key argument",
            getName());

        WhichDataType key_argument_data_type(arguments[0]);

        DataTypePtr key_argument_series_type;
        DataTypePtr value_argument_series_type;

        size_t max_key_argument_index = 0;

        if (key_argument_data_type.isArray())
        {
            DataTypePtr value_type;
            if (1 < arguments.size())
                value_type = arguments[1];

            if (arguments.size() < 2 || (value_type && !isArray(value_type)))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Function {} if array argument is passed as key, additional array argument as value must be passed",
                    getName());

            const auto & key_array_type = assert_cast<const DataTypeArray &>(*arguments[0]);
            const auto & value_array_type = assert_cast<const DataTypeArray &>(*value_type);

            key_argument_series_type = key_array_type.getNestedType();
            value_argument_series_type = value_array_type.getNestedType();

            max_key_argument_index = 2;
        }
        else if (key_argument_data_type.isMap())
        {
            const auto & map_data_type = assert_cast<const DataTypeMap &>(*arguments[0]);

            key_argument_series_type = map_data_type.getKeyType();
            value_argument_series_type = map_data_type.getValueType();

            max_key_argument_index = 1;
        }
        else
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} only accepts one map or arrays, but got {}",
                getName(),
                arguments[0]->getName());

        DataTypePtr max_key_argument_type;
        if (max_key_argument_index < arguments.size())
            max_key_argument_type = arguments[max_key_argument_index];

        checkTypes(key_argument_series_type, value_argument_series_type, max_key_argument_type);

        if (key_argument_data_type.isArray())
            return std::make_shared<DataTypeTuple>(DataTypes{arguments[0], arguments[1]});
        else
            return arguments[0];
    }

    template <typename KeyType, typename ValueType>
    void executeImplTyped(
        const ColumnPtr & key_column,
        const ColumnPtr & value_column,
        const ColumnPtr & offsets_column,
        const ColumnPtr & max_key_column,
        MutableColumnPtr result_key_column,
        MutableColumnPtr result_value_column,
        MutableColumnPtr result_offset_column) const
    {
        const auto & key_column_typed = assert_cast<const ColumnVector<KeyType> &>(*key_column);
        const auto & key_column_data = key_column_typed.getData();

        const auto & offsets_column_typed = assert_cast<const ColumnVector<ColumnArray::Offset> &>(*offsets_column);
        const auto & offsets = offsets_column_typed.getData();

        const auto & value_column_typed = assert_cast<const ColumnVector<ValueType> &>(*value_column);
        const auto & value_column_data = value_column_typed.getData();

        auto & result_key_column_typed = assert_cast<ColumnVector<KeyType> &>(*result_key_column);
        auto & result_key_data = result_key_column_typed.getData();

        auto & result_value_column_typed = assert_cast<ColumnVector<ValueType> &>(*result_value_column);
        auto & result_value_data = result_value_column_typed.getData();

        auto & result_offsets_column_typed = assert_cast<ColumnVector<ColumnArray::Offset> &>(*result_offset_column);
        auto & result_offsets_data = result_offsets_column_typed.getData();

        std::optional<KeyType> max_key_column_const;
        if (max_key_column)
        {
            if (auto * const_max_key_column = checkAndGetColumnConst<ColumnVector<KeyType>>(max_key_column.get()))
                max_key_column_const = const_max_key_column->template getValue<KeyType>();
        }

        PaddedPODArray<std::pair<KeyType, ValueType>> sorted_keys_values;

        size_t key_offsets_size = offsets.size();
        result_key_data.reserve(key_offsets_size);
        result_value_data.reserve(key_offsets_size);

        for (size_t offset_index = 0; offset_index < key_offsets_size; ++offset_index)
        {
            size_t start_offset = offsets[offset_index - 1];
            size_t end_offset = offsets[offset_index];

            sorted_keys_values.clear();

            for (; start_offset < end_offset; ++start_offset)
                sorted_keys_values.emplace_back(key_column_data[start_offset], value_column_data[start_offset]);

            if unlikely(sorted_keys_values.empty())
            {
                result_offsets_data.emplace_back(result_value_data.size());
                continue;
            }

            std::sort(sorted_keys_values.begin(), sorted_keys_values.end());

            KeyType min_key = sorted_keys_values.front().first;
            KeyType max_key = sorted_keys_values.back().first;

            if (max_key_column)
            {
                KeyType max_key_column_value {};

                if (max_key_column_const)
                {
                    max_key_column_value = *max_key_column_const;
                }
                else
                {
                    const auto & max_key_column_typed = assert_cast<const ColumnVector<KeyType> &>(*max_key_column);
                    max_key_column_value = max_key_column_typed.getData()[offset_index];
                }

                max_key = max_key_column_value;

                if (unlikely(max_key < min_key))
                {
                    result_offsets_data.emplace_back(result_value_data.size());
                    continue;
                }
            }

            size_t length = static_cast<size_t>(max_key - min_key);
            static constexpr size_t MAX_ARRAY_SIZE = 1ULL << 30;
            if (length > MAX_ARRAY_SIZE)
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                    "Function {} too large array size {} in the result",
                    getName(),
                    length);

            size_t result_key_data_size = result_key_data.size();
            size_t result_value_data_size = result_value_data.size();
            size_t sorted_keys_values_size = sorted_keys_values.size();

            result_key_data.resize_fill(result_key_data_size + length + 1);
            result_value_data.resize_fill(result_value_data_size + length + 1);

            size_t sorted_values_index = 0;

            for (KeyType current_key = min_key; current_key <= max_key; ++current_key)
            {
                size_t key_offset_index = current_key - min_key;
                size_t insert_index = result_value_data_size + key_offset_index;

                result_key_data[insert_index] = current_key;

                if (sorted_values_index < sorted_keys_values_size &&
                    sorted_keys_values[sorted_values_index].first == current_key)
                {
                    auto & sorted_key_value = sorted_keys_values[sorted_values_index];
                    if (current_key == sorted_key_value.first)
                    {
                        result_value_data[insert_index] = sorted_key_value.second;
                    }

                    ++sorted_values_index;
                    while (sorted_values_index < sorted_keys_values_size &&
                        current_key == sorted_keys_values[sorted_values_index].first)
                    {
                        ++sorted_values_index;
                    }
                }

                if (current_key == max_key)
                    break;
            }

            result_offsets_data.emplace_back(result_value_data.size());
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t) const override
    {
        DataTypePtr key_series_type;
        DataTypePtr value_series_type;

        ColumnPtr key_column;
        ColumnPtr value_column;
        ColumnPtr offsets_column;

        size_t max_key_argument_index = 0;

        auto first_argument_column = arguments[0].column->convertToFullColumnIfConst();
        ColumnPtr second_argument_array_column;

        if (const auto * key_argument_array_column = typeid_cast<const ColumnArray *>(first_argument_column.get()))
        {
            const ColumnArray * value_argument_array_column = nullptr;

            if (1 < arguments.size())
            {
                second_argument_array_column = arguments[1].column->convertToFullColumnIfConst();
                value_argument_array_column = typeid_cast<const ColumnArray *>(second_argument_array_column.get());
            }

            if (!value_argument_array_column)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Function {} if array argument is passed as key, additional array argument as value must be passed",
                    getName());

            key_series_type = assert_cast<const DataTypeArray &>(*arguments[0].type).getNestedType();
            key_column = key_argument_array_column->getDataPtr();
            const auto & key_offsets = key_argument_array_column->getOffsets();

            value_series_type = assert_cast<const DataTypeArray &>(*arguments[1].type).getNestedType();
            value_column = value_argument_array_column->getDataPtr();
            const auto & value_offsets = value_argument_array_column->getOffsets();

            if (key_offsets != value_offsets)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Function {} key and value array should have same amount of elements",
                    getName());

            offsets_column = key_argument_array_column->getOffsetsPtr();
            max_key_argument_index = 2;
        }
        else if (const auto * key_argument_map_column = typeid_cast<const ColumnMap *>(first_argument_column.get()))
        {
            const auto & nested_array = key_argument_map_column->getNestedColumn();
            const auto & nested_data_column = key_argument_map_column->getNestedData();

            const auto & map_argument_type = assert_cast<const DataTypeMap &>(*arguments[0].type);
            key_series_type = map_argument_type.getKeyType();
            value_series_type = map_argument_type.getValueType();

            key_column = nested_data_column.getColumnPtr(0);
            value_column = nested_data_column.getColumnPtr(1);
            offsets_column = nested_array.getOffsetsPtr();

            max_key_argument_index = 1;
        }
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Function {} only accepts one map or arrays, but got {}",
                getName(),
                arguments[0].type->getName());

        ColumnPtr max_key_column;

        if (max_key_argument_index < arguments.size())
        {
            max_key_column = arguments[max_key_argument_index].column;
            auto max_key_column_type = arguments[max_key_argument_index].type;

            if (!max_key_column_type->equals(*key_series_type))
            {
                ColumnWithTypeAndName column_to_cast = {max_key_column, max_key_column_type, ""};
                auto casted_column = castColumnAccurate(std::move(column_to_cast), key_series_type);
                max_key_column = std::move(casted_column);
            }
        }

        auto result_column = result_type->createColumn();
        WhichDataType result_data_type(result_type);

        MutableColumnPtr result_key_column;
        MutableColumnPtr result_value_column;
        MutableColumnPtr result_offset_column;
        IColumn * result_offset_column_raw;
        MutableColumnPtr result_array_additional_offset_column;

        auto * tuple_column = typeid_cast<ColumnTuple *>(result_column.get());

        if (tuple_column && tuple_column->tupleSize() == 2)
        {
            auto key_array_column = tuple_column->getColumnPtr(0)->assumeMutable();
            auto value_array_column = tuple_column->getColumnPtr(1)->assumeMutable();

            auto * key_array_column_typed = typeid_cast<ColumnArray *>(key_array_column.get());
            auto * value_array_column_typed = typeid_cast<ColumnArray *>(value_array_column.get());

            if (!key_array_column_typed || !value_array_column_typed)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Function {} result type should be Tuple with two nested Array columns or Map. Actual {}",
                    getName(),
                    result_type->getName());

            result_key_column = key_array_column_typed->getDataPtr()->assumeMutable();
            result_value_column = value_array_column_typed->getDataPtr()->assumeMutable();

            result_offset_column = key_array_column_typed->getOffsetsPtr()->assumeMutable();
            result_offset_column_raw = result_offset_column.get();

            result_array_additional_offset_column = value_array_column_typed->getOffsetsPtr()->assumeMutable();
        }
        else if (const auto * map_column = typeid_cast<ColumnMap *>(result_column.get()))
        {
            result_key_column = map_column->getNestedData().getColumnPtr(0)->assumeMutable();
            result_value_column = map_column->getNestedData().getColumnPtr(1)->assumeMutable();
            result_offset_column = map_column->getNestedColumn().getOffsetsPtr()->assumeMutable();
            result_offset_column_raw = result_offset_column.get();
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Function {} result type should be Tuple with two nested Array columns or Map. Actual {}",
                    getName(),
                    result_type->getName());
        }

        auto call = [&](const auto & types)
        {
            using Types = std::decay_t<decltype(types)>;
            using KeyType = typename Types::LeftType;
            using ValueType = typename Types::RightType;

            if constexpr (IsDataTypeNumber<KeyType> && IsDataTypeNumber<ValueType>)
            {
                using KeyFieldType = typename KeyType::FieldType;
                using ValueFieldType = typename ValueType::FieldType;

                executeImplTyped<KeyFieldType, ValueFieldType>(
                    key_column,
                    value_column,
                    offsets_column,
                    max_key_column,
                    std::move(result_key_column),
                    std::move(result_value_column),
                    std::move(result_offset_column));

                return true;
            }

            return false;
        };

        if (!callOnTwoTypeIndexes(key_series_type->getTypeId(), value_series_type->getTypeId(), call))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "Function {} illegal columns passed as arguments",
             getName());

        if (result_array_additional_offset_column)
        {
            result_array_additional_offset_column->insertRangeFrom(
                *result_offset_column_raw,
                0,
                result_offset_column_raw->size());
        }

        return result_column;
    }
};

void registerFunctionMapPopulateSeries(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMapPopulateSeries>();
}

}
