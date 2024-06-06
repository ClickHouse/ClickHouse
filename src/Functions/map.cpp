#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnMap.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/Context.h>
#include <Common/HashTable/HashSet.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

// map(x, y, ...) is a function that allows you to make key-value pair
class FunctionMap : public IFunction
{
public:
    static constexpr auto name = "map";

    explicit FunctionMap(bool use_variant_as_common_type_) : use_variant_as_common_type(use_variant_as_common_type_) {}

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionMap>(context->getSettingsRef().allow_experimental_variant_type && context->getSettingsRef().use_variant_as_common_type);
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isInjective(const ColumnsWithTypeAndName &) const override
    {
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }
    /// map(..., Nothing) -> Map(..., Nothing)
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() % 2 != 0)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires even number of arguments, but {} given", getName(), arguments.size());

        DataTypes keys, values;
        for (size_t i = 0; i < arguments.size(); i += 2)
        {
            keys.emplace_back(arguments[i]);
            values.emplace_back(arguments[i + 1]);
        }

        DataTypes tmp;
        if (use_variant_as_common_type)
        {
            tmp.emplace_back(getLeastSupertypeOrVariant(keys));
            tmp.emplace_back(getLeastSupertypeOrVariant(values));
        }
        else
        {
            tmp.emplace_back(getLeastSupertype(keys));
            tmp.emplace_back(getLeastSupertype(values));
        }
        return std::make_shared<DataTypeMap>(tmp);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        size_t num_elements = arguments.size();

        if (num_elements == 0)
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        const auto & result_type_map = static_cast<const DataTypeMap &>(*result_type);
        const DataTypePtr & key_type = result_type_map.getKeyType();
        const DataTypePtr & value_type = result_type_map.getValueType();

        Columns columns_holder(num_elements);
        ColumnRawPtrs column_ptrs(num_elements);

        for (size_t i = 0; i < num_elements; ++i)
        {
            const auto & arg = arguments[i];
            const auto to_type = i % 2 == 0 ? key_type : value_type;

            ColumnPtr preprocessed_column = castColumn(arg, to_type);
            preprocessed_column = preprocessed_column->convertToFullColumnIfConst();

            columns_holder[i] = std::move(preprocessed_column);
            column_ptrs[i] = columns_holder[i].get();
        }

        /// Create and fill the result map.

        MutableColumnPtr keys_data = key_type->createColumn();
        MutableColumnPtr values_data = value_type->createColumn();
        MutableColumnPtr offsets = DataTypeNumber<IColumn::Offset>().createColumn();

        size_t total_elements = input_rows_count * num_elements / 2;
        keys_data->reserve(total_elements);
        values_data->reserve(total_elements);
        offsets->reserve(input_rows_count);

        IColumn::Offset current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            for (size_t j = 0; j < num_elements; j += 2)
            {
                keys_data->insertFrom(*column_ptrs[j], i);
                values_data->insertFrom(*column_ptrs[j + 1], i);
            }

            current_offset += num_elements / 2;
            offsets->insert(current_offset);
        }

        auto nested_column = ColumnArray::create(
            ColumnTuple::create(Columns{std::move(keys_data), std::move(values_data)}),
            std::move(offsets));

        return ColumnMap::create(nested_column);
    }

private:
    bool use_variant_as_common_type = false;
};

/// mapFromArrays(keys, values) is a function that allows you to make key-value pair from a pair of arrays
class FunctionMapFromArrays : public IFunction
{
public:
    static constexpr auto name = "mapFromArrays";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapFromArrays>(); }
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 2 arguments, but {} given",
                getName(),
                arguments.size());

        /// The first argument should always be Array.
        /// Because key type can not be nested type of Map, which is Tuple
        DataTypePtr key_type;
        if (const auto * keys_type = checkAndGetDataType<DataTypeArray>(arguments[0].get()))
            key_type = keys_type->getNestedType();
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be an Array", getName());

        DataTypePtr value_type;
        if (const auto * value_array_type = checkAndGetDataType<DataTypeArray>(arguments[1].get()))
            value_type = value_array_type->getNestedType();
        else if (const auto * value_map_type = checkAndGetDataType<DataTypeMap>(arguments[1].get()))
            value_type = std::make_shared<DataTypeTuple>(value_map_type->getKeyValueTypes());
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument for function {} must be Array or Map", getName());

        DataTypes key_value_types{key_type, value_type};
        return std::make_shared<DataTypeMap>(key_value_types);
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t /* input_rows_count */) const override
    {
        bool is_keys_const = isColumnConst(*arguments[0].column);
        ColumnPtr holder_keys;
        const ColumnArray * col_keys;
        if (is_keys_const)
        {
            holder_keys = arguments[0].column->convertToFullColumnIfConst();
            col_keys = checkAndGetColumn<ColumnArray>(holder_keys.get());
        }
        else
        {
            col_keys = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        }

        if (!col_keys)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The first argument of function {} must be Array", getName());

        bool is_values_const = isColumnConst(*arguments[1].column);
        ColumnPtr holder_values;
        if (is_values_const)
            holder_values = arguments[1].column->convertToFullColumnIfConst();
        else
            holder_values = arguments[1].column;

        const ColumnArray * col_values;
        if (const auto * col_values_array = checkAndGetColumn<ColumnArray>(holder_values.get()))
            col_values = col_values_array;
        else if (const auto * col_values_map = checkAndGetColumn<ColumnMap>(holder_values.get()))
            col_values = &col_values_map->getNestedColumn();
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The second arguments of function {} must be Array or Map", getName());

        if (!col_keys->hasEqualOffsets(*col_values))
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Two arguments for function {} must have equal sizes", getName());

        const auto & data_keys = col_keys->getDataPtr();
        const auto & data_values = col_values->getDataPtr();
        const auto & offsets = col_keys->getOffsetsPtr();
        auto nested_column = ColumnArray::create(ColumnTuple::create(Columns{data_keys, data_values}), offsets);
        return ColumnMap::create(nested_column);
    }
};

class FunctionMapUpdate : public IFunction
{
public:
    static constexpr auto name = "mapUpdate";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapUpdate>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2",
                getName(), arguments.size());

        const auto * left = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());
        const auto * right = checkAndGetDataType<DataTypeMap>(arguments[1].type.get());

        if (!left || !right)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The two arguments for function {} must be both Map type", getName());

        if (!left->getKeyType()->equals(*right->getKeyType()) || !left->getValueType()->equals(*right->getValueType()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The Key And Value type of Map for function {} must be the same", getName());

        return std::make_shared<DataTypeMap>(left->getKeyType(), left->getValueType());
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        bool is_left_const = isColumnConst(*arguments[0].column);
        bool is_right_const = isColumnConst(*arguments[1].column);

        const auto * map_column_left = is_left_const
            ? checkAndGetColumnConstData<ColumnMap>(arguments[0].column.get())
            : checkAndGetColumn<ColumnMap>(arguments[0].column.get());

        const auto * map_column_right = is_right_const
            ? checkAndGetColumnConstData<ColumnMap>(arguments[1].column.get())
            : checkAndGetColumn<ColumnMap>(arguments[1].column.get());

        if (!map_column_left || !map_column_right)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Arguments for function {} must be maps, got {} and {} instead",
                getName(), arguments[0].column->getName(), arguments[1].column->getName());

        const auto & nested_column_left = map_column_left->getNestedColumn();
        const auto & keys_data_left = map_column_left->getNestedData().getColumn(0);
        const auto & values_data_left = map_column_left->getNestedData().getColumn(1);
        const auto & offsets_left = nested_column_left.getOffsets();

        const auto & nested_column_right = map_column_right->getNestedColumn();
        const auto & keys_data_right = map_column_right->getNestedData().getColumn(0);
        const auto & values_data_right = map_column_right->getNestedData().getColumn(1);
        const auto & offsets_right = nested_column_right.getOffsets();

        auto result_keys = keys_data_left.cloneEmpty();
        auto result_values = values_data_left.cloneEmpty();

        size_t size_to_reserve = keys_data_right.size() + (keys_data_left.size() - keys_data_right.size());

        result_keys->reserve(size_to_reserve);
        result_values->reserve(size_to_reserve);

        auto result_offsets = ColumnVector<IColumn::Offset>::create(input_rows_count);
        auto & result_offsets_data = result_offsets->getData();

        using Set = HashSetWithStackMemory<StringRef, StringRefHash, 4>;

        Set right_keys_const;
        if (is_right_const)
        {
            for (size_t i = 0; i < keys_data_right.size(); ++i)
                right_keys_const.insert(keys_data_right.getDataAt(i));
        }

        IColumn::Offset current_offset = 0;
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            size_t left_from = is_left_const ? 0 : offsets_left[row_idx - 1];
            size_t left_to = is_left_const ? offsets_left[0] : offsets_left[row_idx];

            size_t right_from = is_right_const ? 0 : offsets_right[row_idx - 1];
            size_t right_to = is_right_const ? offsets_right[0] : offsets_right[row_idx];

            auto execute_row = [&](const auto & set)
            {
                for (size_t i = left_from; i < left_to; ++i)
                {
                    if (!set.find(keys_data_left.getDataAt(i)))
                    {
                        result_keys->insertFrom(keys_data_left, i);
                        result_values->insertFrom(values_data_left, i);
                        ++current_offset;
                    }
                }
            };

            if (is_right_const)
            {
                execute_row(right_keys_const);
            }
            else
            {
                Set right_keys;
                for (size_t i = right_from; i < right_to; ++i)
                    right_keys.insert(keys_data_right.getDataAt(i));

                execute_row(right_keys);
            }

            size_t right_map_size = right_to - right_from;
            result_keys->insertRangeFrom(keys_data_right, right_from, right_map_size);
            result_values->insertRangeFrom(values_data_right, right_from, right_map_size);

            current_offset += right_map_size;
            result_offsets_data[row_idx] = current_offset;
        }

        auto nested_column = ColumnArray::create(
            ColumnTuple::create(Columns{std::move(result_keys), std::move(result_values)}),
            std::move(result_offsets));

        return ColumnMap::create(nested_column);
    }
};

}

REGISTER_FUNCTION(Map)
{
    factory.registerFunction<FunctionMap>();
    factory.registerFunction<FunctionMapUpdate>();
    factory.registerFunction<FunctionMapFromArrays>();
    factory.registerAlias("MAP_FROM_ARRAYS", "mapFromArrays");
}

}
