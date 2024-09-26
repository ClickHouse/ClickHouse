#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Common/HashTable/HashSet.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_variant_type;
    extern const SettingsBool use_variant_as_common_type;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

namespace
{

// map(x, y, ...) is a function that allows you to make key-value pair
class FunctionMap : public IFunction
{
public:
    static constexpr auto name = "map";

    explicit FunctionMap(ContextPtr context_)
        : context(context_)
        , use_variant_as_common_type(
              context->getSettingsRef()[Setting::allow_experimental_variant_type] && context->getSettingsRef()[Setting::use_variant_as_common_type])
        , function_array(FunctionFactory::instance().get("array", context))
        , function_map_from_arrays(FunctionFactory::instance().get("mapFromArrays", context))
    {
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionMap>(context);
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

        ColumnsWithTypeAndName key_args;
        ColumnsWithTypeAndName value_args;
        for (size_t i = 0; i < num_elements; i += 2)
        {
            key_args.emplace_back(arguments[i]);
            value_args.emplace_back(arguments[i+1]);
        }

        const auto & result_type_map = static_cast<const DataTypeMap &>(*result_type);
        const DataTypePtr & key_type = result_type_map.getKeyType();
        const DataTypePtr & value_type = result_type_map.getValueType();
        const DataTypePtr & key_array_type = std::make_shared<DataTypeArray>(key_type);
        const DataTypePtr & value_array_type = std::make_shared<DataTypeArray>(value_type);

        /// key_array = array(args[0], args[2]...)
        ColumnPtr key_array = function_array->build(key_args)->execute(key_args, key_array_type, input_rows_count);
        /// value_array = array(args[1], args[3]...)
        ColumnPtr value_array = function_array->build(value_args)->execute(value_args, value_array_type, input_rows_count);

        /// result = mapFromArrays(key_array, value_array)
        ColumnsWithTypeAndName map_args{{key_array, key_array_type, ""}, {value_array, value_array_type, ""}};
        return function_map_from_arrays->build(map_args)->execute(map_args, result_type, input_rows_count);
    }

private:
    ContextPtr context;
    bool use_variant_as_common_type = false;
    FunctionOverloadResolverPtr function_array;
    FunctionOverloadResolverPtr function_map_from_arrays;
};

/// mapFromArrays(keys, values) is a function that allows you to make key-value pair from a pair of arrays or maps
class FunctionMapFromArrays : public IFunction
{
public:
    static constexpr auto name = "mapFromArrays";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapFromArrays>(); }
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 2 arguments, but {} given",
                getName(),
                arguments.size());

        auto get_nested_type = [&](const DataTypePtr & type)
        {
            DataTypePtr nested;
            if (const auto * type_as_array = checkAndGetDataType<DataTypeArray>(type.get()))
                nested = type_as_array->getNestedType();
            else if (const auto * type_as_map = checkAndGetDataType<DataTypeMap>(type.get()))
                nested = std::make_shared<DataTypeTuple>(type_as_map->getKeyValueTypes());
            else
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} must be Array or Map, but {} is given",
                    getName(),
                    type->getName());

            return nested;
        };

        auto key_type = get_nested_type(arguments[0]);
        auto value_type = get_nested_type(arguments[1]);

        /// We accept Array(Nullable(T)) or Array(LowCardinality(Nullable(T))) as key types as long as the actual array doesn't contain NULL value(this is checked in executeImpl).
        key_type = removeNullableOrLowCardinalityNullable(key_type);

        DataTypes key_value_types{key_type, value_type};
        return std::make_shared<DataTypeMap>(key_value_types);
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t /* input_rows_count */) const override
    {
        auto get_array_column = [&](const ColumnPtr & column) -> std::pair<const ColumnArray *, ColumnPtr>
        {
            bool is_const = isColumnConst(*column);
            ColumnPtr holder = is_const ? column->convertToFullColumnIfConst() : column;

            const ColumnArray * col_res = nullptr;
            if (const auto * col_array = checkAndGetColumn<ColumnArray>(holder.get()))
                col_res = col_array;
            else if (const auto * col_map = checkAndGetColumn<ColumnMap>(holder.get()))
                col_res = &col_map->getNestedColumn();
            else
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Argument columns of function {} must be Array or Map, but {} is given",
                    getName(),
                    holder->getName());

            return {col_res, holder};
        };

        auto [col_keys, key_holder] = get_array_column(arguments[0].column);
        auto [col_values, values_holder] = get_array_column(arguments[1].column);

        /// Nullable(T) or LowCardinality(Nullable(T)) are okay as nested key types but actual NULL values are not okay.
        ColumnPtr data_keys = col_keys->getDataPtr();
        if (isColumnNullableOrLowCardinalityNullable(*data_keys))
        {
            const NullMap * null_map = nullptr;
            if (const auto * nullable = checkAndGetColumn<ColumnNullable>(data_keys.get()))
            {
                null_map = &nullable->getNullMapData();
                data_keys = nullable->getNestedColumnPtr();
            }
            else if (const auto * low_cardinality = checkAndGetColumn<ColumnLowCardinality>(data_keys.get()))
            {
                if (const auto * nullable_dict = checkAndGetColumn<ColumnNullable>(low_cardinality->getDictionaryPtr().get()))
                {
                    null_map = &nullable_dict->getNullMapData();
                    data_keys = ColumnLowCardinality::create(nullable_dict->getNestedColumnPtr(), low_cardinality->getIndexesPtr());
                }
            }

            if (null_map && !memoryIsZero(null_map->data(), 0, null_map->size()))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "The nested column of first argument in function {} must not contain NULLs", getName());
        }

        if (!col_keys->hasEqualOffsets(*col_values))
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Two arguments of function {} must have equal sizes", getName());

        const auto & data_values = col_values->getDataPtr();
        const auto & offsets = col_keys->getOffsetsPtr();
        auto nested_column = ColumnArray::create(ColumnTuple::create(Columns{std::move(data_keys), data_values}), offsets);
        return ColumnMap::create(nested_column);
    }
};

class FunctionMapUpdate : public IFunction
{
public:
    static constexpr auto name = "mapUpdate";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapUpdate>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2",
                getName(),
                arguments.size());

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
