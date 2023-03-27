#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnMap.h>
#include <Interpreters/castColumn.h>


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

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionMap>();
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
        tmp.emplace_back(getLeastSupertype(keys));
        tmp.emplace_back(getLeastSupertype(values));
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

        const auto * keys_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!keys_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be an Array", getName());

        const auto * values_type = checkAndGetDataType<DataTypeArray>(arguments[1].get());
        if (!values_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument for function {} must be an Array", getName());

        DataTypes key_value_types{keys_type->getNestedType(), values_type->getNestedType()};
        return std::make_shared<DataTypeMap>(key_value_types);
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t /* input_rows_count */) const override
    {
        ColumnPtr holder_keys;
        bool is_keys_const = isColumnConst(*arguments[0].column);
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

        ColumnPtr holder_values;
        bool is_values_const = isColumnConst(*arguments[1].column);
        const ColumnArray * col_values;
        if (is_values_const)
        {
            holder_values = arguments[1].column->convertToFullColumnIfConst();
            col_values = checkAndGetColumn<ColumnArray>(holder_values.get());
        }
        else
        {
            col_values = checkAndGetColumn<ColumnArray>(arguments[1].column.get());
        }

        if (!col_keys || !col_values)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Arguments of function {} must be array", getName());

        if (!col_keys->hasEqualOffsets(*col_values))
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Array arguments for function {} must have equal sizes", getName());

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

        const DataTypeMap * left = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());
        const DataTypeMap * right = checkAndGetDataType<DataTypeMap>(arguments[1].type.get());

        if (!left || !right)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The two arguments for function {} must be both Map type",
                getName());
        if (!left->getKeyType()->equals(*right->getKeyType()) || !left->getValueType()->equals(*right->getValueType()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The Key And Value type of Map for function {} must be the same",
                getName());

        return std::make_shared<DataTypeMap>(left->getKeyType(), left->getValueType());
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnMap * col_map_left = typeid_cast<const ColumnMap *>(arguments[0].column.get());
        const auto * col_const_map_left = checkAndGetColumnConst<ColumnMap>(arguments[0].column.get());
        bool col_const_map_left_flag = false;
        if (col_const_map_left)
        {
            col_const_map_left_flag = true;
            col_map_left = typeid_cast<const ColumnMap *>(&col_const_map_left->getDataColumn());
        }
        if (!col_map_left)
            return nullptr;

        const ColumnMap * col_map_right = typeid_cast<const ColumnMap *>(arguments[1].column.get());
        const auto * col_const_map_right = checkAndGetColumnConst<ColumnMap>(arguments[1].column.get());
        bool col_const_map_right_flag = false;
        if (col_const_map_right)
        {
            col_const_map_right_flag = true;
            col_map_right = typeid_cast<const ColumnMap *>(&col_const_map_right->getDataColumn());
        }
        if (!col_map_right)
            return nullptr;

        const auto & nested_column_left = col_map_left->getNestedColumn();
        const auto & keys_data_left = col_map_left->getNestedData().getColumn(0);
        const auto & values_data_left = col_map_left->getNestedData().getColumn(1);
        const auto & offsets_left = nested_column_left.getOffsets();

        const auto & nested_column_right = col_map_right->getNestedColumn();
        const auto & keys_data_right = col_map_right->getNestedData().getColumn(0);
        const auto & values_data_right = col_map_right->getNestedData().getColumn(1);
        const auto & offsets_right = nested_column_right.getOffsets();

        const auto & result_type_map = static_cast<const DataTypeMap &>(*result_type);
        const DataTypePtr & key_type = result_type_map.getKeyType();
        const DataTypePtr & value_type = result_type_map.getValueType();
        MutableColumnPtr keys_data = key_type->createColumn();
        MutableColumnPtr values_data = value_type->createColumn();
        MutableColumnPtr offsets = DataTypeNumber<IColumn::Offset>().createColumn();

        IColumn::Offset current_offset = 0;
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            size_t left_it_begin = col_const_map_left_flag ? 0 : offsets_left[row_idx - 1];
            size_t left_it_end = col_const_map_left_flag ? offsets_left.size() : offsets_left[row_idx];
            size_t right_it_begin = col_const_map_right_flag ? 0 : offsets_right[row_idx - 1];
            size_t right_it_end = col_const_map_right_flag ? offsets_right.size() : offsets_right[row_idx];

            for (size_t i = left_it_begin; i < left_it_end; ++i)
            {
                bool matched = false;
                auto key = keys_data_left.getDataAt(i);
                for (size_t j = right_it_begin; j < right_it_end; ++j)
                {
                    if (keys_data_right.getDataAt(j).toString() == key.toString())
                    {
                        matched = true;
                        break;
                    }
                }
                if (!matched)
                {
                    keys_data->insertFrom(keys_data_left, i);
                    values_data->insertFrom(values_data_left, i);
                    ++current_offset;
                }
            }

            for (size_t j = right_it_begin; j < right_it_end; ++j)
            {
                keys_data->insertFrom(keys_data_right, j);
                values_data->insertFrom(values_data_right, j);
                ++current_offset;
            }

            offsets->insert(current_offset);
        }

        auto nested_column = ColumnArray::create(
            ColumnTuple::create(Columns{std::move(keys_data), std::move(values_data)}),
            std::move(offsets));

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
