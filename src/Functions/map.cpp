#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/castColumn.h>
#include <memory>

#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include "array/arrayIndex.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

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


struct NameMapContains { static constexpr auto name = "mapContains"; };

class FunctionMapContains : public IFunction
{
public:
    static constexpr auto name = NameMapContains::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapContains>(); }

    String getName() const override
    {
        return NameMapContains::name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());

        if (!map_type)
            throw Exception{"First argument for function " + getName() + " must be a map",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        auto key_type = map_type->getKeyType();

        if (!(isNumber(arguments[1].type) && isNumber(key_type))
            && key_type->getName() != arguments[1].type->getName())
            throw Exception{"Second argument for function " + getName() + " must be a " + key_type->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        bool is_const = isColumnConst(*arguments[0].column);
        const ColumnMap * col_map = is_const ? checkAndGetColumnConstData<ColumnMap>(arguments[0].column.get()) : checkAndGetColumn<ColumnMap>(arguments[0].column.get());
        if (!col_map)
            throw Exception{"First argument for function " + getName() + " must be a map", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        const auto & nested_column = col_map->getNestedColumn();
        const auto & keys_data = col_map->getNestedData().getColumn(0);

        /// Prepare arguments to call arrayIndex for check has the array element.
        ColumnPtr column_array = ColumnArray::create(keys_data.getPtr(), nested_column.getOffsetsPtr());
        ColumnsWithTypeAndName new_arguments =
        {
            {
                is_const ? ColumnConst::create(std::move(column_array), keys_data.size()) : std::move(column_array),
                std::make_shared<DataTypeArray>(result_type),
                ""
            },
            arguments[1]
        };

        return FunctionArrayIndex<HasAction, NameMapContains>().executeImpl(new_arguments, result_type, input_rows_count);
    }
};


class FunctionMapKeys : public IFunction
{
public:
    static constexpr auto name = "mapKeys";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapKeys>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());

        if (!map_type)
            throw Exception{"First argument for function " + getName() + " must be a map",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        auto key_type = map_type->getKeyType();

        return std::make_shared<DataTypeArray>(key_type);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        const ColumnMap * col_map = typeid_cast<const ColumnMap *>(arguments[0].column.get());
        if (!col_map)
            return nullptr;

        const auto & nested_column = col_map->getNestedColumn();
        const auto & keys_data = col_map->getNestedData().getColumn(0);

        return ColumnArray::create(keys_data.getPtr(), nested_column.getOffsetsPtr());
    }
};


class FunctionMapValues : public IFunction
{
public:
    static constexpr auto name = "mapValues";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapValues>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());

        if (!map_type)
            throw Exception{"First argument for function " + getName() + " must be a map",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        auto value_type = map_type->getValueType();

        return std::make_shared<DataTypeArray>(value_type);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        const ColumnMap * col_map = typeid_cast<const ColumnMap *>(arguments[0].column.get());
        if (!col_map)
            return nullptr;

        const auto & nested_column = col_map->getNestedColumn();
        const auto & values_data = col_map->getNestedData().getColumn(1);

        return ColumnArray::create(values_data.getPtr(), nested_column.getOffsetsPtr());
    }
};

}

void registerFunctionsMap(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMap>();
    factory.registerFunction<FunctionMapContains>();
    factory.registerFunction<FunctionMapKeys>();
    factory.registerFunction<FunctionMapValues>();
}

}
