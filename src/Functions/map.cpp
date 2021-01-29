#include <Functions/IFunctionImpl.h>
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

    static FunctionPtr create(const Context &)
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
        DataTypePtr key_type = getLeastSupertype(keys);
        DataTypePtr value_type = getLeastSupertype(values);
        if (!isStringOrFixedString(*key_type))
            throw Exception("Function " + getName() + " key shall be String or FixedString", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeMap>(value_type);
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

        return ColumnMap::create(value_type, std::move(keys_data), std::move(values_data), std::move(offsets));
    }
};


struct NameMapContains { static constexpr auto name = "mapContains"; };

class FunctionMapContains : public IFunction
{
public:
    static constexpr auto name = NameMapContains::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMapContains>(); }

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
            throw Exception{"First argument for function " + getName() + " must be a Map but it has type " + arguments[0].type->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        if (!isString(arguments[1].type))
            throw Exception{"Second argument for function " + getName() + " must be a String but it has type " + arguments[1].type->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const ColumnMap * col_map = typeid_cast<const ColumnMap *>(arguments[0].column.get());
        if (!col_map)
            return nullptr;
        auto res = ColumnVector<UInt8>::create(input_rows_count);
        typename ColumnVector<UInt8>::Container & vec_to = res->getData();

        ColumnPtr col_key = arguments[1].column;
        if (isColumnConst(*col_key))
        {
            const String & key = col_key->getDataAt(0).toString();
            const auto & col_val = col_map->getSubColumn(key);
            if (!col_val)
                return res;
            const auto & col_val_nullable = assert_cast<const ColumnNullable &>(*col_val);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                if (!col_val_nullable.isNullAt(i))
                    vec_to[i] = 0x01;
            }
            return res;
        }
        else
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const String & key = col_key->getDataAt(i).toString();
                const auto & col_val = col_map->getSubColumn(key);
                if (!col_val)
                    continue;
                auto & col_val_nullable = assert_cast<const ColumnNullable &>(*col_val);
                if (!col_val_nullable.isNullAt(i))
                    vec_to[i] = 0x01;
            }
            return res;
        }
    }
};


}

void registerFunctionsMap(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMap>();
    factory.registerFunction<FunctionMapContains>();
}

}
