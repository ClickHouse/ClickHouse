#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/castColumn.h>
#include <memory>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/** map(x, y, ...) is a function that allows you to make key-value pair 
  */

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
        if (arguments.empty() || arguments.size() % 2 != 0)
            throw Exception("Function " + getName() + " requires at least one argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

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

    ColumnPtr executeImpl(ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        
        size_t num_elements = arguments.size();

        if (num_elements == 0)
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        const DataTypePtr & key_type = static_cast<const DataTypeMap &>(*result_type).getKeyType();
        const DataTypePtr & value_type = static_cast<const DataTypeMap &>(*result_type).getValueType();

        Columns columns_holder(num_elements);
        ColumnRawPtrs column_ptrs(num_elements);

        for (size_t i = 0; i < num_elements; ++i)
        {
            const auto & arg = arguments[i];

            ColumnPtr preprocessed_column = arg.column;

            if (0 == i % 2 && !arg.type->equals(*key_type))
                preprocessed_column = castColumn(arg, key_type);
            else if (1 == i % 2 && !arg.type->equals(*value_type))
                preprocessed_column = castColumn(arg, value_type);

            preprocessed_column = preprocessed_column->convertToFullColumnIfConst();

            columns_holder[i] = std::move(preprocessed_column);
            column_ptrs[i] = columns_holder[i].get();
        }

        /// Create and fill the result map.
        Columns map_columns(2);

        auto keys = ColumnArray::create(key_type->createColumn());
        IColumn & keys_data = keys->getData();
        IColumn::Offsets & keys_offsets = keys->getOffsets();
        keys_data.reserve(input_rows_count * num_elements / 2);
        keys_offsets.resize(input_rows_count);

        auto values = ColumnArray::create(value_type->createColumn());
        IColumn & values_data = values->getData();
        IColumn::Offsets & values_offsets = values->getOffsets();
        values_data.reserve(input_rows_count * num_elements / 2);
        values_offsets.resize(input_rows_count);

        IColumn::Offset current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            for (size_t j = 0; j < num_elements; j += 2)
            {
                keys_data.insertFrom(*column_ptrs[j], i);
                values_data.insertFrom(*column_ptrs[j + 1], i);
            }

            current_offset += num_elements / 2;
            keys_offsets[i] = current_offset;
            values_offsets[i] = current_offset;
        }

        map_columns[0] = keys->assumeMutable();
        map_columns[1] = values->assumeMutable();

        return ColumnMap::create(map_columns);
    }
};

}

void registerFunctionsMap(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMap>();
}

}
