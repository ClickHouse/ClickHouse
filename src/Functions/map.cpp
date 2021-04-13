#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
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
            throw Exception("Function " + getName() + " even number of arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

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

}

void registerFunctionsMap(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMap>();
}

}
