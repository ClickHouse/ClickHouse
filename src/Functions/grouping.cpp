#include <base/types.h>
#include "Columns/ColumnsNumber.h"
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{

class FunctionGrouping : public IFunction
{
public:
    static constexpr auto name = "grouping";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionGrouping>();
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool isSuitableForConstantFolding() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    String getName() const override
    {
        return name;
    }
    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        //TODO: add assert for argument types
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeSingleGroupingSet(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        auto grouping_set_map_column = checkAndGetColumnConst<ColumnUInt64>(arguments[0].column.get());
        auto argument_keys_column = checkAndGetColumnConst<ColumnArray>(arguments[1].column.get());

        auto aggregation_keys_number = (*grouping_set_map_column)[0].get<UInt64>();

        auto result = std::make_shared<DataTypeUInt64>()->createColumn();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto indexes = (*argument_keys_column)[i].get<Array>();
            UInt64 value = 0;
            for (auto index : indexes)
                value = (value << 1) + (index.get<UInt64>() < aggregation_keys_number ? 1 : 0);

            result->insert(Field(value));
        }
        return result;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (arguments.size() == 2)
            return executeSingleGroupingSet(arguments, input_rows_count);

        auto grouping_set_column = checkAndGetColumn<ColumnUInt64>(arguments[0].column.get());
        auto grouping_set_map_column = checkAndGetColumnConst<ColumnArray>(arguments[1].column.get());
        auto argument_keys_column = checkAndGetColumnConst<ColumnArray>(arguments[2].column.get());

        auto masks = (*grouping_set_map_column)[0].get<Array>();

        auto result = std::make_shared<DataTypeUInt64>()->createColumn();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            UInt64 set_index = grouping_set_column->get64(i);
            auto mask = masks[set_index].get<const String &>();

            auto indexes = (*argument_keys_column)[i].get<Array>();
            UInt64 value = 0;
            for (auto index : indexes)
                value = (value << 1) + (mask[index.get<UInt64>()] == '1' ? 1 : 0);

            result->insert(Field(value));
        }
        return result;
    }

};

void registerFunctionGrouping(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGrouping>();
}

}
