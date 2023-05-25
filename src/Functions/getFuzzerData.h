#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>

namespace DB
{
class FunctionGetFuzzerData : public IFunction
{
    inline static String fuzz_data;

public:
    static constexpr auto name = "getFuzzerData";

    inline static FunctionPtr create(ContextPtr) { return create(); }

    static FunctionPtr create()
    {
        return std::make_shared<FunctionGetFuzzerData>();
    }

    inline String getName() const override { return name; }

    inline size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    inline bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &,
                          const DataTypePtr &,
                          size_t input_rows_count) const override
    {
        return DataTypeString().createColumnConst(input_rows_count, fuzz_data);
    }

    static void update(const String & fuzz_data_)
    {
        fuzz_data = fuzz_data_;
    }
};

}
