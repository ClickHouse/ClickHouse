#include <Common/ThreadFuzzer.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/Field.h>


namespace DB
{

/** Returns whether Thread Fuzzer is effective.
  * It can be used in tests to prevent too long runs.
  */
class FunctionHasThreadFuzzer : public IFunction
{
public:
    static constexpr auto name = "hasThreadFuzzer";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionHasThreadFuzzer>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) const override
    {
        block.getByPosition(result).column = DataTypeUInt8().createColumnConst(input_rows_count, ThreadFuzzer::instance().isEffective());
    }
};


void registerFunctionHasThreadFuzzer(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHasThreadFuzzer>();
}

}

