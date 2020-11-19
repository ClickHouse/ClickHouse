#include <DataTypes/DataTypeDateTime.h>

#include <Functions/IFunctionImpl.h>
#include <Core/DecimalFunctions.h>
#include <Functions/FunctionFactory.h>
#include <Core/Field.h>

#include <time.h>


namespace DB

{
/// Get the current time. (It is a constant, it is evaluated once for the entire query.)

class ExecutableFunctionNow : public IExecutableFunctionImpl
{
public:
    explicit ExecutableFunctionNow(time_t time_) : time_value(time_) {}

    String getName() const override { return "now"; }

    void execute(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeDateTime().createColumnConst(
                input_rows_count,
                static_cast<UInt64>(time_value));
    }

private:
    time_t time_value;
};

class FunctionBaseNow : public IFunctionBaseImpl
{
public:
    explicit FunctionBaseNow(time_t time_) : time_value(time_), return_type(std::make_shared<DataTypeDateTime>()) {}

    String getName() const override { return "now"; }

    const DataTypes & getArgumentTypes() const override
    {
        static const DataTypes argument_types;
        return argument_types;
    }

    const DataTypePtr & getReturnType() const override
    {
        return return_type;
    }

    ExecutableFunctionImplPtr prepare(const Block &, const ColumnNumbers &, size_t) const override
    {
        return std::make_unique<ExecutableFunctionNow>(time_value);
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

private:
    time_t time_value;
    DataTypePtr return_type;
};

class NowOverloadResolver : public IFunctionOverloadResolverImpl
{
public:
    static constexpr auto name = "now";

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    size_t getNumberOfArguments() const override { return 0; }
    static FunctionOverloadResolverImplPtr create(const Context &) { return std::make_unique<NowOverloadResolver>(); }

    DataTypePtr getReturnType(const DataTypes &) const override { return std::make_shared<DataTypeDateTime>(); }

    FunctionBaseImplPtr build(const ColumnsWithTypeAndName &, const DataTypePtr &) const override
    {
        return std::make_unique<FunctionBaseNow>(time(nullptr));
    }
};

void registerFunctionNow(FunctionFactory & factory)
{
    factory.registerFunction<NowOverloadResolver>(FunctionFactory::CaseInsensitive);
}

}
