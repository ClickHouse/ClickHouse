#include <common/DateLUT.h>

#include <Core/Field.h>

#include <DataTypes/DataTypeDate.h>

#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace
{

class ExecutableFunctionToday : public IExecutableFunctionImpl
{
public:
    explicit ExecutableFunctionToday(time_t time_) : day_value(time_) {}

    String getName() const override { return "today"; }

    ColumnPtr execute(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeDate().createColumnConst(input_rows_count, day_value);
    }

private:
    DayNum day_value;
};

class FunctionBaseToday : public IFunctionBaseImpl
{
public:
    explicit FunctionBaseToday(DayNum day_value_) : day_value(day_value_), return_type(std::make_shared<DataTypeDate>()) {}

    String getName() const override { return "today"; }

    const DataTypes & getArgumentTypes() const override
    {
        static const DataTypes argument_types;
        return argument_types;
    }

    const DataTypePtr & getResultType() const override
    {
        return return_type;
    }

    ExecutableFunctionImplPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionToday>(day_value);
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

private:
    DayNum day_value;
    DataTypePtr return_type;
};

class TodayOverloadResolver : public IFunctionOverloadResolverImpl
{
public:
    static constexpr auto name = "today";

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    size_t getNumberOfArguments() const override { return 0; }

    static FunctionOverloadResolverImplPtr create(const Context &) { return std::make_unique<TodayOverloadResolver>(); }

    DataTypePtr getReturnType(const DataTypes &) const override { return std::make_shared<DataTypeDate>(); }

    FunctionBaseImplPtr build(const ColumnsWithTypeAndName &, const DataTypePtr &) const override
    {
        return std::make_unique<FunctionBaseToday>(DateLUT::instance().toDayNum(time(nullptr)));
    }
};

}

void registerFunctionToday(FunctionFactory & factory)
{
    factory.registerFunction<TodayOverloadResolver>();
}

}
