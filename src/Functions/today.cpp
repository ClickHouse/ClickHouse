#include <Common/DateLUT.h>

#include <Core/Field.h>

#include <DataTypes/DataTypeDate.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace
{

class ExecutableFunctionToday : public IExecutableFunction
{
public:
    explicit ExecutableFunctionToday(time_t time_) : day_value(time_) {}

    String getName() const override { return "today"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeDate().createColumnConst(input_rows_count, day_value);
    }

private:
    DayNum day_value;
};

class FunctionBaseToday : public IFunctionBase
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

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionToday>(day_value);
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    DayNum day_value;
    DataTypePtr return_type;
};

class TodayOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "today";

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    size_t getNumberOfArguments() const override { return 0; }

    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<TodayOverloadResolver>(); }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeDate>(); }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName &, const DataTypePtr &) const override
    {
        return std::make_unique<FunctionBaseToday>(DayNum(DateLUT::instance().toDayNum(time(nullptr)).toUnderType()));
    }
};

}

REGISTER_FUNCTION(Today)
{
    factory.registerFunction<TodayOverloadResolver>();
}

}
