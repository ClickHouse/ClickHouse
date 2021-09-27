#include <common/DateLUT.h>

#include <Core/Field.h>

#include <DataTypes/DataTypeDate.h>

#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

class ExecutableFunctionYesterday : public IExecutableFunctionImpl
{
public:
    explicit ExecutableFunctionYesterday(time_t time_) : day_value(time_) {}

    String getName() const override { return "yesterday"; }

    ColumnPtr execute(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeDate().createColumnConst(input_rows_count, day_value);
    }

private:
    DayNum day_value;
};

class FunctionBaseYesterday : public IFunctionBaseImpl
{
public:
    explicit FunctionBaseYesterday(DayNum day_value_) : day_value(day_value_), return_type(std::make_shared<DataTypeDate>()) {}

    String getName() const override { return "yesterday"; }

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
        return std::make_unique<ExecutableFunctionYesterday>(day_value);
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

private:
    DayNum day_value;
    DataTypePtr return_type;
};

class YesterdayOverloadResolver : public IFunctionOverloadResolverImpl
{
public:
    static constexpr auto name = "yesterday";

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    size_t getNumberOfArguments() const override { return 0; }

    static FunctionOverloadResolverImplPtr create(const Context &) { return std::make_unique<YesterdayOverloadResolver>(); }

    DataTypePtr getReturnType(const DataTypes &) const override { return std::make_shared<DataTypeDate>(); }

    FunctionBaseImplPtr build(const ColumnsWithTypeAndName &, const DataTypePtr &) const override
    {
        auto day_num = DateLUT::instance().toDayNum(time(nullptr)) - 1;
        return std::make_unique<FunctionBaseYesterday>(static_cast<DayNum>(day_num));
    }
};

void registerFunctionYesterday(FunctionFactory & factory)
{
    factory.registerFunction<YesterdayOverloadResolver>();
}

}
