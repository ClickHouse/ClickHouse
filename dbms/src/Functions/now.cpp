#include <DataTypes/DataTypeDateTime.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Core/Field.h>


namespace DB

{
/// Get the current time. (It is a constant, it is evaluated once for the entire query.)

class PreparedFunctionNow : public PreparedFunctionImpl
{
public:
    explicit PreparedFunctionNow(time_t time_) : time_value(time_) {}

    String getName() const override { return "now"; }

protected:
    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeDateTime().createColumnConst(
                input_rows_count,
                static_cast<UInt64>(time_value));
    }

private:
    time_t time_value;
};

class FunctionBaseNow : public IFunctionBase
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

    PreparedFunctionPtr prepare(const Block &, const ColumnNumbers &, size_t) const override
    {
        return std::make_shared<PreparedFunctionNow>(time_value);
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

private:
    time_t time_value;
    DataTypePtr return_type;
};

class FunctionBuilderNow : public FunctionBuilderImpl
{
public:
    static constexpr auto name = "now";

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    size_t getNumberOfArguments() const override { return 0; }
    static FunctionBuilderPtr create(const Context &) { return std::make_shared<FunctionBuilderNow>(); }

protected:
    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeDateTime>(); }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName &, const DataTypePtr &) const override
    {
        return std::make_shared<FunctionBaseNow>(time(nullptr));
    }
};

void registerFunctionNow(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBuilderNow>(FunctionFactory::CaseInsensitive);
}

}
