#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <common/DateLUTImpl.h>
#include <Core/Field.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


namespace
{


/** timezoneOf(x) - get the name of the timezone of DateTime data type.
  * Example: Europe/Moscow.
  */
class ExecutableFunctionTimezoneOf : public IExecutableFunctionImpl
{
public:
    static constexpr auto name = "timezoneOf";
    String getName() const override { return name; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    /// Execute the function on the columns.
    ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        DataTypePtr type_no_nullable = removeNullable(arguments[0].type);

        return DataTypeString().createColumnConst(input_rows_count,
            dynamic_cast<const TimezoneMixin &>(*type_no_nullable).getTimeZone().getTimeZone());
    }
};


class BaseFunctionTimezoneOf : public IFunctionBaseImpl
{
public:
    BaseFunctionTimezoneOf(DataTypes argument_types_, DataTypePtr return_type_)
        : argument_types(std::move(argument_types_)), return_type(std::move(return_type_)) {}

    static constexpr auto name = "timezoneOf";
    String getName() const override { return name; }

    bool isDeterministic() const override { return true; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return return_type; }

    ExecutableFunctionImplPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionTimezoneOf>();
    }

    ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(const ColumnsWithTypeAndName & arguments) const override
    {
        DataTypePtr type_no_nullable = removeNullable(arguments[0].type);

        return DataTypeString().createColumnConst(1,
            dynamic_cast<const TimezoneMixin &>(*type_no_nullable).getTimeZone().getTimeZone());
    }

private:
    DataTypes argument_types;
    DataTypePtr return_type;
};


class FunctionTimezoneOfBuilder : public IFunctionOverloadResolverImpl
{
public:
    static constexpr auto name = "timezoneOf";
    String getName() const override { return name; }
    static FunctionOverloadResolverImplPtr create(ContextPtr) { return std::make_unique<FunctionTimezoneOfBuilder>(); }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnType(const DataTypes & types) const override
    {
        DataTypePtr type_no_nullable = removeNullable(types[0]);

        if (isDateTime(type_no_nullable) || isDateTime64(type_no_nullable))
            return std::make_shared<DataTypeString>();
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad argument for function {}, should be DateTime or DateTime64", name);
    }

    FunctionBaseImplPtr build(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        return std::make_unique<BaseFunctionTimezoneOf>(DataTypes{arguments[0].type}, return_type);
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }
};

}

void registerFunctionTimezoneOf(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTimezoneOfBuilder>();
    factory.registerAlias("timeZoneOf", "timezoneOf");
}

}

