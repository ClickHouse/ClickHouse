#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>


namespace DB
{
namespace
{

class FunctionCurrentUser : public IFunction
{
    const String user_name;

public:
    static constexpr auto name = "currentUser";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionCurrentUser>(context->getClientInfo().initial_user);
    }

    explicit FunctionCurrentUser(const String & user_name_) : user_name{user_name_}
    {
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
        return std::make_shared<DataTypeString>();
    }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeString().createColumnConst(input_rows_count, user_name);
    }
};

}

REGISTER_FUNCTION(CurrentUser)
{
    factory.registerFunction<FunctionCurrentUser>();
    factory.registerAlias("user", FunctionCurrentUser::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("current_user", FunctionCurrentUser::name, FunctionFactory::Case::Insensitive);
}

}
