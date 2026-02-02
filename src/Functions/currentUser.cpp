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
    FunctionDocumentation::Description description = R"(
Returns the name of the current user.
In case of a distributed query, the name of the user who initiated the query is returned.
    )";
    FunctionDocumentation::Syntax syntax = "currentUser()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the name of the current user, otherwise the login of the user who initiated the query.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example", R"(
SELECT currentUser()
        )",
        R"(
┌─currentUser()─┐
│ default       │
└───────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionCurrentUser>(documentation);
    factory.registerAlias("user", FunctionCurrentUser::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("current_user", FunctionCurrentUser::name, FunctionFactory::Case::Insensitive);
}

}
