#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>


namespace DB
{
namespace
{

class FunctionAuthenticatedUser final : public IFunction
{
    const String user_name;

public:
    static constexpr auto name = "authenticatedUser";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionAuthenticatedUser>(context->getClientInfo().authenticated_user);
    }

    explicit FunctionAuthenticatedUser(const String & user_name_) : user_name{user_name_}
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

    /// The result must not be a constant column: `authenticated_user` is not propagated to
    /// secondary queries (see `ClientInfo::write`), so folding the call on the initiator of a
    /// distributed query would ship the initiator's authenticated user to every shard instead
    /// of letting each shard observe its own value (same as `queryID` and `currentQueryID`).
    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeString().createColumnConst(input_rows_count, user_name)->convertToFullColumnIfConst();
    }
};

}

REGISTER_FUNCTION(AuthenticatedUser)
{
    factory.registerFunction<FunctionAuthenticatedUser>(FunctionDocumentation{
        .description=R"(
If the session user has been switched using the EXECUTE AS command, this function returns the name of the original user that was used for authentication and creating the session.
Alias: authUser()
        )",
        .syntax=R"(authenticatedUser())",
        .arguments={},
        .returned_value={R"(The name of the authenticated user.)", {"String"}},
        .examples{
            {"Usage example",
            R"(
            EXECUTE as u1;
            SELECT currentUser(), authenticatedUser();
            )",
            R"(
┌─currentUser()─┬─authenticatedUser()─┐
│ u1            │ default             │
└───────────────┴─────────────────────┘
        )"
        }},
        .introduced_in = {25, 11},
        .category = FunctionDocumentation::Category::Other
    });

    factory.registerAlias("authUser", "authenticatedUser");
}

}
