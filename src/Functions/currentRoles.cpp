#include <base/sort.h>

#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/User.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace
{
    enum class Kind : uint8_t
    {
        CURRENT_ROLES,
        ENABLED_ROLES,
        DEFAULT_ROLES,
        CURRENT_INVOKER_ROLES,
        ENABLED_INVOKER_ROLES,
    };

    String toString(Kind kind)
    {
        switch (kind)
        {
            case Kind::CURRENT_ROLES: return "currentRoles";
            case Kind::ENABLED_ROLES: return "enabledRoles";
            case Kind::DEFAULT_ROLES: return "defaultRoles";
            case Kind::CURRENT_INVOKER_ROLES: return "currentInvokerRoles";
            case Kind::ENABLED_INVOKER_ROLES: return "enabledInvokerRoles";
        }
    }

    class FunctionCurrentRoles final : public IFunction
    {
    public:
        static FunctionPtr create(const ContextPtr & context, Kind kind)
        {
            return std::make_shared<FunctionCurrentRoles>(context, kind);
        }

        [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        [[nodiscard]] String getName() const override { return toString(kind); }

        explicit FunctionCurrentRoles(const ContextPtr & context, Kind kind_)
            : kind(kind_)
        {
            switch (kind)
            {
                case Kind::CURRENT_ROLES:
                    role_names = context->getRolesInfo()->getCurrentRolesNames();
                    break;
                case Kind::ENABLED_ROLES:
                    role_names = context->getRolesInfo()->getEnabledRolesNames();
                    break;
                case Kind::DEFAULT_ROLES:
                {
                    const auto & manager = context->getAccessControl();
                    if (const auto user = context->getAccess()->tryGetUser())
                        role_names = manager.tryReadNames(user->granted_roles.findGranted(user->default_roles));
                    break;
                }
                case Kind::CURRENT_INVOKER_ROLES:
                    role_names = context->getInvokerRolesInfo()->getCurrentRolesNames();
                    break;
                case Kind::ENABLED_INVOKER_ROLES:
                    role_names = context->getInvokerRolesInfo()->getEnabledRolesNames();
                    break;
            }

            /// We sort the names because the result of the function should not depend on the order of UUIDs.
            ::sort(role_names.begin(), role_names.end());
        }

        [[nodiscard]] size_t getNumberOfArguments() const override { return 0; }
        [[nodiscard]] bool isDeterministic() const override { return false; }

        [[nodiscard]] String getSignatureString() const override
        {
            return "() -> Array(String)";
        }

        [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
        {
            auto res_strings_column = ColumnString::create();
            auto res_offsets_column = ColumnArray::ColumnOffsets::create();
            ColumnString & res_strings = *res_strings_column;
            ColumnArray::Offsets & res_offsets = res_offsets_column->getData();
            for (const String & role_name : role_names)
                res_strings.insertData(role_name.data(), role_name.length());
            res_offsets.push_back(res_strings.size());
            auto col_res = ColumnArray::create(std::move(res_strings_column), std::move(res_offsets_column));
            return ColumnConst::create(std::move(col_res), input_rows_count);
        }

    private:
        Kind kind;
        Strings role_names;
    };
}

REGISTER_FUNCTION(CurrentRoles)
{
    FunctionDocumentation::Description description_currentRoles = R"(
Returns an array of the roles which are assigned to the current user.
    )";
    FunctionDocumentation::Syntax syntax_currentRoles = "currentRoles()";
    FunctionDocumentation::Arguments arguments_currentRoles = {};
    FunctionDocumentation::ReturnedValue returned_value_currentRoles = {"Returns an array of the roles which are assigned to the current user.", {"Array(String)"}};
    FunctionDocumentation::Examples examples_currentRoles = {
    {
        "Usage example",
        R"(
SELECT currentRoles();
        )",
        R"(
┌─currentRoles()─┐
│ []             │
└────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_currentRoles = {21, 9};
    FunctionDocumentation::Category category_currentRoles = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_currentRoles = {description_currentRoles, syntax_currentRoles, arguments_currentRoles, {}, returned_value_currentRoles, examples_currentRoles, introduced_in_currentRoles, category_currentRoles};

    FunctionDocumentation::Description description_enabledRoles = R"(
Returns an array of the roles which are enabled for the current user.
    )";
    FunctionDocumentation::Syntax syntax_enabledRoles = "enabledRoles()";
    FunctionDocumentation::Arguments arguments_enabledRoles = {};
    FunctionDocumentation::ReturnedValue returned_value_enabledRoles = {"Returns an array of role names which are enabled for the current user.", {"Array(String)"}};
    FunctionDocumentation::Examples examples_enabledRoles = {
    {
        "Usage example",
        R"(
SELECT enabledRoles();
        )",
        R"(
┌─enabledRoles()─┐
│ []             │
└────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_enabledRoles = {21, 9};
    FunctionDocumentation::Category category_enabledRoles = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_enabledRoles = {description_enabledRoles, syntax_enabledRoles, arguments_enabledRoles, {}, returned_value_enabledRoles, examples_enabledRoles, introduced_in_enabledRoles, category_enabledRoles};

    FunctionDocumentation::Description description_defaultRoles = R"(
Returns an array of default roles for the current user.
    )";
    FunctionDocumentation::Syntax syntax_defaultRoles = "defaultRoles()";
    FunctionDocumentation::Arguments arguments_defaultRoles = {};
    FunctionDocumentation::ReturnedValue returned_value_defaultRoles = {"Returns an array of default roles for the current user.", {"Array(String)"}};
    FunctionDocumentation::Examples examples_defaultRoles = {
    {
        "Usage example",
        R"(
SELECT defaultRoles();
        )",
        R"(
┌─defaultRoles()─┐
│ []             │
└────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_defaultRoles = {21, 9};
    FunctionDocumentation::Category category_defaultRoles = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_defaultRoles = {description_defaultRoles, syntax_defaultRoles, arguments_defaultRoles, {}, returned_value_defaultRoles, examples_defaultRoles, introduced_in_defaultRoles, category_defaultRoles};

    FunctionDocumentation::Description description_currentInvokerRoles = R"(
Returns an array of the roles which are assigned to the invoker of the view whose query contains the function call.

Inside the query of a view with `SQL SECURITY DEFINER` or `SQL SECURITY NONE`, the function returns the current roles
of the user who runs the query reading from the view, while [`currentRoles`](#currentRoles) returns the roles of the definer.
If that query is itself the query of another view with `SQL SECURITY DEFINER`, the invoker is the definer of that other view.
Elsewhere the function returns the same as [`currentRoles`](#currentRoles).
    )";
    FunctionDocumentation::Syntax syntax_currentInvokerRoles = "currentInvokerRoles()";
    FunctionDocumentation::Arguments arguments_currentInvokerRoles = {};
    FunctionDocumentation::ReturnedValue returned_value_currentInvokerRoles = {"Returns an array of the roles which are assigned to the invoker.", {"Array(String)"}};
    FunctionDocumentation::Examples examples_currentInvokerRoles = {
    {
        "Usage example",
        R"(
CREATE VIEW v SQL SECURITY DEFINER DEFINER = default AS SELECT currentInvokerRoles() AS roles;
SET ROLE r;
SELECT * FROM v;
        )",
        R"(
┌─roles─┐
│ ['r'] │
└───────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_currentInvokerRoles = {26, 12};
    FunctionDocumentation::Category category_currentInvokerRoles = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_currentInvokerRoles = {description_currentInvokerRoles, syntax_currentInvokerRoles, arguments_currentInvokerRoles, {}, returned_value_currentInvokerRoles, examples_currentInvokerRoles, introduced_in_currentInvokerRoles, category_currentInvokerRoles};

    FunctionDocumentation::Description description_enabledInvokerRoles = R"(
Returns an array of the roles which are enabled for the invoker of the view whose query contains the function call.

Inside the query of a view with `SQL SECURITY DEFINER` or `SQL SECURITY NONE`, the function returns the enabled roles
of the user who runs the query reading from the view, while [`enabledRoles`](#enabledRoles) returns the roles of the definer.
If that query is itself the query of another view with `SQL SECURITY DEFINER`, the invoker is the definer of that other view.
Elsewhere the function returns the same as [`enabledRoles`](#enabledRoles).
    )";
    FunctionDocumentation::Syntax syntax_enabledInvokerRoles = "enabledInvokerRoles()";
    FunctionDocumentation::Arguments arguments_enabledInvokerRoles = {};
    FunctionDocumentation::ReturnedValue returned_value_enabledInvokerRoles = {"Returns an array of role names which are enabled for the invoker.", {"Array(String)"}};
    FunctionDocumentation::Examples examples_enabledInvokerRoles = {
    {
        "Usage example",
        R"(
GRANT inherited_role TO r;
CREATE VIEW v SQL SECURITY DEFINER DEFINER = default AS SELECT enabledInvokerRoles() AS roles;
SET ROLE r;
SELECT * FROM v;
        )",
        R"(
┌─roles──────────────────┐
│ ['inherited_role','r'] │
└────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_enabledInvokerRoles = {26, 12};
    FunctionDocumentation::Category category_enabledInvokerRoles = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_enabledInvokerRoles = {description_enabledInvokerRoles, syntax_enabledInvokerRoles, arguments_enabledInvokerRoles, {}, returned_value_enabledInvokerRoles, examples_enabledInvokerRoles, introduced_in_enabledInvokerRoles, category_enabledInvokerRoles};

    factory.registerFunction("currentRoles", [](ContextPtr context){ return FunctionCurrentRoles::create(context, Kind::CURRENT_ROLES); }, documentation_currentRoles);
    factory.registerFunction("enabledRoles", [](ContextPtr context){ return FunctionCurrentRoles::create(context, Kind::ENABLED_ROLES); }, documentation_enabledRoles);
    factory.registerFunction("defaultRoles", [](ContextPtr context){ return FunctionCurrentRoles::create(context, Kind::DEFAULT_ROLES); }, documentation_defaultRoles);
    factory.registerFunction("currentInvokerRoles", [](ContextPtr context){ return FunctionCurrentRoles::create(context, Kind::CURRENT_INVOKER_ROLES); }, documentation_currentInvokerRoles);
    factory.registerFunction("enabledInvokerRoles", [](ContextPtr context){ return FunctionCurrentRoles::create(context, Kind::ENABLED_INVOKER_ROLES); }, documentation_enabledInvokerRoles);
}

}
