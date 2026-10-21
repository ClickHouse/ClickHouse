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
    };

    String toString(Kind kind)
    {
        switch (kind)
        {
            case Kind::CURRENT_ROLES: return "currentRoles";
            case Kind::ENABLED_ROLES: return "enabledRoles";
            case Kind::DEFAULT_ROLES: return "defaultRoles";
        }
    }

    class FunctionCurrentRoles : public IFunction
    {
    public:
        static FunctionPtr create(const ContextPtr & context, Kind kind)
        {
            return std::make_shared<FunctionCurrentRoles>(context, kind);
        }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        String getName() const override { return toString(kind); }

        explicit FunctionCurrentRoles(const ContextPtr & context_, Kind kind_)
            : context(context_), kind(kind_)
        {}

        size_t getNumberOfArguments() const override { return 0; }
        bool isDeterministic() const override { return false; }

        DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
        {
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
        {
            std::call_once(initialized_flag, [&]{ initialize(); });

            auto col_res = ColumnArray::create(ColumnString::create());
            ColumnString & res_strings = typeid_cast<ColumnString &>(col_res->getData());
            ColumnArray::Offsets & res_offsets = col_res->getOffsets();
            for (const String & role_name : role_names)
                res_strings.insertData(role_name.data(), role_name.length());
            res_offsets.push_back(res_strings.size());
            return ColumnConst::create(std::move(col_res), input_rows_count);
        }

    private:
        void initialize() const
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
            }

            /// We sort the names because the result of the function should not depend on the order of UUIDs.
            ::sort(role_names.begin(), role_names.end());
        }

        mutable std::once_flag initialized_flag;
        ContextPtr context;
        Kind kind;
        mutable Strings role_names;
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
┌─currentRoles()─────────────────────────────────┐
│ ['sql-console-role:jane.smith@clickhouse.com'] │
└────────────────────────────────────────────────┘
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
┌─enabledRoles()─────────────────────────────────────────────────┐
│ ['general_data', 'sql-console-role:jane.smith@clickhouse.com'] │
└────────────────────────────────────────────────────────────────┘
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
┌─defaultRoles()─────────────────────────────────┐
│ ['sql-console-role:jane.smith@clickhouse.com'] │
└────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_defaultRoles = {21, 9};
    FunctionDocumentation::Category category_defaultRoles = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_defaultRoles = {description_defaultRoles, syntax_defaultRoles, arguments_defaultRoles, {}, returned_value_defaultRoles, examples_defaultRoles, introduced_in_defaultRoles, category_defaultRoles};

    factory.registerFunction("currentRoles", [](ContextPtr context){ return FunctionCurrentRoles::create(context, Kind::CURRENT_ROLES); }, documentation_currentRoles);
    factory.registerFunction("enabledRoles", [](ContextPtr context){ return FunctionCurrentRoles::create(context, Kind::ENABLED_ROLES); }, documentation_enabledRoles);
    factory.registerFunction("defaultRoles", [](ContextPtr context){ return FunctionCurrentRoles::create(context, Kind::DEFAULT_ROLES); }, documentation_defaultRoles);
}

}
