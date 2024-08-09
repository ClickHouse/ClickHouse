#include <base/sort.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Access/AccessControl.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/User.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>


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

    template <Kind kind>
    class FunctionCurrentRoles : public IFunction
    {
    public:
        static constexpr auto name = (kind == Kind::CURRENT_ROLES) ? "currentRoles" : ((kind == Kind::ENABLED_ROLES) ? "enabledRoles" : "defaultRoles");
        static FunctionPtr create(const ContextPtr & context) { return std::make_shared<FunctionCurrentRoles>(context); }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        String getName() const override { return name; }

        explicit FunctionCurrentRoles(const ContextPtr & context_)
            : context(context_)
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
            if constexpr (kind == Kind::CURRENT_ROLES)
            {
                role_names = context->getRolesInfo()->getCurrentRolesNames();
            }
            else if constexpr (kind == Kind::ENABLED_ROLES)
            {
                role_names = context->getRolesInfo()->getEnabledRolesNames();
            }
            else
            {
                static_assert(kind == Kind::DEFAULT_ROLES);
                const auto & manager = context->getAccessControl();
                auto user = context->getUser();
                role_names = manager.tryReadNames(user->granted_roles.findGranted(user->default_roles));
            }

            /// We sort the names because the result of the function should not depend on the order of UUIDs.
            ::sort(role_names.begin(), role_names.end());
        }

        mutable std::once_flag initialized_flag;
        ContextPtr context;
        mutable Strings role_names;
    };
}

REGISTER_FUNCTION(CurrentRoles)
{
    factory.registerFunction<FunctionCurrentRoles<Kind::CURRENT_ROLES>>();
    factory.registerFunction<FunctionCurrentRoles<Kind::ENABLED_ROLES>>();
    factory.registerFunction<FunctionCurrentRoles<Kind::DEFAULT_ROLES>>();
}

}
