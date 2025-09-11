#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
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
        currentProfiles,
        enabledProfiles,
        defaultProfiles,
    };

    String toString(Kind kind)
    {
        switch (kind)
        {
            case Kind::currentProfiles: return "currentProfiles";
            case Kind::enabledProfiles: return "enabledProfiles";
            case Kind::defaultProfiles: return "defaultProfiles";
        }
    }

    class FunctionProfiles : public IFunction
    {
    public:
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
        {
            return false;
        }

        String getName() const override
        {
            return toString(kind);
        }

        explicit FunctionProfiles(const ContextPtr & context_, Kind kind_)
            : kind(kind_)
            , context(context_)
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
            for (const String & profile_name : profile_names)
                res_strings.insertData(profile_name.data(), profile_name.length());
            res_offsets.push_back(res_strings.size());
            return ColumnConst::create(std::move(col_res), input_rows_count);
        }

    private:
        void initialize() const
        {
            const auto & manager = context->getAccessControl();

            std::vector<UUID> profile_ids;

            switch (kind)
            {
                case Kind::currentProfiles:
                    profile_ids = context->getCurrentProfiles();
                    break;
                case Kind::enabledProfiles:
                    profile_ids = context->getEnabledProfiles();
                    break;
                case Kind::defaultProfiles:
                    const auto user = context->getAccess()->tryGetUser();
                    if (user)
                        profile_ids = user->settings.toProfileIDs();
                    break;
            }

            profile_names = manager.tryReadNames(profile_ids);
        }

        mutable std::once_flag initialized_flag;

        Kind kind;
        ContextPtr context;
        mutable Strings profile_names;
    };
}

REGISTER_FUNCTION(Profiles)
{
    FunctionDocumentation::Description description_currentProfiles = R"(
Returns an array of the setting profiles for the current user.
)";
    FunctionDocumentation::Syntax syntax_currentProfiles = "currentProfiles()";
    FunctionDocumentation::Arguments arguments_currentProfiles = {};
    FunctionDocumentation::ReturnedValue returned_value_currentProfiles = {"Returns an array of setting profiles for the current user.", {"Array(String)"}};
    FunctionDocumentation::Examples examples_currentProfiles = {
    {
        "Usage example",
        R"(
SELECT currentProfiles();
        )",
        R"(
┌─currentProfiles()─────────────────────────────┐
│ ['default', 'readonly_user', 'web_analytics'] │
└───────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_currentProfiles = {21, 9};
    FunctionDocumentation::Category category_currentProfiles = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_currentProfiles = {description_currentProfiles, syntax_currentProfiles, arguments_currentProfiles, returned_value_currentProfiles, examples_currentProfiles, introduced_in_currentProfiles, category_currentProfiles};

    FunctionDocumentation::Description description_enabledProfiles = R"(
Returns an array of setting profile names which are enabled for the current user.
)";
    FunctionDocumentation::Syntax syntax_enabledProfiles = "enabledProfiles()";
    FunctionDocumentation::Arguments arguments_enabledProfiles = {};
    FunctionDocumentation::ReturnedValue returned_value_enabledProfiles = {"Returns an array of setting profile names which are enabled for the current user.", {"Array(String)"}};
    FunctionDocumentation::Examples examples_enabledProfiles = {
    {
        "Usage example",
        R"(
SELECT enabledProfiles();
        )",
        R"(
┌─enabledProfiles()─────────────────────────────────────────────────┐
│ ['default', 'readonly_user', 'web_analytics', 'batch_processing'] │
└───────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_enabledProfiles = {21, 9};
    FunctionDocumentation::Category category_enabledProfiles = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_enabledProfiles = {description_enabledProfiles, syntax_enabledProfiles, arguments_enabledProfiles, returned_value_enabledProfiles, examples_enabledProfiles, introduced_in_enabledProfiles, category_enabledProfiles};

    FunctionDocumentation::Description description_defaultProfiles = R"(
Returns an array of default setting profile names for the current user.
)";
    FunctionDocumentation::Syntax syntax_defaultProfiles = "defaultProfiles()";
    FunctionDocumentation::Arguments arguments_defaultProfiles = {};
    FunctionDocumentation::ReturnedValue returned_value_defaultProfiles = {"Returns an array of default setting profile names for the current user.", {"Array(String)"}};
    FunctionDocumentation::Examples examples_defaultProfiles = {
    {
        "Usage example",
        R"(
SELECT defaultProfiles();
        )",
        R"(
┌─defaultProfiles()─┐
│ ['default']       │
└───────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_defaultProfiles = {21, 9};
    FunctionDocumentation::Category category_defaultProfiles = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_defaultProfiles = {description_defaultProfiles, syntax_defaultProfiles, arguments_defaultProfiles, returned_value_defaultProfiles, examples_defaultProfiles, introduced_in_defaultProfiles, category_defaultProfiles};

    factory.registerFunction("currentProfiles", [](ContextPtr context){ return std::make_shared<FunctionProfiles>(context, Kind::currentProfiles); }, documentation_currentProfiles);
    factory.registerFunction("enabledProfiles", [](ContextPtr context){ return std::make_shared<FunctionProfiles>(context, Kind::enabledProfiles); }, documentation_enabledProfiles);
    factory.registerFunction("defaultProfiles", [](ContextPtr context){ return std::make_shared<FunctionProfiles>(context, Kind::defaultProfiles); }, documentation_defaultProfiles);
}

}
