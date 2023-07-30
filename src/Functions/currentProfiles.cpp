#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Access/AccessControl.h>
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
    enum class Kind
    {
        CURRENT_PROFILES,
        ENABLED_PROFILES,
        DEFAULT_PROFILES,
    };

    template <Kind kind>
    class FunctionCurrentProfiles : public IFunction
    {
    public:
        static constexpr auto name = (kind == Kind::CURRENT_PROFILES) ? "currentProfiles" : ((kind == Kind::ENABLED_PROFILES) ? "enabledProfiles" : "defaultProfiles");
        static FunctionPtr create(const ContextPtr & context) { return std::make_shared<FunctionCurrentProfiles>(context); }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        String getName() const override { return name; }

        explicit FunctionCurrentProfiles(const ContextPtr & context)
        {
            const auto & manager = context->getAccessControl();

            std::vector<UUID> profile_ids;
            if constexpr (kind == Kind::CURRENT_PROFILES)
            {
                profile_ids = context->getCurrentProfiles();
            }
            else if constexpr (kind == Kind::ENABLED_PROFILES)
            {
                profile_ids = context->getEnabledProfiles();
            }
            else
            {
                static_assert(kind == Kind::DEFAULT_PROFILES);
                profile_ids = context->getUser()->settings.toProfileIDs();
            }

            profile_names = manager.tryReadNames(profile_ids);
        }

        size_t getNumberOfArguments() const override { return 0; }
        bool isDeterministic() const override { return false; }

        DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
        {
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
        {
            auto col_res = ColumnArray::create(ColumnString::create());
            ColumnString & res_strings = typeid_cast<ColumnString &>(col_res->getData());
            ColumnArray::Offsets & res_offsets = col_res->getOffsets();
            for (const String & profile_name : profile_names)
                res_strings.insertData(profile_name.data(), profile_name.length());
            res_offsets.push_back(res_strings.size());
            return ColumnConst::create(std::move(col_res), input_rows_count);
        }

    private:
        Strings profile_names;
    };
}

REGISTER_FUNCTION(CurrentProfiles)
{
    factory.registerFunction<FunctionCurrentProfiles<Kind::CURRENT_PROFILES>>();
    factory.registerFunction<FunctionCurrentProfiles<Kind::ENABLED_PROFILES>>();
    factory.registerFunction<FunctionCurrentProfiles<Kind::DEFAULT_PROFILES>>();
}

}
