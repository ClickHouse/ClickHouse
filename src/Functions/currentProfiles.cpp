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
                case Kind::currentProfiles: profile_ids = context->getCurrentProfiles(); break;
                case Kind::enabledProfiles: profile_ids = context->getEnabledProfiles(); break;
                case Kind::defaultProfiles: profile_ids = context->getUser()->settings.toProfileIDs(); break;
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
    factory.registerFunction("currentProfiles", [](ContextPtr context){ return std::make_shared<FunctionProfiles>(context, Kind::currentProfiles); });
    factory.registerFunction("enabledProfiles", [](ContextPtr context){ return std::make_shared<FunctionProfiles>(context, Kind::enabledProfiles); });
    factory.registerFunction("defaultProfiles", [](ContextPtr context){ return std::make_shared<FunctionProfiles>(context, Kind::defaultProfiles); });
}

}
