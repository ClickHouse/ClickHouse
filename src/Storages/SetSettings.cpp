#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Core/FormatFactorySettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/SetSettings.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define SET_RELATED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Bool, persistent, true, "Disable setting to avoid the overhead of writing to disk for StorageSet", 0) \
    DECLARE(String, disk, "default", "Name of the disk used to persist set data", 0)

#define LIST_OF_SET_SETTINGS(M, ALIAS) \
    SET_RELATED_SETTINGS(M, ALIAS) \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(SetSettingsTraits, LIST_OF_SET_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(SetSettingsTraits, LIST_OF_SET_SETTINGS)


struct SetSettingsImpl : public BaseSettings<SetSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) SetSettings##TYPE NAME = &SetSettingsImpl ::NAME;

namespace SetSetting
{
LIST_OF_SET_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

SetSettings::SetSettings() : impl(std::make_unique<SetSettingsImpl>())
{
}

SetSettings::SetSettings(const SetSettings & settings) : impl(std::make_unique<SetSettingsImpl>(*settings.impl))
{
}

SetSettings::SetSettings(SetSettings && settings) noexcept : impl(std::make_unique<SetSettingsImpl>(std::move(*settings.impl)))
{
}

SetSettings::~SetSettings() = default;

SET_SETTINGS_SUPPORTED_TYPES(SetSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void SetSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            impl->applyChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }
}

}
