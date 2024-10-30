#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Storages/KeeperMapSettings.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_SETTING;
extern const int SETTING_CONSTRAINT_VIOLATION;
}

#define KEEPER_MAP_SETTINGS(DECLARE, ALIAS) DECLARE(Bool, read_only, false, "Make the table read-only", 0)

DECLARE_SETTINGS_TRAITS(KeeperMapSettingsTraits, KEEPER_MAP_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(KeeperMapSettingsTraits, KEEPER_MAP_SETTINGS)


struct KeeperMapSettingsImpl : public BaseSettings<KeeperMapSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) KeeperMapSettings##TYPE NAME = &KeeperMapSettingsImpl ::NAME;

namespace KeeperMapSetting
{
KEEPER_MAP_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

KeeperMapSettings::KeeperMapSettings() : impl(std::make_unique<KeeperMapSettingsImpl>())
{
}

KeeperMapSettings::KeeperMapSettings(const KeeperMapSettings & settings) : impl(std::make_unique<KeeperMapSettingsImpl>(*settings.impl))
{
}

KeeperMapSettings::KeeperMapSettings(KeeperMapSettings && settings) noexcept
    : impl(std::make_unique<KeeperMapSettingsImpl>(std::move(*settings.impl)))
{
}

KeeperMapSettings::~KeeperMapSettings() = default;

KeeperMapSettings & KeeperMapSettings::operator=(KeeperMapSettings && settings) noexcept
{
    *impl = std::move(*settings.impl);
    return *this;
}

KEEPER_MAP_SETTINGS_SUPPORTED_TYPES(KeeperMapSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void KeeperMapSettings::loadFromQuery(ASTStorage & storage_def)
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
}

ASTPtr KeeperMapSettings::getSettingsChangesQuery()
{
    auto settings_ast = std::make_shared<ASTSetQuery>();
    settings_ast->is_standalone = false;
    for (const auto & change : impl->changes())
        settings_ast->changes.push_back(change);

    return settings_ast;
}

void KeeperMapSettings::applyChanges(const DB::SettingsChanges & changes)
{
    impl->applyChanges(changes);
}
}
