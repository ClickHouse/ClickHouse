#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>

namespace DB
{
class ASTStorage;
struct KeeperMapSettingsImpl;

class IAST;
using ASTPtr = std::shared_ptr<IAST>;

class SettingsChanges;

/// List of available types supported in KeeperMapSettings object
#define KEEPER_MAP_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) M(CLASS_NAME, Bool)

KEEPER_MAP_SETTINGS_SUPPORTED_TYPES(KeeperMapSettings, DECLARE_SETTING_TRAIT)

/** Settings for the KeeperMap engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct KeeperMapSettings
{
    KeeperMapSettings();
    KeeperMapSettings(const KeeperMapSettings & settings);
    KeeperMapSettings(KeeperMapSettings && settings) noexcept;
    ~KeeperMapSettings();

    KeeperMapSettings & operator=(KeeperMapSettings && settings) noexcept;

    KEEPER_MAP_SETTINGS_SUPPORTED_TYPES(KeeperMapSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(ASTStorage & storage_def);
    ASTPtr getSettingsChangesQuery();
    void applyChanges(const SettingsChanges & changes);

private:
    std::unique_ptr<KeeperMapSettingsImpl> impl;
};

}
