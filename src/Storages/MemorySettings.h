#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>

namespace DB
{
class ASTStorage;
struct MemorySettingsImpl;

class IAST;
using ASTPtr = std::shared_ptr<IAST>;

class SettingsChanges;

/// List of available types supported in MemorySettings object
#define MEMORY_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, UInt64)

MEMORY_SETTINGS_SUPPORTED_TYPES(MemorySettings, DECLARE_SETTING_TRAIT)

/** Settings for the Memory engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct MemorySettings
{
    MemorySettings();
    MemorySettings(const MemorySettings & settings);
    MemorySettings(MemorySettings && settings) noexcept;
    ~MemorySettings();

    MemorySettings & operator=(MemorySettings && settings) noexcept;

    MEMORY_SETTINGS_SUPPORTED_TYPES(MemorySettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(ASTStorage & storage_def);
    ASTPtr getSettingsChangesQuery();
    void sanityCheck() const;
    void applyChanges(const SettingsChanges & changes);

private:
    std::unique_ptr<MemorySettingsImpl> impl;
};

}

