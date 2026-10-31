#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
class ASTStorage;
struct GenerateRandomSettingsImpl;

class SettingsChanges;

/// List of available types supported in GenerateRandomSettings object
#define GENERATE_RANDOM_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Float) \
    M(CLASS_NAME, UInt64)

GENERATE_RANDOM_SETTINGS_SUPPORTED_TYPES(GenerateRandomSettings, DECLARE_SETTING_TRAIT)

/** Settings for the `GenerateRandom` engine and the `generateRandom` table function.
  * Could be loaded from a `CREATE TABLE` query (`SETTINGS` clause) or from the `SETTINGS` argument
  * of the table function.
  */
struct GenerateRandomSettings
{
    GenerateRandomSettings();
    GenerateRandomSettings(const GenerateRandomSettings & settings);
    GenerateRandomSettings(GenerateRandomSettings && settings) noexcept;
    ~GenerateRandomSettings();

    GenerateRandomSettings & operator=(GenerateRandomSettings && settings) noexcept;

    GENERATE_RANDOM_SETTINGS_SUPPORTED_TYPES(GenerateRandomSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(ASTStorage & storage_def);
    void sanityCheck() const;
    void applyChanges(const SettingsChanges & changes);

    static bool hasBuiltin(std::string_view name);

private:
    std::unique_ptr<GenerateRandomSettingsImpl> impl;
};

}
