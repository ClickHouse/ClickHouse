#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>
#include <Parsers/IAST_fwd.h>
#include <Common/SettingsChanges.h>

namespace DB
{

class ASTStorage;
class ASTSetQuery;
struct ElasticsearchSettingsImpl;

#define ELASTICSEARCH_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, Seconds)

ELASTICSEARCH_SETTINGS_SUPPORTED_TYPES(ElasticsearchSettings, DECLARE_SETTING_TRAIT)

struct ElasticsearchSettings
{
    ElasticsearchSettings();
    ElasticsearchSettings(const ElasticsearchSettings & settings);
    ElasticsearchSettings(ElasticsearchSettings && settings) noexcept;
    ~ElasticsearchSettings();

    ELASTICSEARCH_SETTINGS_SUPPORTED_TYPES(ElasticsearchSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void applyChanges(const SettingsChanges & changes);

    void loadFromQuery(const ASTStorage & storage_def);

    void loadFromQuery(const ASTSetQuery & settings_def);

    static bool hasBuiltin(std::string_view name);

    static void checkCanSet(std::string_view name, const Field & value);

    ASTPtr getSettingsChangesQuery();

    void sanityCheck() const;
private:
    std::unique_ptr<ElasticsearchSettingsImpl> impl;
};
}
