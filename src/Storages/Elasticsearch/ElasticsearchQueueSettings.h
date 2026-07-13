#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>
#include <Common/NamedCollections/NamedCollections_fwd.h>

#include <Interpreters/Context_fwd.h>

#include <memory>
#include <string_view>

namespace DB
{
class ASTStorage;
struct ElasticsearchQueueSettingsImpl;

#define ELASTICSEARCH_QUEUE_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, Milliseconds) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, UInt64)

ELASTICSEARCH_QUEUE_SETTINGS_SUPPORTED_TYPES(ElasticsearchQueueSettings, DECLARE_SETTING_TRAIT)

struct ElasticsearchQueueSettings
{
    ElasticsearchQueueSettings();
    ElasticsearchQueueSettings(const ElasticsearchQueueSettings & settings);
    ElasticsearchQueueSettings(ElasticsearchQueueSettings && settings) noexcept;
    ~ElasticsearchQueueSettings();

    ELASTICSEARCH_QUEUE_SETTINGS_SUPPORTED_TYPES(ElasticsearchQueueSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(ASTStorage & storage_def);
    void loadFromNamedCollection(const MutableNamedCollectionPtr & named_collection);

    static bool hasBuiltin(std::string_view name);

private:
    std::unique_ptr<ElasticsearchQueueSettingsImpl> impl;
};

}
