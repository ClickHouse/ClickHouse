#include <Storages/Elasticsearch/ElasticsearchQueueSettings.h>

#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollections.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define ELASTICSEARCH_QUEUE_RELATED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(String, elasticsearch_url, "", "Base URL of Elasticsearch for the ElasticsearchQueue engine.", 0) \
    DECLARE(String, elasticsearch_index, "", "Elasticsearch index or index pattern for the ElasticsearchQueue engine.", 0) \
    DECLARE(String, elasticsearch_cursor_field, "", "Field used as an ascending search_after cursor for the ElasticsearchQueue engine.", 0) \
    DECLARE(String, elasticsearch_tiebreaker_field, "", "Optional second ascending sort field used to make search_after deterministic.", 0) \
    DECLARE(String, elasticsearch_query, "", "Optional Elasticsearch JSON request body fragment. The engine overwrites size, sort, pit, and search_after.", 0) \
    DECLARE(String, elasticsearch_auth_type, "auto", "Authentication type for Elasticsearch. Supported values: auto, none, basic, api_key, bearer.", 0) \
    DECLARE(String, elasticsearch_user, "", "HTTP basic authentication user for Elasticsearch.", 0) \
    DECLARE(String, elasticsearch_password, "", "HTTP basic authentication password for Elasticsearch.", 0) \
    DECLARE(String, elasticsearch_api_key, "", "Elasticsearch API key value used in Authorization: ApiKey header.", 0) \
    DECLARE(String, elasticsearch_bearer_token, "", "Elasticsearch bearer token value used in Authorization: Bearer header.", 0) \
    DECLARE(Bool, elasticsearch_use_point_in_time, false, "Use Elasticsearch point-in-time search lifecycle for consistent polling.", 0) \
    DECLARE(String, elasticsearch_pit_keep_alive, "1m", "Elasticsearch point-in-time keep_alive value.", 0) \
    DECLARE(String, elasticsearch_keeper_path, "", "Optional Keeper path used to store a shared ElasticsearchQueue checkpoint across replicas.", 0) \
    DECLARE(String, elasticsearch_keeper_checkpoint_name, "checkpoint", "Keeper node name used to store the ElasticsearchQueue checkpoint.", 0) \
    DECLARE(UInt64, elasticsearch_max_block_size, 0, "Number of rows collected for flushing data from ElasticsearchQueue.", 0) \
    DECLARE(UInt64, elasticsearch_poll_max_batch_size, 0, "Maximum number of rows requested from Elasticsearch in a single poll.", 0) \
    DECLARE(Milliseconds, elasticsearch_poll_timeout_ms, 0, "Timeout for a single ElasticsearchQueue poll.", 0) \
    DECLARE(Milliseconds, elasticsearch_flush_interval_ms, 0, "Maximum background work interval before rescheduling ElasticsearchQueue.", 0) \
    DECLARE(Milliseconds, elasticsearch_consumer_reschedule_ms, 500, "Interval for rescheduling ElasticsearchQueue background task when it stalls.", 0) \
    DECLARE(Bool, elasticsearch_commit_on_select, false, "Commit ElasticsearchQueue checkpoint when a direct SELECT query is made.", 0) \

#define LIST_OF_ELASTICSEARCH_QUEUE_SETTINGS(M, ALIAS) \
    ELASTICSEARCH_QUEUE_RELATED_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(ElasticsearchQueueSettingsTraits, LIST_OF_ELASTICSEARCH_QUEUE_SETTINGS, ELASTICSEARCH_QUEUE_SETTINGS_SUPPORTED_TYPES)
IMPLEMENT_SETTINGS_TRAITS(
    ElasticsearchQueueSettingsTraits,
    LIST_OF_ELASTICSEARCH_QUEUE_SETTINGS,
    ElasticsearchQueueSettings,
    ElasticsearchQueueSetting)

ElasticsearchQueueSettings::ElasticsearchQueueSettings() : impl(std::make_unique<ElasticsearchQueueSettingsImpl>())
{
}

ElasticsearchQueueSettings::ElasticsearchQueueSettings(const ElasticsearchQueueSettings & settings)
    : impl(std::make_unique<ElasticsearchQueueSettingsImpl>(*settings.impl))
{
}

ElasticsearchQueueSettings::ElasticsearchQueueSettings(ElasticsearchQueueSettings && settings) noexcept
    : impl(std::make_unique<ElasticsearchQueueSettingsImpl>(std::move(*settings.impl)))
{
}

ElasticsearchQueueSettings::~ElasticsearchQueueSettings() = default;

ELASTICSEARCH_QUEUE_SETTINGS_SUPPORTED_TYPES(ElasticsearchQueueSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void ElasticsearchQueueSettings::loadFromQuery(ASTStorage & storage_def)
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
        auto settings_ast = make_intrusive<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }
}

void ElasticsearchQueueSettings::loadFromNamedCollection(const MutableNamedCollectionPtr & named_collection)
{
    for (const auto & setting : impl->all())
    {
        const auto & setting_name = setting.getName();
        if (named_collection->has(setting_name))
            impl->set(setting_name, named_collection->get<String>(setting_name));
    }

    if (named_collection->has("url") && !named_collection->has("elasticsearch_url"))
        impl->set("elasticsearch_url", named_collection->get<String>("url"));
    if (named_collection->has("index") && !named_collection->has("elasticsearch_index"))
        impl->set("elasticsearch_index", named_collection->get<String>("index"));
    if (named_collection->has("cursor_field") && !named_collection->has("elasticsearch_cursor_field"))
        impl->set("elasticsearch_cursor_field", named_collection->get<String>("cursor_field"));
    if (named_collection->has("user") && !named_collection->has("elasticsearch_user"))
        impl->set("elasticsearch_user", named_collection->get<String>("user"));
    if (named_collection->has("password") && !named_collection->has("elasticsearch_password"))
        impl->set("elasticsearch_password", named_collection->get<String>("password"));
    if (named_collection->has("api_key") && !named_collection->has("elasticsearch_api_key"))
        impl->set("elasticsearch_api_key", named_collection->get<String>("api_key"));
    if (named_collection->has("auth_type") && !named_collection->has("elasticsearch_auth_type"))
        impl->set("elasticsearch_auth_type", named_collection->get<String>("auth_type"));
    if (named_collection->has("bearer_token") && !named_collection->has("elasticsearch_bearer_token"))
        impl->set("elasticsearch_bearer_token", named_collection->get<String>("bearer_token"));
    if (named_collection->has("use_point_in_time") && !named_collection->has("elasticsearch_use_point_in_time"))
        impl->set("elasticsearch_use_point_in_time", named_collection->get<String>("use_point_in_time"));
    if (named_collection->has("pit_keep_alive") && !named_collection->has("elasticsearch_pit_keep_alive"))
        impl->set("elasticsearch_pit_keep_alive", named_collection->get<String>("pit_keep_alive"));
    if (named_collection->has("keeper_path") && !named_collection->has("elasticsearch_keeper_path"))
        impl->set("elasticsearch_keeper_path", named_collection->get<String>("keeper_path"));
    if (named_collection->has("keeper_checkpoint_name") && !named_collection->has("elasticsearch_keeper_checkpoint_name"))
        impl->set("elasticsearch_keeper_checkpoint_name", named_collection->get<String>("keeper_checkpoint_name"));
}

bool ElasticsearchQueueSettings::hasBuiltin(std::string_view name)
{
    return ElasticsearchQueueSettingsImpl::hasBuiltin(name);
}

}
