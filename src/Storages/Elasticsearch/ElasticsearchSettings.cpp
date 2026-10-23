#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/Elasticsearch/ElasticsearchSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
    extern const int SETTING_CONSTRAINT_VIOLATION;
}

#define LIST_OF_ELASTICSEARCH_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Seconds, keep_alive, 60, "How long Elasticsearch keeps the point in time open between two consecutive page requests. It is extended on every request.", 0) \
    DECLARE(UInt64, page_size, 10'000, "The number of hits returned from Elasticsearch on every request. Each page becomes one block of the read.", 0)

DECLARE_SETTINGS_TRAITS(ElasticsearchSettingsTraits, LIST_OF_ELASTICSEARCH_SETTINGS, ELASTICSEARCH_SETTINGS_SUPPORTED_TYPES)
IMPLEMENT_SETTINGS_TRAITS(ElasticsearchSettingsTraits, LIST_OF_ELASTICSEARCH_SETTINGS, ElasticsearchSettings, ElasticsearchSetting)

ElasticsearchSettings::ElasticsearchSettings() : impl(std::make_unique<ElasticsearchSettingsImpl>())
{
}

ElasticsearchSettings::ElasticsearchSettings(const ElasticsearchSettings & settings) : impl(std::make_unique<ElasticsearchSettingsImpl>(*settings.impl))
{
}

ElasticsearchSettings::ElasticsearchSettings(ElasticsearchSettings && settings) noexcept = default;

ElasticsearchSettings::~ElasticsearchSettings() = default;

ELASTICSEARCH_SETTINGS_SUPPORTED_TYPES(ElasticsearchSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void ElasticsearchSettings::applyChanges(const SettingsChanges & changes)
{
    impl->applyChanges(changes);
}

void ElasticsearchSettings::loadFromQuery(const ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            auto changes = storage_def.settings->changes;
            impl->applyChanges(changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
}

void ElasticsearchSettings::loadFromQuery(const ASTSetQuery & settings_def)
{
    impl->applyChanges(settings_def.changes);
}

bool ElasticsearchSettings::hasBuiltin(std::string_view name)
{
    return ElasticsearchSettingsImpl::hasBuiltin(name);
}

void ElasticsearchSettings::checkCanSet(std::string_view name, const Field & value)
{
    ElasticsearchSettingsImpl::checkCanSet(name, value);
}

ASTPtr ElasticsearchSettings::getSettingsChangesQuery()
{
    auto settings_ast = make_intrusive<ASTSetQuery>();
    settings_ast->is_standalone = false;
    for (const auto & change : impl->changes())
        settings_ast->changes.push_back(change);

    return settings_ast;
}

void ElasticsearchSettings::sanityCheck() const
{
    if ((*impl)[ElasticsearchSetting::keep_alive].totalSeconds() < 1)
        throw Exception(
            ErrorCodes::SETTING_CONSTRAINT_VIOLATION,
            "Setting `keep_alive` must be positive. `keep_alive`: {}",
            (*impl)[ElasticsearchSetting::keep_alive].totalSeconds());


    if ((*impl)[ElasticsearchSetting::page_size] < 1)
        throw Exception(
            ErrorCodes::SETTING_CONSTRAINT_VIOLATION,
            "Setting `page_size` must be positive. `page_size`: {}",
            (*impl)[ElasticsearchSetting::page_size].value);
}
}
