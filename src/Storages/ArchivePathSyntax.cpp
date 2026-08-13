#include <Storages/ArchivePathSyntax.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>

#include <string>
#include <string_view>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_archive_path_syntax;
}

bool isFreshTableDefinition(
    LoadingStrictnessLevel mode,
    bool attach_short_syntax,
    bool is_restore_from_backup)
{
    return (mode <= LoadingStrictnessLevel::SECONDARY_CREATE && !is_restore_from_backup)
        || (mode == LoadingStrictnessLevel::ATTACH && !attach_short_syntax);
}

ContextPtr contextWithArchivePathSyntax(const ContextPtr & context, bool enabled)
{
    if (context->getSettingsRef()[Setting::allow_archive_path_syntax] == enabled)
        return context;

    auto pinned_context = Context::createCopy(context);
    pinned_context->setSetting("allow_archive_path_syntax", enabled);
    return pinned_context;
}

PersistedArchivePathSyntax resolveAndPersistArchivePathSyntax(
    ASTStorage & storage_def,
    const ContextPtr & context,
    bool is_fresh_definition)
{
    static constexpr std::string_view setting_name = "allow_archive_path_syntax";

    bool enabled;
    const SettingChange * persisted_change = storage_def.settings
        ? storage_def.settings->changes.tryGetChange(setting_name)
        : nullptr;

    if (persisted_change)
    {
        /// Parse through Settings instead of reading the Field directly so all
        /// accepted `Bool` spellings and the value-less shorthand behave exactly
        /// like the session setting.
        Settings settings = context->getSettingsCopy();
        SettingsChanges change{*persisted_change};
        settings.applyChanges(change);
        enabled = settings[Setting::allow_archive_path_syntax];
    }
    else if (is_fresh_definition)
    {
        enabled = context->getSettingsRef()[Setting::allow_archive_path_syntax];
    }
    else
    {
        /// Metadata written before `allow_archive_path_syntax` was persisted is ambiguous. Use
        /// the historical/default interpretation deterministically; consulting
        /// the reload session here would recreate the original bug.
        enabled = true;
    }

    if (!storage_def.settings)
    {
        auto settings_ast = make_intrusive<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }

    storage_def.settings->changes.setSetting(std::string(setting_name), Field(enabled));
    if (auto * normalized_change = storage_def.settings->changes.tryGetChange(setting_name))
        normalized_change->shorthand = false;
    return {enabled, contextWithArchivePathSyntax(context, enabled)};
}

}
