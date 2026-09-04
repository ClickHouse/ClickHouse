#pragma once

#include <Storages/TableSetting.h>

#if USE_YTSAURUS
#include <Core/BaseSettingsFwdMacros.h>
#include <Columns/IColumn_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
#include <Common/VectorWithMemoryTracking.h>
namespace Poco::Util
{
    class AbstractConfiguration;
}
namespace DB
{
class ASTStorage;
class ASTSetQuery;
class Context;
using ContextPtr = std::shared_ptr<const Context>;
class NamedCollection;
struct YTsaurusSettingsImpl;

#define YTSAURUS_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, Milliseconds) \
    M(CLASS_NAME, UInt64) \

YTSAURUS_SETTINGS_SUPPORTED_TYPES(YTsaurusSettings, DECLARE_SETTING_TRAIT)

struct YTsaurusSettings
{
    YTsaurusSettings();
    YTsaurusSettings(const YTsaurusSettings & settings);
    YTsaurusSettings(YTsaurusSettings && settings) noexcept;
    ~YTsaurusSettings();
    YTSAURUS_SETTINGS_SUPPORTED_TYPES(YTsaurusSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    VectorWithMemoryTracking<std::string_view> getAllRegisteredNames() const;
    void loadFromQuery(ASTStorage & storage_def);
    void loadFromQuery(const ASTSetQuery & settings_def);
    void loadFromNamedCollection(const NamedCollection & named_collection);
    void set(const std::string & name, const std::string & value);

    static YTsaurusSettings createFromQuery(ASTStorage & storage_def);
    static YTsaurusSettings createFromQuery(const ASTSetQuery & settings_def);
    static bool hasBuiltin(std::string_view name);
    /// Every setting of this instance, for `system.table_settings`. The caller refines `origin`.
    TableSettings enumerateSettings() const;
    /// The engine's own settings, for `system.engine_settings`.
    static TableSettings enumerateEngineSettings(ContextPtr context);

private:
    std::unique_ptr<YTsaurusSettingsImpl> impl;
};
}

#endif
