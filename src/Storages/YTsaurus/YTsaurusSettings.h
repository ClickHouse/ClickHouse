#pragma once

#if USE_YTSAURUS
#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
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
    M(CLASS_NAME, Bool)

YTSAURUS_SETTINGS_SUPPORTED_TYPES(YTsaurusSettings, DECLARE_SETTING_TRAIT)

struct YTsaurusSettings
{
    YTsaurusSettings();
    YTsaurusSettings(const YTsaurusSettings & settings);
    YTsaurusSettings(YTsaurusSettings && settings) noexcept;
    ~YTsaurusSettings();
    YTSAURUS_SETTINGS_SUPPORTED_TYPES(YTsaurusSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    std::vector<std::string_view> getAllRegisteredNames() const;
    void loadFromQuery(ASTStorage & storage_def);
    // For table engine
    void loadFromQuery(const ASTSetQuery & settings_def);
    void loadFromNamedCollection(const NamedCollection & named_collection);
    // For Dictionary source
    void loadFromConfig(const Poco::Util::AbstractConfiguration & config, const String & prefix);
    void set(const std::string & name, const std::string & value);

    static bool hasBuiltin(std::string_view name);

private:
    std::unique_ptr<YTsaurusSettingsImpl> impl;
};
}

#endif
