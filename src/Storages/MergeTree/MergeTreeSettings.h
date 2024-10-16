#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/Field.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
#include <base/types.h>
#include <Common/SettingsChanges.h>

namespace boost
{
namespace program_options
{
class options_description;
}
}

namespace Poco
{
namespace Util
{
class AbstractConfiguration;
}
}

namespace DB
{
class ASTStorage;
class Context;
using ContextPtr = std::shared_ptr<const Context>;
struct MergeTreeSettingsImpl;
struct MergeTreeSettings;
using MergeTreeSettingsPtr = std::shared_ptr<const MergeTreeSettings>;
struct MutableColumnsAndConstraints;

/// List of available types supported in MergeTreeSettings object
#define MERGETREE_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, CleanDeletedRows) \
    M(CLASS_NAME, DeduplicateMergeProjectionMode) \
    M(CLASS_NAME, Float) \
    M(CLASS_NAME, Int64) \
    M(CLASS_NAME, LightweightMutationProjectionMode) \
    M(CLASS_NAME, MaxThreads) \
    M(CLASS_NAME, MergeSelectorAlgorithm) \
    M(CLASS_NAME, Milliseconds) \
    M(CLASS_NAME, Seconds) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, UInt64)

MERGETREE_SETTINGS_SUPPORTED_TYPES(MergeTreeSettings, DECLARE_SETTING_TRAIT)

struct MergeTreeSettings
{
    MergeTreeSettings();
    MergeTreeSettings(const MergeTreeSettings & settings);
    MergeTreeSettings(MergeTreeSettings && settings) noexcept;
    ~MergeTreeSettings();

    MERGETREE_SETTINGS_SUPPORTED_TYPES(MergeTreeSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    bool has(std::string_view name) const;

    bool tryGet(std::string_view name, Field & value) const;
    Field get(std::string_view name) const;

    void set(std::string_view name, const Field & value);

    SettingsChanges changes() const;
    void applyChanges(const SettingsChanges & changes);
    void applyChange(const SettingChange & change);
    std::vector<std::string_view> getAllRegisteredNames() const;
    void applyCompatibilitySetting(const String & compatibility_value);

    /// NOTE: will rewrite the AST to add immutable settings.
    void loadFromQuery(ASTStorage & storage_def, ContextPtr context, bool is_attach);
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);

    bool needSyncPart(size_t input_rows, size_t input_bytes) const;
    void sanityCheck(size_t background_pool_tasks) const;

    void dumpToSystemMergeTreeSettingsColumns(MutableColumnsAndConstraints & params) const;

    void addToProgramOptionsIfNotPresent(boost::program_options::options_description & main_options, bool allow_repeated_settings);

    static Field castValueUtil(std::string_view name, const Field & value);
    static String valueToStringUtil(std::string_view name, const Field & value);
    static Field stringToValueUtil(std::string_view name, const String & str);
    static bool hasBuiltin(std::string_view name);
    static std::string_view resolveName(std::string_view name);
    static bool isReadonlySetting(const String & name);
    static void checkCanSet(std::string_view name, const Field & value);
    static bool isPartFormatSetting(const String & name);

private:
    std::unique_ptr<MergeTreeSettingsImpl> impl;
};

/// Column-level Merge-Tree settings which overwrite MergeTree settings
namespace MergeTreeColumnSettings
{
    void validate(const SettingsChanges & changes);
}
}
