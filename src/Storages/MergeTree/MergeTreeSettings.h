#pragma once

#include <Storages/SettingDescription.h>

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/Field.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
#include <Core/SettingsTierType.h>
#include <base/types.h>
#include <Common/SettingsChanges.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Columns/IColumn_fwd.h>

#include <optional>

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
class SettingsConstraints;
class ASTStorage;
class Context;
using ContextPtr = std::shared_ptr<const Context>;
struct MergeTreeSettingsImpl;
struct MergeTreeSettings;
using MergeTreeSettingsPtr = std::shared_ptr<const MergeTreeSettings>;

/// List of available types supported in MergeTreeSettings object
#define MERGETREE_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, AlterColumnSecondaryIndexMode) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, CleanDeletedRows) \
    M(CLASS_NAME, DeduplicateMergeProjectionMode) \
    M(CLASS_NAME, Float) \
    M(CLASS_NAME, Int64) \
    M(CLASS_NAME, LightweightMutationProjectionMode) \
    M(CLASS_NAME, MaxThreads) \
    M(CLASS_NAME, MergeCoordinatorDistributionAlgorithm) \
    M(CLASS_NAME, MergeSelectorAlgorithm) \
    M(CLASS_NAME, Milliseconds) \
    M(CLASS_NAME, NonZeroUInt64) \
    M(CLASS_NAME, Seconds) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, UInt32) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, UInt64Auto) \
    M(CLASS_NAME, MergeTreeSerializationInfoVersion) \
    M(CLASS_NAME, MergeTreeStringSerializationVersion) \
    M(CLASS_NAME, MergeTreeNullableSerializationVersion) \
    M(CLASS_NAME, MergeTreeObjectSerializationVersion) \
    M(CLASS_NAME, MergeTreeObjectSharedDataSerializationVersion) \
    M(CLASS_NAME, MergeTreeDynamicSerializationVersion) \
    M(CLASS_NAME, MergeTreePatchPartsVersion) \
    M(CLASS_NAME, MergeTreeMapBucketsStrategy) \
    M(CLASS_NAME, MergeTreeMapSerializationVersion) \
    M(CLASS_NAME, MergeTreePartMinMaxIndexColumns) \
    M(CLASS_NAME, SearchOrphanedPartsDisks) \
    M(CLASS_NAME, TextIndexPostingListCodec) \
    M(CLASS_NAME, MergeTreeTextIndexSerializationVersion)

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

    /// The same by the setting's typed index, so that a misspelled or renamed setting does not compile. Like
    /// the by-name form it counts as an assignment, which is what clears the source the server's baseline
    /// recorded - the value is this engine argument's, not the config's.
    template <typename FieldType>
    void set(SettingIndex<MergeTreeSettings, FieldType> setting, const Field & value)
    {
        setAtOffset(setting.offset, value);
    }

    /// For the typed `set` above, which holds `Impl` behind an incomplete type and so can pass only the offset.
    void setAtOffset(size_t offset, const Field & value);

    SettingsChanges changes() const;
    /// Every setting whose value differs from `base`, i.e. what changes when `base` is replaced by this.
    SettingsChanges changesFrom(const MergeTreeSettings & base) const;
    void applyChanges(const SettingsChanges & changes, ContextPtr context, bool is_loading_from_existing_metadata);
    /// The table's whole `SETTINGS` clause as an `ALTER` leaves it, recorded as the definition, as `loadFromQuery`
    /// records it. `applyChanges` records nothing, for the copies built only to check what a change would do.
    void applyDefinition(const SettingsChanges & changes, ContextPtr context, bool is_loading_from_existing_metadata);
    void applyChange(const SettingChange & change, ContextPtr context, bool is_loading_from_existing_metadata);
    VectorWithMemoryTracking<std::string_view> getAllRegisteredNames() const;
    static std::vector<std::string_view> getAllAliasNames();
    std::string_view getDescription(std::string_view name) const;
    std::string_view getTypeName(std::string_view name) const;
    String getDefaultValueString(std::string_view name) const;
    SettingsTierType getTier(std::string_view name) const;
    void applyCompatibilitySetting(const String & compatibility_value);

    /// What `loadFromQuery` needs to know about the query it is loading from, named at the call site rather than
    /// read off three trailing booleans.
    struct LoadFromQuery
    {
        /// The table is being loaded from metadata that already exists, rather than created or fully attached.
        bool is_loading_from_existing_metadata = false;
        /// The table belongs to the `system` database, whose tables resolve their disk differently.
        bool for_system_database = false;
        /// Whether `storage_def` is the definition that will be stored - true for `CREATE`, a full `ATTACH`, a
        /// replay or `RESTORE`; false where the table is loaded from what is already stored, and what this writes
        /// into `storage_def` stays in memory.
        bool stores_definition = true;
    };

    /// NOTE: will rewrite the AST to add immutable settings.
    void loadFromQuery(ASTStorage & storage_def, ContextPtr context, LoadFromQuery from);
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);

    bool needSyncPart(size_t input_rows, size_t input_bytes) const;
    void sanityCheck(size_t background_pool_tasks, bool background_pool_auto_lowered) const;

    void dumpToSystemCompletionsColumns(MutableColumns & columns) const;
    /// The engine's own settings, for `system.engine_settings`.
    static SettingDescriptions enumerateEngineSettings(ContextPtr context);
    static SettingDescriptions enumerateReplicatedEngineSettings(ContextPtr context);

    void addToProgramOptionsIfNotPresent(boost::program_options::options_description & main_options, bool allow_repeated_settings);

    static Field castValueUtil(std::string_view name, const Field & value);
    static String valueToStringUtil(std::string_view name, const Field & value);
    static Field stringToValueUtil(std::string_view name, const String & str);
    static bool hasBuiltin(std::string_view name);
    /// Every setting of this instance, for `system.table_settings`. The caller refines `origin`.
    SettingDescriptions enumerateSettings() const;
    /// Fills in what the user's settings constraints say about each of `settings`. `MergeTreeSettings`
    /// is the only engine settings type `SettingsConstraints` can describe - a profile reaches it
    /// through the `merge_tree_` name prefix - so no other struct has an equivalent.
    void applyConstraints(SettingDescriptions & settings, const SettingsConstraints & constraints) const;
    static std::optional<SettingsTierType> tryGetTierOfBuiltin(std::string_view name);
    static std::string_view resolveName(std::string_view name);
    static bool isReadonlySetting(const String & name);
    static void checkCanSet(std::string_view name, const Field & value);
    static bool isPartFormatSetting(const String & name);

    static bool isDiskSettingChanged(const SettingsChanges & old_changes, const SettingsChanges & new_changes);
    static void resolveDiskSetting(SettingsChanges & changes, ContextPtr context, bool is_loading_from_existing_metadata, bool for_system_database = false);
    static void resolveDiskSetting(SettingChange & change, ContextPtr context, bool is_loading_from_existing_metadata, bool for_system_database = false);

    /// Cloud only
    static bool isSMTReadonlySetting(const String & name);

private:
    std::unique_ptr<MergeTreeSettingsImpl> impl;
};

/// Column-level Merge-Tree settings which overwrite MergeTree settings
namespace MergeTreeColumnSettings
{
    void validate(const SettingsChanges & changes);
}
}
