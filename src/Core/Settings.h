#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/Field.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
#include <Core/SettingsWriteFormat.h>
#include <base/types.h>
#include <Common/SettingConstraintWritability.h>
#include <Common/SettingsChanges.h>

#include <string_view>
#include <unordered_map>
#include <vector>

namespace boost
{
namespace program_options
{
class options_description;
class variables_map;
}
}

namespace Poco
{
namespace Util
{
class AbstractConfiguration;
class LayeredConfiguration;
}
}

using NameToNameMap = std::unordered_map<std::string, std::string>;

namespace DB
{
class IColumn;
struct MutableColumnsAndConstraints;
class ReadBuffer;
struct SettingsImpl;
class WriteBuffer;

/// List of available types supported in Settings object (!= MergeTreeSettings, MySQLSettings, etc)
#define COMMON_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, ArrowCompression) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, CapnProtoEnumComparingMode) \
    M(CLASS_NAME, Char) \
    M(CLASS_NAME, DateTimeInputFormat) \
    M(CLASS_NAME, DateTimeOutputFormat) \
    M(CLASS_NAME, DateTimeOverflowBehavior) \
    M(CLASS_NAME, DefaultDatabaseEngine) \
    M(CLASS_NAME, DefaultTableEngine) \
    M(CLASS_NAME, Dialect) \
    M(CLASS_NAME, DistributedDDLOutputMode) \
    M(CLASS_NAME, DistributedProductMode) \
    M(CLASS_NAME, Double) \
    M(CLASS_NAME, EscapingRule) \
    M(CLASS_NAME, Float) \
    M(CLASS_NAME, IdentifierQuotingStyle) \
    M(CLASS_NAME, Int32) \
    M(CLASS_NAME, Int64) \
    M(CLASS_NAME, IntervalOutputFormat) \
    M(CLASS_NAME, JoinAlgorithm) \
    M(CLASS_NAME, JoinStrictness) \
    M(CLASS_NAME, LoadBalancing) \
    M(CLASS_NAME, LocalFSReadMethod) \
    M(CLASS_NAME, LogQueriesType) \
    M(CLASS_NAME, LogsLevel) \
    M(CLASS_NAME, Map) \
    M(CLASS_NAME, MaxThreads) \
    M(CLASS_NAME, Milliseconds) \
    M(CLASS_NAME, MsgPackUUIDRepresentation) \
    M(CLASS_NAME, MySQLDataTypesSupport) \
    M(CLASS_NAME, NonZeroUInt64) \
    M(CLASS_NAME, ORCCompression) \
    M(CLASS_NAME, OverflowMode) \
    M(CLASS_NAME, OverflowModeGroupBy) \
    M(CLASS_NAME, ParallelReplicasCustomKeyFilterType) \
    M(CLASS_NAME, ParquetCompression) \
    M(CLASS_NAME, ParquetVersion) \
    M(CLASS_NAME, QueryCacheNondeterministicFunctionHandling) \
    M(CLASS_NAME, QueryCacheSystemTableHandling) \
    M(CLASS_NAME, SchemaInferenceMode) \
    M(CLASS_NAME, Seconds) \
    M(CLASS_NAME, SetOperationMode) \
    M(CLASS_NAME, ShortCircuitFunctionEvaluation) \
    M(CLASS_NAME, SQLSecurityType) \
    M(CLASS_NAME, StreamingHandleErrorMode) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, Timezone) \
    M(CLASS_NAME, TotalsMode) \
    M(CLASS_NAME, TransactionsWaitCSNMode) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, UInt64Auto) \
    M(CLASS_NAME, URI)


COMMON_SETTINGS_SUPPORTED_TYPES(Settings, DECLARE_SETTING_TRAIT)
struct Settings
{
    Settings();
    Settings(const Settings & settings);
    Settings(Settings && settings) noexcept;
    ~Settings();

    Settings & operator=(const Settings & other);
    bool operator==(const Settings & other) const;

    COMMON_SETTINGS_SUPPORTED_TYPES(Settings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    /// General API as needed
    bool has(std::string_view name) const;
    bool isChanged(std::string_view name) const;

    bool tryGet(std::string_view name, Field & value) const;
    Field get(std::string_view name) const;

    void set(std::string_view name, const Field & value);
    void setDefaultValue(std::string_view name);

    std::vector<String> getHints(const String & name) const;
    String toString() const;

    SettingsChanges changes() const;
    void applyChanges(const SettingsChanges & changes);
    std::vector<std::string_view> getAllRegisteredNames() const;
    std::vector<std::string_view> getChangedAndObsoleteNames() const;
    std::vector<std::string_view> getUnchangedNames() const;

    void dumpToSystemSettingsColumns(MutableColumnsAndConstraints & params) const;
    void dumpToMapColumn(IColumn * column, bool changed_only = true) const;
    NameToNameMap toNameToNameMap() const;

    void write(WriteBuffer & out, SettingsWriteFormat format = SettingsWriteFormat::DEFAULT) const;
    void read(ReadBuffer & in, SettingsWriteFormat format = SettingsWriteFormat::DEFAULT);

    void addToProgramOptions(boost::program_options::options_description & options);
    void addToProgramOptions(std::string_view setting_name, boost::program_options::options_description & options);
    void addToProgramOptionsAsMultitokens(boost::program_options::options_description & options) const;
    void addToClientOptions(
        Poco::Util::LayeredConfiguration & config, const boost::program_options::variables_map & options, bool repeated_settings) const;

    static Field castValueUtil(std::string_view name, const Field & value);
    static String valueToStringUtil(std::string_view name, const Field & value);
    static Field stringToValueUtil(std::string_view name, const String & str);
    static bool hasBuiltin(std::string_view name);
    static std::string_view resolveName(std::string_view name);
    static void checkNoSettingNamesAtTopLevel(const Poco::Util::AbstractConfiguration & config, const String & config_path);

private:
    std::unique_ptr<SettingsImpl> impl;
};
}
