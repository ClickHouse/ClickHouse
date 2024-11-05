#pragma once

#include <Access/Common/SQLSecurityDefs.h>
#include <Core/Joins.h>
#include <Core/LoadBalancing.h>
#include <Core/LogsLevel.h>
#include <Core/MergeSelectorAlgorithm.h>
#include <Core/ParallelReplicasMode.h>
#include <Core/QueryLogElementType.h>
#include <Core/SchemaInferenceMode.h>
#include <Core/SettingsFields.h>
#include <Core/ShortCircuitFunctionEvaluation.h>
#include <Core/StreamingHandleErrorMode.h>
#include <Formats/FormatSettings.h>
#include <IO/DistributedCacheLogMode.h>
#include <IO/DistributedCachePoolBehaviourOnLimit.h>
#include <IO/ReadMethod.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/ShellCommandSettings.h>


namespace DB
{

template <typename Type>
constexpr auto getEnumValues();

/// NOLINTNEXTLINE
#define DECLARE_SETTING_ENUM(ENUM_TYPE) \
    DECLARE_SETTING_ENUM_WITH_RENAME(ENUM_TYPE, ENUM_TYPE)

/// NOLINTNEXTLINE
#define DECLARE_SETTING_ENUM_WITH_RENAME(NEW_NAME, ENUM_TYPE) \
    struct SettingField##NEW_NAME##Traits \
    { \
        using EnumType = ENUM_TYPE; \
        using EnumValuePairs = std::pair<const char *, EnumType>[]; \
        static const String & toString(EnumType value); \
        static EnumType fromString(std::string_view str); \
    }; \
    \
    using SettingField##NEW_NAME = SettingFieldEnum<ENUM_TYPE, SettingField##NEW_NAME##Traits>;

/// NOLINTNEXTLINE
#define IMPLEMENT_SETTING_ENUM(NEW_NAME, ERROR_CODE_FOR_UNEXPECTED_NAME, ...) \
    IMPLEMENT_SETTING_ENUM_IMPL(NEW_NAME, ERROR_CODE_FOR_UNEXPECTED_NAME, EnumValuePairs, __VA_ARGS__)

/// NOLINTNEXTLINE
#define IMPLEMENT_SETTING_AUTO_ENUM(NEW_NAME, ERROR_CODE_FOR_UNEXPECTED_NAME) \
    IMPLEMENT_SETTING_ENUM_IMPL(NEW_NAME, ERROR_CODE_FOR_UNEXPECTED_NAME, , getEnumValues<EnumType>())

/// NOLINTNEXTLINE
#define IMPLEMENT_SETTING_ENUM_IMPL(NEW_NAME, ERROR_CODE_FOR_UNEXPECTED_NAME, PAIRS_TYPE, ...) \
    const String & SettingField##NEW_NAME##Traits::toString(typename SettingField##NEW_NAME::EnumType value) \
    { \
        static const std::unordered_map<EnumType, String> map = [] { \
            std::unordered_map<EnumType, String> res; \
            for (const auto & [name, val] : PAIRS_TYPE __VA_ARGS__) \
                res.emplace(val, name); \
            return res; \
        }(); \
        auto it = map.find(value); \
        if (it != map.end()) \
            return it->second; \
        throw Exception(ERROR_CODE_FOR_UNEXPECTED_NAME, \
            "Unexpected value of " #NEW_NAME ":{}", std::to_string(std::underlying_type_t<EnumType>(value))); \
    } \
    \
    typename SettingField##NEW_NAME::EnumType SettingField##NEW_NAME##Traits::fromString(std::string_view str) \
    { \
        static const std::unordered_map<std::string_view, EnumType> map = [] { \
            std::unordered_map<std::string_view, EnumType> res; \
            for (const auto & [name, val] : PAIRS_TYPE __VA_ARGS__) \
                res.emplace(name, val); \
            return res; \
        }(); \
        auto it = map.find(str); \
        if (it != map.end()) \
            return it->second; \
        String msg; \
        bool need_comma = false; \
        for (auto & name : map | boost::adaptors::map_keys) \
        { \
            if (std::exchange(need_comma, true)) \
                msg += ", "; \
            msg += "'" + String{name} + "'"; \
        } \
        throw Exception(ERROR_CODE_FOR_UNEXPECTED_NAME, "Unexpected value of " #NEW_NAME ": '{}'. Must be one of [{}]", String{str}, msg); \
    }

/// NOLINTNEXTLINE
#define DECLARE_SETTING_MULTI_ENUM(ENUM_TYPE) \
    DECLARE_SETTING_MULTI_ENUM_WITH_RENAME(ENUM_TYPE, ENUM_TYPE)

/// NOLINTNEXTLINE
#define DECLARE_SETTING_MULTI_ENUM_WITH_RENAME(ENUM_TYPE, NEW_NAME) \
    struct SettingField##NEW_NAME##Traits \
    { \
        using EnumType = ENUM_TYPE; \
        using EnumValuePairs = std::pair<const char *, EnumType>[]; \
        static size_t getEnumSize(); \
        static const String & toString(EnumType value); \
        static EnumType fromString(std::string_view str); \
    }; \
    \
    using SettingField##NEW_NAME = SettingFieldMultiEnum<ENUM_TYPE, SettingField##NEW_NAME##Traits>; \
    using NEW_NAME##List = typename SettingField##NEW_NAME::ValueType;

/// NOLINTNEXTLINE
#define IMPLEMENT_SETTING_MULTI_ENUM(ENUM_TYPE, ERROR_CODE_FOR_UNEXPECTED_NAME, ...) \
    IMPLEMENT_SETTING_MULTI_ENUM_WITH_RENAME(ENUM_TYPE, ERROR_CODE_FOR_UNEXPECTED_NAME, __VA_ARGS__)

/// NOLINTNEXTLINE
#define IMPLEMENT_SETTING_MULTI_ENUM_WITH_RENAME(NEW_NAME, ERROR_CODE_FOR_UNEXPECTED_NAME, ...) \
    IMPLEMENT_SETTING_ENUM(NEW_NAME, ERROR_CODE_FOR_UNEXPECTED_NAME, __VA_ARGS__)\
    size_t SettingField##NEW_NAME##Traits::getEnumSize() {\
        return std::initializer_list<std::pair<const char*, NEW_NAME>> __VA_ARGS__ .size();\
    }

/// NOLINTNEXTLINE
#define IMPLEMENT_SETTING_MULTI_AUTO_ENUM(NEW_NAME, ERROR_CODE_FOR_UNEXPECTED_NAME) \
    IMPLEMENT_SETTING_AUTO_ENUM(NEW_NAME, ERROR_CODE_FOR_UNEXPECTED_NAME)\
    size_t SettingField##NEW_NAME##Traits::getEnumSize() {\
        return getEnumValues<EnumType>().size();\
    }

DECLARE_SETTING_ENUM(LoadBalancing)

DECLARE_SETTING_ENUM(JoinStrictness)
DECLARE_SETTING_MULTI_ENUM(JoinAlgorithm)
DECLARE_SETTING_ENUM(JoinInnerTableSelectionMode)


/// Which rows should be included in TOTALS.
enum class TotalsMode : uint8_t
{
    BEFORE_HAVING            = 0, /// Count HAVING for all read rows;
                                  ///  including those not in max_rows_to_group_by
                                  ///  and have not passed HAVING after grouping.
    AFTER_HAVING_INCLUSIVE    = 1, /// Count on all rows except those that have not passed HAVING;
                                   ///  that is, to include in TOTALS all the rows that did not pass max_rows_to_group_by.
    AFTER_HAVING_EXCLUSIVE    = 2, /// Include only the rows that passed and max_rows_to_group_by, and HAVING.
    AFTER_HAVING_AUTO         = 3, /// Automatically select between INCLUSIVE and EXCLUSIVE,
};

DECLARE_SETTING_ENUM(TotalsMode)


/// The settings keeps OverflowMode which cannot be OverflowMode::ANY.
DECLARE_SETTING_ENUM(OverflowMode)

/// The settings keeps OverflowMode which can be OverflowMode::ANY.
DECLARE_SETTING_ENUM_WITH_RENAME(OverflowModeGroupBy, OverflowMode)


/// The setting for executing distributed subqueries inside IN or JOIN sections.
enum class DistributedProductMode : uint8_t
{
    DENY = 0,    /// Disable
    LOCAL,       /// Convert to local query
    GLOBAL,      /// Convert to global query
    ALLOW        /// Enable
};

DECLARE_SETTING_ENUM(DistributedProductMode)

/// How the query cache handles queries with non-deterministic functions, e.g. now()
enum class QueryCacheNondeterministicFunctionHandling : uint8_t
{
    Throw,
    Save,
    Ignore
};

DECLARE_SETTING_ENUM(QueryCacheNondeterministicFunctionHandling)

/// How the query cache handles queries against system tables, tables in databases 'system.*' and 'information_schema.*'
enum class QueryCacheSystemTableHandling : uint8_t
{
    Throw,
    Save,
    Ignore
};

DECLARE_SETTING_ENUM(QueryCacheSystemTableHandling)

DECLARE_SETTING_ENUM_WITH_RENAME(DateTimeInputFormat, FormatSettings::DateTimeInputFormat)

DECLARE_SETTING_ENUM_WITH_RENAME(DateTimeOutputFormat, FormatSettings::DateTimeOutputFormat)

DECLARE_SETTING_ENUM_WITH_RENAME(IntervalOutputFormat, FormatSettings::IntervalOutputFormat)

DECLARE_SETTING_ENUM_WITH_RENAME(ParquetVersion, FormatSettings::ParquetVersion)

DECLARE_SETTING_ENUM(LogsLevel)

DECLARE_SETTING_ENUM_WITH_RENAME(LogQueriesType, QueryLogElementType)


enum class DefaultDatabaseEngine : uint8_t
{
    Ordinary,
    Atomic,
};

DECLARE_SETTING_ENUM(DefaultDatabaseEngine)

enum class DefaultTableEngine : uint8_t
{
    None = 0, /// Disable. Need to use ENGINE =
    Log,
    StripeLog,
    MergeTree,
    ReplacingMergeTree,
    ReplicatedMergeTree,
    ReplicatedReplacingMergeTree,
    SharedMergeTree,
    SharedReplacingMergeTree,
    Memory,
};

DECLARE_SETTING_ENUM(DefaultTableEngine)

DECLARE_SETTING_ENUM(DistributedCacheLogMode)

DECLARE_SETTING_ENUM(DistributedCachePoolBehaviourOnLimit)

enum class CleanDeletedRows : uint8_t
{
    Never = 0, /// Disable.
    Always,
};

DECLARE_SETTING_ENUM(CleanDeletedRows)

enum class MySQLDataTypesSupport : uint8_t
{
    DECIMAL, // convert MySQL's decimal and number to ClickHouse Decimal when applicable
    DATETIME64, // convert MySQL's DATETIME and TIMESTAMP and ClickHouse DateTime64 if precision is > 0 or range is greater that for DateTime.
    DATE2DATE32, // convert MySQL's date type to ClickHouse Date32
    DATE2STRING  // convert MySQL's date type to ClickHouse String(This is usually used when your mysql date is less than 1925)
};

DECLARE_SETTING_MULTI_ENUM(MySQLDataTypesSupport)

enum class SetOperationMode : uint8_t
{
    Unspecified = 0, // Query UNION / EXCEPT / INTERSECT without SetOperationMode will throw exception
    ALL, // Query UNION / EXCEPT / INTERSECT without SetOperationMode -> SELECT ... UNION / EXCEPT / INTERSECT ALL SELECT ...
    DISTINCT // Query UNION / EXCEPT / INTERSECT without SetOperationMode -> SELECT ... UNION / EXCEPT / INTERSECT DISTINCT SELECT ...
};

DECLARE_SETTING_ENUM(SetOperationMode)

enum class DistributedDDLOutputMode : uint8_t
{
    NONE,
    THROW,
    NULL_STATUS_ON_TIMEOUT,
    NEVER_THROW,
    THROW_ONLY_ACTIVE,
    NULL_STATUS_ON_TIMEOUT_ONLY_ACTIVE,
    NONE_ONLY_ACTIVE,
};

DECLARE_SETTING_ENUM(DistributedDDLOutputMode)

DECLARE_SETTING_ENUM(StreamingHandleErrorMode)

DECLARE_SETTING_ENUM(ShortCircuitFunctionEvaluation)

enum class TransactionsWaitCSNMode : uint8_t
{
    ASYNC,
    WAIT,
    WAIT_UNKNOWN,
};

DECLARE_SETTING_ENUM(TransactionsWaitCSNMode)

DECLARE_SETTING_ENUM_WITH_RENAME(CapnProtoEnumComparingMode, FormatSettings::CapnProtoEnumComparingMode)

DECLARE_SETTING_ENUM_WITH_RENAME(EscapingRule, FormatSettings::EscapingRule)

DECLARE_SETTING_ENUM_WITH_RENAME(MsgPackUUIDRepresentation, FormatSettings::MsgPackUUIDRepresentation)

DECLARE_SETTING_ENUM_WITH_RENAME(ParquetCompression, FormatSettings::ParquetCompression)

DECLARE_SETTING_ENUM_WITH_RENAME(ArrowCompression, FormatSettings::ArrowCompression)

DECLARE_SETTING_ENUM_WITH_RENAME(ORCCompression, FormatSettings::ORCCompression)

enum class Dialect : uint8_t
{
    clickhouse,
    kusto,
    prql,
};

DECLARE_SETTING_ENUM(Dialect)

DECLARE_SETTING_ENUM(ParallelReplicasCustomKeyFilterType)

enum class LightweightMutationProjectionMode : uint8_t
{
    THROW,
    DROP,
    REBUILD,
};

DECLARE_SETTING_ENUM(LightweightMutationProjectionMode)

enum class DeduplicateMergeProjectionMode : uint8_t
{
    IGNORE,
    THROW,
    DROP,
    REBUILD,
};

DECLARE_SETTING_ENUM(DeduplicateMergeProjectionMode)

DECLARE_SETTING_ENUM(ParallelReplicasMode)

DECLARE_SETTING_ENUM(LocalFSReadMethod)

enum class ObjectStorageQueueMode : uint8_t
{
    ORDERED,
    UNORDERED,
};

DECLARE_SETTING_ENUM(ObjectStorageQueueMode)

enum class ObjectStorageQueueAction : uint8_t
{
    KEEP,
    DELETE,
};

DECLARE_SETTING_ENUM(ObjectStorageQueueAction)

DECLARE_SETTING_ENUM(ExternalCommandStderrReaction)

DECLARE_SETTING_ENUM(SchemaInferenceMode)

DECLARE_SETTING_ENUM_WITH_RENAME(DateTimeOverflowBehavior, FormatSettings::DateTimeOverflowBehavior)

DECLARE_SETTING_ENUM(SQLSecurityType)

DECLARE_SETTING_ENUM(IdentifierQuotingRule)
DECLARE_SETTING_ENUM(IdentifierQuotingStyle)

enum class GroupArrayActionWhenLimitReached : uint8_t
{
    THROW,
    DISCARD
};
DECLARE_SETTING_ENUM(GroupArrayActionWhenLimitReached)

DECLARE_SETTING_ENUM(MergeSelectorAlgorithm)

}
