#pragma once

#include <Core/Joins.h>
#include <Core/LogsLevel.h>
#include <Core/SettingsFields.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadSettings.h>
#include <Parsers/ASTSQLSecurity.h>
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

enum class LoadBalancing
{
    /// among replicas with a minimum number of errors selected randomly
    RANDOM = 0,
    /// a replica is selected among the replicas with the minimum number of errors
    /// with the minimum number of distinguished characters in the replica name prefix and local hostname prefix
    NEAREST_HOSTNAME,
    /// just like NEAREST_HOSTNAME, but it count distinguished characters in a levenshtein distance manner
    HOSTNAME_LEVENSHTEIN_DISTANCE,
    // replicas with the same number of errors are accessed in the same order
    // as they are specified in the configuration.
    IN_ORDER,
    /// if first replica one has higher number of errors,
    ///   pick a random one from replicas with minimum number of errors
    FIRST_OR_RANDOM,
    // round robin across replicas with the same number of errors.
    ROUND_ROBIN,
};

DECLARE_SETTING_ENUM(LoadBalancing)

DECLARE_SETTING_ENUM(JoinStrictness)

DECLARE_SETTING_MULTI_ENUM(JoinAlgorithm)


/// Which rows should be included in TOTALS.
enum class TotalsMode
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
enum class DistributedProductMode
{
    DENY = 0,    /// Disable
    LOCAL,       /// Convert to local query
    GLOBAL,      /// Convert to global query
    ALLOW        /// Enable
};

DECLARE_SETTING_ENUM(DistributedProductMode)

/// How the query cache handles queries with non-deterministic functions, e.g. now()
enum class QueryCacheNondeterministicFunctionHandling
{
    Throw,
    Save,
    Ignore
};

DECLARE_SETTING_ENUM(QueryCacheNondeterministicFunctionHandling)


DECLARE_SETTING_ENUM_WITH_RENAME(DateTimeInputFormat, FormatSettings::DateTimeInputFormat)

DECLARE_SETTING_ENUM_WITH_RENAME(DateTimeOutputFormat, FormatSettings::DateTimeOutputFormat)

DECLARE_SETTING_ENUM_WITH_RENAME(IntervalOutputFormat, FormatSettings::IntervalOutputFormat)

DECLARE_SETTING_ENUM_WITH_RENAME(ParquetVersion, FormatSettings::ParquetVersion)

DECLARE_SETTING_ENUM(LogsLevel)


// Make it signed for compatibility with DataTypeEnum8
enum QueryLogElementType : int8_t
{
    QUERY_START = 1,
    QUERY_FINISH = 2,
    EXCEPTION_BEFORE_START = 3,
    EXCEPTION_WHILE_PROCESSING = 4,
};

DECLARE_SETTING_ENUM_WITH_RENAME(LogQueriesType, QueryLogElementType)


enum class DefaultDatabaseEngine
{
    Ordinary,
    Atomic,
};

DECLARE_SETTING_ENUM(DefaultDatabaseEngine)

enum class DefaultTableEngine
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


enum class CleanDeletedRows
{
    Never = 0, /// Disable.
    Always,
};

DECLARE_SETTING_ENUM(CleanDeletedRows)

enum class MySQLDataTypesSupport
{
    DECIMAL, // convert MySQL's decimal and number to ClickHouse Decimal when applicable
    DATETIME64, // convert MySQL's DATETIME and TIMESTAMP and ClickHouse DateTime64 if precision is > 0 or range is greater that for DateTime.
    DATE2DATE32, // convert MySQL's date type to ClickHouse Date32
    DATE2STRING  // convert MySQL's date type to ClickHouse String(This is usually used when your mysql date is less than 1925)
};

DECLARE_SETTING_MULTI_ENUM(MySQLDataTypesSupport)

enum class SetOperationMode
{
    Unspecified = 0, // Query UNION / EXCEPT / INTERSECT without SetOperationMode will throw exception
    ALL, // Query UNION / EXCEPT / INTERSECT without SetOperationMode -> SELECT ... UNION / EXCEPT / INTERSECT ALL SELECT ...
    DISTINCT // Query UNION / EXCEPT / INTERSECT without SetOperationMode -> SELECT ... UNION / EXCEPT / INTERSECT DISTINCT SELECT ...
};

DECLARE_SETTING_ENUM(SetOperationMode)

enum class DistributedDDLOutputMode
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

enum class StreamingHandleErrorMode
{
    DEFAULT = 0, // Ignore errors with threshold.
    STREAM, // Put errors to stream in the virtual column named ``_error.
    /*FIXED_SYSTEM_TABLE, Put errors to in a fixed system table likely system.kafka_errors. This is not implemented now.  */
    /*CUSTOM_SYSTEM_TABLE, Put errors to in a custom system table. This is not implemented now.  */
};

DECLARE_SETTING_ENUM(StreamingHandleErrorMode)

enum class ShortCircuitFunctionEvaluation
{
    ENABLE, // Use short-circuit function evaluation for functions that are suitable for it.
    FORCE_ENABLE, // Use short-circuit function evaluation for all functions.
    DISABLE, // Disable short-circuit function evaluation.
};

DECLARE_SETTING_ENUM(ShortCircuitFunctionEvaluation)

enum class TransactionsWaitCSNMode
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

enum class Dialect
{
    clickhouse,
    kusto,
    prql,
};

DECLARE_SETTING_ENUM(Dialect)

enum class ParallelReplicasCustomKeyFilterType : uint8_t
{
    DEFAULT,
    RANGE,
};

DECLARE_SETTING_ENUM(ParallelReplicasCustomKeyFilterType)

DECLARE_SETTING_ENUM(LocalFSReadMethod)

enum class S3QueueMode
{
    ORDERED,
    UNORDERED,
};

DECLARE_SETTING_ENUM(S3QueueMode)

enum class S3QueueAction
{
    KEEP,
    DELETE,
};

DECLARE_SETTING_ENUM(S3QueueAction)

DECLARE_SETTING_ENUM(ExternalCommandStderrReaction)

enum class SchemaInferenceMode
{
    DEFAULT,
    UNION,
};

DECLARE_SETTING_ENUM(SchemaInferenceMode)

DECLARE_SETTING_ENUM_WITH_RENAME(DateTimeOverflowBehavior, FormatSettings::DateTimeOverflowBehavior)

DECLARE_SETTING_ENUM(SQLSecurityType)
}
