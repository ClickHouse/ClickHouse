#pragma once

#include <Core/SettingsFields.h>
#include <Core/Joins.h>
#include <QueryPipeline/SizeLimits.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadSettings.h>
#include <Common/ShellCommandSettings.h>


namespace DB
{

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

enum class LogsLevel
{
    none = 0,    /// Disable
    fatal,
    error,
    warning,
    information,
    debug,
    trace,
    test,
};

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

}
