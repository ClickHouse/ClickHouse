#include <Core/SettingsEnums.h>
#include <magic_enum.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_LOAD_BALANCING;
    extern const int UNKNOWN_OVERFLOW_MODE;
    extern const int UNKNOWN_TOTALS_MODE;
    extern const int UNKNOWN_DISTRIBUTED_PRODUCT_MODE;
    extern const int UNKNOWN_JOIN;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_MYSQL_DATATYPES_SUPPORT_LEVEL;
    extern const int UNKNOWN_UNION;
}

IMPLEMENT_SETTING_ENUM(LoadBalancing, ErrorCodes::UNKNOWN_LOAD_BALANCING,
    {{"random",           LoadBalancing::RANDOM},
     {"nearest_hostname", LoadBalancing::NEAREST_HOSTNAME},
     {"in_order",         LoadBalancing::IN_ORDER},
     {"first_or_random",  LoadBalancing::FIRST_OR_RANDOM},
     {"round_robin",      LoadBalancing::ROUND_ROBIN}})


IMPLEMENT_SETTING_ENUM(JoinStrictness, ErrorCodes::UNKNOWN_JOIN,
    {{"",    JoinStrictness::Unspecified},
     {"ALL", JoinStrictness::All},
     {"ANY", JoinStrictness::Any}})


IMPLEMENT_SETTING_MULTI_ENUM(JoinAlgorithm, ErrorCodes::UNKNOWN_JOIN,
    {{"default",              JoinAlgorithm::DEFAULT},
     {"auto",                 JoinAlgorithm::AUTO},
     {"hash",                 JoinAlgorithm::HASH},
     {"partial_merge",        JoinAlgorithm::PARTIAL_MERGE},
     {"prefer_partial_merge", JoinAlgorithm::PREFER_PARTIAL_MERGE},
     {"parallel_hash",        JoinAlgorithm::PARALLEL_HASH},
     {"direct",               JoinAlgorithm::DIRECT},
     {"full_sorting_merge",   JoinAlgorithm::FULL_SORTING_MERGE},
     {"grace_hash",           JoinAlgorithm::GRACE_HASH}})


IMPLEMENT_SETTING_ENUM(TotalsMode, ErrorCodes::UNKNOWN_TOTALS_MODE,
    {{"before_having",          TotalsMode::BEFORE_HAVING},
     {"after_having_exclusive", TotalsMode::AFTER_HAVING_EXCLUSIVE},
     {"after_having_inclusive", TotalsMode::AFTER_HAVING_INCLUSIVE},
     {"after_having_auto",      TotalsMode::AFTER_HAVING_AUTO}})


IMPLEMENT_SETTING_ENUM(OverflowMode, ErrorCodes::UNKNOWN_OVERFLOW_MODE,
    {{"throw", OverflowMode::THROW},
     {"break", OverflowMode::BREAK}})


IMPLEMENT_SETTING_ENUM(OverflowModeGroupBy, ErrorCodes::UNKNOWN_OVERFLOW_MODE,
    {{"throw", OverflowMode::THROW},
     {"break", OverflowMode::BREAK},
     {"any", OverflowMode::ANY}})


IMPLEMENT_SETTING_ENUM(DistributedProductMode, ErrorCodes::UNKNOWN_DISTRIBUTED_PRODUCT_MODE,
    {{"deny",   DistributedProductMode::DENY},
     {"local",  DistributedProductMode::LOCAL},
     {"global", DistributedProductMode::GLOBAL},
     {"allow",  DistributedProductMode::ALLOW}})


IMPLEMENT_SETTING_ENUM(DateTimeInputFormat, ErrorCodes::BAD_ARGUMENTS,
    {{"basic",       FormatSettings::DateTimeInputFormat::Basic},
     {"best_effort", FormatSettings::DateTimeInputFormat::BestEffort},
     {"best_effort_us", FormatSettings::DateTimeInputFormat::BestEffortUS}})


IMPLEMENT_SETTING_ENUM(DateTimeOutputFormat, ErrorCodes::BAD_ARGUMENTS,
    {{"simple",         FormatSettings::DateTimeOutputFormat::Simple},
     {"iso",            FormatSettings::DateTimeOutputFormat::ISO},
     {"unix_timestamp", FormatSettings::DateTimeOutputFormat::UnixTimestamp}})

IMPLEMENT_SETTING_ENUM(IntervalOutputFormat, ErrorCodes::BAD_ARGUMENTS,
    {{"kusto",     FormatSettings::IntervalOutputFormat::Kusto},
     {"numeric", FormatSettings::IntervalOutputFormat::Numeric}})

IMPLEMENT_SETTING_AUTO_ENUM(LogsLevel, ErrorCodes::BAD_ARGUMENTS)

IMPLEMENT_SETTING_AUTO_ENUM(LogQueriesType, ErrorCodes::BAD_ARGUMENTS)

IMPLEMENT_SETTING_AUTO_ENUM(DefaultDatabaseEngine, ErrorCodes::BAD_ARGUMENTS)

IMPLEMENT_SETTING_AUTO_ENUM(DefaultTableEngine, ErrorCodes::BAD_ARGUMENTS)

IMPLEMENT_SETTING_AUTO_ENUM(CleanDeletedRows, ErrorCodes::BAD_ARGUMENTS)

IMPLEMENT_SETTING_MULTI_ENUM(MySQLDataTypesSupport, ErrorCodes::UNKNOWN_MYSQL_DATATYPES_SUPPORT_LEVEL,
    {{"decimal",    MySQLDataTypesSupport::DECIMAL},
     {"datetime64", MySQLDataTypesSupport::DATETIME64},
     {"date2Date32", MySQLDataTypesSupport::DATE2DATE32},
     {"date2String", MySQLDataTypesSupport::DATE2STRING}})

IMPLEMENT_SETTING_ENUM(SetOperationMode, ErrorCodes::UNKNOWN_UNION,
    {{"",         SetOperationMode::Unspecified},
     {"ALL",      SetOperationMode::ALL},
     {"DISTINCT", SetOperationMode::DISTINCT}})

IMPLEMENT_SETTING_ENUM(DistributedDDLOutputMode, ErrorCodes::BAD_ARGUMENTS,
    {{"none",         DistributedDDLOutputMode::NONE},
     {"throw",    DistributedDDLOutputMode::THROW},
     {"null_status_on_timeout", DistributedDDLOutputMode::NULL_STATUS_ON_TIMEOUT},
     {"never_throw", DistributedDDLOutputMode::NEVER_THROW}})

IMPLEMENT_SETTING_ENUM(HandleKafkaErrorMode, ErrorCodes::BAD_ARGUMENTS,
    {{"default",      HandleKafkaErrorMode::DEFAULT},
     {"stream",       HandleKafkaErrorMode::STREAM}})

IMPLEMENT_SETTING_ENUM(ShortCircuitFunctionEvaluation, ErrorCodes::BAD_ARGUMENTS,
    {{"enable",          ShortCircuitFunctionEvaluation::ENABLE},
     {"force_enable",    ShortCircuitFunctionEvaluation::FORCE_ENABLE},
     {"disable",         ShortCircuitFunctionEvaluation::DISABLE}})

IMPLEMENT_SETTING_ENUM(TransactionsWaitCSNMode, ErrorCodes::BAD_ARGUMENTS,
    {{"async",          TransactionsWaitCSNMode::ASYNC},
     {"wait",           TransactionsWaitCSNMode::WAIT},
     {"wait_unknown",   TransactionsWaitCSNMode::WAIT_UNKNOWN}})

IMPLEMENT_SETTING_ENUM(CapnProtoEnumComparingMode, ErrorCodes::BAD_ARGUMENTS,
    {{"by_names",   FormatSettings::CapnProtoEnumComparingMode::BY_NAMES},
     {"by_values",  FormatSettings::CapnProtoEnumComparingMode::BY_VALUES},
     {"by_names_case_insensitive", FormatSettings::CapnProtoEnumComparingMode::BY_NAMES_CASE_INSENSITIVE}})

IMPLEMENT_SETTING_AUTO_ENUM(EscapingRule, ErrorCodes::BAD_ARGUMENTS)

IMPLEMENT_SETTING_ENUM(MsgPackUUIDRepresentation, ErrorCodes::BAD_ARGUMENTS,
                       {{"bin", FormatSettings::MsgPackUUIDRepresentation::BIN},
                        {"str", FormatSettings::MsgPackUUIDRepresentation::STR},
                        {"ext", FormatSettings::MsgPackUUIDRepresentation::EXT}})

IMPLEMENT_SETTING_ENUM(Dialect, ErrorCodes::BAD_ARGUMENTS,
    {{"clickhouse", Dialect::clickhouse},
     {"kusto", Dialect::kusto},
     {"prql", Dialect::prql}})
    // FIXME: do not add 'kusto_auto' to the list. Maybe remove it from code completely?

IMPLEMENT_SETTING_ENUM(ParallelReplicasCustomKeyFilterType, ErrorCodes::BAD_ARGUMENTS,
    {{"default", ParallelReplicasCustomKeyFilterType::DEFAULT},
     {"range", ParallelReplicasCustomKeyFilterType::RANGE}})

IMPLEMENT_SETTING_AUTO_ENUM(LocalFSReadMethod, ErrorCodes::BAD_ARGUMENTS)

IMPLEMENT_SETTING_ENUM(ParquetVersion, ErrorCodes::BAD_ARGUMENTS,
    {{"1.0",       FormatSettings::ParquetVersion::V1_0},
     {"2.4", FormatSettings::ParquetVersion::V2_4},
     {"2.6", FormatSettings::ParquetVersion::V2_6},
     {"2.latest", FormatSettings::ParquetVersion::V2_LATEST}})

IMPLEMENT_SETTING_ENUM(ParquetCompression, ErrorCodes::BAD_ARGUMENTS,
    {{"none", FormatSettings::ParquetCompression::NONE},
     {"snappy", FormatSettings::ParquetCompression::SNAPPY},
     {"zstd", FormatSettings::ParquetCompression::ZSTD},
     {"gzip", FormatSettings::ParquetCompression::GZIP},
     {"lz4", FormatSettings::ParquetCompression::LZ4},
     {"brotli", FormatSettings::ParquetCompression::BROTLI}})

IMPLEMENT_SETTING_ENUM(ArrowCompression, ErrorCodes::BAD_ARGUMENTS,
    {{"none", FormatSettings::ArrowCompression::NONE},
     {"lz4_frame", FormatSettings::ArrowCompression::LZ4_FRAME},
     {"zstd", FormatSettings::ArrowCompression::ZSTD}})

IMPLEMENT_SETTING_ENUM(ORCCompression, ErrorCodes::BAD_ARGUMENTS,
    {{"none", FormatSettings::ORCCompression::NONE},
     {"snappy", FormatSettings::ORCCompression::SNAPPY},
     {"zstd", FormatSettings::ORCCompression::ZSTD},
     {"zlib", FormatSettings::ORCCompression::ZLIB},
     {"lz4", FormatSettings::ORCCompression::LZ4}})

IMPLEMENT_SETTING_ENUM(S3QueueMode, ErrorCodes::BAD_ARGUMENTS,
                       {{"ordered", S3QueueMode::ORDERED},
                        {"unordered", S3QueueMode::UNORDERED}})

IMPLEMENT_SETTING_ENUM(S3QueueAction, ErrorCodes::BAD_ARGUMENTS,
                       {{"keep", S3QueueAction::KEEP},
                        {"delete", S3QueueAction::DELETE}})

IMPLEMENT_SETTING_ENUM(ExternalCommandStderrReaction, ErrorCodes::BAD_ARGUMENTS,
    {{"none", ExternalCommandStderrReaction::NONE},
     {"log", ExternalCommandStderrReaction::LOG},
     {"log_first", ExternalCommandStderrReaction::LOG_FIRST},
     {"log_last", ExternalCommandStderrReaction::LOG_LAST},
     {"throw", ExternalCommandStderrReaction::THROW}})

}
