#include <Core/SettingsEnums.h>
#include <magic_enum.hpp>
#include <Access/Common/SQLSecurityDefs.h>
#include <boost/range/adaptor/map.hpp>


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

template <typename Type>
constexpr auto getEnumValues()
{
    std::array<std::pair<std::string_view, Type>, magic_enum::enum_count<Type>()> enum_values{};
    size_t index = 0;
    for (auto value : magic_enum::enum_values<Type>())
        enum_values[index++] = std::pair{magic_enum::enum_name(value), value};
    return enum_values;
}

IMPLEMENT_SETTING_ENUM(LoadBalancing, ErrorCodes::UNKNOWN_LOAD_BALANCING,
    {{"random",           LoadBalancing::RANDOM},
     {"nearest_hostname", LoadBalancing::NEAREST_HOSTNAME},
     {"hostname_levenshtein_distance", LoadBalancing::HOSTNAME_LEVENSHTEIN_DISTANCE},
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

IMPLEMENT_SETTING_ENUM(JoinInnerTableSelectionMode, ErrorCodes::BAD_ARGUMENTS,
    {{"left",       JoinInnerTableSelectionMode::Left},
     {"right",      JoinInnerTableSelectionMode::Right},
     {"auto",       JoinInnerTableSelectionMode::Auto}})

IMPLEMENT_SETTING_ENUM(TotalsMode, ErrorCodes::UNKNOWN_TOTALS_MODE,
    {{"before_having",          TotalsMode::BEFORE_HAVING},
     {"after_having_exclusive", TotalsMode::AFTER_HAVING_EXCLUSIVE},
     {"after_having_inclusive", TotalsMode::AFTER_HAVING_INCLUSIVE},
     {"after_having_auto",      TotalsMode::AFTER_HAVING_AUTO}})


IMPLEMENT_SETTING_ENUM(OverflowMode, ErrorCodes::UNKNOWN_OVERFLOW_MODE,
    {{"throw", OverflowMode::THROW},
     {"break", OverflowMode::BREAK}})

IMPLEMENT_SETTING_ENUM(DistributedCacheLogMode, ErrorCodes::BAD_ARGUMENTS,
    {{"nothing", DistributedCacheLogMode::LOG_NOTHING},
     {"on_error", DistributedCacheLogMode::LOG_ON_ERROR},
     {"all", DistributedCacheLogMode::LOG_ALL}})

IMPLEMENT_SETTING_ENUM(DistributedCachePoolBehaviourOnLimit, ErrorCodes::BAD_ARGUMENTS,
    {{"wait", DistributedCachePoolBehaviourOnLimit::WAIT},
     {"allocate_bypassing_pool", DistributedCachePoolBehaviourOnLimit::ALLOCATE_NEW_BYPASSING_POOL}});

IMPLEMENT_SETTING_ENUM(OverflowModeGroupBy, ErrorCodes::UNKNOWN_OVERFLOW_MODE,
    {{"throw", OverflowMode::THROW},
     {"break", OverflowMode::BREAK},
     {"any", OverflowMode::ANY}})


IMPLEMENT_SETTING_ENUM(DistributedProductMode, ErrorCodes::UNKNOWN_DISTRIBUTED_PRODUCT_MODE,
    {{"deny",   DistributedProductMode::DENY},
     {"local",  DistributedProductMode::LOCAL},
     {"global", DistributedProductMode::GLOBAL},
     {"allow",  DistributedProductMode::ALLOW}})


IMPLEMENT_SETTING_ENUM(QueryCacheNondeterministicFunctionHandling, ErrorCodes::BAD_ARGUMENTS,
    {{"throw",  QueryCacheNondeterministicFunctionHandling::Throw},
     {"save",   QueryCacheNondeterministicFunctionHandling::Save},
     {"ignore", QueryCacheNondeterministicFunctionHandling::Ignore}})

IMPLEMENT_SETTING_ENUM(QueryCacheSystemTableHandling, ErrorCodes::BAD_ARGUMENTS,
    {{"throw",  QueryCacheSystemTableHandling::Throw},
     {"save",   QueryCacheSystemTableHandling::Save},
     {"ignore", QueryCacheSystemTableHandling::Ignore}})

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
     {"throw_only_active", DistributedDDLOutputMode::THROW_ONLY_ACTIVE},
     {"null_status_on_timeout_only_active", DistributedDDLOutputMode::NULL_STATUS_ON_TIMEOUT_ONLY_ACTIVE},
     {"none_only_active", DistributedDDLOutputMode::NONE_ONLY_ACTIVE},
     {"never_throw", DistributedDDLOutputMode::NEVER_THROW}})

IMPLEMENT_SETTING_ENUM(StreamingHandleErrorMode, ErrorCodes::BAD_ARGUMENTS,
    {{"default",      StreamingHandleErrorMode::DEFAULT},
     {"stream",       StreamingHandleErrorMode::STREAM}})

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

IMPLEMENT_SETTING_ENUM(ParallelReplicasCustomKeyFilterType, ErrorCodes::BAD_ARGUMENTS,
    {{"default", ParallelReplicasCustomKeyFilterType::DEFAULT},
     {"range", ParallelReplicasCustomKeyFilterType::RANGE}})

IMPLEMENT_SETTING_ENUM(LightweightMutationProjectionMode, ErrorCodes::BAD_ARGUMENTS,
    {{"throw", LightweightMutationProjectionMode::THROW},
     {"drop", LightweightMutationProjectionMode::DROP},
     {"rebuild", LightweightMutationProjectionMode::REBUILD}})

IMPLEMENT_SETTING_ENUM(DeduplicateMergeProjectionMode, ErrorCodes::BAD_ARGUMENTS,
    {{"ignore", DeduplicateMergeProjectionMode::IGNORE},
     {"throw", DeduplicateMergeProjectionMode::THROW},
     {"drop", DeduplicateMergeProjectionMode::DROP},
     {"rebuild", DeduplicateMergeProjectionMode::REBUILD}})

IMPLEMENT_SETTING_ENUM(ParallelReplicasMode, ErrorCodes::BAD_ARGUMENTS,
    {{"auto", ParallelReplicasMode::AUTO},
     {"read_tasks", ParallelReplicasMode::READ_TASKS},
     {"custom_key_sampling", ParallelReplicasMode::CUSTOM_KEY_SAMPLING},
     {"custom_key_range", ParallelReplicasMode::CUSTOM_KEY_RANGE},
     {"sampling_key", ParallelReplicasMode::SAMPLING_KEY}})

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

IMPLEMENT_SETTING_ENUM(ObjectStorageQueueMode, ErrorCodes::BAD_ARGUMENTS,
                       {{"ordered", ObjectStorageQueueMode::ORDERED},
                        {"unordered", ObjectStorageQueueMode::UNORDERED}})

IMPLEMENT_SETTING_ENUM(ObjectStorageQueueAction, ErrorCodes::BAD_ARGUMENTS,
                       {{"keep", ObjectStorageQueueAction::KEEP},
                        {"delete", ObjectStorageQueueAction::DELETE}})

IMPLEMENT_SETTING_ENUM(ExternalCommandStderrReaction, ErrorCodes::BAD_ARGUMENTS,
    {{"none", ExternalCommandStderrReaction::NONE},
     {"log", ExternalCommandStderrReaction::LOG},
     {"log_first", ExternalCommandStderrReaction::LOG_FIRST},
     {"log_last", ExternalCommandStderrReaction::LOG_LAST},
     {"throw", ExternalCommandStderrReaction::THROW}})

IMPLEMENT_SETTING_ENUM(SchemaInferenceMode, ErrorCodes::BAD_ARGUMENTS,
    {{"default", SchemaInferenceMode::DEFAULT},
     {"union", SchemaInferenceMode::UNION}})

IMPLEMENT_SETTING_ENUM(DateTimeOverflowBehavior, ErrorCodes::BAD_ARGUMENTS,
    {{"throw", FormatSettings::DateTimeOverflowBehavior::Throw},
     {"ignore", FormatSettings::DateTimeOverflowBehavior::Ignore},
     {"saturate", FormatSettings::DateTimeOverflowBehavior::Saturate}})

IMPLEMENT_SETTING_ENUM(SQLSecurityType, ErrorCodes::BAD_ARGUMENTS,
    {{"DEFINER", SQLSecurityType::DEFINER},
     {"INVOKER", SQLSecurityType::INVOKER},
     {"NONE", SQLSecurityType::NONE}})

IMPLEMENT_SETTING_ENUM(
    GroupArrayActionWhenLimitReached,
    ErrorCodes::BAD_ARGUMENTS,
    {{"throw", GroupArrayActionWhenLimitReached::THROW}, {"discard", GroupArrayActionWhenLimitReached::DISCARD}})

IMPLEMENT_SETTING_ENUM(
    IdentifierQuotingStyle,
    ErrorCodes::BAD_ARGUMENTS,
    {{"Backticks", IdentifierQuotingStyle::Backticks},
     {"DoubleQuotes", IdentifierQuotingStyle::DoubleQuotes},
     {"BackticksMySQL", IdentifierQuotingStyle::BackticksMySQL}})

IMPLEMENT_SETTING_ENUM(
    IdentifierQuotingRule,
    ErrorCodes::BAD_ARGUMENTS,
    {{"user_display", IdentifierQuotingRule::UserDisplay},
     {"when_necessary", IdentifierQuotingRule::WhenNecessary},
     {"always", IdentifierQuotingRule::Always}})

IMPLEMENT_SETTING_ENUM(
    MergeSelectorAlgorithm,
    ErrorCodes::BAD_ARGUMENTS,
    {{"Simple", MergeSelectorAlgorithm::SIMPLE},
     {"StochasticSimple", MergeSelectorAlgorithm::STOCHASTIC_SIMPLE},
     {"Trivial", MergeSelectorAlgorithm::TRIVIAL}})

}
