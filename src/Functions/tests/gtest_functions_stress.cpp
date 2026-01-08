#include <gtest/gtest.h>

#include <absl/log/globals.h>
#include <boost/program_options.hpp>
#include <fmt/ranges.h>
#include <thread>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Common/logger_useful.h>
#include <Common/SignalHandlers.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Common/thread_local_rng.h>
#include <Common/ThreadStatus.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionGenerateRandomStructure.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>
#include <Storages/StorageGenerateRandom.h>

using namespace DB;
namespace po = boost::program_options;

namespace DB::ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int BAD_ARGUMENTS;
    extern const int BAD_GET;
    extern const int BAD_TYPE_OF_FIELD;
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int CANNOT_CONVERT_TYPE;
    extern const int CANNOT_CREATE_CHARSET_CONVERTER;
    extern const int CANNOT_FORMAT_DATETIME;
    extern const int CANNOT_NORMALIZE_STRING;
    extern const int CANNOT_PARSE_BOOL;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_PARSE_IPV4;
    extern const int CANNOT_PARSE_IPV6;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_PARSE_TEXT;
    extern const int CANNOT_PARSE_UUID;
    extern const int CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CANNOT_READ_ARRAY_FROM_TEXT;
    extern const int DATA_TYPE_CANNOT_BE_PROMOTED;
    extern const int DECIMAL_OVERFLOW;
    extern const int FUNCTION_THROW_IF_VALUE_IS_NON_ZERO;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_DIVISION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_DATA;
    extern const int INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int NO_COMMON_TYPE;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int SYNTAX_ERROR;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int TYPE_MISMATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int UNKNOWN_ELEMENT_OF_ENUM;
    extern const int UNKNOWN_TYPE;
    extern const int UNSUPPORTED_METHOD;
    extern const int ZERO_ARRAY_OR_TUPLE_INDEX;
    extern const int UNICODE_ERROR;
    extern const int CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN;
    extern const int CANNOT_SET_SIGNAL_HANDLER;
}

namespace
{

struct Options
{
    int num_threads = -1;
    int duration_seconds = 60;

    /// avoid_* are flags to ignore some known issues.
    /// Would be good to fix the issues and disable the flags.

    /// Many functions return ColumnNullable where null_map says some value is null, but nested column
    /// has non-default value for that row. Some other functions break when receiving such column
    /// as input (e.g. maybe they look at the nested column, see invalid value, and throw exception
    /// before checking the null map; or maybe a hash function hashes the nested column value even
    /// if null_map says null).
    bool avoid_nondefault_null = true;

    /// Some IFunction implementations expect one IFunction instance to be used for only one overload
    /// and from only one thread. E.g. see `mutable bool to_nullable` in FunctionsConversion.h.
    /// But FunctionToOverloadResolverAdaptor uses one IFunction to resolve any overload.
    /// So currently there's a weird unspoken rule that one instance of IFunctionOverloadResolver
    /// must be used to resolve at most one overload, from one thread. This rule is probably not
    /// consistently followed.
    bool avoid_reusing_overload_resolver = true;

    void parse(int argc, const char * const * argv)
    {
        po::options_description desc("gtest_functions_stress");
        desc.add_options()
            ("threads", po::value<int>(&num_threads)->default_value(num_threads), "how many instances of the test to run in parallel, -1 for num cores")
            ("duration", po::value<int>(&duration_seconds)->default_value(duration_seconds), "run for this many seconds, -1 to run forever")
            ("avoid-nondefault-null", po::value<bool>(&avoid_nondefault_null)->default_value(avoid_nondefault_null), "avoid using Nullable values where the null_map says the value is NULL, but the nested column has nondefault value; if a function returns such value, fix it up before passing it to other functions; this makes the test unrealistic, we should ideally fix all cases where this breaks functions and disable this option")
            ("avoid-reusing-overload-resolver", po::value<bool>(&avoid_reusing_overload_resolver)->default_value(avoid_reusing_overload_resolver), "create a new instance of IFunctionOverloadResolver for every overload resolution");

        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);
    }
};

/// Errors that are allowed for IFunctionOverloadResolver::build and IFunctionBase::prepare.
static const std::unordered_set<int> early_typecheck_errors = {
    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
    ErrorCodes::BAD_ARGUMENTS,
    ErrorCodes::CANNOT_COMPILE_REGEXP,
    ErrorCodes::CANNOT_CONVERT_TYPE,
    ErrorCodes::DATA_TYPE_CANNOT_BE_PROMOTED,
    ErrorCodes::DECIMAL_OVERFLOW,
    ErrorCodes::ILLEGAL_COLUMN,
    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
    ErrorCodes::NO_COMMON_TYPE,
    ErrorCodes::NOT_IMPLEMENTED,
    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
    ErrorCodes::SYNTAX_ERROR,
    ErrorCodes::TYPE_MISMATCH,
    ErrorCodes::UNEXPECTED_AST_STRUCTURE,
    ErrorCodes::UNKNOWN_TYPE,
    ErrorCodes::UNSUPPORTED_METHOD,
};

/// Additional errors that are allowed for IFunctionOverloadResolver::build and IFunctionBase::prepare
/// for functions with isVariadic() == true.
static const std::unordered_set<int> variadic_typecheck_errors = {
    ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
    ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
};

/// Errors that are reluctantly allowed for IExecutableFunction::execute, but indicate that the
/// function probably unnecessarily does typechecking at execution time instead of analysis time.
static const std::unordered_set<int> late_typecheck_errors = {
    ErrorCodes::BAD_TYPE_OF_FIELD,
    ErrorCodes::ILLEGAL_COLUMN,
    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
    ErrorCodes::NO_COMMON_TYPE,
    ErrorCodes::NOT_IMPLEMENTED,
    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
    ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
    ErrorCodes::TYPE_MISMATCH,
    ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN,
    ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
};

/// Errors that are allowed for IExecutableFunction::execute.
static const std::unordered_set<int> execution_errors = {
    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
    ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF,
    ErrorCodes::BAD_ARGUMENTS,
    ErrorCodes::BAD_GET,
    ErrorCodes::CANNOT_COMPILE_REGEXP,
    ErrorCodes::CANNOT_CONVERT_TYPE,
    ErrorCodes::CANNOT_CREATE_CHARSET_CONVERTER,
    ErrorCodes::CANNOT_FORMAT_DATETIME,
    ErrorCodes::CANNOT_NORMALIZE_STRING,
    ErrorCodes::CANNOT_PARSE_BOOL,
    ErrorCodes::CANNOT_PARSE_DATE,
    ErrorCodes::CANNOT_PARSE_DATETIME,
    ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE,
    ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED,
    ErrorCodes::CANNOT_PARSE_IPV4,
    ErrorCodes::CANNOT_PARSE_IPV6,
    ErrorCodes::CANNOT_PARSE_NUMBER,
    ErrorCodes::CANNOT_PARSE_TEXT,
    ErrorCodes::CANNOT_PARSE_UUID,
    ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER,
    ErrorCodes::CANNOT_READ_ALL_DATA,
    ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT,
    ErrorCodes::DECIMAL_OVERFLOW,
    ErrorCodes::FUNCTION_THROW_IF_VALUE_IS_NON_ZERO,
    ErrorCodes::ILLEGAL_DIVISION,
    ErrorCodes::INCORRECT_DATA,
    ErrorCodes::INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE,
    ErrorCodes::PARAMETER_OUT_OF_BOUND,
    ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
    ErrorCodes::SYNTAX_ERROR,
    ErrorCodes::TOO_LARGE_ARRAY_SIZE,
    ErrorCodes::TOO_LARGE_STRING_SIZE,
    ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM,
    ErrorCodes::UNKNOWN_TYPE,
    ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX,
    ErrorCodes::UNICODE_ERROR,
    ErrorCodes::ILLEGAL_COLUMN,
};

const std::unordered_set<std::string_view> excluded_functions = {
    /// Avoid depending on environment (e.g. current query, configuration, settings).
    "synonyms",
    "catboostEvaluate",
    "naiveBayesClassifier",
    "transactionLatestSnapshot",
    "transactionOldestSnapshot",
    "showCertificate",
    "getClientHTTPHeader",
    "queryID",
    "currentQueryID",
    "initialQueryID",
    "initialQueryStartTime",
    "joinGet",
    "joinGetOrNull",
    "arrayJoin",
    "getSetting",
    "getSettingOrDefault",
    "getMergeTreeSetting",
    "getServerSetting",
    "getServerPort",
    "serverUUID",
    "generateSnowflakeID",
    "serverTimezone",
    "tcpPort",
    "hostName",
    "timeSeriesIdToTags",
    "timeSeriesTagsGroupToTags",
    "timeSeriesIdToTagsGroup",
    "timeSeriesStoreTags",
    "getMacro",
    "currentProfiles",
    "defaultProfiles",
    "enabledProfiles",
    "cutToFirstSignificantSubdomain",
    "cutToFirstSignificantSubdomainCustom",
    "cutToFirstSignificantSubdomainCustomRFC",
    "cutToFirstSignificantSubdomainCustomWithWWW",
    "cutToFirstSignificantSubdomainCustomWithWWWRFC",
    "cutToFirstSignificantSubdomainRFC",
    "cutToFirstSignificantSubdomainWithWWW",
    "cutToFirstSignificantSubdomainWithWWWRFC",
    "domain",
    "domainRFC",
    "domainWithoutWWW",
    "domainWithoutWWWRFC",
    "firstSignificantSubdomain",
    "firstSignificantSubdomainCustom",
    "firstSignificantSubdomainCustomRFC",
    "firstSignificantSubdomainRFC",
    "topLevelDomain",
    "topLevelDomainRFC",
    "__filterContains",
    "generateSerialID",
    "zookeeperSessionUptime",
    "cutToFirstSignificantSubdomainCustomWithWWW",
    "lemmatize",

    /// Avoid aggregate functions (for no strong reason).
    "initializeAggregation",
    "finalizeAggregation",
    "arrayReduce",
    "arrayReduceInRanges",

    "numericIndexedVectorShortDebugString",
    "numericIndexedVectorAllValueSum",
    "numericIndexedVectorCardinality",
    "numericIndexedVectorPointwiseGreaterEqual",
    "numericIndexedVectorPointwiseGreater",
    "numericIndexedVectorPointwiseLess",
    "numericIndexedVectorPointwiseEqual",
    "numericIndexedVectorPointwiseAdd",
    "numericIndexedVectorGetValue",
    "numericIndexedVectorToMap",
    "numericIndexedVectorPointwiseSubtract",
    "numericIndexedVectorPointwiseNotEqual",
    "numericIndexedVectorPointwiseLessEqual",
    "numericIndexedVectorPointwiseMultiply",
    "numericIndexedVectorBuild",
    "numericIndexedVectorPointwiseDivide",

    "bitmapContains",
    "bitmapXor",
    "bitmapOr",
    "bitmapAnd",
    "bitmapAndnotCardinality",
    "bitmapOrCardinality",
    "bitmapCardinality",
    "bitmapBuild",
    "bitmapMin",
    "bitmapAndCardinality",
    "bitmapAndnot",
    "bitmapXorCardinality",
    "bitmapTransform",
    "subBitmap",
    "bitmapSubsetInRange",
    "bitmapMax",
    "bitmapHasAny",
    "bitmapToArray",
    "bitmapHasAll",
    "bitmapSubsetLimit",

    /// Avoid sleeping.
    "sleep",
    "sleepEachRow",

    /// Avoid IO.
    "file",
    "filesystemAvailable",
    "filesystemCapacity",
    "filesystemUnreserved",

    /// Lambdas are not supported.
    "mapFilter",

    /// Sets are not supported.
    "in",
    "globalIn",
    "notIn",
    "globalNotIn",
    "nullIn",
    "globalNullIn",
    "notNullIn",
    "globalNotNullIn",
    "inIgnoreSet",
    "globalInIgnoreSet",
    "notInIgnoreSet",
    "globalNotInIgnoreSet",
    "nullInIgnoreSet",
    "globalNullInIgnoreSet",
    "notNullInIgnoreSet",
    "globalNotNullInIgnoreSet",

    /// Avoid dictionaries for now.
    "regionHierarchy",
    "regionIn",
    "regionToArea",
    "regionToCity",
    "regionToContinent",
    "regionToCountry",
    "regionToDistrict",
    "regionToName",
    "regionToPopulation",
    "regionToTopContinent",
    "dictGet",
    "dictGetAll",
    "dictGetChildren",
    "dictGetDate",
    "dictGetDateOrDefault",
    "dictGetDateTime",
    "dictGetDateTimeOrDefault",
    "dictGetDescendants",
    "dictGetFloat32",
    "dictGetFloat32OrDefault",
    "dictGetFloat64",
    "dictGetFloat64OrDefault",
    "dictGetHierarchy",
    "dictGetIPv4",
    "dictGetIPv4OrDefault",
    "dictGetIPv6",
    "dictGetIPv6OrDefault",
    "dictGetInt16",
    "dictGetInt16OrDefault",
    "dictGetInt32",
    "dictGetInt32OrDefault",
    "dictGetInt64",
    "dictGetInt64OrDefault",
    "dictGetInt8",
    "dictGetInt8OrDefault",
    "dictGetOrDefault",
    "dictGetOrNull",
    "dictGetString",
    "dictGetStringOrDefault",
    "dictGetUInt16",
    "dictGetUInt16OrDefault",
    "dictGetUInt32",
    "dictGetUInt32OrDefault",
    "dictGetUInt64",
    "dictGetUInt64OrDefault",
    "dictGetUInt8",
    "dictGetUInt8OrDefault",
    "dictGetUUID",
    "dictGetUUIDOrDefault",
    "dictHas",
    "dictIsIn",

    /// Avoid covering lots of non-function-related code (e.g. output formats).
    "formatRow",
    "formatRowNoNewline",
    "formatQuery",
    "formatQueryOrNull",
    "formatQuerySingleLine",
    "formatQuerySingleLineOrNull",
    "normalizeQuery",
    "normalizeQueryKeepNames",
    "normalizedQueryHashKeepNames",
    "structureToProtobufSchema",
    "structureToCapnProtoSchema",
    "generateRandomStructure",

    /// Needs query context.
    "__applyFilter",
};

/// Normally, if a function succeeded on some args, it should also succeed if we change some of
/// these args from non-const to const. These functions are exceptions, usually due to sloppy type
/// checking (checked at execution time instead of query analysis time).
/// But when these functions happen to still succeed after changing constness, we check that the
/// return value didn't change.
const std::unordered_set<std::string_view> functions_with_const_dependent_error_checks = {
    /// FunctionsRound.h is sloppy about range checking and overflows.
    /// Stricter range check in const case:
    ///   select ceil(materialize(0), materialize(18446744073709535122)) -- succeeds
    ///   select ceil(materialize(0),             18446744073709535122 ) -- fails.
    /// It's also doesn't do overflow checks:
    ///   select ceil(1.12345,18446744073709535122) -- 18446744073709552000.
    /// Would be nice to fix both.
    "round",
    "roundBankers",
    "ceil",
    "floor",
    "trunc",

    /// "Function '{}' doesn't support search with non-constant needles in constant haystack".
    "multiFuzzyMatchAllIndices",
    "multiFuzzyMatchAny",
    "multiFuzzyMatchAnyIndex",
    "multiMatchAllIndices",
    "multiMatchAny",
    "multiMatchAnyIndex",
    "multiSearchAny",
    "multiSearchAnyCaseInsensitive",
    "multiSearchAnyCaseInsensitiveUTF8",
    "multiSearchAnyUTF8",
    "multiSearchFirstIndex",
    "multiSearchFirstIndexCaseInsensitive",
    "multiSearchFirstIndexCaseInsensitiveUTF8",
    "multiSearchFirstIndexUTF8",
    "multiSearchFirstPosition",
    "multiSearchFirstPositionCaseInsensitive",
    "multiSearchFirstPositionCaseInsensitiveUTF8",
    "multiSearchFirstPositionUTF8",
    "multiSearchAllPositions",
    "multiSearchAllPositionsCaseInsensitive",
    "multiSearchAllPositionsCaseInsensitiveUTF8",
    "multiSearchAllPositionsUTF8",
    "ilike",
    "like",
    "match",
    "notILike",
    "notLike",

    /// Different overflow checks for const vs non-const.
    /// Some overflow checks while resolving overload if args are const.
    "minus",

    /// E.g. this succeeds with materialize but throws DECIMAL_OVERFLOW without it:
    ///   SELECT subtractWeeks(materialize(CAST('1970-01-16 06:39:51' AS DateTime)), CAST(-3.4390291839770234e307 AS Float64))
    "subtractNanoseconds",
    "subtractMicroseconds",
    "subtractMilliseconds",
    "subtractSeconds",
    "subtractMinutes",
    "subtractHours",
    "subtractDays",
    "subtractWeeks",
    "subtractMonths",
    "subtractQuarters",
    "subtractYears",
    "addNanoseconds",
    "addMicroseconds",
    "addMilliseconds",
    "addSeconds",
    "addMinutes",
    "addHours",
    "addDays",
    "addWeeks",
    "addMonths",
    "addQuarters",
    "addYears",

    /// E.g. this fails iff you remove the second materialize:
    ///   SELECT substringUTF8(materialize(CAST('0007c454-b371-d83b-0000-000000000000' AS String)), materialize(CAST(7.201535e-38 AS Float32)))
    "substringUTF8",
    "substring",

    "polygonsDistanceCartesian",
};

/// Normally, we expect a deterministic function to return the same result on the same arguments
/// even if we change some of these arguments from non-const to const.
/// These functions are weird exceptions.
const std::unordered_set<std::string_view> functions_with_const_dependent_semantics = {
    /// Different semantics: const string is type name, non-const string is example value.
    "getTypeSerializationStreams",
};

/// These functions throw when called on default value, and don't do anything special to avoid
/// throwing when called on NULL (which has default value behind it).
/// (Some of them do try to avoid it, but fail in some cases.)
/// See https://github.com/ClickHouse/ClickHouse/issues/93660
/// Remove this when that issue is fixed.
const std::unordered_set<std::string_view> functions_with_broken_nullable_input_handling = {
    "intDiv",
    "modulo",
    "moduloLegacy",
    "gcd",
    "lcm",
    "concat",
    "concatAssumeInjective",
    "parseDateTimeBestEffort",
    "parseDateTimeBestEffortUS",
    "parseDateTime32BestEffort",
    "parseDateTime64BestEffort",
    "parseDateTime64BestEffortUS",
};

static constexpr size_t MEMORY_LIMIT_BYTES_PER_THREAD = 256 << 20;
static constexpr size_t ROWS_PER_BATCH = 32;


size_t generateRandomNumberOfArgs()
{
    uint64_t n = thread_local_rng() % 16;
    //// Usually 1 or 2 args, sometimes 0, sometimes 3-7.
    if (n < 5)
        return 1;
    else if (n < 10)
        return 2;
    else if (n < 11)
        return 0;
    else
        return n - 11 + 3;
}

struct ArgConstraints
{
    auto toTuple() const { return std::make_tuple(); }
    bool operator<(const ArgConstraints & rhs) const { return toTuple() < rhs.toTuple(); }
};

ContextMutablePtr makeContext()
{
    ContextPtr global_context = getContext().context;
    ContextMutablePtr context = Context::createCopy(global_context);
    context->setSetting("allow_suspicious_low_cardinality_types", 1);
    context->setSetting("allow_experimental_nlp_functions", 1);
    context->setSetting("allow_deprecated_error_prone_window_functions", 1);
    context->setSetting("allow_deprecated_snowflake_conversion_functions", 1);
    context->setSetting("allow_not_comparable_types_in_comparison_functions", 1);
    context->setSetting("allow_experimental_time_time64_type", 1);
    context->setSetting("allow_introspection_functions", 1);
    context->setSetting("allow_experimental_full_text_index", 1);
    return context;
}

/// Function that we might be able to test. At this point, we don't know what the types of arguments are.
struct FunctionInfo
{
    String name;
    FunctionOverloadResolverPtr overload_resolver;
    /// TODO: maybe put hardcoded ArgConstraints here
};

Options options;
LoggerPtr logger;
std::vector<FunctionInfo> testable_functions;

void listTestableFunctions()
{
    chassert(testable_functions.empty());
    Strings all_names = FunctionFactory::instance().getAllNames();
    ContextMutablePtr context = makeContext();

    size_t num_excluded = 0;
    for (const String & name : all_names)
    {
        if (excluded_functions.contains(name))
        {
            ++num_excluded;
            continue;
        }

        FunctionOverloadResolverPtr resolver;
        try
        {
            resolver = FunctionFactory::instance().get(name, context);
        }
        catch (Exception &)
        {
            LOG_ERROR(logger, "Failed to create function {}. Please either make it work with this test or add it to `excluded_functions`.", name);
            throw;
        }

        if (options.avoid_reusing_overload_resolver)
            resolver.reset();

        testable_functions.push_back(FunctionInfo {.name = name, .overload_resolver = resolver});
    }

    LOG_INFO(logger, "testable functions: {} / {}; excluded: {}", testable_functions.size(), all_names.size(), num_excluded);
}

enum Stat
{
    S_CRITICAL_ERRORS = 0,
    S_OVERLOAD_ATTEMPTS,
    S_OVERLOAD_OK,
    S_EXEC_ROW_ATTEMPTS,
    S_EXEC_ROW_OK,
    S_EXEC_LATE_TYPECHECK_ERRORS,
    S_NONDEFAULT_NULL,
    S_TIME_TOTAL_NS,
    S_TIME_MAX_NS,
    S_MEMORY_BALANCE,
    S_MEMORY_PEAK,
    S_MEMORY_LIMIT_EXCEEDED,
    S_MEMORY_LEAKS,

    S_COUNT,
};

class FunctionStats
{
public:
    Int64 get(Stat idx) const
    {
        return a[idx];
    }

    void add(Stat idx, Int64 val)
    {
        chassert(!(which_stats_use_max & (1ul << idx)));
        a.at(idx) += val;
    }

    void max(Stat idx, Int64 val)
    {
        which_stats_use_max |= 1ul << idx;
        a.at(idx) = std::max(a.at(idx), val);
    }

    void merge(const FunctionStats & s)
    {
        UInt64 mask_diff = which_stats_use_max ^ s.which_stats_use_max;
        UInt64 mask_union = which_stats_use_max | s.which_stats_use_max;
        for (size_t i = 0; i < a.size(); ++i)
        {
            if (a[i] && s.a[i]) chassert(!(mask_diff & (1ul << i)));
            if (mask_union & (1ul << i))
                a[i] = std::max(a[i], s.a[i]);
            else
                a[i] += s.a[i];
        }
        which_stats_use_max = mask_union;
    }

private:
    std::array<Int64, S_COUNT> a {};

    /// Bit mask telling which elements of `a` should be aggregated using max instead of sum.
    /// Autodetected based on which method was called for each stat.
    /// (If neither method was called, it doesn't matter how we aggregate the value because it's 0.)
    UInt64 which_stats_use_max = 0;
    static_assert(S_COUNT <= 64, "mask doesn't fit in UInt64");
};

/// What a thread is currently doing. Written only by the thread itself.
/// Watchdog thread may periodically collect copies of this struct from all threads.
struct Operation
{
    enum class Step
    {
        None,
        GeneratingArguments,
        ResolvingOverload,
        ExecutingFunctionInBulk,
        ExecutingFunctionOnOneRow,
        ReExecutingFunctionOnOneRow,
    };

    std::chrono::steady_clock::time_point iteration_start_time;
    Step step = Step::None;
    size_t function_idx = UINT64_MAX;
    /// Function, argument types and constness. Values of const args. Random columns for non-const args.
    ColumnsWithTypeAndName args;
    size_t row_idx = 0; // if ExecutingFunctionOnOneRow or ReExecutingFunctionOnOneRow
    std::vector<size_t> args_became_const; // for ReExecutingFunctionOnOneRow


    /// Returns a string that fits in the sentence "... while {}", e.g. "executing function: SELECT f(42)".
    /// If `multirow_query_only`, returns just the query (without "executing function: ") and includes
    /// an ARRAY JOIN with all rows even if `step` is ExecutingFunctionOnOneRow.
    String describe(bool multirow_query_only = false) const
    {
        WriteBufferFromOwnString buf;

        /// What argument values to print.
        enum class Mode
        {
            OnlyConstants,
            OneRow,
            AllRows,
        };

        Mode mode = Mode::OnlyConstants;
        if (multirow_query_only)
        {
            mode = Mode::AllRows;
        }
        else
        {
            switch (step)
            {
                case Step::None:
                    return "unknown";
                case Step::GeneratingArguments:
                    writeString("picking arguments for function ", buf);
                    writeString(testable_functions.at(function_idx).name, buf);
                    return std::move(buf.str());
                case Step::ResolvingOverload:
                    writeString("resolving overload", buf);
                    mode = Mode::OnlyConstants;
                    break;
                case Step::ExecutingFunctionInBulk:
                    writeString("executing", buf);
                    mode = Mode::AllRows;
                    break;
                case Step::ExecutingFunctionOnOneRow:
                    writeString("executing", buf);
                    mode = Mode::OneRow;
                    break;
                case Step::ReExecutingFunctionOnOneRow:
                    if (args_became_const.empty())
                    {
                        writeString("re-resolving and re-executing", buf);
                    }
                    else
                    {
                        writeString("re-executing after making arguments {", buf);
                        bool first = true;
                        for (size_t idx : args_became_const)
                        {
                            if (!first)
                                writeString(", ", buf);
                            first = false;
                            writeIntText(idx + 1, buf);
                        }
                        writeString("} const", buf);
                    }
                    mode = Mode::OneRow;
                    break;
            }
            writeString(": ", buf);
        }

        writeString("SELECT ", buf);
        writeString(testable_functions.at(function_idx).name, buf);

        auto gen_name = [](size_t i) -> String
        {
            if (i < 26)
                return String(1, char('a' + i));
            return fmt::format("c{}", i);
        };

        bool need_array_join = false;
        writeString("(", buf);
        for (size_t i = 0; i < args.size(); ++i)
        {
            const auto & arg = args[i];

            if (i != 0)
                writeString(", ", buf);

            bool is_const = arg.column->isConst();

            if (!is_const && mode == Mode::AllRows)
            {
                need_array_join = true;
                writeString(gen_name(i), buf);
                continue;
            }

            if (!is_const)
                writeString("materialize(", buf);
            writeString("CAST(", buf);

            /// Make a non-const non-sparse etc column (for ISerialization) with one value.
            MutableColumnPtr mutable_column = arg.column->cloneEmpty();
            if (is_const || mode == Mode::OneRow)
                mutable_column->insertRangeFrom(*arg.column, row_idx, 1);
            else
                arg.type->insertDefaultInto(*mutable_column);
            ColumnPtr column = std::move(mutable_column);
            column = column->convertToFullColumnIfConst()->convertToFullColumnIfReplicated()->convertToFullColumnIfSparse();

            auto serialization = arg.type->getDefaultSerialization();
            serialization->serializeTextQuoted(*column, /*row_num=*/ 0, buf, FormatSettings());

            writeString(" AS ", buf);
            writeString(arg.type->getName(), buf);

            writeString(")", buf); // CAST(
            if (!is_const)
                writeString(")", buf); // materialize(
        }
        writeString(")", buf);

        if (need_array_join)
        {
            writeString(" FROM system.one ARRAY JOIN ", buf);
            bool first = true;
            for (size_t i = 0; i < args.size(); ++i)
            {
                const auto & arg = args[i];
                if (arg.column->isConst())
                    continue;
                if (!first)
                    writeString(", ", buf);
                first = false;

                writeString("CAST(", buf);

                /// Wrap the column value in array.
                DataTypePtr array_type = std::make_shared<DataTypeArray>(arg.type);
                ColumnPtr column = arg.column->convertToFullColumnIfConst()->convertToFullColumnIfReplicated()->convertToFullColumnIfSparse();
                MutableColumnPtr mutable_column = IColumn::mutate(std::move(column));
                MutableColumnPtr offsets_column = ColumnArray::ColumnOffsets::create();
                offsets_column->insert(UInt64(mutable_column->size()));
                ColumnPtr array_column = ColumnArray::create(std::move(mutable_column), std::move(offsets_column));

                auto serialization = array_type->getDefaultSerialization();
                serialization->serializeTextQuoted(*array_column, /*row_num=*/ 0, buf, FormatSettings());

                writeString(" AS ", buf);
                writeString(array_type->getName(), buf);
                writeString(") AS ", buf);
                writeString(gen_name(i), buf);
            }
        }

        writeString(";", buf);

        return std::move(buf.str());
    }
};

String valueToString(const DataTypePtr & type, const ColumnPtr & column, size_t row_idx)
{
    auto serialization = type->getDefaultSerialization();
    WriteBufferFromOwnString buf;
    serialization->serializeTextQuoted(*column->convertToFullColumnIfConst(), /*row_num=*/ row_idx, buf, FormatSettings());
    return std::move(buf.str());
}

bool reportResults(const std::vector<FunctionStats> & function_stats, size_t stuck_threads)
{
    FunctionStats totals;
    /// Names should fit in sentences "functions with {}".
    std::map<String, std::vector<String>> function_lists;
    std::vector<std::pair<Int64, String>> by_time_max;
    std::vector<std::pair<Int64, String>> by_time_total;
    std::vector<std::pair<Int64, String>> by_memory_peak;
    for (size_t i = 0; i < testable_functions.size(); ++i)
    {
        const String & name = testable_functions[i].name;
        const FunctionStats & stats = function_stats.at(i);
        totals.merge(stats);

        if (stats.get(S_CRITICAL_ERRORS) != 0)
        {
            function_lists["critical error"].push_back(name);
            continue;
        }

        if (stats.get(S_OVERLOAD_OK) == 0)
            function_lists["no valid overload found"].push_back(name);
        else if (stats.get(S_EXEC_ROW_OK) == 0)
            function_lists["no successful execution"].push_back(name);

        if (stats.get(S_EXEC_LATE_TYPECHECK_ERRORS) != 0)
            function_lists["type checking during execution"].push_back(name);
        if (stats.get(S_NONDEFAULT_NULL) != 0)
            function_lists["nondefault values behind NULLs"].push_back(name);

        by_time_max.emplace_back(stats.get(S_TIME_MAX_NS), name);
        by_time_total.emplace_back(stats.get(S_TIME_TOTAL_NS), name);

        if (stats.get(S_MEMORY_LIMIT_EXCEEDED) != 0)
            function_lists["memory limit exceeded"].push_back(name);
        else
            by_memory_peak.emplace_back(stats.get(S_MEMORY_PEAK) , name);

        if (stats.get(S_MEMORY_LEAKS) != 0)
            function_lists["unbalanced memory"].push_back(name);
    }

    String function_counts;
    for (const auto & [name, list] : function_lists)
        function_counts += fmt::format("\n    {}: {}", name, list.size());

    LOG_INFO(logger, R"(Total stats:
  stuck threads: {}
  overload resolution success: {} / {}
  execution rows success: {} / {}
  slowest iteration: {:.3f}s
  max memory usage: {:.3f} MiB
  number of functions with... (categories may overlap):{})",
        stuck_threads,
        totals.get(S_OVERLOAD_OK), totals.get(S_OVERLOAD_ATTEMPTS),
        totals.get(S_EXEC_ROW_OK), totals.get(S_EXEC_ROW_ATTEMPTS),
        totals.get(S_TIME_MAX_NS) / 1e9,
        totals.get(S_MEMORY_PEAK) * 1. / (1ul << 20),
        function_counts);

    for (auto & [name, list] : function_lists)
    {
        std::sort(list.begin(), list.end());
        const size_t limit = 30;
        bool truncate = list.size() > limit;
        if (truncate)
            list.resize(limit);
        LOG_INFO(logger, "functions with {}: {}{}", name, list, truncate ? "..." : "");
    }
    auto print_top_few = [](std::string_view what, double unit_value, std::string_view unit_name, std::vector<std::pair<Int64, String>> & list)
    {
        std::sort(list.begin(), list.end(), std::greater());
        const size_t limit = 30;
        if (list.size() > limit)
            list.resize(limit);
        String out;
        for (size_t i = 0; i < list.size(); ++i)
        {
            if (i != 0)
                out += ", ";
            out += fmt::format("{} ({:.3f} {})", list[i].second, list[i].first / unit_value, unit_name);
        }
        LOG_INFO(logger, "top by {}: {}", what, out);
    };
    print_top_few("max time", 1e9, "s", by_time_max);
    print_top_few("total time", 1e9, "s", by_time_total);
    print_top_few("memory peak", 1 << 20, "MiB", by_memory_peak);

    return totals.get(S_CRITICAL_ERRORS) == 0 && stuck_threads == 0;
}

/// Quirk in string vs enum comparison when the string value is not in the enum:
/// for non-const strings it does string comparison, but for const string
/// the comparison result is just 0.
///   select '' as x, 0::Enum8('a' = 0) as y, x < y, materialize(x) < y
///      ┌─x─┬─y─┬─less(x, y)─┬─less(materialize(x), y)─┐
///   1. │   │ a │          0 │                       1 │
///      └───┴───┴────────────┴─────────────────────────┘
/// Currently we suppress the error about this in this test, but it would be nice to fix the function instead.
bool isStringEnumComparisonQuirk(const String & function_name, const ColumnsWithTypeAndName & args)
{
    static const std::unordered_set<String> names = {"less", "greater", "lessOrEquals", "greaterOrEquals"};
    if (!names.contains(function_name) || args.size() != 2)
        return false;
    bool found_const_string = false;
    bool found_enum = false;
    for (const auto & arg : args)
    {
        bool has_string = false;
        auto apply = [&](const IDataType & type)
        {
            has_string |= isStringOrFixedString(type);
            found_enum |= isEnum(type);
        };
        apply(*arg.type);
        /// The string and enum may be inside a tuple, nullable, low-cardinality, maybe other things.
        arg.type->forEachChild(apply);
        found_const_string |= has_string && arg.column->isConst();
    }
    return found_const_string && found_enum;
}

bool isAnyArgumentNullable(const ColumnsWithTypeAndName & args)
{
    bool found_nullable = false;
    for (const auto & arg : args)
    {
        auto apply = [&](const IDataType & type)
        {
            found_nullable |= type.isNullable();
        };
        apply(*arg.type);
        arg.type->forEachChild(apply);
    }
    return found_nullable;
}

}

struct FunctionsStressTestThread
{
    size_t thread_idx;
    std::thread thread;
    std::condition_variable thread_stop_cv;
    std::atomic<bool> thread_should_stop {false};
    bool thread_stopped = false;

    std::optional<ThreadStatus> thread_status;
    ThreadGroupPtr thread_group;

    ContextMutablePtr context;

    std::vector<FunctionStats> function_stats; // parallel to testable_functions

    /// Stash of random types + columns to use. Just for speed, to avoid generating new ones every time.
    /// (I didn't check whether generating new ones every time would actually be slow.)
    std::map<ArgConstraints, std::vector<ColumnWithTypeAndName>> random_values;

    /// Some outputs of previously executed functions, to be used as inputs later.
    /// E.g. to sometimes pass the output of toString(DateTime) as input to toDateTime(String),
    /// because toDateTime(String) would ~never succeed on randomly generated strings.
    std::vector<ColumnWithTypeAndName> additional_random_values;

    /// Result of overload resolution.
    FunctionBasePtr resolved_function;
    ExecutableFunctionPtr executable_function;
    DataTypePtr result_type;

    /// Result of function execution.
    ColumnsWithTypeAndName valid_args; // rows of `args` on which the function didn't throw
    ColumnPtr result;

    /// Protects `operation`.
    /// Locked by watchdog thread when reading other threads' `operation` values (without mutating them).
    /// Correspondingly, must be locked by this thread when mutating `operation`, but not necessarily
    /// when reading it.
    std::mutex mutex;

    Operation operation;

    void run()
    {
        thread_status.emplace();
        chassert(current_thread == &*thread_status);
        context = makeContext();
        thread_group = std::make_shared<ThreadGroup>(context, 0, [&] { onCrash(); });
        CurrentThread::attachToGroup(thread_group);

        function_stats.resize(testable_functions.size());

        while (!thread_should_stop.load(std::memory_order_relaxed))
        {
            /// Pick random function.
            /// TODO: bias
            size_t function_idx = thread_local_rng() % testable_functions.size();
            FunctionStats & stats = function_stats[function_idx];
            if (stats.get(S_CRITICAL_ERRORS) != 0)
                continue;

            {
                std::unique_lock lock(mutex);
                chassert(operation.step == Operation::Step::None);
                operation.iteration_start_time = std::chrono::steady_clock::now();
                operation.function_idx = function_idx;
                operation.step = Operation::Step::GeneratingArguments;
            }

            thread_status->memory_tracker.resetCounters(); // reset the peak
            thread_status->memory_tracker.setHardLimit(MEMORY_LIMIT_BYTES_PER_THREAD);
            thread_status->untracked_memory = 0;

            auto handle_unexpected_exception = [&]
            {
                String msg = fmt::format("Error while {}", operation.describe());
                tryLogCurrentException(logger, msg);
                stats.add(S_CRITICAL_ERRORS, 1);
            };

            try
            {
                if (tryGenerateRandomOverload() && executeFunction())
                {
                    stats.add(S_EXEC_ROW_OK, result->size());
                    checkFunctionExecutionResults();
                }
            }
            catch (Exception & e)
            {
                if (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED)
                {
                    stats.add(S_MEMORY_LIMIT_EXCEEDED, 1);
                }
                else
                {
                    handle_unexpected_exception();
                }
            }
            catch (...)
            {
                handle_unexpected_exception();
            }
            if (current_thread != &*thread_status)
            {
                LOG_FATAL(logger, "current_thread changed while {}", operation.describe());
                std::abort();
            }
            if (current_thread->getThreadGroup() != thread_group)
            {
                LOG_FATAL(logger, "ThreadGroup changed while {}", operation.describe());
                std::abort();
            }

            auto end_time = std::chrono::steady_clock::now();
            Int64 ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - operation.iteration_start_time).count();
            stats.add(S_TIME_TOTAL_NS, ns);

            Int64 log_threshold_ns = 10'000'000'000L;
            if (ns >= log_threshold_ns && stats.get(S_TIME_MAX_NS) < log_threshold_ns)
            {
                {
                    std::unique_lock lock(mutex);
                    /// Print all rows.
                    if (operation.step > Operation::Step::ExecutingFunctionInBulk)
                        operation.step = Operation::Step::ExecutingFunctionInBulk;
                }
                LOG_INFO(logger, "testing function took {:.3}s: {}", ns / 1e9, operation.describe());
            }
            stats.max(S_TIME_MAX_NS, ns);

            Int64 memory_balance = thread_status->memory_tracker.get() + thread_status->untracked_memory;
            Int64 memory_peak = thread_status->memory_tracker.getPeak();

            stats.add(S_MEMORY_BALANCE, memory_balance);
            stats.max(S_MEMORY_PEAK, memory_peak);
            if (memory_balance != 0)
                stats.add(S_MEMORY_LEAKS, 1);

            executable_function.reset();
            resolved_function.reset();
            valid_args.clear();
            result.reset();

            {
                std::unique_lock lock(mutex);
                operation = Operation();
            }
        }

        thread_status.reset(); // must be done from this thread

        {
            std::unique_lock lock(mutex);
            chassert(operation.step == Operation::Step::None);
            thread_stopped = true;
        }
        thread_stop_cv.notify_all();
    }

    void onCrash()
    {
        LOG_FATAL(logger, "(while {})", operation.describe());
    }

    FunctionOverloadResolverPtr getOverloadResolver(const FunctionInfo & function_info)
    {
        if (function_info.overload_resolver)
            return function_info.overload_resolver;
        return FunctionFactory::instance().get(function_info.name, context);
    }

    bool tryGenerateRandomOverload()
    {
        const FunctionInfo & function_info = testable_functions[operation.function_idx];
        FunctionStats & stats = function_stats[operation.function_idx];
        FunctionOverloadResolverPtr resolver = getOverloadResolver(function_info);
        size_t num_args = resolver->isVariadic() ? generateRandomNumberOfArgs() : resolver->getNumberOfArguments();
        ColumnNumbers always_const_args = resolver->getArgumentsThatAreAlwaysConstant();
        ColumnsWithTypeAndName args;
        for (size_t i = 0; i < num_args; ++i)
        {
            bool always_const = std::find(always_const_args.begin(), always_const_args.end(), i) != always_const_args.end();
            ArgConstraints constraints; // TODO: lookup from hardcoded table or something
            args.push_back(pickRandomArg(constraints, always_const));
        }

        stats.add(S_OVERLOAD_ATTEMPTS, 1);

        {
            std::unique_lock lock(mutex);
            operation.step = Operation::Step::ResolvingOverload;
            operation.args = args;
        }

        ColumnsWithTypeAndName args_without_non_const_columns = operation.args;
        for (auto & arg : args_without_non_const_columns)
        {
            if (!arg.column->isConst())
                arg.column.reset();
        }

        try
        {
            resolved_function = resolver->build(args_without_non_const_columns);
        }
        catch (Exception & e)
        {
            if (early_typecheck_errors.contains(e.code()) ||
                (variadic_typecheck_errors.contains(e.code()) && resolver->isVariadic()))
            {
                return false;
            }
            else
            {
                throw;
            }
        }

        if (resolved_function->isDeterministic() && !resolved_function->isDeterministicInScopeOfQuery())
            throw Exception(ErrorCodes::INCORRECT_DATA, "isDeterministic is true, but isDeterministicInScopeOfQuery is false");

        try
        {
            executable_function = resolved_function->prepare(args_without_non_const_columns);
        }
        catch (Exception & e)
        {
            /// This function does pieces of typechecking everywhere for some reason:
            /// `build`, `prepare`, and `execute` can all throw type errors.
            if ((function_info.name == "CAST" || function_info.name == "_CAST" || function_info.name == "accurateCast") &&
                early_typecheck_errors.contains(e.code()))
                return false;
            else
                throw;
        }

        result_type = resolved_function->getResultType();
        stats.add(S_OVERLOAD_OK, 1);

        return true;
    }

    /// Random type and a column with plenty of random values of that type.
    ColumnWithTypeAndName generateRandomTypeAndColumn(const ArgConstraints &)
    {
        ColumnWithTypeAndName res;
        res.name = fmt::format("c{}", thread_local_rng());

        /// Generate simple types more often because most functions don't support complex types.
        bool allow_complex_types = thread_local_rng() % 2 == 0;
        String type_name = FunctionGenerateRandomStructure::generateRandomDataType(thread_local_rng, /*allow_suspicious_lc_types=*/ true, allow_complex_types);
        res.type = DataTypeFactory::instance().get(type_name);

        res.column = fillColumnWithRandomData(res.type, ROWS_PER_BATCH, /*max_array_length=*/ 4, /*max_string_length=*/ 80, thread_local_rng, /*fuzzy=*/ true);

        if (options.avoid_nondefault_null)
            checkAndFixupColumn(res.column, res.type.get(), ROWS_PER_BATCH, nullptr);

        return res;
    }

    /// Random type and maybe constant value.
    ColumnWithTypeAndName pickRandomArg(const ArgConstraints & constraints, bool always_const)
    {
        ColumnWithTypeAndName res;
        if (!additional_random_values.empty() && thread_local_rng() % 8 == 0)
        {
            res = additional_random_values[thread_local_rng() % additional_random_values.size()];
        }
        else
        {
            /// Cache generated types and values for speed. (I didn't check whether this helps.)
            auto & candidates = random_values[constraints];
            if (candidates.size() < 128)
                candidates.push_back(generateRandomTypeAndColumn(constraints));
            auto & candidate_ref = candidates[thread_local_rng() % candidates.size()];
            /// Sometimes update cached values with freshly generated ones.
            if (thread_local_rng() % 16 == 0)
                candidate_ref = generateRandomTypeAndColumn(constraints);
            res = candidate_ref;
        }

        if (always_const || thread_local_rng() % 2 == 0)
        {
            /// Const value.
            auto new_col = res.column->cloneEmpty();
            new_col->insertFrom(*res.column, thread_local_rng() % res.column->size());
            res.column = ColumnConst::create(std::move(new_col), ROWS_PER_BATCH);
        }
        return res;
    }

    void checkAndFixupColumn(ColumnPtr & column, const IDataType * data_type, size_t expected_rows, FunctionStats * stats)
    {
        if (!column)
            throw Exception(ErrorCodes::INCORRECT_DATA, "function returned nullptr column");
        if (column->size() != expected_rows)
            throw Exception(ErrorCodes::INCORRECT_DATA, "function returned unexpected number of rows: {} instead of {}", column->size(), expected_rows);
        if (column->getDataType() != data_type->getColumnType())
            throw Exception(ErrorCodes::INCORRECT_DATA, "function returned column of unexpected type: {} instead of {}", magic_enum::enum_name<TypeIndex>(column->getDataType()), magic_enum::enum_name<TypeIndex>(data_type->getColumnType()));

        auto apply = [&](IColumn & col)
        {
            if (auto * nullable = typeid_cast<ColumnNullable *>(&col))
            {
                const auto & null_map = nullable->getNullMapData();
                const auto & nested = nullable->getNestedColumn();
                if (!typeid_cast<const ColumnNothing *>(&nested))
                {
                    MutableColumnPtr fixed_nested;
                    for (size_t i = 0; i < null_map.size(); ++i)
                    {
                        if (null_map[i] && !nested.isDefaultAt(i))
                        {
                            if (stats)
                                stats->add(S_NONDEFAULT_NULL, 1);

                            if (options.avoid_nondefault_null)
                            {
                                if (!fixed_nested)
                                {
                                    fixed_nested = nested.cloneEmpty();
                                }
                                if (i > fixed_nested->size())
                                    fixed_nested->insertRangeFrom(nested, fixed_nested->size(), i - fixed_nested->size());
                                fixed_nested->insertDefault();
                            }
                        }
                    }
                    if (fixed_nested)
                    {
                        fixed_nested->insertRangeFrom(nested, fixed_nested->size(), null_map.size() - fixed_nested->size());
                        nullable->getNestedColumnPtr() = std::move(fixed_nested);
                    }
                }
            }
        };
        MutableColumnPtr mutable_column = IColumn::mutate(std::move(column));
        apply(*mutable_column);
        mutable_column->forEachMutableSubcolumnRecursively([&](IColumn & col) { apply(col); });
        column = std::move(mutable_column);
    }

    /// Run function on multiple rows together and separately. Sometimes run it multiple times,
    /// sometimes with different constness of args. Check determinism across such repeated runs.
    /// If the function succeeded on one or more rows, leaves the inputs and outputs for those rows
    /// in `valid_args` and `result` and returns true.
    bool executeFunction()
    {
        const FunctionInfo & function_info = testable_functions[operation.function_idx];
        FunctionStats & stats = function_stats[operation.function_idx];
        bool is_deterministic = resolved_function->isDeterministic();

        stats.add(S_EXEC_ROW_ATTEMPTS, ROWS_PER_BATCH);

        /// Try to execute on all rows at once.

        {
            std::unique_lock lock(mutex);
            operation.step = Operation::Step::ExecutingFunctionInBulk;
        }

        std::exception_ptr bulk_exception;
        chassert(!result && valid_args.empty());
        try
        {
            result = executable_function->execute(operation.args, result_type, ROWS_PER_BATCH, /*dry_run=*/ false);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED)
                throw;
            bulk_exception = std::current_exception();
        }
        catch (...)
        {
            bulk_exception = std::current_exception();
        }

        if (!bulk_exception)
            checkAndFixupColumn(result, result_type.get(), ROWS_PER_BATCH, &stats);

        /// If bulk run succeeded, sometimes still proceed to single-row runs to check that the outputs match.
        if (!bulk_exception && thread_local_rng() % 16 != 0)
        {
            valid_args = operation.args;
            return true;
        }

        /// Execute on each row separately.

        std::vector<MutableColumnPtr> mutable_valid_args;
        MutableColumnPtr mutable_result;
        std::optional<size_t> any_failed_row;
        for (size_t row_idx = 0; row_idx < ROWS_PER_BATCH; ++row_idx)
        {
            {
                std::unique_lock lock(mutex);
                operation.step = Operation::Step::ExecutingFunctionOnOneRow;
                operation.row_idx = row_idx;
            }

            ColumnsWithTypeAndName row_args = operation.args;
            for (auto & arg : row_args)
            {
                auto new_col = arg.column->cloneEmpty();
                new_col->insertRangeFrom(*arg.column, row_idx, 1);
                arg.column = std::move(new_col);
            }
            ColumnPtr row_result;
            try
            {
                row_result = executable_function->execute(row_args, result_type, /*input_rows_count=*/ 1, /*dry_run=*/ false);
            }
            catch (Exception & e)
            {
                bool is_exception_ok = false;

                /// Some functions don't check types of some arguments in IFunctionOverloadResolver::build,
                /// and only fail during execution if types are wrong. Not very nice of them.
                /// Would be good to fix, but there are hundreds of such functions.
                if (late_typecheck_errors.contains(e.code()))
                {
                    stats.add(S_EXEC_LATE_TYPECHECK_ERRORS, 1);
                    is_exception_ok = true;
                }

                if (execution_errors.contains(e.code()))
                    is_exception_ok = true;

                if (!is_exception_ok)
                    throw;

                if (!bulk_exception)
                    /// Note: we avoid throwing LOGICAL_ERROR in this test because then debug executable
                    ///       crashes without reaching our outer `catch` that prints more information,
                    ///       e.g. function name and argument list.
                    throw Exception(ErrorCodes::INCORRECT_DATA, "function succeeded when executed on multiple rows, but failed when executed on one of those rows; multi-row query: {}", operation.describe(true));

                any_failed_row = row_idx;
                continue;
            }

            checkAndFixupColumn(row_result, result_type.get(), 1, &stats);

            /// Check that the value from single-row run matches the value from multi-row run.
            if (resolved_function->isDeterministicInScopeOfQuery() && !bulk_exception)
            {
                /// Inconvenience: constness of the result might depend on the input values or number of rows.
                /// This is unusual, and this test currently reports it by default, but probably
                /// doesn't cause actual problems.
                ///
                /// Result can even be const in the bulk run and non-const in the single-row run:
                ///   select dumpColumnStructure(less(toFixedString('S', 1), materialize(cast(if(number=0,null,0) as Nullable(Enum16('a'=0)))))) from numbers(2);
                /// says const, but if you change numbers(2) to numbers(1) it becomes non-const.
                int compare_result = 1;
                if (result->isConst() != row_result->isConst())
                {
                    if (!functions_with_const_dependent_semantics.contains(function_info.name))
                        throw Exception(ErrorCodes::INCORRECT_DATA, "function result had different constness when run on many rows vs one row (with same constness of inputs)");

                    ColumnPtr left_column = result;
                    size_t left_idx = row_idx;
                    if (const ColumnConst * c = typeid_cast<const ColumnConst *>(left_column.get()))
                    {
                        left_column = c->getDataColumnPtr();
                        left_idx = 0;
                    }
                    compare_result = left_column->compareAt(left_idx, 0, *row_result->convertToFullColumnIfConst(), /*nan_direction_hint=*/ 1);
                }
                else
                {
                    compare_result = result->compareAt(row_idx, 0, *row_result, /*nan_direction_hint=*/ 1);
                }

                if (compare_result != 0)
                {
                    throw Exception(ErrorCodes::INCORRECT_DATA, "function returned different result on the same input row when executed on one row ({}) vs multiple rows: {} vs {}; multi-row query: {}", row_idx, valueToString(result_type, row_result, 0), valueToString(result_type, result, row_idx), operation.describe(true));
                }
            }

            /// Sometimes re-run the function, possibly making more of the args const and
            /// re-resolving overload. Check that return type doesn't change and no exception is thrown
            /// (even for nondeterministic functions we expect this level of determinism).
            /// If the function is deterministic, additionally check that the return value matches.
            if (thread_local_rng() % 32 == 0)
            {
                {
                    std::unique_lock lock(mutex);
                    operation.step = Operation::Step::ReExecutingFunctionOnOneRow;
                    operation.args_became_const.clear();
                }

                bool can_change_constness = !functions_with_const_dependent_semantics.contains(function_info.name);
                std::vector<size_t> args_became_const;

                ColumnsWithTypeAndName new_args = row_args;
                ColumnsWithTypeAndName new_args_without_non_const_columns;
                for (size_t idx = 0; idx < new_args.size(); ++idx)
                {
                    auto & arg = new_args[idx];

                    if (can_change_constness && !arg.column->isConst() && thread_local_rng() % 2 == 0)
                    {
                        arg.column = ColumnConst::create(arg.column, 1);
                        args_became_const.push_back(idx);
                    }

                    auto arg_without_non_const_column = arg;
                    if (!arg.column->isConst())
                        arg_without_non_const_column.column.reset();
                    new_args_without_non_const_columns.push_back(std::move(arg_without_non_const_column));
                }

                if (!args_became_const.empty())
                {
                    std::unique_lock lock(mutex);
                    operation.args_became_const = args_became_const;
                }

                /// Changing arg constness may change result type to LowCardinality, e.g. if one arg
                /// is low-cardinality and all other args became const.
                auto unwrap_type = [](DataTypePtr type) -> DataTypePtr
                {
                    if (const DataTypeLowCardinality * lc = typeid_cast<const DataTypeLowCardinality *>(type.get()))
                        type = lc->getDictionaryType();
                    return type;
                };
                auto unwrap_column = [](const ColumnPtr & column) -> ColumnPtr
                {
                    return column->convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality();
                };

                ExecutableFunctionPtr new_executable_function;
                ColumnPtr new_result;

                try
                {
                    auto new_resolver = getOverloadResolver(function_info);
                    auto new_resolved_function = new_resolver->build(new_args_without_non_const_columns);
                    auto new_result_type = new_resolved_function->getResultType();

                    if (is_deterministic && !unwrap_type(new_result_type)->equals(*unwrap_type(result_type)))
                        throw Exception(ErrorCodes::INCORRECT_DATA,
                            "result type changed: {} to {}",
                            result_type->getName(), new_result_type->getName());

                    new_executable_function = new_resolved_function->prepare(new_args_without_non_const_columns);
                    new_result = new_executable_function->execute(new_args, result_type, /*input_rows_count=*/ 1, /*dry_run=*/ false);
                }
                catch (Exception & e)
                {
                    /// Known quirk: when useDefaultImplementationForNulls() and
                    /// useDefaultImplementationForConstants() are both true,
                    ///   f(const null, non-const)
                    /// doesn't run the underlying function (just returns const null), but
                    ///   f(const null, const)
                    /// runs the underlying function (then returns const null anyway).
                    /// Combined with the quirk where lots of functions do typechecking at execution
                    /// time, this means that making args const can make the function fail.
                    /// Ignore this for now.
                    bool ok = false;
                    if (new_executable_function &&
                        new_executable_function->useDefaultImplementationForConstants() &&
                        new_executable_function->useDefaultImplementationForNulls() &&
                        late_typecheck_errors.contains(e.code()))
                        ok = true;
                    if (!args_became_const.empty() &&
                        functions_with_const_dependent_error_checks.contains(function_info.name))
                        ok = true;
                    if (!ok)
                        throw;
                }

                if (new_result && is_deterministic && unwrap_column(new_result)->compareAt(0, 0, *unwrap_column(row_result), /*nan_direction_hint=*/ 1) != 0)
                {
                    bool ignore = !args_became_const.empty() && isStringEnumComparisonQuirk(function_info.name, new_args);
                    if (!ignore)
                        throw Exception(ErrorCodes::INCORRECT_DATA,
                            "function returned different result on the same input: {} vs {}",
                            valueToString(unwrap_type(result_type), unwrap_column(row_result), 0),
                            valueToString(unwrap_type(result_type), unwrap_column(new_result), 0));
                }
            }

            if (!mutable_result)
            {
                chassert(mutable_valid_args.empty());
                for (auto & arg : row_args)
                    mutable_valid_args.push_back(IColumn::mutate(std::move(arg.column)));
                mutable_result = IColumn::mutate(std::move(row_result));
            }
            else
            {
                chassert(mutable_valid_args.size() == row_args.size());
                for (size_t i = 0; i < row_args.size(); ++i)
                    mutable_valid_args[i]->insertRangeFrom(*row_args[i].column, 0, 1);
                mutable_result->insertRangeFrom(*row_result, 0, 1);
            }
        }
        chassert(any_failed_row.has_value() == (!mutable_result || mutable_result->size() != ROWS_PER_BATCH));

        if (!!bulk_exception != any_failed_row.has_value())
        {
            {
                std::unique_lock lock(mutex);
                if (bulk_exception)
                {
                    operation.step = Operation::Step::ExecutingFunctionInBulk;
                }
                else
                {
                    operation.step = Operation::Step::ExecutingFunctionOnOneRow;
                    operation.row_idx = any_failed_row.value_or(0);
                }
            }
            if (bulk_exception)
            {
                bool ignore = functions_with_broken_nullable_input_handling.contains(function_info.name) && isAnyArgumentNullable(operation.args);
                if (!ignore)
                {
                    LOG_ERROR(logger, "function failed when executed on multiple rows (see below), but succeeded when executed on each of those rows separately");
                    std::rethrow_exception(bulk_exception);
                }
            }
            else
                throw Exception(ErrorCodes::INCORRECT_DATA, "function succeeded when executed on multiple rows, but failed when executed on one of those rows; multirow query: {}", operation.describe(true));
        }

        if (mutable_result)
        {
            chassert(mutable_result->size() > 0);
            valid_args = operation.args;
            for (size_t i = 0; i < valid_args.size(); ++i)
                valid_args[i].column = std::move(mutable_valid_args[i]);
            result = std::move(mutable_result);

            return true;
        }
        else
        {
            return false;
        }
    }

    void checkFunctionExecutionResults()
    {
        /// Maybe add result to additional_random_values.
        if (thread_local_rng() % 8 == 0 && result->byteSize() < (16ul << 10))
        {
            ColumnPtr column = result;
            if (column->size() != ROWS_PER_BATCH)
            {
                /// Repeat some values to get to the standard number of rows.
                chassert(column->size() > 0);
                chassert(column->size() < ROWS_PER_BATCH);
                auto indices_col = ColumnUInt64::create(ROWS_PER_BATCH);
                auto & indices = indices_col->getData();
                for (size_t i = 0; i < ROWS_PER_BATCH; ++i)
                    indices[i] = i < column->size() ? i : thread_local_rng() % column->size();
                column = column->index(*indices_col, 0);
                chassert(column->size() == ROWS_PER_BATCH);
            }
            ColumnWithTypeAndName c(std::move(column), result_type, fmt::format("c{}", thread_local_rng()));
            if (additional_random_values.size() < 128)
                additional_random_values.push_back(std::move(c));
            else
                additional_random_values[thread_local_rng() % additional_random_values.size()] = std::move(c);
        }

        // TODO: check monotonicity (query on full or half-infinite range sometimes) and injectivity
    }
};

TEST(FunctionsStress, DISABLED_stress)
{
    chassert(!logger);
    logger = getLogger("stress");

    options.parse(getTestCommandLineOptions().argc, getTestCommandLineOptions().argv);

    int num_threads = options.num_threads;
    if (num_threads <= 0)
        num_threads = std::thread::hardware_concurrency();
    if (num_threads <= 0)
        num_threads = 1;
    std::vector<FunctionsStressTestThread> threads(static_cast<size_t>(num_threads));
    const std::chrono::seconds stop_timeout(30);

    auto request_shutdown = [&]
        {
            for (auto & t : threads)
                t.thread_should_stop.store(true);
        };

    /// Print stack trace and function name on crash.
    HandledSignals::instance().setupTerminateHandler();
    HandledSignals::instance().setupCommonDeadlySignalHandlers();
    /// Print test results before quitting on ctrl-C.
    HandledSignals::instance().setupCommonTerminateRequestSignalHandlers();
    SignalListener signal_listener(nullptr, logger, [&](int, bool) { request_shutdown(); });
    std::thread signal_listener_thread([&] { signal_listener.run(); });

    tryRegisterFunctions();
    listTestableFunctions();
    absl::SetMinLogLevel(absl::LogSeverityAtLeast::kFatal);


    LOG_INFO(logger, "Will run in {} threads on {} functions for {} seconds", threads.size(), testable_functions.size(), options.duration_seconds);

    for (size_t i = 0; i < threads.size(); ++i)
    {
        threads[i].thread_idx = i;
        threads[i].thread = std::thread([t = &threads[i]] { t->run(); });
    }

    signal_listener.waitForTerminationRequest(std::chrono::seconds(options.duration_seconds));

    for (auto & t : threads)
        t.thread_should_stop.store(true);

    LOG_INFO(logger, "Waiting for threads to stop for up to {} seconds", stop_timeout.count());

    std::vector<FunctionStats> total_stats(testable_functions.size());
    size_t stuck_threads = 0;

    auto deadline = std::chrono::steady_clock::now() + stop_timeout;
    for (auto & t : threads)
    {
        std::optional<Operation> stuck_operation;
        {
            std::unique_lock lock(t.mutex);
            t.thread_stop_cv.wait_until(lock, deadline, [&] { return t.thread_stopped; });
            if (!t.thread_stopped)
                stuck_operation = t.operation;
        }
        if (stuck_operation.has_value())
        {
            LOG_ERROR(logger, "Thread is stuck while {}", stuck_operation->describe());
            ++stuck_threads;
        }
        else
        {
            t.thread.join();
            for (size_t i = 0; i < testable_functions.size(); ++i)
                total_stats[i].merge(t.function_stats[i]);
        }
    }

    if (!reportResults(total_stats, stuck_threads))
        _Exit(1);

    writeSignalIDtoSignalPipe(SignalListener::StopThread);
    signal_listener_thread.join();
    HandledSignals::instance().reset();
}

// TODO:
// * generateRandom and GenerateRandomStructure support for Interval types and JSON; also add it to appendFuzzyRandomString
// * better float/double distribution in generateRandom
// * maybe test constant folding
// * sanity check that the two known monotonicity bugs are found
// * maybe fix SerializationMap outputting invalid SQL
// * investigate memory tracker
// * run with sanitizers
// * randomize settings: decimal_check_overflow, cast_string_to_date_time_mode, enable_extended_results_for_datetime_functions, allow_nonconst_timezone_arguments, use_legacy_to_time, function_locate_has_mysql_compatible_argument_order, allow_simdjson, splitby_max_substrings_includes_remaining_string, least_greatest_legacy_null_behavior, h3togeo_lon_lat_result_order, geotoh3_argument_order, cast_keep_nullable, cast_ipv4_ipv6_default_on_conversion_error, enable_named_columns_in_function_tuple, function_visible_width_behavior, function_json_value_return_type_allow_nullable, function_json_value_return_type_allow_complex, use_variant_as_common_type, geo_distance_returns_float64_on_float64_arguments, session_timezone, function_date_trunc_return_type_behavior, date_time_input_format, date_time_output_format, date_time_overflow_behavior
