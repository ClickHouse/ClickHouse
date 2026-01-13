#include <gtest/gtest.h>

#include <absl/log/globals.h>
#include <boost/program_options.hpp>
#include <fmt/ranges.h>
#include <thread>

#include <base/phdr_cache.h>
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

/// Comma-separated strings in command line arguments.
struct VectorOfStrings
{
    std::vector<String> v;
};
std::istream & operator>>(std::istream & in, VectorOfStrings & strs)
{
    String s;
    in >> s;
    size_t i = 0;
    while (i < s.size())
    {
        size_t ii = s.find(',', i);
        if (ii == String::npos)
            ii = s.size();
        strs.v.push_back(s.substr(i, ii - i));
        i = ii + 1;
    }
    return in;
}
std::ostream & operator<<(std::ostream & out, const VectorOfStrings & strs)
{
    for (size_t i = 0; i < strs.v.size(); ++i)
    {
        if (i > 0)
            out << ",";
        out << strs.v[i];
    }
    return out;
}

/// Different sins that a function implementation may commit, of varying severity.
/// See `problemInfo` for documentation.
/// (We have to classify and aggregate errors like this because there are lots of known problems in
///  lots of functions, so we can't just stop the test on first error.)
enum Problem
{
    P_LATE_TYPECHECK = 0,
    P_EXCEPTION_IN_PREPARE,
    /// Partial list of functions with this error:
    /// https://github.com/ClickHouse/ClickHouse/blob/eca34edf9ceef5d36a57dbef160a3653191d6bc7/src/Functions/tests/gtest_functions_stress.cpp#L413
    P_CONST_DEPENDENT_CHECKS,
    P_DATA_DEPENDENT_CONST,
    P_BROKEN_NULLABLE_INPUT,
    P_BULK_SUCCESS_BUT_ROW_ERROR,
    P_BULK_ERROR_BUT_ROW_SUCCESS,
    P_BROKEN_DETERMINISM,
    P_UNEXPECTED_ERROR,

    P_COUNT,
};

std::pair<String, String> problemInfo(Problem p)
{
    switch (p)
    {
        case P_LATE_TYPECHECK: return {"late_typecheck",
            "type error throws from IExecutableFunction::execute (instead of IFunctionOverloadResolver::build)"};
        case P_EXCEPTION_IN_PREPARE: return {"exception_in_prepare",
            "exception in IFunctionBase::prepare (instead of IFunctionOverloadResolver::build"};
        case P_CONST_DEPENDENT_CHECKS: return {"const_dependent_checks",
            "function failed after making some arguments const"};
        case P_DATA_DEPENDENT_CONST: return {"data_dependent_const",
            "function result had different constness when run on many rows vs one row (with same constness of inputs)"};
        case P_BROKEN_NULLABLE_INPUT: return {"broken_nullable_input",
            "function failed on multiple rows, but succeeded on each of those rows separately; some arguments are nullable, so this is probably a known problem: https://github.com/ClickHouse/ClickHouse/issues/93660"};
        case P_BULK_SUCCESS_BUT_ROW_ERROR: return {"bulk_success_but_row_error",
            "function succeeded on multiple rows, but failed on one of those rows separately"};
        case P_BULK_ERROR_BUT_ROW_SUCCESS: return {"bulk_error_but_row_success",
            "function failed on multiple rows, but succeeded on each of those rows separately"};
        case P_BROKEN_DETERMINISM: return {"broken_determinism",
            "function says it's deterministic, but returned different values when executed twice"};
        case P_UNEXPECTED_ERROR: return {"unexpected_error",
            "function threw from unexpected place or with unexpected error code, or misc test failure"};

        case P_COUNT: std::abort();
    }
    std::abort();
}

struct Options
{
    int num_threads = -1;
    int duration_seconds = 60;

    size_t rows_per_batch = 32;

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

    //asdqwe use these 3
    VectorOfStrings ignore_problems = {{"late_typecheck", "const_dependent_checks", "broken_nullable_input"}};
    VectorOfStrings functions;
    VectorOfStrings skip_functions;

    std::array<bool, P_COUNT> ignore_problem {};

    void parse(int argc, const char * const * argv)
    {
        po::options_description desc("gtest_functions_stress");
        desc.add_options()
            ("threads", po::value<int>(&num_threads)->default_value(num_threads), "how many instances of the test to run in parallel, -1 for num cores")
            ("duration", po::value<int>(&duration_seconds)->default_value(duration_seconds), "run for this many seconds, -1 to run forever")
            ("rows-per-batch", po::value<size_t>(&rows_per_batch)->default_value(rows_per_batch), "number of rows to feed into a function at once")
            ("avoid-nondefault-null", po::value<bool>(&avoid_nondefault_null)->default_value(avoid_nondefault_null), "avoid using Nullable values where the null_map says the value is NULL, but the nested column has nondefault value; if a function returns such value, fix it up before passing it to other functions; this makes the test unrealistic, we should ideally fix all cases where this breaks functions and disable this option")
            ("avoid-reusing-overload-resolver", po::value<bool>(&avoid_reusing_overload_resolver)->default_value(avoid_reusing_overload_resolver), "create a new instance of IFunctionOverloadResolver for every overload resolution")
            ("ignore-problems", po::value<VectorOfStrings>(&ignore_problems)->default_value(ignore_problems), "comma-separated list of types of problems to ignore; see problemInfo for the list of accepted names")
            ("functions", po::value<VectorOfStrings>(&functions)->default_value(functions), "comma-separated list of functions to test; if empty, test all functions we can")
            ("skip-functions", po::value<VectorOfStrings>(&skip_functions)->default_value(skip_functions), "comma-separated list of functions to not test; this is in addition to a hard-coded list inside the test");

        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);

        for (int pi = 0; pi < P_COUNT; ++pi)
        {
            Problem p = static_cast<Problem>(pi);
            if (std::count(ignore_problems.v.begin(), ignore_problems.v.end(), problemInfo(p).first) != 0)
                ignore_problem.at(pi) = true;
        }
    }
};

/// Errors that are reluctantly allowed for IExecutableFunction::execute, but indicate that the
/// function probably unnecessarily does typechecking at execution time instead of analysis time.
/// This doesn't apply when any of the arguments has type Dynamic/Variant/Object.
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

    /// These functions do something weird with array offsets, making the result for one row depend on other rows sometimes.
    /// E.g. this outputs ([],[NULL]) for the first row:
    ///   SELECT kql_array_sort_desc(a, []::Array(Int8)) FROM system.one ARRAY JOIN CAST([[], [1]] AS Array(Array(UInt64))) AS a
    /// but if you leave only the first row in the ARRAY JOIN (remove ", [1]"), it outputs ([],[]).
    /// These functions are not documented and not meant to be called by users directly, so maybe
    /// this behavior is intended and makes sense somehow.
    "kql_array_sort_asc",
    "kql_array_sort_desc",
};

/// For these functions, IFunctionBase::prepare may throw (for no good reason).
const std::unordered_set<std::string_view> functions_with_checks_in_prepare = {
    "CAST",
    "_CAST",
    "accurateCast",
    "accurateCastOrNull",
};

/// Normally, we expect a deterministic function to return the same result on the same arguments
/// even if we change some of these arguments from non-const to const.
/// These functions are weird exceptions to this rule.
const std::unordered_set<std::string_view> functions_with_const_dependent_semantics = {
    /// Different semantics: const string is type name, non-const string is example value.
    "getTypeSerializationStreams",
};

/// Functions whose result may be const or non-const depending on the input *data*, not just on
/// types and constness of inputs.
const std::unordered_set<std::string_view> functions_with_data_dependent_const = {
    /// These have a fast path to return ColumnConst if IColumn pointers are equal.
    "isDistinctFrom",
    "isNotDistinctFrom",
};

static constexpr size_t MEMORY_LIMIT_BYTES_PER_THREAD = 256 << 20;

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
    Strings all_names;
    if (options.functions.v.empty())
        all_names = FunctionFactory::instance().getAllNames();
    else
        all_names = options.functions.v;
    ContextMutablePtr context = makeContext();

    size_t num_excluded = 0;
    for (const String & name : all_names)
    {
        if ((options.functions.v.empty() && excluded_functions.contains(name)) ||
            std::count(options.skip_functions.v.begin(), options.skip_functions.v.end(), name) != 0)
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
    S_OVERLOAD_ATTEMPTS = 0,
    S_OVERLOAD_OK,
    S_EXEC_ROW_ATTEMPTS,
    S_EXEC_ROW_OK,
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
        return counters[idx];
    }

    void add(Stat idx, Int64 val)
    {
        chassert(!(which_stats_use_max & (1ul << idx)));
        counters.at(idx) += val;
    }

    void max(Stat idx, Int64 val)
    {
        which_stats_use_max |= 1ul << idx;
        counters.at(idx) = std::max(counters.at(idx), val);
    }

    /// The error string should include function name (usually through operation.describe()).
    void reportProblem(Problem p, String error)
    {
        chassert(!error.empty());
        if (problems.at(p).empty() || problems[p].size() > error.size())
        {
            if (problems[p].empty() && !options.ignore_problem.at(p))
            {
                /// Deduplicate across threads.
                static std::array<std::atomic<bool>, P_COUNT> printed {};
                if (!printed.at(p).exchange(true))
                    LOG_ERROR(logger, "{}: {}\n", problemInfo(Problem(p)).first, error);
            }

            problems[p] = error;
        }
    }

    bool hasProblem(Problem p) const
    {
        return !problems.at(p).empty();
    }

    void merge(const FunctionStats & s)
    {
        UInt64 mask_diff = which_stats_use_max ^ s.which_stats_use_max;
        UInt64 mask_union = which_stats_use_max | s.which_stats_use_max;
        for (size_t i = 0; i < counters.size(); ++i)
        {
            if (counters[i] && s.counters[i]) chassert(!(mask_diff & (1ul << i)));
            if (mask_union & (1ul << i))
                counters[i] = std::max(counters[i], s.counters[i]);
            else
                counters[i] += s.counters[i];
        }
        which_stats_use_max = mask_union;

        for (size_t i = 0; i < problems.size(); ++i)
            if (!s.problems.at(i).empty() && (problems.at(i).empty() || problems[i].size() > s.problems[i].size()))
                problems[i] = s.problems[i];
    }

private:
    std::array<Int64, S_COUNT> counters {};
    /// If the problem occurred at least once, ths String has an error message from one of the occurrences.
    /// Otherwise the String is empty.
    std::array<String, P_COUNT> problems {};

    /// Bit mask telling which elements of `counters` should be aggregated using max instead of sum.
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

            try
            {
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
            }
            catch (...)
            {
                /// This happens for invalid enum values.
                writeString(" <failed to format value: ", buf);
                writeString(getCurrentExceptionMessage(false), buf);
                writeString(">", buf);
            }

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

                String array_type_name;
                try
                {
                    /// Wrap the column value in array.
                    DataTypePtr array_type = std::make_shared<DataTypeArray>(arg.type);
                    array_type_name = array_type->getName();
                    ColumnPtr column = arg.column->convertToFullColumnIfConst()->convertToFullColumnIfReplicated()->convertToFullColumnIfSparse();
                    MutableColumnPtr mutable_column = IColumn::mutate(std::move(column));
                    MutableColumnPtr offsets_column = ColumnArray::ColumnOffsets::create();
                    offsets_column->insert(UInt64(mutable_column->size()));
                    ColumnPtr array_column = ColumnArray::create(std::move(mutable_column), std::move(offsets_column));

                    auto serialization = array_type->getDefaultSerialization();
                    serialization->serializeTextQuoted(*array_column, /*row_num=*/ 0, buf, FormatSettings());
                }
                catch (...)
                {
                    writeString(" <failed to format value: ", buf);
                    writeString(getCurrentExceptionMessage(false), buf);
                    writeString(">", buf);
                }

                writeString(" AS ", buf);
                writeString(array_type_name, buf);
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
    bool have_unignored_problems = false;
    for (size_t i = 0; i < testable_functions.size(); ++i)
    {
        const String & name = testable_functions[i].name;
        const FunctionStats & stats = function_stats.at(i);
        totals.merge(stats);

        for (int pi = 0; pi < P_COUNT; ++pi)
        {
            Problem p = static_cast<Problem>(pi);
            if (stats.hasProblem(p))
            {
                function_lists[problemInfo(p).first].push_back(name);

                if (!options.ignore_problem.at(p))
                    have_unignored_problems = true;
            }
        }

        if (stats.get(S_OVERLOAD_OK) == 0)
            function_lists["no valid overload found"].push_back(name);
        else if (stats.get(S_EXEC_ROW_OK) == 0)
            function_lists["no successful execution"].push_back(name);

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

    return !have_unignored_problems && stuck_threads == 0;
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

bool isAnyArgumentDynamicallyTyped(const ColumnsWithTypeAndName & args)
{
    bool found = false;
    for (const auto & arg : args)
    {
        auto apply = [&](const IDataType & type)
        {
            found |= isVariant(type) || isDynamic(type) || isObject(type);
        };
        apply(*arg.type);
        arg.type->forEachChild(apply);
    }
    return found;
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

            String error;

            auto handle_unexpected_exception = [&]
            {
                String msg = fmt::format("{} {}", operation.describe(), getCurrentExceptionMessage(true));
                stats.reportProblem(P_UNEXPECTED_ERROR, msg);
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
            if (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED || e.code() == ErrorCodes::LOGICAL_ERROR)
                throw;

            return false;
        }

        if (resolved_function->isDeterministic() && !resolved_function->isDeterministicInScopeOfQuery())
            throw Exception(ErrorCodes::INCORRECT_DATA, "isDeterministic is true, but isDeterministicInScopeOfQuery is false");

        try
        {
            executable_function = resolved_function->prepare(args_without_non_const_columns);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED || e.code() == ErrorCodes::LOGICAL_ERROR)
                throw;
            if (!functions_with_checks_in_prepare.contains(function_info.name))
                stats.reportProblem(P_EXCEPTION_IN_PREPARE, fmt::format("{} exception: {}", operation.describe(), getCurrentExceptionMessage(true)));
            return false;
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

        res.column = fillColumnWithRandomData(res.type, options.rows_per_batch, /*max_array_length=*/ 4, /*max_string_length=*/ 80, thread_local_rng, /*fuzzy=*/ true);

        checkAndFixupColumn(res.column, res.type.get(), options.rows_per_batch, nullptr);

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
            res.column = ColumnConst::create(std::move(new_col), options.rows_per_batch);
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
                        /// Note: this is incorrect (?) for nullable enums, where the IColumn's
                        /// "default" value (0) is different from the IDataType's "default" value
                        /// (first enumerand) and may not be a valid enum value.
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

        stats.add(S_EXEC_ROW_ATTEMPTS, options.rows_per_batch);

        /// Try to execute on all rows at once.

        {
            std::unique_lock lock(mutex);
            operation.step = Operation::Step::ExecutingFunctionInBulk;
        }

        std::exception_ptr bulk_exception;
        chassert(!result && valid_args.empty());
        try
        {
            result = executable_function->execute(operation.args, result_type, options.rows_per_batch, /*dry_run=*/ false);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED || e.code() == ErrorCodes::LOGICAL_ERROR)
                throw;
            bulk_exception = std::current_exception();
        }
        catch (...)
        {
            bulk_exception = std::current_exception();
        }

        if (!bulk_exception)
            checkAndFixupColumn(result, result_type.get(), options.rows_per_batch, &stats);

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
        for (size_t row_idx = 0; row_idx < options.rows_per_batch; ++row_idx)
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
                if (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED || e.code() == ErrorCodes::LOGICAL_ERROR)
                    throw;

                if (late_typecheck_errors.contains(e.code()) && !isAnyArgumentDynamicallyTyped(row_args) && !stats.hasProblem(P_LATE_TYPECHECK))
                    stats.reportProblem(P_LATE_TYPECHECK, fmt::format("{}: {}", operation.describe(), getCurrentExceptionMessage(true)));

                if (!bulk_exception)
                    stats.reportProblem(P_BULK_SUCCESS_BUT_ROW_ERROR, fmt::format("single-row query: {} multi-row query: {}; exception: {}", operation.describe(), operation.describe(true), getCurrentExceptionMessage(true)));

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
                    if (!functions_with_data_dependent_const.contains(function_info.name))
                        stats.reportProblem(P_DATA_DEPENDENT_CONST, fmt::format("{}", operation.describe()));

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
                    stats.reportProblem(P_BROKEN_DETERMINISM, fmt::format("different result when executed on one row vs multiple rows: {} vs {}; single-row query: {} multi-row query (error in row {}): {}", valueToString(result_type, row_result, 0), valueToString(result_type, result, row_idx), operation.describe(), row_idx + 1, operation.describe(true)));
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
                        stats.reportProblem(P_BROKEN_DETERMINISM, fmt::format("result type changed: {} to {}; while {}", result_type->getName(), new_result_type->getName(), operation.describe()));

                    new_executable_function = new_resolved_function->prepare(new_args_without_non_const_columns);
                    new_result = new_executable_function->execute(new_args, result_type, /*input_rows_count=*/ 1, /*dry_run=*/ false);
                }
                catch (Exception & e)
                {
                    if (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED || e.code() == ErrorCodes::LOGICAL_ERROR)
                        throw;

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

                    if (!ok && !args_became_const.empty())
                    {
                        stats.reportProblem(P_CONST_DEPENDENT_CHECKS, fmt::format("{}", operation.describe()));
                        ok = true;
                    }

                    if (!ok)
                        throw;
                }

                if (new_result && is_deterministic && unwrap_column(new_result)->compareAt(0, 0, *unwrap_column(row_result), /*nan_direction_hint=*/ 1) != 0)
                {
                    bool ignore = !args_became_const.empty() && isStringEnumComparisonQuirk(function_info.name, new_args);
                    if (!ignore)
                        stats.reportProblem(P_BROKEN_DETERMINISM, fmt::format("different result on the same input: {} vs {}; while {}",
                            valueToString(unwrap_type(result_type), unwrap_column(row_result), 0),
                            valueToString(unwrap_type(result_type), unwrap_column(new_result), 0), operation.describe()));
                }
            }

            /// Avoid insertRangeFrom between columns of different constness (if the function is
            /// being weird and returning different constness for different rows).
            row_result = row_result->convertToFullColumnIfConst()->convertToFullColumnIfReplicated()->convertToFullColumnIfSparse();
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
        chassert(any_failed_row.has_value() == (!mutable_result || mutable_result->size() != options.rows_per_batch));

        if (bulk_exception && !any_failed_row.has_value())
        {
            {
                std::unique_lock lock(mutex);
                operation.step = Operation::Step::ExecutingFunctionInBulk;
            }
            String message;
            try
            {
                std::rethrow_exception(bulk_exception);
            }
            catch (Exception &)
            {
                message = fmt::format("{} exception: {}", operation.describe(), getCurrentExceptionMessage(true));
            }
            if (isAnyArgumentNullable(operation.args))
                stats.reportProblem(P_BROKEN_NULLABLE_INPUT, message);
            else
                stats.reportProblem(P_BULK_ERROR_BUT_ROW_SUCCESS, message);
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
            if (column->size() != options.rows_per_batch)
            {
                /// Repeat some values to get to the standard number of rows.
                chassert(column->size() > 0);
                chassert(column->size() < options.rows_per_batch);
                auto indices_col = ColumnUInt64::create(options.rows_per_batch);
                auto & indices = indices_col->getData();
                for (size_t i = 0; i < options.rows_per_batch; ++i)
                    indices[i] = i < column->size() ? i : thread_local_rng() % column->size();
                column = column->index(*indices_col, 0);
                chassert(column->size() == options.rows_per_batch);
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

    /// (This makes exception stack traces much faster, and this test spends a lot of time throwing and catching exceptions.)
    updatePHDRCache();

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
// * maybe fix SerializationTuple outputting non-tuple SQL for single-element tuples
// * investigate memory tracker
// * run with sanitizers
// * randomize settings: decimal_check_overflow, cast_string_to_date_time_mode, enable_extended_results_for_datetime_functions, allow_nonconst_timezone_arguments, use_legacy_to_time, function_locate_has_mysql_compatible_argument_order, allow_simdjson, splitby_max_substrings_includes_remaining_string, least_greatest_legacy_null_behavior, h3togeo_lon_lat_result_order, geotoh3_argument_order, cast_keep_nullable, cast_ipv4_ipv6_default_on_conversion_error, enable_named_columns_in_function_tuple, function_visible_width_behavior, function_json_value_return_type_allow_nullable, function_json_value_return_type_allow_complex, use_variant_as_common_type, geo_distance_returns_float64_on_float64_arguments, session_timezone, function_date_trunc_return_type_behavior, date_time_input_format, date_time_output_format, date_time_overflow_behavior
