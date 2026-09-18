/// Structure-aware libFuzzer target that executes generated SQL in an in-process `clickhouse local`.
///
/// Same input and generation stages as `json_ast_sql_parser_fuzzer` (see
/// src/Parsers/fuzzers/json_ast_sql_parser_fuzzer/JSONASTFuzzerPipeline.h):
///
///     protobuf -> JSON AST text -> IAST::createFromJSON -> depth/size limits -> SQL
///
/// but instead of re-parsing the SQL, the target hands it to `clickhouse local` running on a
/// runner thread (`LocalFuzzerRunner.h`, shared with `clickhouse_fuzzer`). Before the first input
/// the runner executes the fixture in json_ast_sql_execution_fuzzer_schema.sql: tables whose names
/// and columns appear in the protobuf vocabulary and in the seed corpus, filled with a few hundred
/// rows, plus a view, a materialized view, a dictionary, `Join` and `Set` tables. The fixture ends
/// with `SET readonly = 2`, so a mutated statement cannot drop or modify it.
///
/// Only read-only statement kinds are executed (`SELECT`, `EXPLAIN`, `SHOW`, `CHECK TABLE`); the
/// rest is counted as skipped. Deterministic `SELECT`s additionally go through a differential
/// oracle (planner optimizations on vs. off, see `runOracle`) whose mismatches are logged. Query errors are expected outcomes. Crashes, sanitizer reports,
/// `LOGICAL_ERROR` exceptions (fatal in sanitizer and debug builds) and hangs (`-timeout`,
/// `--max_execution_time`) are the findings.
///
/// `JSON_AST_FUZZER_SCHEMA=<path>` replaces the built-in fixture with the statements from a file.
/// `JSON_AST_FUZZER_DUMP`, `JSON_AST_FUZZER_STATS` and the `-max_*` arguments work as for the
/// parser fuzzer; arguments after `-ignore_remaining_args=1` that do not start with `-max_` are
/// passed to `clickhouse local` (e.g. `--max_execution_time=10`).

#include <LocalFuzzerRunner.h>
#include <json_ast_sql_execution_fuzzer_schema.h>

#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/ASTShowIndexesQuery.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/fuzzers/json_ast_sql_parser_fuzzer/JSONASTFuzzerPipeline.h>

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Common/QueryScope.h>
#include <Common/ThreadStatus.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>

#include <libfuzzer/libfuzzer_macro.h>

#include <json_ast.pb.h>

#include <cctype>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <optional>
#include <string>
#include <thread>

extern "C" int LLVMFuzzerInitialize(const int * argc, char *** argv);

namespace
{

/// Statement kinds that are executed. Everything else only goes through the generation stages.
/// `readonly = 2` in the fixture is the second line of defence.
bool isExecutable(const DB::IAST & ast)
{
    return ast.as<DB::ASTSelectWithUnionQuery>()
        || ast.as<DB::ASTSelectIntersectExceptQuery>()
        || ast.as<DB::ASTExplainQuery>()
        || ast.as<DB::ASTShowTablesQuery>()
        || ast.as<DB::ASTShowColumnsQuery>()
        || ast.as<DB::ASTShowIndexesQuery>()
        || ast.as<DB::ASTCheckTableQuery>()
        || ast.as<DB::ASTCheckAllTablesQuery>();
}

/// ------------------------------------------------------------------------------------------------
/// Differential oracle: a deterministic `SELECT` must give the same multiset of rows with the planner
/// optimizations on and off. Both runs use one thread and the same session, so the comparison is exact.
/// Mismatches are appended to `JSON_AST_FUZZER_ORACLE_LOG` (default `oracle_mismatches.log` in the
/// working directory) together with the query, and counted; they do not stop the fuzzer.
/// `JSON_AST_FUZZER_ORACLE=0` disables the oracle.
/// ------------------------------------------------------------------------------------------------

bool oracle_enabled = true;
std::string oracle_log_path = "oracle_mismatches.log";
size_t oracle_runs = 0;
size_t oracle_mismatches = 0;
size_t oracle_error_asymmetries = 0;

/// Functions and clauses whose result legitimately depends on the run: randomness, time, the
/// environment, ordering-dependent aggregates, approximate algorithms, non-total-order `LIMIT`.
bool isDeterministicForOracle(const std::string & sql_original)
{
    /// Function names are case-insensitive for the aliases (`ARRAY_AGG`, `ANY`) and the SQL keeps the
    /// user's case, so compare in lower case; the entries below are lower case too.
    std::string sql = " " + sql_original + " ";
    for (char & c : sql)
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    static const char * forbidden[] = {
        /// lower case; clauses whose result is not a multiset of rows or depends on the plan, randomness,
        /// time, environment, ordering-dependent or sampling aggregates and their aliases, approximate
        /// algorithms, floating point (accumulation order and JIT differ between the variants)
        " limit ", " offset ", " fetch ", " over (", " over ", " window ", " settings ", " format ", "into outfile", "system.", "rand",
        "generateuuid", "generaterandom", "generateserialid", "now(", "now64(", "today(", "yesterday(",
        "currentdatabase(", "currentuser(", "hostname(", "uptime(", "version(", "timezone(", "servertimezone(",
        "getsetting(", "getmacro", "shardnum", "shardcount", "sleep", "randomstring", "randomprintable",
        "randomfixed", "fuzzbits", "file(", "url(", "s3(", "remote(", "cluster(", "arrayshuffle",
        "arraypartialshuffle", "viewexplain", "explain", "any(", "anylast(", "anyheavy(", "first_value", "last_value",
        "nth_value", "argmin(", "argmax(", "anyif(", "singlevalueornull", "grouparray", "groupuniqarray",
        "groupconcat", "arraystringconcat", "topk", "uniq", "quantile", "median", "histogram", "sequence",
        "windowfunnel", "retention", "kolmogorov", "studentttest", "welchttest", "mannwhitney", "largesttriangle",
        "sparkbar", "exponential", "arrayenumerateuniq", "arrayenumeratedense", "blocknumber(", "rownumber",
        "runningdifference", "runningaccumulate", "neighbor(", "isconstant(", "throwif", "defaultvalueofargumenttype",
        "dumpcolumnstructure", "bytesize", "tofloat", "float32", "float64", "e()", "pi()", "/ ", "divide(", "avg(",
        "avgweighted", "corr", "covar", "stddev", "varpop", "varsamp", "skew", "kurt", "geo", "point", "polygon",
        "sum(", "sumkahan", "sumwithoverflow", "deltasum", "ifnotfinite", "array_agg", "grouparraysample",
        "grouparraylast", "arrayconcatagg", "any_value", "anyrespectnulls", "any_respect_nulls",
        "anylastrespectnulls", "approx_top", "groupbitmap", "sumdistinct", "sum_distinct", "avgdistinct",
        "groupnumericindexedvector", "mapagg", "maparg", "distinctdynamictypes", "distinctjsonpaths", "string_agg",
        "listagg",
    };
    for (const char * f : forbidden)
        if (sql.find(f) != std::string::npos)
            return false;
    return true;
}

struct OracleResult
{
    bool ok = false;
    UInt64 rows = 0;
    UInt64 hash = 0;
    int error_code = 0;
    std::string error;
};

/// Runs `sql` with `settings` appended and folds every row into an order-independent hash.
OracleResult runOracleQuery(DB::ContextMutablePtr session_context, const std::string & sql, const std::string & settings)
{
    OracleResult result;
    try
    {
        auto context = DB::Context::createCopy(session_context);
        context->makeQueryContext();
        context->setCurrentQueryId("");
        /// Attaches the runner thread to the query's thread group, as `LocalConnection` does.
        auto query_scope = DB::QueryScope::create(context);
        std::string text = "SELECT * FROM (" + sql + ") SETTINGS " + settings;
        auto io = DB::executeQuery(text, context, DB::QueryFlags{.internal = true}).second;
        DB::PullingPipelineExecutor executor(io.pipeline);
        DB::Block block;
        while (executor.pull(block))
        {
            for (size_t row = 0; row < block.rows(); ++row)
            {
                SipHash row_hash;
                for (const auto & column : block)
                    column.column->updateHashWithValue(row, row_hash);
                result.hash += row_hash.get64();
                ++result.rows;
            }
        }
        /// `pull` returns false without throwing when the soft time limit is hit; the caller has to
        /// turn the partial result into the `TIMEOUT_EXCEEDED` error, like `LocalConnection` does.
        if (auto process_list_element = context->getProcessListElement())
            process_list_element->checkTimeLimit();
        io.onFinish();
        result.ok = true;
    }
    catch (const DB::Exception & e)
    {
        result.error_code = e.code();
        result.error = e.message();
    }
    return result;
}

void runOracle(const std::string & sql, const std::string & json)
{
    if (!oracle_enabled || !isDeterministicForOracle(sql))
        return;

    static const std::string baseline_settings = "max_threads = 1, max_execution_time = 2, max_rows_to_read = 1000000";
    static const std::string flipped_settings = baseline_settings
        + ", max_block_size = 1, query_plan_enable_optimizations = 0, optimize_move_to_prewhere = 0, optimize_read_in_order = 0"
          ", optimize_aggregation_in_order = 0, optimize_distinct_in_order = 0, compile_expressions = 0, compile_aggregate_expressions = 0"
          ", compile_sort_description = 0, enable_optimize_predicate_expression = 0, optimize_trivial_count_query = 0"
          ", optimize_functions_to_subcolumns = 0, optimize_rewrite_aggregate_function_with_if = 0, optimize_arithmetic_operations_in_aggregate_functions = 0"
          ", optimize_injective_functions_inside_uniq = 0, optimize_group_by_function_keys = 0, optimize_redundant_functions_in_order_by = 0"
          ", optimize_if_chain_to_multiif = 0, optimize_multiif_to_if = 0, optimize_substitute_columns = 0, optimize_use_projections = 0"
          ", optimize_use_implicit_projections = 0, use_skip_indexes = 0, use_query_condition_cache = 0, optimize_uniq_to_count = 0"
          ", optimize_syntax_fuse_functions = 0, optimize_sorting_by_input_stream_properties = 0, optimize_rewrite_sum_if_to_count_if = 0"
          ", optimize_normalize_count_variants = 0, optimize_or_like_chain = 0, optimize_time_filter_with_preimage = 0"
          ", query_plan_filter_push_down = 0, query_plan_optimize_prewhere = 0, query_plan_join_swap_table = 'false'"
          ", enable_optimize_predicate_expression_to_final_subquery = 0, optimize_extract_common_expressions = 0, optimize_and_compare_chain = 0";

    /// Third variant: many threads, tiny blocks, two-level and external (spilling) aggregation and
    /// sorting, the non-default join algorithms. The row hash is order-independent, floats are
    /// excluded by the filter, so parallelism must not change the multiset of rows.
    static const std::string parallel_settings =
        "max_threads = 8, max_execution_time = 2, max_rows_to_read = 1000000, max_block_size = 7, max_insert_block_size = 7"
        ", group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1, max_bytes_before_external_group_by = 1"
        ", max_bytes_before_external_sort = 1, prefer_external_sort_block_bytes = 1, aggregation_in_order_max_block_bytes = 1"
        ", join_algorithm = 'grace_hash,partial_merge,hash', parallel_hash_join_threshold = 0, min_joined_block_size_bytes = 1"
        ", cross_join_min_rows_to_compress = 1, cross_join_min_bytes_to_compress = 1, max_streams_to_max_threads_ratio = 4"
        ", optimize_aggregators_of_group_by_keys = 0, optimize_group_by_constant_keys = 0, optimize_rewrite_array_exists_to_has = 0"
        ", optimize_rewrite_regexp_functions = 0, query_plan_use_new_logical_join_step = 0, optimize_min_equality_disjunction_chain_length = 1";

    OracleResult results[3];
    static const char * variant_names[3] = {"default", "optimizations off", "parallel/external"};
    static const std::string * variant_settings[3] = {&baseline_settings, &flipped_settings, &parallel_settings};
    DB::LocalFuzzerRunner::runOnRunnerThread([&](DB::ContextMutablePtr context)
    {
        /// A fresh thread with its own `ThreadStatus`: the runner thread may still be attached to the
        /// query group of the statement `clickhouse local` just executed (`LocalConnection` resets its
        /// state lazily), and `QueryScope` refuses to attach twice.
        std::thread worker([&]
        {
            DB::ThreadStatus thread_status;
            for (size_t i = 0; i < 3; ++i)
                results[i] = runOracleQuery(context, sql, *variant_settings[i]);
        });
        worker.join();
    });
    ++oracle_runs;

    const OracleResult & baseline = results[0];
    for (size_t i = 1; i < 3; ++i)
    {
        const OracleResult & other = results[i];
        if (baseline.ok && other.ok)
        {
            if (baseline.rows == other.rows && baseline.hash == other.hash)
                continue;
            ++oracle_mismatches;
            std::ofstream out(oracle_log_path, std::ios::app);
            out << "=== ORACLE MISMATCH default vs " << variant_names[i] << " (rows " << baseline.rows << " vs " << other.rows
                << ", hash " << baseline.hash << " vs " << other.hash << ")\n"
                << "--- SQL ---\n" << sql << "\n--- JSON AST ---\n" << json << "\n\n";
        }
        else if (baseline.ok != other.ok)
        {
            /// Resource limits differ legitimately between plans; everything else is worth a look.
            const OracleResult & failed = baseline.ok ? other : baseline;
            if (failed.error_code == 159 || failed.error_code == 158 || failed.error_code == 241 || failed.error_code == 396)
                continue;
            ++oracle_error_asymmetries;
            std::ofstream out(oracle_log_path, std::ios::app);
            out << "=== ORACLE ERROR ASYMMETRY (" << (baseline.ok ? variant_names[i] : "default") << " failed: Code " << failed.error_code << ": "
                << failed.error << ")\n--- SQL ---\n" << sql << "\n--- JSON AST ---\n" << json << "\n\n";
        }
    }
}

void printOracleStats()
{
    if (oracle_runs)
        std::cerr << "json_ast_sql_execution_fuzzer oracle: runs " << oracle_runs << ", mismatches " << oracle_mismatches
            << ", error asymmetries " << oracle_error_asymmetries << " (see " << oracle_log_path << ")\n";
}

/// ClickHouse installs its own fatal signal handler inside `clickhouse local`, which replaces libFuzzer's,
/// so an abort during query execution ends the process without a libFuzzer artifact. The JSON and the SQL
/// of the input being executed are therefore written to `JSON_AST_FUZZER_LAST_INPUT` (default
/// `json_ast_last_input.txt` in the working directory) right before every execution.
std::string last_input_path = "json_ast_last_input.txt";

void recordLastInput(const DB::JSONASTFuzzer::PipelineInput & input)
{
    std::ofstream out(last_input_path, std::ios::trunc);
    out << "--- JSON AST ---\n" << input.json << "\n--- generated SQL ---\n" << input.sql << '\n';
}

std::string loadSchema()
{
    const char * path = getenv("JSON_AST_FUZZER_SCHEMA");
    if (!path || !*path)
        return DB::JSONASTFuzzer::EXECUTION_FUZZER_SCHEMA;
    std::ifstream in(path);
    if (!in)
    {
        std::cerr << "Cannot open JSON_AST_FUZZER_SCHEMA file " << path << '\n';
        exit(1);
    }
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

}

extern "C" int LLVMFuzzerInitialize(const int * argc, char *** argv)
{
    if (DB::LocalFuzzerRunner::isMergeRun(*argc, *argv))
        return 0;

    DB::JSONASTFuzzer::initializePipeline("json_ast_sql_execution_fuzzer", argc, argv);
    if (const char * value = getenv("JSON_AST_FUZZER_ORACLE"))
        oracle_enabled = std::string_view(value) != "0";
    if (const char * value = getenv("JSON_AST_FUZZER_ORACLE_LOG"); value && *value)
        oracle_log_path = value;
    if (const char * value = getenv("JSON_AST_FUZZER_LAST_INPUT"); value && *value)
        last_input_path = value;
    atexit(printOracleStats);
    DB::LocalFuzzerRunner::initialize(argc, argv, loadSchema());
    return 0;
}

DEFINE_BINARY_PROTO_FUZZER(const json_ast_fuzzer::Node & root)
{
    DB::JSONASTFuzzer::PipelineInput input;
    DB::ASTPtr ast = DB::JSONASTFuzzer::generateSQL(root, input);
    if (!ast)
        return;

    auto & stats = DB::JSONASTFuzzer::pipelineStats();
    if (!isExecutable(*ast))
    {
        ++stats.execution_skipped;
        return;
    }
    ++stats.executed;
    recordLastInput(input);
    DB::LocalFuzzerRunner::runQuery(input.sql);

    if (ast->as<DB::ASTSelectWithUnionQuery>())
        runOracle(input.sql, input.json);
}
