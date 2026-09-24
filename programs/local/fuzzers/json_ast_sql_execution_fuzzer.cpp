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

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTCreateIndexQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTDropIndexQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/ASTShowIndexesQuery.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/fuzzers/json_ast_sql_parser_fuzzer/JSONASTFuzzerPipeline.h>

#include <Columns/IColumn.h>
#include <Common/Stopwatch.h>
#include <unistd.h>
#include <Common/StringUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Common/QueryScope.h>
#include <Common/quoteString.h>
#include <Common/ThreadStatus.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>

#include <libfuzzer/libfuzzer_macro.h>
#include <google/protobuf/descriptor.h>

#include <json_ast.pb.h>

#include <cctype>
#include <cmath>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <chrono>
#include <atomic>
#include <future>
#include <unordered_set>
#include <vector>

extern "C" int LLVMFuzzerInitialize(const int * argc, char *** argv);

namespace
{

/// Objects of the fixture that no fuzzed statement may create, alter, drop, rename, truncate or write to.
const std::unordered_set<std::string> fixture_objects = {"t", "t1", "t2", "t3", "src", "dst", "tbl", "empsalary", "lg", "nul", "jt", "st", "v", "mv", "d"};
const std::unordered_set<std::string> protected_databases = {"default", "system", "information_schema", "INFORMATION_SCHEMA"};

bool isFixtureTable(const std::string & database, const std::string & table)
{
    return (database.empty() || database == "default") && fixture_objects.contains(table);
}

/// Which statements are executed. Read-only statements always; statements that create or change
/// objects only when their target is not part of the fixture (they may read from it). `SYSTEM`,
/// `SET`, `USE`, `KILL`, `BACKUP`/`RESTORE`, database-level `DROP`/`ALTER`/`RENAME` and `INTO OUTFILE`
/// are never executed. `readonly` is not used, so the fixture is protected by this policy alone.
enum class Verdict
{
    SKIP,
    READ_ONLY,
    MODIFYING,
};

/// A fuzzed `SETTINGS` clause can lift the limits the harness runs under (`max_memory_usage = 0` let a
/// `generateRandom` query ask for 64 GiB at once). Statements that touch a resource limit are not executed.
bool overridesResourceLimits(const std::string & sql_original)
{
    std::string sql = sql_original;
    for (char & c : sql)
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    static const char * limit_settings[] = {
        "max_memory_usage", "max_untracked_memory", "memory_overcommit", "max_execution_time", "max_estimated_execution_time",
        "max_rows_to_read", "max_bytes_to_read", "max_result_rows", "max_result_bytes", "read_overflow_mode", "timeout_overflow_mode",
        "max_rows_to_group_by", "max_bytes_before_external", "max_bytes_ratio_before_external", "max_rows_in_set", "max_bytes_in_set",
        "max_rows_in_join", "max_bytes_in_join", "max_rows_to_sort", "max_bytes_to_sort", "max_rows_to_transfer", "max_bytes_to_transfer",
        "max_temporary_columns", "max_temporary_non_const_columns", "max_query_size", "max_parser_depth", "max_parser_backtracks",
        "max_ast_depth", "max_ast_elements", "max_expanded_ast_elements", "max_network_bandwidth", "max_network_bytes",
        "priority", "max_threads", "max_insert_threads", "max_final_threads", "max_concurrent_queries_for_user",
        /// Any timeout setting: the harness runs with one-second network timeouts, a fuzzed
        /// `SETTINGS connect_timeout = 100` on an external table function waits for minutes.
        "timeout", "retries", "retry",
    };
    for (const char * setting : limit_settings)
        if (sql.find(setting) != std::string::npos)
            return true;
    return false;
}

/// Table functions that reach an external system (object storage, another server, an external database) or
/// read a local path. The harness has no such systems configured, so these either fail after a network
/// timeout, make real outbound requests to whatever host the fuzzer invented, or (for the data-lake
/// functions built on the Rust `delta_kernel`/`iceberg` FFI) panic inside Rust, which ASan reports as a
/// stack-buffer-underflow in `memset`/`_Unwind_Resume` (a false positive of ASan over the Rust unwinder,
/// see the HINT in the report). None of that exercises the SQL layer, so skip such statements.
bool usesExternalTableFunction(const std::string & sql_original)
{
    std::string sql = sql_original;
    for (char & c : sql)
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    static const char * table_functions[] = {
        "deltalake", "iceberg", "hudi", "s3(", "s3cluster", "gcs(", "cosn(", "oss(",
        "azureblobstorage", "hdfs", "url(", "urlcluster", "remote(", "remotesecure", "cluster(",
        "mysql(", "postgresql(", "mongodb(", "redis(", "sqlite(", "jdbc(", "odbc(", "file(",
        "kafka(", "nats(", "rabbitmq(", "s3queue", "azurequeue", "hdfscluster", "prometheus(",
    };
    for (const char * fn : table_functions)
        if (sql.find(fn) != std::string::npos)
            return true;
    return false;
}

Verdict classify(const DB::IAST & ast)
{
    using namespace DB;
    if (const auto * with_output = dynamic_cast<const ASTQueryWithOutput *>(&ast); with_output && with_output->out_file)
        return Verdict::SKIP;

    if (ast.as<ASTSelectWithUnionQuery>() || ast.as<ASTSelectIntersectExceptQuery>() || ast.as<ASTExplainQuery>()
        || ast.as<ASTShowTablesQuery>() || ast.as<ASTShowColumnsQuery>() || ast.as<ASTShowIndexesQuery>()
        || ast.as<ASTCheckTableQuery>() || ast.as<ASTCheckAllTablesQuery>())
        return Verdict::READ_ONLY;

    if (const auto * create = ast.as<ASTCreateQuery>())
    {
        if (!create->database.get() && create->getTable().empty()) /// CREATE DATABASE
            return protected_databases.contains(create->getDatabase()) ? Verdict::SKIP : Verdict::MODIFYING;
        if (create->getTable().empty() || isFixtureTable(create->getDatabase(), create->getTable()))
            return Verdict::SKIP;
        if (create->hasTargetTableID(ViewTarget::To))
        {
            const auto target = create->getTargetTableID(ViewTarget::To);
            if (isFixtureTable(target.database_name, target.table_name))
                return Verdict::SKIP;
        }
        return Verdict::MODIFYING;
    }
    if (const auto * drop = ast.as<ASTDropQuery>())
        return (drop->getTable().empty() || isFixtureTable(drop->getDatabase(), drop->getTable())) ? Verdict::SKIP : Verdict::MODIFYING;
    if (const auto * alter = ast.as<ASTAlterQuery>())
        return (alter->alter_object != ASTAlterQuery::AlterObjectType::TABLE || alter->getTable().empty()
                || isFixtureTable(alter->getDatabase(), alter->getTable())) ? Verdict::SKIP : Verdict::MODIFYING;
    if (const auto * rename = ast.as<ASTRenameQuery>())
    {
        if (rename->database)
            return Verdict::SKIP;
        for (const auto & element : rename->getElements())
            if (isFixtureTable(element.from.getDatabase(), element.from.getTable()) || isFixtureTable(element.to.getDatabase(), element.to.getTable()))
                return Verdict::SKIP;
        return Verdict::MODIFYING;
    }
    if (const auto * insert = ast.as<ASTInsertQuery>())
        return (!insert->table_function && isFixtureTable(insert->getDatabase(), insert->getTable())) ? Verdict::SKIP : Verdict::MODIFYING;
    if (ast.as<ASTOptimizeQuery>() || ast.as<ASTDeleteQuery>() || ast.as<ASTUpdateQuery>() || ast.as<ASTCreateIndexQuery>() || ast.as<ASTDropIndexQuery>())
    {
        const auto & with_table = dynamic_cast<const ASTQueryWithTableAndOutput &>(ast);
        return (with_table.getTable().empty() || isFixtureTable(with_table.getDatabase(), with_table.getTable())) ? Verdict::SKIP : Verdict::MODIFYING;
    }
    if (ast.as<ASTCreateSQLFunctionQuery>())
        return Verdict::MODIFYING;
    return Verdict::SKIP;
}

/// Everything the fuzzer created is dropped after this many modifying statements, so objects do not
/// accumulate and the fixture stays the only long-lived state. Kept small so that a single cleanup (one
/// `DROP ... SYNC` per object on the ASan build) stays well under libFuzzer's per-input timeout: at 200 a
/// cleanup once ran for 166 s and tripped the 120 s timeout.
constexpr size_t cleanup_every = 40;
size_t modifying_since_cleanup = 0;
size_t executed_modifying = 0;

std::vector<std::string> runNamesQuery(DB::ContextMutablePtr session_context, const std::string & sql)
{
    std::vector<std::string> names;
    try
    {
        auto context = DB::Context::createCopy(session_context);
        context->makeQueryContext();
        context->setCurrentQueryId("");
        auto query_scope = DB::QueryScope::create(context);
        auto io = DB::executeQuery(sql, context, DB::QueryFlags{.internal = true}).second;
        DB::PullingPipelineExecutor executor(io.pipeline);
        DB::Block block;
        while (executor.pull(block))
            for (size_t row = 0; row < block.rows(); ++row)
                names.push_back(std::string(block.getByPosition(0).column->getDataAt(row)));
        io.onFinish();
    }
    catch (const DB::Exception &)
    {
    }
    return names;
}

void runStatement(DB::ContextMutablePtr session_context, const std::string & sql)
{
    try
    {
        auto context = DB::Context::createCopy(session_context);
        context->makeQueryContext();
        context->setCurrentQueryId("");
        auto query_scope = DB::QueryScope::create(context);
        auto io = DB::executeQuery(sql, context, DB::QueryFlags{.internal = true}).second;
        DB::executeTrivialBlockIO(io, context);
    }
    catch (const DB::Exception &)
    {
    }
}

void cleanupFuzzerObjects()
{
    /// `DROP ... SYNC` of some engines blocks in the storage's `shutdown` (it joins the engine's background
    /// threads): a `Distributed` table flushing pending sends to an unreachable host, a `Kafka`/`RabbitMQ`/`NATS`
    /// consumer, a dictionary reloading from a dead source. Such a drop can run past libFuzzer's per-input timeout
    /// (observed: a single cleanup at 155 s vs the 120 s limit). Run the drops on a detachable worker with its own
    /// copy of the context (so it stays alive if detached) and give up after a bounded wait; a stuck cleanup then
    /// leaks its objects into the rest of the session instead of aborting it, and cleanup is not attempted again
    /// (the session ends soon on its time or RSS limit, and each worker process starts from a fresh fixture).
    static std::atomic<bool> cleanup_disabled{false};
    if (cleanup_disabled.load(std::memory_order_acquire))
        return;
    DB::LocalFuzzerRunner::runOnRunnerThread([&](DB::ContextMutablePtr context)
    {
        auto finished = std::make_shared<std::promise<void>>();
        std::future<void> future = finished->get_future();
        std::thread worker([context, finished]
        {
            DB::ThreadStatus thread_status;
            std::string fixture_list;
            for (const auto & name : fixture_objects)
                fixture_list += (fixture_list.empty() ? "'" : ", '") + name + "'";
            for (const auto & name : runNamesQuery(context, "SELECT name FROM system.tables WHERE database = 'default' AND name NOT IN (" + fixture_list + ")"))
                runStatement(context, "DROP TABLE IF EXISTS default." + DB::backQuoteIfNeed(name) + " SYNC");
            for (const auto & name : runNamesQuery(context, "SELECT name FROM system.databases WHERE name NOT IN ('default', 'system', 'information_schema', 'INFORMATION_SCHEMA')"))
                runStatement(context, "DROP DATABASE IF EXISTS " + DB::backQuoteIfNeed(name) + " SYNC");
            for (const auto & name : runNamesQuery(context, "SELECT name FROM system.functions WHERE origin = 'SQLUserDefined'"))
                runStatement(context, "DROP FUNCTION IF EXISTS " + DB::backQuoteIfNeed(name));
            finished->set_value();
        });
        if (future.wait_for(std::chrono::seconds(30)) == std::future_status::ready)
        {
            worker.join();
        }
        else
        {
            cleanup_disabled.store(true, std::memory_order_release);
            worker.detach();
        }
    });
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
size_t oracle_variants_skipped = 0; /// baseline hit a resource limit or was too slow for the variants
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
        /// algorithms. Floating point values are allowed: they are hashed rounded (see `hashRow`).
        " limit ", " offset ", " fetch ", " over (", " over ", " window ", " settings ", " format ", "into outfile", "system.", "information_schema", "stochastic", "rand",
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
        "dumpcolumnstructure", "bytesize", "utctimestamp", "utc_timestamp", "current", "curdate", "localtimestamp", "nowinblock",
        "generateulid", "generatesnowflakeid", "serveruuid", "queryid", "tcpport", "getos", "filesystem", "fqdn", "displayname",
        "buildid", "revision(", "lowcardinalityindices", "lowcardinalitykeys", "blockserializedsize", "blocksize", "transactionid",
        "transactionlatestsnapshot", "runningconcurrency", "getclienthttpheader", "hasthreadfuzzer", "array_agg", "grouparraysample",
        "grouparraylast", "arrayconcatagg", "any_value", "anyrespectnulls", "any_respect_nulls",
        "anylastrespectnulls", "approx_top", "groupbitmap", "sumdistinct", "sum_distinct", "avgdistinct",
        "groupnumericindexedvector", "mapagg", "maparg", "distinctdynamictypes", "distinctjsonpaths", "string_agg",
        "listagg",
        /// `formatRow('XML'/'JSON*', ...)` embeds the format's statistics (`elapsed`, `rows_read`) per row.
        "formatrow",
        /// UUIDv7 generators fill the low bits randomly, `dateTimeToUUIDv7` included.
        "uuidv7",
        /// Random replacement words unless a seed is given.
        "obfuscatequery",
        /// The planner may fold `ignore(...)` to 0 without evaluating a throwing argument; with the plan
        /// optimizations off the argument is evaluated. The result is a constant either way.
        "ignore(",
        /// Random by design, and expensive enough under sanitizers to time out when run five times.
        "fuzzquery", "fuzzjson",
    };
    for (const char * f : forbidden)
        if (sql.find(f) != std::string::npos)
            return false;

    /// Zero-argument functions may be written without parentheses (`SELECT now FROM t` calls `now()`), so the
    /// entries above that end with `(` do not catch them; these are matched as whole words.
    static const char * forbidden_words[] = {
        "now", "now64", "today", "yesterday", "currentdatabase", "currentuser", "hostname", "uptime", "version",
        "timezone", "servertimezone", "serveruuid", "queryid", "tcpport", "getos", "fqdn", "displayname", "buildid",
        "revision", "blocknumber", "blocksize", "rownumberinblock", "rownumberinallblocks", "transactionid",
        "randcanonical", "generateuuidv4", "generateuuidv7", "currentschemas", "currentroles", "currentprofiles",
        "enabledroles", "enabledprofiles", "defaultroles", "defaultprofiles", "initialqueryid", "initialquerystarttime",
        "initial_query_id", "initial_query_start_time", "query_id", "current_schemas", "current_user", "current_database",
        "current_timestamp", "current_date", "current_role", "localtime", "localtimestamp", "curdate", "current_time",
        "sysdate", "utc_timestamp", "utctimestamp", "nowinblock", "now64",
        "getservermacro", "getclienthttpheader", "hasthreadfuzzer", "runningconcurrency", "zookeepersessionuptime",
    };
    const auto is_word_char = [](char c) { return isAlphaNumericASCII(c) || c == '_'; };
    for (const char * word : forbidden_words)
    {
        const size_t len = strlen(word);
        for (size_t pos = sql.find(word); pos != std::string::npos; pos = sql.find(word, pos + 1))
            if (!is_word_char(sql[pos - 1]) && !is_word_char(sql[pos + len]))
                return false;
    }
    return true;
}

struct OracleResult
{
    bool ok = false;
    bool comparable = true; /// false when a column type cannot be compared across plans (nested floats)
    UInt64 rows = 0;
    UInt64 hash = 0;
    int error_code = 0;
    std::string error;
};

/// Floating point results legitimately differ in the last bits between the variants (accumulation
/// order, JIT); compare them rounded to nine significant digits. NaN and infinities hash as themselves.
UInt64 roundedFloatHash(Float64 value)
{
    if (std::isnan(value))
        return 0x7ff8000000000001ULL;
    if (std::isinf(value))
        return value > 0 ? 0x7ff0000000000000ULL : 0xfff0000000000000ULL;
    if (value == 0)
        return 0;
    const Float64 scale = std::pow(10.0, 8 - std::floor(std::log10(std::fabs(value))));
    const Float64 rounded = std::round(value * scale) / scale;
    UInt64 bits;
    memcpy(&bits, &rounded, sizeof(bits));
    return bits;
}

/// Order-independent hash of one row. Returns false when a column cannot be compared.
bool hashRow(const DB::Block & block, size_t row, SipHash & row_hash)
{
    for (const auto & column : block)
    {
        const auto type_id = column.type->getTypeId();
        if (type_id == DB::TypeIndex::Float32 || type_id == DB::TypeIndex::Float64)
        {
            row_hash.update(roundedFloatHash(column.column->getFloat64(row)));
            continue;
        }
        if (column.type->getName().find("Float") != std::string::npos)
        {
            /// Nullable(Float) is handled by the branch below only when not null; nested floats
            /// (arrays, tuples, maps of floats) cannot be rounded generically.
            if (const auto * nullable = typeid_cast<const DB::DataTypeNullable *>(column.type.get());
                nullable && (nullable->getNestedType()->getTypeId() == DB::TypeIndex::Float32 || nullable->getNestedType()->getTypeId() == DB::TypeIndex::Float64))
            {
                if (column.column->isNullAt(row))
                    row_hash.update(0xdeadbeefULL);
                else
                    row_hash.update(roundedFloatHash(column.column->getFloat64(row)));
                continue;
            }
            return false;
        }
        column.column->updateHashWithValue(row, row_hash);
    }
    return true;
}

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
                if (!hashRow(block, row, row_hash))
                    result.comparable = false;
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
    catch (const std::exception & e)
    {
        /// `executeQuery` lets `Poco::Exception` and standard exceptions through; the server reports
        /// them as `POCO_EXCEPTION` / `STD_EXCEPTION`, so they are query errors here as well.
        result.error_code = DB::getCurrentExceptionCode();
        result.error = e.what();
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

    /// Fourth variant: force the JIT compilation of expressions, aggregates and sort descriptions
    /// (the default only compiles after a few repetitions, so a single fuzzed query never does), with
    /// short-circuit evaluation forced and lazy materialization disabled.
    static const std::string jit_settings = baseline_settings
        + ", min_count_to_compile_expression = 0, min_count_to_compile_aggregate_expression = 0, min_count_to_compile_sort_description = 0"
          ", compile_expressions = 1, compile_aggregate_expressions = 1, compile_sort_description = 1"
          ", short_circuit_function_evaluation = 'force_enable', query_plan_optimize_lazy_materialization = 0, max_block_size = 3";

    OracleResult results[4];
    bool variants_run = false;
    static const char * variant_names[4] = {"default", "optimizations off", "parallel/external", "jit"};
    static const std::string * variant_settings[4] = {&baseline_settings, &flipped_settings, &parallel_settings, &jit_settings};
    DB::LocalFuzzerRunner::runOnRunnerThread([&](DB::ContextMutablePtr context)
    {
        /// A fresh thread with its own `ThreadStatus`: the runner thread may still be attached to the
        /// query group of the statement `clickhouse local` just executed (`LocalConnection` resets its
        /// state lazily), and `QueryScope` refuses to attach twice.
        std::thread worker([&]
        {
            DB::ThreadStatus thread_status;
            Stopwatch watch;
            results[0] = runOracleQuery(context, sql, *variant_settings[0]);
            /// The variants are only compared when both runs finished, and a resource-limit error on either
            /// side is ignored below. A baseline that hit a limit, or that used most of the time budget
            /// already (the `max_block_size = 1` variant is many times slower), would only burn three more
            /// timeouts, so skip the variants; the timeouts were a quarter of all query errors before.
            const bool baseline_hit_limit = !results[0].ok
                && (results[0].error_code == 159 || results[0].error_code == 158 || results[0].error_code == 241 || results[0].error_code == 396);
            if (baseline_hit_limit || watch.elapsedMilliseconds() > 700)
            {
                ++oracle_variants_skipped;
                return;
            }
            variants_run = true;
            for (size_t i = 1; i < 4; ++i)
                results[i] = runOracleQuery(context, sql, *variant_settings[i]);
        });
        worker.join();
    });
    ++oracle_runs;
    if (!variants_run)
        return;

    const OracleResult & baseline = results[0];
    for (size_t i = 1; i < 4; ++i)
    {
        const OracleResult & other = results[i];
        if (baseline.ok && other.ok)
        {
            if (!baseline.comparable || !other.comparable)
                continue;
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
        std::cerr << "json_ast_sql_execution_fuzzer oracle: runs " << oracle_runs << " (variants skipped after a slow baseline: "
            << oracle_variants_skipped << "), mismatches " << oracle_mismatches << ", error asymmetries " << oracle_error_asymmetries
            << " (see " << oracle_log_path << ")\n";
    if (executed_modifying)
        std::cerr << "json_ast_sql_execution_fuzzer: executed " << executed_modifying << " statements that create or change objects\n";
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

/// ------------------------------------------------------------------------------------------------
/// Mutation post-processor (opt-in, see `fixupEnabled`): after every mutation, unknown table names
/// become fixture tables, unknown column names fixture columns, and unknown function names real
/// functions. A small share is left untouched so that name-resolution error paths stay covered.
/// ------------------------------------------------------------------------------------------------

struct FixtureTable
{
    std::string_view name;
    std::vector<std::string_view> columns;
};

/// Tables of json_ast_sql_execution_fuzzer_schema.sql with their columns.
const std::vector<FixtureTable> fixture_tables = {
    {"t", {"a", "b", "c", "d", "e", "f", "s", "x", "y", "z", "id", "key", "value", "n", "arr", "m", "tup", "dt", "dt64", "lc", "u", "dec", "ip", "en",
           "fs", "bl", "d32", "nested.k", "nested.v", "j", "dyn", "var", "i128", "u256", "fl32", "ipv6", "dec2", "t_arr", "arr_null", "m2", "lc_null"}},
    {"t1", {"k", "ts", "a", "b", "v"}},
    {"t2", {"k", "ts", "a", "b", "v"}},
    {"t3", {"id", "name", "value", "tags"}},
    {"src", {"a", "b", "c"}},
    {"dst", {"a", "b", "c"}},
    {"tbl", {"t", "name", "arr"}},
    {"empsalary", {"depname", "empno", "salary", "enroll_date"}},
    {"lg", {"a", "s"}},
    {"nul", {"a", "s"}},
    {"jt", {"k", "jv"}},
    {"st", {"k"}},
    {"v", {"a", "c", "total"}},
    {"mv", {"a", "b", "c"}},
    {"d", {"k", "dv"}},
};

const FixtureTable * findFixtureTable(std::string_view name)
{
    for (const auto & table : fixture_tables)
        if (table.name == name)
            return &table;
    return nullptr;
}

bool textToEnumTable_hasFunction(const std::string & name)
{
    static const std::unordered_set<std::string> names = []
    {
        std::unordered_set<std::string> result;
        const auto * descriptor = json_ast_fuzzer::FunctionName_descriptor();
        for (int i = 0; i < descriptor->value_count(); ++i)
            result.insert(descriptor->value(i)->options().GetExtension(json_ast_fuzzer::json_text));
        return result;
    }();
    return names.contains(name);
}

/// What a query refers to, collected in a first pass over the tree.
struct QueryNames
{
    std::vector<const FixtureTable *> tables;   /// fixture tables referenced by `TableIdentifier`s
    std::unordered_set<std::string> aliases;    /// `AS alias` of expressions and tables, `WITH ... AS name`
    bool uses_numbers = false;
};

void collectNames(const json_ast_fuzzer::Node & node, QueryNames & names)
{
    using namespace json_ast_fuzzer;
    for (const auto & prop : node.props())
    {
        if (prop.key() == K_alias && prop.has_string_value() && !prop.string_value().empty())
            names.aliases.insert(prop.string_value());
        if (prop.key() == K_name && node.type() == T_TableIdentifier && prop.has_string_value())
        {
            if (const auto * table = findFixtureTable(prop.string_value()))
                names.tables.push_back(table);
        }
        if (prop.key() == K_name && node.type() == T_Function && prop.has_string_value() && prop.string_value().starts_with("numbers"))
            names.uses_numbers = true;
        if (prop.key() == K_name && node.type() == T_Function && prop.has_function_name())
            names.uses_numbers = true; /// cannot tell; be permissive about `number`
        if (prop.has_node_value())
            collectNames(prop.node_value(), names);
        else if (prop.has_node_list())
            for (const auto & child : prop.node_list().items())
                collectNames(child, names);
    }
    for (const auto & child : node.children())
        collectNames(child, names);
}

bool isKnownColumn(std::string_view name, const QueryNames & names)
{
    if (name == "*" || name.size() <= 1 || names.aliases.contains(std::string(name)))
        return true;
    if (name == "number" && names.uses_numbers)
        return true;
    std::string_view column = name;
    /// `table.column` or `alias.column`
    if (auto dot = name.find('.'); dot != std::string_view::npos && name.substr(0, dot) != "nested")
    {
        std::string_view qualifier = name.substr(0, dot);
        column = name.substr(dot + 1);
        if (names.aliases.contains(std::string(qualifier)))
            return true;
        if (const auto * table = findFixtureTable(qualifier))
        {
            for (auto c : table->columns)
                if (c == column)
                    return true;
            return false;
        }
        return false;
    }
    for (const auto * table : names.tables)
        for (auto c : table->columns)
            if (c == name)
                return true;
    return false;
}

void rewriteNames(json_ast_fuzzer::Node & node, const QueryNames & names, const FixtureTable & target, std::mt19937 & rng)
{
    using namespace json_ast_fuzzer;
    const bool is_table = node.type() == T_TableIdentifier;
    const bool is_identifier = node.type() == T_Identifier;
    const bool is_function = node.type() == T_Function;
    for (auto & prop : *node.mutable_props())
    {
        if (prop.key() == K_name && prop.has_string_value() && rng() % 10 != 0)
        {
            const std::string & name = prop.string_value();
            if (is_table && !findFixtureTable(name) && !names.aliases.contains(name))
                prop.set_string_value(std::string(target.name));
            else if (is_identifier && !isKnownColumn(name, names))
                prop.set_string_value(std::string(target.columns[rng() % target.columns.size()]));
            else if (is_function && !textToEnumTable_hasFunction(name))
                prop.set_function_name(static_cast<FunctionName>(1 + rng() % (FunctionName_descriptor()->value_count() - 1)));
        }
        else if ((is_table || is_identifier) && prop.key() == K_name_parts)
        {
            /// `name_parts` must agree with `name`; the reader rebuilds them from `name` when absent.
            prop.clear_value();
        }
        if (prop.has_node_value())
            rewriteNames(*prop.mutable_node_value(), names, target, rng);
        else if (prop.has_node_list())
            for (auto & child : *prop.mutable_node_list()->mutable_items())
                rewriteNames(child, names, target, rng);
    }
    for (auto & child : *node.mutable_children())
        rewriteNames(child, names, target, rng);
}

/// Unknown table names become one fixture table chosen for the query (the first fixture table it
/// already references, otherwise a random one), unknown column names become columns of that table,
/// unknown function names real functions. Aliases defined in the query are kept.
void fixIdentifiers(json_ast_fuzzer::Node & root, std::mt19937 & rng)
{
    QueryNames names;
    collectNames(root, names);
    const FixtureTable * target = names.tables.empty() ? nullptr : names.tables.front();
    if (!target)
    {
        static const std::vector<size_t> weighted = {0, 0, 0, 0, 1, 2, 3, 4, 6, 7};
        target = &fixture_tables[weighted[rng() % weighted.size()]];
    }
    /// Unknown tables are rewritten to the target, so its columns are resolvable everywhere.
    bool present = false;
    for (const auto * table : names.tables)
        present = present || table == target;
    if (!present)
        names.tables.push_back(target);
    rewriteNames(root, names, *target, rng);
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

/// Opt-in (`JSON_AST_FUZZER_FIXUP=1`): measured on 600 mined seeds, the rewrite *increased* the number of
/// failing queries (244 -> 296): it also renames lambda parameters, CTE names and aggregate combinators
/// such as `sumIf` that are not in `system.functions`, and picks columns of the wrong type. Kept for
/// experiments with a smarter, type-aware version.
bool fixupEnabled()
{
    static const bool enabled = []
    {
        const char * value = getenv("JSON_AST_FUZZER_FIXUP"); // NOLINT(concurrency-mt-unsafe)
        return value && std::string_view(value) == "1";
    }();
    return enabled;
}

/// Registered for the whole session; libprotobuf-mutator calls it after every mutation and crossover.
static protobuf_mutator::libfuzzer::PostProcessorRegistration<json_ast_fuzzer::Node> fix_identifiers_registration = {
    [](json_ast_fuzzer::Node * root, unsigned int seed)
    {
        if (!fixupEnabled())
            return;
        std::mt19937 rng(seed);
        fixIdentifiers(*root, rng);
    }};

extern "C" int LLVMFuzzerInitialize(const int * argc, char *** argv)
{
    if (DB::LocalFuzzerRunner::isMergeRun(*argc, *argv))
        return 0;

    /// First: its `atexit` handler ends the process with `_exit` and must run after the statistics printers
    /// registered below (`atexit` handlers run in reverse order of registration).
    DB::LocalFuzzerRunner::initialize(argc, argv, loadSchema());

    DB::JSONASTFuzzer::initializePipeline("json_ast_sql_execution_fuzzer", argc, argv);
    if (const char * value = getenv("JSON_AST_FUZZER_ORACLE"))
        oracle_enabled = std::string_view(value) != "0";
    if (const char * value = getenv("JSON_AST_FUZZER_ORACLE_LOG"); value && *value)
        oracle_log_path = value;
    if (const char * value = getenv("JSON_AST_FUZZER_LAST_INPUT"); value && *value)
        last_input_path = value;
    atexit(printOracleStats);
    return 0;
}

DEFINE_BINARY_PROTO_FUZZER(const json_ast_fuzzer::Node & original_root)
{
    /// Apply the identifier rewrite to the input itself as well (deterministically, seeded by its
    /// serialized form): seeds mined from the test suite reference their own tables and columns and
    /// would otherwise never resolve against the fixture. Mutations are rewritten by the post-processor
    /// already; a second pass is a no-op for them.
    json_ast_fuzzer::Node root = original_root;
    if (fixupEnabled())
    {
        std::mt19937 rng(static_cast<unsigned int>(std::hash<std::string>{}(original_root.SerializeAsString())));
        fixIdentifiers(root, rng);
    }

    DB::JSONASTFuzzer::PipelineInput input;
    DB::ASTPtr ast = DB::JSONASTFuzzer::generateSQL(root, input);
    if (!ast)
        return;

    auto & stats = DB::JSONASTFuzzer::pipelineStats();
    const Verdict verdict = classify(*ast);
    if (verdict == Verdict::SKIP || overridesResourceLimits(input.sql) || usesExternalTableFunction(input.sql))
    {
        ++stats.execution_skipped;
        return;
    }
    ++stats.executed;
    recordLastInput(input);
    DB::LocalFuzzerRunner::runQuery(input.sql);
    if (verdict == Verdict::MODIFYING)
    {
        ++executed_modifying;
        if (++modifying_since_cleanup >= cleanup_every)
        {
            modifying_since_cleanup = 0;
            cleanupFuzzerObjects();
        }
    }

    if (ast->as<DB::ASTSelectWithUnionQuery>())
        runOracle(input.sql, input.json);
}
