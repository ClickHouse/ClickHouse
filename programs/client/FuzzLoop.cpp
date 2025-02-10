#include <base/scope_guard.h>
#include "Client.h"
#include "Parsers/formatAST.h"

#include <IO/WriteBufferFromOStream.h>
#include <IO/copyData.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTUseQuery.h>

#include <Processors/Transforms/getSourceFromASTInsertQuery.h>

#if USE_BUZZHOUSE
#    include <Client/BuzzHouse/AST/SQLProtoStr.h>
#    include <Client/BuzzHouse/Generator/QueryOracle.h>
#    include <Client/BuzzHouse/Generator/StatementGenerator.h>
#endif

namespace DB
{
namespace Setting
{
extern const SettingsDialect dialect;
}

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int SYNTAX_ERROR;
extern const int TOO_DEEP_RECURSION;
}

std::optional<bool> Client::processFuzzingStep(const String & query_to_execute, const ASTPtr & parsed_query, const bool permissive)
{
    processParsedSingleQuery(query_to_execute, query_to_execute, parsed_query);

    const auto * exception = server_exception ? server_exception.get() : client_exception.get();
    // Sometimes you may get TOO_DEEP_RECURSION from the server,
    // and TOO_DEEP_RECURSION should not fail the fuzzer check.
    if (permissive && have_error && exception->code() == ErrorCodes::TOO_DEEP_RECURSION)
    {
        have_error = false;
        server_exception.reset();
        client_exception.reset();
        return true;
    }

    if (have_error)
    {
        fmt::print(stderr, "Error on processing query '{}': {}\n", parsed_query->formatForErrorMessage(), exception->message());

        // Try to reconnect after errors, for two reasons:
        // 1. We might not have realized that the server died, e.g. if
        //    it sent us a <Fatal> trace and closed connection properly.
        // 2. The connection might have gotten into a wrong state and
        //    the next query will get false positive about
        //    "Unknown packet from server".
        try
        {
            connection->forceConnected(connection_parameters.timeouts);
        }
        catch (...)
        {
            // Just report it, we'll terminate below.
            fmt::print(stderr, "Error while reconnecting to the server: {}\n", getCurrentExceptionMessage(true));

            // The reconnection might fail, but we'll still be connected
            // in the sense of `connection->isConnected() = true`,
            // in case when the requested database doesn't exist.
            // Disconnect manually now, so that the following code doesn't
            // have any doubts, and the connection state is predictable.
            connection->disconnect();
        }
    }

    if (!connection->isConnected())
    {
        // Probably the server is dead because we found an assertion
        // failure. Fail fast.
        fmt::print(stderr, "Lost connection to the server.\n");

        // Print the changed settings because they might be needed to
        // reproduce the error.
        printChangedSettings();

        return permissive; //for BuzzHouse, don't continue on error
    }

    return std::nullopt;
}

/// Returns false when server is not available.
bool Client::processWithFuzzing(const String & full_query)
{
    ASTPtr orig_ast;

    try
    {
        const char * begin = full_query.data();
        orig_ast = parseQuery(
            begin,
            begin + full_query.size(),
            client_context->getSettingsRef(),
            /*allow_multi_statements=*/true);
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::SYNTAX_ERROR && e.code() != ErrorCodes::TOO_DEEP_RECURSION)
            throw;
    }

    if (!orig_ast)
    {
        // Can't continue after a parsing error
        return true;
    }

    // `USE db` should not be executed
    // since this will break every query after `DROP db`
    if (orig_ast->as<ASTUseQuery>())
    {
        return true;
    }

    // Kusto is not a subject for fuzzing (yet)
    if (client_context->getSettingsRef()[Setting::dialect] == DB::Dialect::kusto)
    {
        return true;
    }
    if (auto * q = orig_ast->as<ASTSetQuery>())
    {
        if (auto * set_dialect = q->changes.tryGet("dialect"); set_dialect && set_dialect->safeGet<String>() == "kusto")
            return true;
    }

    // Don't repeat:
    // - INSERT -- Because the tables may grow too big.
    // - CREATE -- Because first we run the unmodified query, it will succeed,
    //             and the subsequent queries will fail.
    //             When we run out of fuzzer errors, it may be interesting to
    //             add fuzzing of create queries that wraps columns into
    //             LowCardinality or Nullable.
    //             Also there are other kinds of create queries such as CREATE
    //             DICTIONARY, we could fuzz them as well.
    // - DROP   -- No point in this (by the same reasons).
    // - SET    -- The time to fuzz the settings has not yet come
    //             (see comments in Client/QueryFuzzer.cpp)
    size_t this_query_runs = query_fuzzer_runs;
    ASTs queries_for_fuzzed_tables;

    if (orig_ast->as<ASTSetQuery>())
    {
        this_query_runs = 1;
    }
    else if (const auto * create = orig_ast->as<ASTCreateQuery>())
    {
        if (QueryFuzzer::isSuitableForFuzzing(*create))
            this_query_runs = create_query_fuzzer_runs;
        else
            this_query_runs = 1;
    }
    else if (const auto * /*insert*/ _ = orig_ast->as<ASTInsertQuery>())
    {
        this_query_runs = 1;
        queries_for_fuzzed_tables = fuzzer.getInsertQueriesForFuzzedTables(full_query);
    }
    else if (const auto * drop = orig_ast->as<ASTDropQuery>())
    {
        this_query_runs = 1;
        queries_for_fuzzed_tables = fuzzer.getDropQueriesForFuzzedTables(*drop);
    }

    String query_to_execute;
    ASTPtr fuzz_base = orig_ast;

    for (size_t fuzz_step = 0; fuzz_step < this_query_runs; ++fuzz_step)
    {
        fmt::print(stderr, "Fuzzing step {} out of {}\n", fuzz_step, this_query_runs);

        ASTPtr ast_to_process;
        try
        {
            WriteBufferFromOwnString dump_before_fuzz;
            fuzz_base->dumpTree(dump_before_fuzz);
            auto base_before_fuzz = fuzz_base->formatForErrorMessage();

            ast_to_process = fuzz_base->clone();

            WriteBufferFromOwnString dump_of_cloned_ast;
            ast_to_process->dumpTree(dump_of_cloned_ast);

            // Run the original query as well.
            if (fuzz_step > 0)
            {
                fuzzer.fuzzMain(ast_to_process);
            }

            auto base_after_fuzz = fuzz_base->formatForErrorMessage();

            // Check that the source AST didn't change after fuzzing. This
            // helps debug AST cloning errors, where the cloned AST doesn't
            // clone all its children, and erroneously points to some source
            // child elements.
            if (base_before_fuzz != base_after_fuzz)
            {
                printChangedSettings();

                fmt::print(
                    stderr,
                    "Base before fuzz: {}\n"
                    "Base after fuzz: {}\n",
                    base_before_fuzz,
                    base_after_fuzz);
                fmt::print(stderr, "Dump before fuzz:\n{}\n", dump_before_fuzz.str());
                fmt::print(stderr, "Dump of cloned AST:\n{}\n", dump_of_cloned_ast.str());
                fmt::print(stderr, "Dump after fuzz:\n");

                WriteBufferFromOStream cerr_buf(std::cerr, 4096);
                fuzz_base->dumpTree(cerr_buf);
                cerr_buf.finalize();

                fmt::print(
                    stderr,
                    "Found error: IAST::clone() is broken for some AST node. This is a bug. The original AST ('dump before fuzz') and its "
                    "cloned copy ('dump of cloned AST') refer to the same nodes, which must never happen. This means that their parent "
                    "node doesn't implement clone() correctly.");

                _exit(1);
            }

            auto fuzzed_text = ast_to_process->formatForErrorMessage();
            if (fuzz_step > 0 && fuzzed_text == base_before_fuzz)
            {
                fmt::print(stderr, "Got boring AST\n");
                continue;
            }

            query_to_execute = ast_to_process->formatForErrorMessage();
            if (auto res = processFuzzingStep(query_to_execute, ast_to_process, true))
                return *res;
        }
        catch (...)
        {
            if (!ast_to_process)
                fmt::print(stderr, "Error while forming new query: {}\n", getCurrentExceptionMessage(true));

            // Some functions (e.g. protocol parsers) don't throw, but
            // set last_exception instead, so we'll also do it here for
            // uniformity.
            // Surprisingly, this is a client exception, because we get the
            // server exception w/o throwing (see onReceiveException()).
            client_exception
                = std::make_unique<Exception>(getCurrentExceptionMessageAndPattern(print_stack_trace), getCurrentExceptionCode());
            have_error = true;
        }

        // The server is still alive, so we're going to continue fuzzing.
        // Determine what we're going to use as the starting AST.
        if (have_error)
        {
            // Query completed with error, keep the previous starting AST.
            // Also discard the exception that we now know to be non-fatal,
            // so that it doesn't influence the exit code.
            server_exception.reset();
            client_exception.reset();
            fuzzer.notifyQueryFailed(ast_to_process);
            have_error = false;
        }
        else if (ast_to_process->formatForErrorMessage().size() > 500)
        {
            // ast too long, start from original ast
            fmt::print(stderr, "Current AST is too long, discarding it and using the original AST as a start\n");
            fuzz_base = orig_ast;
        }
        else
        {
            // fuzz starting from this successful query
            fmt::print(stderr, "Query succeeded, using this AST as a start\n");
            fuzz_base = ast_to_process;
        }
    }

    for (const auto & query : queries_for_fuzzed_tables)
    {
        std::cout << std::endl;
        WriteBufferFromOStream ast_buf(std::cout, 4096);
        formatAST(*query, ast_buf, false /*highlight*/);
        ast_buf.finalize();
        if (const auto * insert = query->as<ASTInsertQuery>())
        {
            /// For inserts with data it's really useful to have the data itself available in the logs, as formatAST doesn't print it
            if (insert->hasInlinedData())
            {
                String bytes;
                {
                    auto read_buf = getReadBufferFromASTInsertQuery(query);
                    WriteBufferFromString write_buf(bytes);
                    copyData(*read_buf, write_buf);
                }
                std::cout << std::endl << bytes;
            }
        }
        std::cout << std::endl << std::endl;

        try
        {
            query_to_execute = query->formatForErrorMessage();
            if (auto res = processFuzzingStep(query_to_execute, query, false))
                return *res;
        }
        catch (...)
        {
            client_exception
                = std::make_unique<Exception>(getCurrentExceptionMessageAndPattern(print_stack_trace), getCurrentExceptionCode());
            have_error = true;
        }

        if (have_error)
        {
            server_exception.reset();
            client_exception.reset();
            fuzzer.notifyQueryFailed(query);
            have_error = false;
        }
    }

    return true;
}

#if USE_BUZZHOUSE

bool Client::logAndProcessQuery(std::ofstream & outf, const String & full_query)
{
    outf << full_query << std::endl;
    return processTextAsSingleQuery(full_query);
}

bool Client::processBuzzHouseQuery(const String & full_query)
{
    bool server_up = true;
    ASTPtr orig_ast;

    have_error = false;
    try
    {
        const char * begin = full_query.data();

        if ((orig_ast = parseQuery(begin, begin + full_query.size(), client_context->getSettingsRef(), false)))
        {
            String query_to_execute = orig_ast->formatForAnything();
            const auto res = processFuzzingStep(query_to_execute, orig_ast, false);
            server_up &= res.value_or(true);
        }
        else
        {
            have_error = true;
        }
    }
    catch (...)
    {
        // Some functions (e.g. protocol parsers) don't throw, but
        // set last_exception instead, so we'll also do it here for
        // uniformity.
        // Surprisingly, this is a client exception, because we get the
        // server exception w/o throwing (see onReceiveException()).
        server_up &= connection->isConnected();
        client_exception = std::make_unique<Exception>(getCurrentExceptionMessageAndPattern(print_stack_trace), getCurrentExceptionCode());
        have_error = true;
    }
    if (have_error)
    {
        // Query completed with error, keep the previous starting AST.
        // Also discard the exception that we now know to be non-fatal,
        // so that it doesn't influence the exit code.
        server_exception.reset();
        client_exception.reset();
    }
    return server_up;
}

using sighandler_t = void (*)(int);
sighandler_t volatile prev_signal = nullptr;
std::sig_atomic_t volatile buzz_done = 0;

static void finishBuzzHouse(int num)
{
    if (prev_signal)
    {
        prev_signal(num);
    }
    buzz_done = 1;
}

/// Returns false when server is not available.
bool Client::buzzHouse()
{
    bool server_up = true;
    String full_query;
    BuzzHouse::FuzzConfig fc(this, buzz_house_options_path);
    BuzzHouse::ExternalIntegrations ei(fc);

    /// Set time to run, but what if a query runs for too long?
    buzz_done = 0;
    if (fc.time_to_run > 0)
    {
        prev_signal = std::signal(SIGALRM, finishBuzzHouse);
    }
    alarm(fc.time_to_run);
    full_query.reserve(8192);
    if (fc.read_log)
    {
        std::ifstream infile(fc.log_path);

        while (server_up && !buzz_done && std::getline(infile, full_query))
        {
            server_up &= processBuzzHouseQuery(full_query);
            full_query.resize(0);
        }
    }
    else
    {
        String full_query2;
        std::vector<BuzzHouse::SQLQuery> peer_queries;
        bool first = true;
        bool replica_setup = true;
        bool has_cloud_features = true;
        BuzzHouse::RandomGenerator rg(fc.seed);
        std::ofstream outf(fc.log_path, std::ios::out | std::ios::trunc);
        BuzzHouse::SQLQuery sq1;
        BuzzHouse::SQLQuery sq2;
        BuzzHouse::SQLQuery sq3;
        BuzzHouse::SQLQuery sq4;
        uint32_t nsuccessfull_create_database = 0;
        uint32_t total_create_database_tries = 0;
        const uint32_t max_initial_databases = std::min(UINT32_C(3), fc.max_databases);
        uint32_t nsuccessfull_create_table = 0;
        uint32_t total_create_table_tries = 0;
        const uint32_t max_initial_tables = std::min(UINT32_C(10), fc.max_tables);

        GOOGLE_PROTOBUF_VERIFY_VERSION;

        has_cloud_features &= processTextAsSingleQuery("DROP DATABASE IF EXISTS fuzztest;");
        has_cloud_features &= processTextAsSingleQuery("CREATE DATABASE fuzztest Engine=Shared;");
        std::cout << "Cloud features " << (has_cloud_features ? "" : "not ") << "detected" << std::endl;
        replica_setup &= processTextAsSingleQuery("CREATE TABLE tx (c0 Int) Engine=ReplicatedMergeTree() ORDER BY tuple();");
        std::cout << "Replica setup " << (replica_setup ? "" : "not ") << "detected" << std::endl;
        auto u = processTextAsSingleQuery("DROP TABLE IF EXISTS tx;");
        UNUSED(u);
        auto v = processTextAsSingleQuery("DROP DATABASE IF EXISTS fuzztest;");
        UNUSED(v);

        outf << "--Session seed: " << rg.getSeed() << std::endl;
        DB::Strings defaultSettings
            = {"engine_file_truncate_on_insert",
               "allow_aggregate_partitions_independently",
               "allow_archive_path_syntax",
               "allow_asynchronous_read_from_io_pool_for_merge_tree",
               "allow_changing_replica_until_first_data_packet",
               "allow_create_index_without_type",
               "allow_custom_error_code_in_throwif",
               "allow_ddl",
               "allow_deprecated_database_ordinary",
               "allow_deprecated_error_prone_window_functions",
               "allow_deprecated_snowflake_conversion_functions",
               "allow_deprecated_syntax_for_merge_tree",
               "allow_distributed_ddl",
               "allow_drop_detached",
               "allow_execute_multiif_columnar",
               "allow_experimental_analyzer",
               "allow_experimental_codecs",
               "allow_experimental_database_materialized_mysql",
               "allow_experimental_database_materialized_postgresql",
               "allow_experimental_dynamic_type",
               "allow_experimental_full_text_index",
               "allow_experimental_funnel_functions",
               "allow_experimental_hash_functions",
               "allow_experimental_inverted_index",
               "allow_experimental_join_right_table_sorting",
               "allow_experimental_json_type",
               "allow_experimental_kafka_offsets_storage_in_keeper",
               "allow_experimental_live_view",
               "allow_experimental_materialized_postgresql_table",
               "allow_experimental_nlp_functions",
               "allow_experimental_parallel_reading_from_replicas",
               "allow_experimental_query_deduplication",
               "allow_experimental_shared_set_join",
               "allow_experimental_statistics",
               "allow_experimental_time_series_table",
               "allow_experimental_variant_type",
               "allow_experimental_vector_similarity_index",
               "allow_experimental_window_view",
               "allow_get_client_http_header",
               "allow_hyperscan",
               "allow_introspection_functions",
               "allow_materialized_view_with_bad_select",
               "allow_named_collection_override_by_default",
               "allow_non_metadata_alters",
               "allow_nonconst_timezone_arguments",
               "allow_nondeterministic_mutations",
               "allow_nondeterministic_optimize_skip_unused_shards",
               "allow_prefetched_read_pool_for_local_filesystem",
               "allow_prefetched_read_pool_for_remote_filesystem",
               "allow_push_predicate_when_subquery_contains_with",
               "allow_reorder_prewhere_conditions",
               "allow_settings_after_format_in_insert",
               "allow_simdjson",
               "allow_statistics_optimize",
               "allow_suspicious_codecs",
               "allow_suspicious_fixed_string_types",
               "allow_suspicious_indices",
               "allow_suspicious_low_cardinality_types",
               "allow_suspicious_primary_key",
               "allow_suspicious_ttl_expressions",
               "allow_suspicious_variant_types",
               "allow_suspicious_types_in_group_by",
               "allow_suspicious_types_in_order_by",
               "allow_unrestricted_reads_from_keeper",
               "enable_analyzer",
               "enable_zstd_qat_codec",
               "type_json_skip_duplicated_paths",
               "allow_experimental_database_iceberg",
               "allow_experimental_bfloat16_type",
               "allow_not_comparable_types_in_order_by",
               "allow_not_comparable_types_in_comparison_functions"};
        defaultSettings.emplace_back(rg.nextBool() ? "s3_truncate_on_insert" : "s3_create_new_file_on_insert");

        full_query.resize(0);
        for (const auto & entry : defaultSettings)
        {
            full_query += fmt::format("{}{} = 1", first ? "" : ", ", entry);
            first = false;
        }
        auto w = logAndProcessQuery(outf, fmt::format("SET {};", full_query));
        UNUSED(w);
        if (ei.hasClickHouseExtraServerConnection())
        {
            ei.setDefaultSettings(BuzzHouse::PeerTableDatabase::ClickHouse, defaultSettings);
        }

        /// Load server configurations for the fuzzer
        fc.loadServerConfigurations();
        loadFuzzerServerSettings(fc);
        loadFuzzerTableSettings(fc);
        loadFuzzerOracleSettings(fc);
        loadSystemTables(fc);

        full_query2.reserve(8192);
        BuzzHouse::StatementGenerator gen(fc, ei, has_cloud_features, replica_setup);
        BuzzHouse::QueryOracle qo(fc);
        while (server_up && !buzz_done)
        {
            sq1.Clear();
            full_query.resize(0);

            if (total_create_database_tries < 10 && nsuccessfull_create_database < max_initial_databases)
            {
                gen.generateNextCreateDatabase(rg, sq1.mutable_inner_query()->mutable_create_database());
                BuzzHouse::SQLQueryToString(full_query, sq1);
                outf << full_query << std::endl;
                server_up &= processBuzzHouseQuery(full_query);

                gen.updateGenerator(sq1, ei, !have_error);
                nsuccessfull_create_database += (have_error ? 0 : 1);
                total_create_database_tries++;
            }
            else if (
                gen.collectionHas<std::shared_ptr<BuzzHouse::SQLDatabase>>(gen.attached_databases) && total_create_table_tries < 50
                && nsuccessfull_create_table < max_initial_tables)
            {
                gen.generateNextCreateTable(rg, sq1.mutable_inner_query()->mutable_create_table());
                BuzzHouse::SQLQueryToString(full_query, sq1);
                outf << full_query << std::endl;
                server_up &= processBuzzHouseQuery(full_query);

                gen.updateGenerator(sq1, ei, !have_error);
                nsuccessfull_create_table += (have_error ? 0 : 1);
                total_create_table_tries++;
            }
            else
            {
                const uint32_t correctness_oracle = 30;
                const uint32_t settings_oracle = 30;
                const uint32_t dump_oracle = 15
                    * static_cast<uint32_t>(fc.use_dump_table_oracle
                                            && gen.collectionHas<BuzzHouse::SQLTable>(gen.attached_tables_for_dump_table_oracle));
                const uint32_t peer_oracle
                    = 30 * static_cast<uint32_t>(gen.collectionHas<BuzzHouse::SQLTable>(gen.attached_tables_for_table_peer_oracle));
                const uint32_t run_query = 910;
                const uint32_t prob_space = correctness_oracle + settings_oracle + dump_oracle + peer_oracle + run_query;
                std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
                const uint32_t nopt = next_dist(rg.generator);

                if (nopt < (correctness_oracle + settings_oracle + dump_oracle + peer_oracle + 1))
                {
                    qo.resetOracleValues();
                }
                if (correctness_oracle && nopt < (correctness_oracle + 1))
                {
                    /// Correctness test query
                    qo.generateCorrectnessTestFirstQuery(rg, gen, sq1);
                    BuzzHouse::SQLQueryToString(full_query, sq1);
                    outf << full_query << std::endl;
                    server_up &= processBuzzHouseQuery(full_query);
                    qo.processFirstOracleQueryResult(!have_error, ei);

                    sq2.Clear();
                    full_query.resize(0);
                    qo.generateCorrectnessTestSecondQuery(sq1, sq2);
                    BuzzHouse::SQLQueryToString(full_query, sq2);
                    outf << full_query << std::endl;
                    server_up &= processBuzzHouseQuery(full_query);
                    qo.processSecondOracleQueryResult(!have_error, ei, "Correctness query");
                }
                else if (settings_oracle && nopt < (correctness_oracle + settings_oracle + 1))
                {
                    /// Test running query with different settings
                    qo.generateFirstSetting(rg, sq1);
                    BuzzHouse::SQLQueryToString(full_query, sq1);
                    outf << full_query << std::endl;
                    server_up &= processBuzzHouseQuery(full_query);
                    qo.setIntermediateStepSuccess(!have_error);

                    sq2.Clear();
                    full_query2.resize(0);
                    qo.generateOracleSelectQuery(rg, BuzzHouse::PeerQuery::None, gen, sq2);
                    BuzzHouse::SQLQueryToString(full_query2, sq2);
                    outf << full_query2 << std::endl;
                    server_up &= processBuzzHouseQuery(full_query2);
                    qo.processFirstOracleQueryResult(!have_error, ei);

                    sq3.Clear();
                    full_query.resize(0);
                    qo.generateSecondSetting(sq1, sq3);
                    BuzzHouse::SQLQueryToString(full_query, sq3);
                    outf << full_query << std::endl;
                    server_up &= processBuzzHouseQuery(full_query);
                    qo.setIntermediateStepSuccess(!have_error);

                    outf << full_query2 << std::endl;
                    server_up &= processBuzzHouseQuery(full_query2);
                    qo.processSecondOracleQueryResult(!have_error, ei, "Multi setting query");
                }
                else if (dump_oracle && nopt < (correctness_oracle + settings_oracle + dump_oracle + 1))
                {
                    const BuzzHouse::SQLTable & t
                        = rg.pickRandomlyFromVector(gen.filterCollection<BuzzHouse::SQLTable>(gen.attached_tables_for_dump_table_oracle));

                    /// Test in and out formats
                    full_query2.resize(0);
                    qo.dumpTableContent(rg, gen, t, sq1);
                    BuzzHouse::SQLQueryToString(full_query2, sq1);
                    outf << full_query2 << std::endl;
                    server_up &= processBuzzHouseQuery(full_query2);
                    qo.processFirstOracleQueryResult(!have_error, ei);

                    sq2.Clear();
                    qo.generateExportQuery(rg, gen, t, sq2);
                    BuzzHouse::SQLQueryToString(full_query, sq2);
                    outf << full_query << std::endl;
                    server_up &= processBuzzHouseQuery(full_query);
                    qo.setIntermediateStepSuccess(!have_error);

                    sq3.Clear();
                    full_query.resize(0);
                    qo.generateClearQuery(t, sq3);
                    BuzzHouse::SQLQueryToString(full_query, sq3);
                    outf << full_query << std::endl;
                    server_up &= processBuzzHouseQuery(full_query);
                    qo.setIntermediateStepSuccess(!have_error);

                    sq4.Clear();
                    full_query.resize(0);
                    qo.generateImportQuery(gen, t, sq2, sq4);
                    BuzzHouse::SQLQueryToString(full_query, sq4);
                    outf << full_query << std::endl;
                    server_up &= processBuzzHouseQuery(full_query);
                    qo.setIntermediateStepSuccess(!have_error);

                    outf << full_query2 << std::endl;
                    server_up &= processBuzzHouseQuery(full_query2);
                    qo.processSecondOracleQueryResult(!have_error, ei, "Dump and read table");
                }
                else if (peer_oracle && nopt < (correctness_oracle + settings_oracle + dump_oracle + peer_oracle + 1))
                {
                    /// Test results with peer tables
                    bool has_success = false;
                    BuzzHouse::PeerQuery nquery
                        = ((!ei.hasMySQLConnection() && !ei.hasPostgreSQLConnection() && !ei.hasSQLiteConnection()) || rg.nextBool())
                            && gen.collectionHas<BuzzHouse::SQLTable>(gen.attached_tables_for_clickhouse_table_peer_oracle)
                        ? BuzzHouse::PeerQuery::ClickHouseOnly
                        : BuzzHouse::PeerQuery::AllPeers;
                    const bool clickhouse_only = nquery == BuzzHouse::PeerQuery::ClickHouseOnly;

                    sq1.Clear();
                    sq2.Clear();
                    qo.generateOracleSelectQuery(rg, nquery, gen, sq1);
                    qo.replaceQueryWithTablePeers(rg, sq1, gen, peer_queries, sq2);

                    if (clickhouse_only)
                    {
                        ei.replicateSettings(BuzzHouse::PeerTableDatabase::ClickHouse);
                    }
                    qo.truncatePeerTables(gen);
                    for (const auto & entry : peer_queries)
                    {
                        full_query2.resize(0);
                        BuzzHouse::SQLQueryToString(full_query2, entry);
                        outf << full_query2 << std::endl;
                        server_up &= processBuzzHouseQuery(full_query2);
                        qo.setIntermediateStepSuccess(!have_error);
                    }
                    qo.optimizePeerTables(gen);

                    full_query.resize(0);
                    BuzzHouse::SQLQueryToString(full_query, sq1);
                    outf << full_query << std::endl;
                    server_up &= processBuzzHouseQuery(full_query);
                    qo.processFirstOracleQueryResult(!have_error, ei);

                    full_query2.resize(0);
                    BuzzHouse::SQLQueryToString(full_query2, sq2);
                    outf << full_query2 << std::endl;
                    if (clickhouse_only)
                    {
                        has_success = ei.performQuery(BuzzHouse::PeerTableDatabase::ClickHouse, full_query2);
                    }
                    else
                    {
                        server_up &= processBuzzHouseQuery(full_query2);
                        has_success = !have_error;
                    }
                    qo.processSecondOracleQueryResult(has_success, ei, "Peer table query");
                }
                else if (run_query && nopt < (correctness_oracle + settings_oracle + dump_oracle + peer_oracle + run_query + 1))
                {
                    gen.generateNextStatement(rg, sq1);
                    BuzzHouse::SQLQueryToString(full_query, sq1);
                    outf << full_query << std::endl;
                    server_up &= processBuzzHouseQuery(full_query);
                    gen.updateGenerator(sq1, ei, !have_error);
                }
                else
                {
                    chassert(0);
                }
            }
        }
    }
    return server_up;
}
#else
bool Client::buzzHouse()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Clickhouse was compiled without BuzzHouse enabled");
}
#endif

}
