#include <base/scope_guard.h>
#include <Client.h>

#include <Core/Settings.h>

#include <IO/WriteBufferFromOStream.h>
#include <IO/copyData.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ParserOptimizeQuery.h>

#include <Processors/Transforms/getSourceFromASTInsertQuery.h>

#if USE_BUZZHOUSE
#    include <Client/BuzzHouse/AST/SQLProtoStr.h>
#    include <Client/BuzzHouse/Generator/FuzzConfig.h>
#    include <Client/BuzzHouse/Generator/QueryOracle.h>
#    include <Client/BuzzHouse/Generator/StatementGenerator.h>
namespace BuzzHouse
{
extern void loadFuzzerServerSettings(const FuzzConfig & fc);
}
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
extern const int BUZZHOUSE;
using ErrorCode = int;
extern std::string_view getName(ErrorCode error_code);
}

bool Client::tryToReconnect(const uint32_t max_reconnection_attempts, const uint32_t time_to_sleep_between_reconnects)
{
    chassert(max_reconnection_attempts);
    if (!connection->isConnected())
    {
        // Try to reconnect after errors, for two reasons:
        // 1. We might not have realized that the server died, e.g. if
        //    it sent us a <Fatal> trace and closed connection properly.
        // 2. The connection might have gotten into a wrong state and
        //    the next query will get false positive about
        //    "Unknown packet from server".
        for (uint32_t i = 0; i < max_reconnection_attempts; i++)
        {
            try
            {
                connection->forceConnected(connection_parameters.timeouts);
                break;
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
                if (i < max_reconnection_attempts - 1)
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(time_to_sleep_between_reconnects));
                }
            }
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

        return false;
    }
    return true;
}

bool Client::processASTFuzzerStep(const String & query_to_execute, const ASTPtr & parsed_query)
{
    bool async_insert = false;
    processParsedSingleQuery(query_to_execute, parsed_query, async_insert);

    const auto * exception = server_exception ? server_exception.get() : client_exception.get();
    // Sometimes you may get TOO_DEEP_RECURSION from the server,
    // and TOO_DEEP_RECURSION should not fail the fuzzer check.
    if (have_error && exception->code() == ErrorCodes::TOO_DEEP_RECURSION)
    {
        have_error = false;
        server_exception.reset();
        client_exception.reset();
        return true;
    }
    if (have_error)
    {
        fmt::print(stderr, "Error on processing query '{}': {}\n", parsed_query->formatForErrorMessage(), exception->message());
    }
    return tryToReconnect(1, 10);
}

/// Returns false when server is not available.
bool Client::processWithASTFuzzer(std::string_view full_query)
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
        queries_for_fuzzed_tables = fuzzer.getQueriesForFuzzedTables<ASTInsertQuery, ParserInsertQuery>(full_query);
    }
    else if (const auto * /*optimize*/ _ = orig_ast->as<ASTOptimizeQuery>())
    {
        this_query_runs = 1;
        queries_for_fuzzed_tables = fuzzer.getQueriesForFuzzedTables<ASTOptimizeQuery, ParserOptimizeQuery>(full_query);
    }
    else if (const auto * drop = orig_ast->as<ASTDropQuery>())
    {
        this_query_runs = 1;
        queries_for_fuzzed_tables = fuzzer.getDropQueriesForFuzzedTables(*drop);
    }

    String query_to_execute;
    ASTPtr fuzz_base = orig_ast;
#if USE_BUZZHOUSE
    BuzzHouse::PerformanceResult res1;
    BuzzHouse::PerformanceResult res2;
    const bool can_compare = fuzz_config && (fuzz_config->measure_performance || fuzz_config->compare_success_results)
        && external_integrations && external_integrations->hasClickHouseExtraServerConnection();
    const bool try_measure_performance_in_loop
        = can_compare && fuzz_config->measure_performance && (orig_ast->as<ASTSelectQuery>() || orig_ast->as<ASTSelectWithUnionQuery>());
    auto insert_into = std::make_shared<ASTInsertQuery>();
    insert_into->table_function = makeASTFunction("file", std::make_shared<ASTLiteral>("/dev/null"), std::make_shared<ASTLiteral>("CSV"));
#endif

    for (size_t fuzz_step = 0; fuzz_step < this_query_runs; ++fuzz_step)
    {
#if USE_BUZZHOUSE
        bool peer_success = true;
        bool measure_performance = try_measure_performance_in_loop;
        ASTPtr old_settings = nullptr;
        ASTSelectQuery * select_query = nullptr;
#endif
        fmt::print(stderr, "Fuzzing step {} out of {}\n", fuzz_step, this_query_runs);

        ASTPtr ast_to_process;
        try
        {
            auto base_before_fuzz = fuzz_base->formatForErrorMessage();

            ast_to_process = fuzz_base->clone();
            // Run the original query as well.
            if (fuzz_step > 0)
            {
                fuzzer.fuzzMain(ast_to_process);
            }

            query_to_execute = ast_to_process->formatForErrorMessage();
            if (fuzz_step > 0 && query_to_execute == base_before_fuzz)
            {
                fmt::print(stderr, "Got boring AST\n");
                continue;
            }

#if USE_BUZZHOUSE
            if (measure_performance)
            {
                /// Add tag to find query later on
                auto * union_sel = ast_to_process->as<ASTSelectWithUnionQuery>();

                if ((select_query
                     = typeid_cast<ASTSelectQuery *>(union_sel ? union_sel->list_of_selects->children[0].get() : ast_to_process.get())))
                {
                    if (!select_query->settings())
                    {
                        auto settings_query = std::make_shared<ASTSetQuery>();
                        SettingsChanges settings_changes;
                        settings_changes.setSetting("log_comment", "measure_performance");

                        /// Sometimes change settings
                        fuzzer.getRandomSettings(settings_changes);
                        settings_query->changes = std::move(settings_changes);
                        settings_query->is_standalone = false;
                        select_query->setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(settings_query));
                    }
                    else
                    {
                        auto * set_query = select_query->settings()->as<ASTSetQuery>();

                        old_settings = set_query->clone();
                        set_query->changes.setSetting("log_comment", "measure_performance");
                        fuzzer.getRandomSettings(set_query->changes);
                    }
                    /// Dump into /dev/null, we are not interested in sending the results back to the client
                    insert_into->select = ast_to_process;
                    ast_to_process = insert_into;
                    query_to_execute = ast_to_process->formatForErrorMessage();
                }
                else
                {
                    measure_performance = false;
                }
            }
#endif
#if 0
            /// Somehow this code is not running
            /// `base_after_fuzz` should format from `ast_to_process`
            WriteBufferFromOwnString dump_before_fuzz;
            fuzz_base->dumpTree(dump_before_fuzz);
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
#endif

            fmt::print(stdout, "Dump of fuzzed AST:\n{}\n", query_to_execute);
            const auto res = processASTFuzzerStep(query_to_execute, ast_to_process);
            if (!res)
                return res;

#if USE_BUZZHOUSE
            if (measure_performance)
            {
                /// Don't keep insert into in the AST
                ast_to_process = insert_into->select;
                /// Don't keep performance settings in AST
                if (select_query && old_settings)
                {
                    select_query->setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(old_settings));
                }
                else if (select_query)
                {
                    select_query->setExpression(ASTSelectQuery::Expression::SETTINGS, {});
                }
            }
#endif
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

#if USE_BUZZHOUSE
        measure_performance &= !have_error;
        if (measure_performance)
        {
            measure_performance &= external_integrations->getPerformanceMetricsForLastQuery(BuzzHouse::PeerTableDatabase::None, res1);
            /// Replicate settings, so both servers have same configuration
            external_integrations->replicateSettings(BuzzHouse::PeerTableDatabase::ClickHouse);
        }
        if (can_compare)
        {
            /// Always run query on peer server
            fmt::print(stdout, "Running query on peer server\n");
            peer_success &= external_integrations->performQuery(BuzzHouse::PeerTableDatabase::ClickHouse, query_to_execute);
        }
        if (can_compare && fuzz_config->compare_success_results && peer_success != !have_error)
        {
            throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "AST Fuzzer: The peer server got a different success result");
        }
        if (measure_performance)
        {
            measure_performance
                &= peer_success && external_integrations->getPerformanceMetricsForLastQuery(BuzzHouse::PeerTableDatabase::ClickHouse, res2);
            if (measure_performance)
            {
                fuzz_config->comparePerformanceResults("AST fuzzer", res1, res2);
            }
        }
#endif
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
        else if (ast_to_process->formatForErrorMessage().size() > 2000)
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
        std::cout << query->formatWithSecretsOneLine() << std::endl;
        if (const auto * insert = query->as<ASTInsertQuery>())
        {
            /// For inserts with data it's really useful to have the data itself available in the logs
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
            const auto res = processASTFuzzerStep(query_to_execute, query);
            if (!res)
                return res;
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
#if USE_BUZZHOUSE
        if (can_compare)
        {
            const auto u = external_integrations->performQuery(BuzzHouse::PeerTableDatabase::ClickHouse, query_to_execute);
            UNUSED(u);
        }
#endif
    }

    return true;
}

#if USE_BUZZHOUSE

bool Client::processBuzzHouseQuery(const String & full_query)
{
    bool server_up = true;

    processQueryText(full_query);
    if (error_code > 0)
    {
        if (fuzz_config->disallowed_error_codes.find(error_code) != fuzz_config->disallowed_error_codes.end())
        {
            throw Exception(ErrorCodes::BUZZHOUSE, "Found disallowed error code {} - {}", error_code, ErrorCodes::getName(error_code));
        }
        server_up &= tryToReconnect(fuzz_config->max_reconnection_attempts, fuzz_config->time_to_sleep_between_reconnects);
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

    /// Set time to run, but what if a query runs for too long?
    buzz_done = 0;
    if (fuzz_config->time_to_run > 0)
    {
        prev_signal = std::signal(SIGALRM, finishBuzzHouse);
    }
    alarm(fuzz_config->time_to_run);
    full_query.reserve(8192);
    if (fuzz_config->read_log)
    {
        std::ifstream infile(fuzz_config->log_path);

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
        bool replica_setup = true;
        bool has_cloud_features = true;
        BuzzHouse::RandomGenerator rg(fuzz_config->seed, fuzz_config->min_string_length, fuzz_config->max_string_length);
        BuzzHouse::SQLQuery sq1;
        BuzzHouse::SQLQuery sq2;
        BuzzHouse::SQLQuery sq3;
        BuzzHouse::SQLQuery sq4;
        uint32_t nsuccessfull_create_database = 0;
        uint32_t total_create_database_tries = 0;
        const uint32_t max_initial_databases = std::min(UINT32_C(3), fuzz_config->max_databases);
        uint32_t nsuccessfull_create_table = 0;
        uint32_t total_create_table_tries = 0;
        const uint32_t max_initial_tables = std::min(UINT32_C(10), fuzz_config->max_tables);

        GOOGLE_PROTOBUF_VERIFY_VERSION;

        has_cloud_features &= processTextAsSingleQuery("DROP DATABASE IF EXISTS fuzztest;");
        has_cloud_features &= processTextAsSingleQuery("CREATE DATABASE fuzztest Engine=Shared;");
        std::cout << "Cloud features " << (has_cloud_features ? "" : "not ") << "detected" << std::endl;
        replica_setup &= processTextAsSingleQuery("CREATE TABLE tx (c0 Int) Engine=ReplicatedMergeTree() ORDER BY tuple();");
        std::cout << "Replica setup " << (replica_setup ? "" : "not ") << "detected" << std::endl;
        const auto u = processTextAsSingleQuery("DROP TABLE IF EXISTS tx;");
        UNUSED(u);
        const auto v = processTextAsSingleQuery("DROP DATABASE IF EXISTS fuzztest;");
        UNUSED(v);

        fuzz_config->outf << "--Session seed: " << rg.getSeed() << std::endl;
        /// Load server configurations for the fuzzer
        fuzz_config->loadServerConfigurations();
        loadFuzzerServerSettings(*fuzz_config);
        loadFuzzerTableSettings(*fuzz_config);
        loadSystemTables(*fuzz_config);

        full_query2.reserve(8192);
        BuzzHouse::StatementGenerator gen(*fuzz_config, *external_integrations, has_cloud_features, replica_setup);
        BuzzHouse::QueryOracle qo(*fuzz_config);
        while (server_up && !buzz_done)
        {
            sq1.Clear();
            full_query.resize(0);

            if (total_create_database_tries < 10 && nsuccessfull_create_database < max_initial_databases)
            {
                gen.generateNextCreateDatabase(
                    rg, sq1.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_create_database());
                BuzzHouse::SQLQueryToString(full_query, sq1);
                fuzz_config->outf << full_query << std::endl;
                server_up &= processBuzzHouseQuery(full_query);

                gen.updateGenerator(sq1, *external_integrations, !have_error);
                nsuccessfull_create_database += (have_error ? 0 : 1);
                total_create_database_tries++;
            }
            else if (
                gen.collectionHas<std::shared_ptr<BuzzHouse::SQLDatabase>>(gen.attached_databases) && total_create_table_tries < 50
                && nsuccessfull_create_table < max_initial_tables)
            {
                gen.generateNextCreateTable(
                    rg, false, sq1.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_create_table());
                BuzzHouse::SQLQueryToString(full_query, sq1);
                fuzz_config->outf << full_query << std::endl;
                server_up &= processBuzzHouseQuery(full_query);

                gen.updateGenerator(sq1, *external_integrations, !have_error);
                nsuccessfull_create_table += (have_error ? 0 : 1);
                total_create_table_tries++;
            }
            else
            {
                const uint32_t correctness_oracle = 30;
                const uint32_t settings_oracle = 30;
                const uint32_t dump_oracle = 30
                    * static_cast<uint32_t>(fuzz_config->use_dump_table_oracle > 0
                                            && gen.collectionHas<BuzzHouse::SQLTable>(gen.attached_tables_to_test_format));
                const uint32_t peer_oracle
                    = 30 * static_cast<uint32_t>(gen.collectionHas<BuzzHouse::SQLTable>(gen.attached_tables_for_table_peer_oracle));
                const uint32_t restart_client = 1 * static_cast<uint32_t>(fuzz_config->allow_client_restarts);
                const uint32_t run_query = 910;
                const uint32_t prob_space = correctness_oracle + settings_oracle + dump_oracle + peer_oracle + restart_client + run_query;
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
                    fuzz_config->outf << full_query << std::endl;
                    server_up &= processBuzzHouseQuery(full_query);
                    qo.processFirstOracleQueryResult(!have_error, *external_integrations);

                    sq2.Clear();
                    full_query.resize(0);
                    qo.generateCorrectnessTestSecondQuery(sq1, sq2);
                    BuzzHouse::SQLQueryToString(full_query, sq2);
                    fuzz_config->outf << full_query << std::endl;
                    server_up &= processBuzzHouseQuery(full_query);
                    qo.processSecondOracleQueryResult(!have_error, *external_integrations, "Correctness query");
                }
                else if (settings_oracle && nopt < (correctness_oracle + settings_oracle + 1))
                {
                    /// Test running query with different settings, but some times, call system commands
                    const bool use_settings = qo.generateFirstSetting(rg, sq1);

                    if (use_settings)
                    {
                        /// Run query only when something was generated
                        BuzzHouse::SQLQueryToString(full_query, sq1);
                        fuzz_config->outf << full_query << std::endl;
                        server_up &= processBuzzHouseQuery(full_query);
                        qo.setIntermediateStepSuccess(!have_error);
                    }

                    sq2.Clear();
                    full_query2.resize(0);
                    qo.generateOracleSelectQuery(rg, BuzzHouse::PeerQuery::None, gen, sq2);
                    BuzzHouse::SQLQueryToString(full_query2, sq2);
                    fuzz_config->outf << full_query2 << std::endl;
                    server_up &= processBuzzHouseQuery(full_query2);
                    qo.processFirstOracleQueryResult(!have_error, *external_integrations);

                    sq3.Clear();
                    full_query.resize(0);
                    qo.generateSecondSetting(rg, gen, use_settings, sq1, sq3);
                    BuzzHouse::SQLQueryToString(full_query, sq3);
                    fuzz_config->outf << full_query << std::endl;
                    server_up &= processBuzzHouseQuery(full_query);
                    qo.setIntermediateStepSuccess(!have_error);

                    fuzz_config->outf << full_query2 << std::endl;
                    server_up &= processBuzzHouseQuery(full_query2);
                    qo.processSecondOracleQueryResult(!have_error, *external_integrations, "Multi setting query");
                }
                else if (dump_oracle && nopt < (correctness_oracle + settings_oracle + dump_oracle + 1))
                {
                    /// Test in and out formats
                    /// When testing content, we have to export and import to the same table
                    const bool test_content = fuzz_config->use_dump_table_oracle > 1 && rg.nextBool()
                        && gen.collectionHas<BuzzHouse::SQLTable>(gen.attached_tables_to_compare_content);
                    const auto & t1 = rg.pickRandomly(gen.filterCollection<BuzzHouse::SQLTable>(
                        test_content ? gen.attached_tables_to_compare_content : gen.attached_tables_to_test_format));
                    const auto & t2 = test_content
                        ? t1
                        : rg.pickRandomly(gen.filterCollection<BuzzHouse::SQLTable>(gen.attached_tables_to_test_format));
                    const bool use_optimize = test_content && t1.get().supportsOptimize() && rg.nextMediumNumber() < 21;

                    if (test_content)
                    {
                        /// Dump table content and read it later to look for correctness
                        full_query2.resize(0);
                        qo.dumpTableContent(rg, gen, t1, sq1);
                        BuzzHouse::SQLQueryToString(full_query2, sq1);
                        fuzz_config->outf << full_query2 << std::endl;
                        server_up &= processBuzzHouseQuery(full_query2);
                        qo.processFirstOracleQueryResult(!have_error, *external_integrations);
                    }

                    if (!use_optimize)
                    {
                        sq2.Clear();
                        qo.generateExportQuery(rg, gen, test_content, t1, sq2);
                        BuzzHouse::SQLQueryToString(full_query, sq2);
                        fuzz_config->outf << full_query << std::endl;
                        server_up &= processBuzzHouseQuery(full_query);
                    }

                    if (test_content)
                    {
                        /// The intermediate step could be either clearing or optimizing the table
                        qo.setIntermediateStepSuccess(!have_error);

                        sq3.Clear();
                        full_query.resize(0);
                        qo.dumpOracleIntermediateStep(rg, gen, t1, use_optimize, sq3);
                        BuzzHouse::SQLQueryToString(full_query, sq3);
                        fuzz_config->outf << full_query << std::endl;
                        server_up &= processBuzzHouseQuery(full_query);
                        qo.setIntermediateStepSuccess(!have_error);
                    }

                    if (!use_optimize)
                    {
                        sq4.Clear();
                        full_query.resize(0);
                        qo.generateImportQuery(rg, gen, t2, sq2, sq4);
                        BuzzHouse::SQLQueryToString(full_query, sq4);
                        fuzz_config->outf << full_query << std::endl;
                        server_up &= processBuzzHouseQuery(full_query);
                    }

                    if (test_content)
                    {
                        qo.setIntermediateStepSuccess(!have_error);

                        fuzz_config->outf << full_query2 << std::endl;
                        server_up &= processBuzzHouseQuery(full_query2);
                        qo.processSecondOracleQueryResult(!have_error, *external_integrations, "Dump and read table");
                    }
                }
                else if (peer_oracle && nopt < (correctness_oracle + settings_oracle + dump_oracle + peer_oracle + 1))
                {
                    /// Test results with peer tables
                    bool has_success = false;
                    BuzzHouse::PeerQuery nquery
                        = ((!external_integrations->hasMySQLConnection() && !external_integrations->hasPostgreSQLConnection()
                            && !external_integrations->hasSQLiteConnection())
                           || rg.nextBool())
                            && gen.collectionHas<BuzzHouse::SQLTable>(gen.attached_tables_for_clickhouse_table_peer_oracle)
                        ? BuzzHouse::PeerQuery::ClickHouseOnly
                        : BuzzHouse::PeerQuery::AllPeers;
                    const bool clickhouse_only = nquery == BuzzHouse::PeerQuery::ClickHouseOnly;

                    sq2.Clear();
                    qo.generateOracleSelectQuery(rg, nquery, gen, sq1);
                    qo.replaceQueryWithTablePeers(rg, sq1, gen, peer_queries, sq2);

                    if (clickhouse_only)
                    {
                        external_integrations->replicateSettings(BuzzHouse::PeerTableDatabase::ClickHouse);
                    }
                    qo.truncatePeerTables(gen);
                    for (const auto & entry : peer_queries)
                    {
                        full_query2.resize(0);
                        BuzzHouse::SQLQueryToString(full_query2, entry);
                        fuzz_config->outf << full_query2 << std::endl;
                        server_up &= processBuzzHouseQuery(full_query2);
                        qo.setIntermediateStepSuccess(!have_error);
                    }
                    qo.optimizePeerTables(gen);

                    full_query.resize(0);
                    BuzzHouse::SQLQueryToString(full_query, sq1);
                    fuzz_config->outf << full_query << std::endl;
                    server_up &= processBuzzHouseQuery(full_query);
                    qo.processFirstOracleQueryResult(!have_error, *external_integrations);

                    full_query2.resize(0);
                    BuzzHouse::SQLQueryToString(full_query2, sq2);
                    fuzz_config->outf << full_query2 << std::endl;
                    if (clickhouse_only)
                    {
                        has_success = external_integrations->performQuery(BuzzHouse::PeerTableDatabase::ClickHouse, full_query2);
                    }
                    else
                    {
                        server_up &= processBuzzHouseQuery(full_query2);
                        has_success = !have_error;
                    }
                    qo.processSecondOracleQueryResult(has_success, *external_integrations, "Peer table query");
                }
                else if (restart_client && nopt < (correctness_oracle + settings_oracle + dump_oracle + peer_oracle + restart_client + 1))
                {
                    fuzz_config->outf << "--Reconnecting client" << std::endl;
                    connection->disconnect();
                    gen.setInTransaction(false);
                    server_up &= tryToReconnect(fuzz_config->max_reconnection_attempts, fuzz_config->time_to_sleep_between_reconnects);
                }
                else if (
                    run_query && nopt < (correctness_oracle + settings_oracle + dump_oracle + peer_oracle + restart_client + run_query + 1))
                {
                    gen.generateNextStatement(rg, sq1);
                    BuzzHouse::SQLQueryToString(full_query, sq1);
                    fuzz_config->outf << full_query << std::endl;
                    server_up &= processBuzzHouseQuery(full_query);
                    gen.updateGenerator(sq1, *external_integrations, !have_error);
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
