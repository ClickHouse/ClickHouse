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
/// rest is counted as skipped. Query errors are expected outcomes. Crashes, sanitizer reports,
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

#include <libfuzzer/libfuzzer_macro.h>

#include <json_ast.pb.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

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
    DB::LocalFuzzerRunner::runQuery(input.sql);
}
