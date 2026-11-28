/// Structure-aware libFuzzer target for the ClickHouse SQL parser.
///
/// The fuzz input is a binary protobuf message (`json_ast.proto`) that mirrors the native JSON
/// AST representation. libprotobuf-mutator mutates the message structurally, the target renders
/// it as JSON, deserializes it with `IAST::createFromJSON`, formats the resulting AST as SQL and
/// feeds that SQL to `ParserQuery`, then formats and parses once more:
///
///     protobuf -> JSON AST text -> IAST::createFromJSON -> depth/size limits
///              -> IAST::formatWithSecretsOneLine -> parseQuery -> format -> parseQuery
///
/// The stages, their limits, the expected-exception policy and the `JSON_AST_FUZZER_*` environment
/// variables are documented in `JSONASTFuzzerPipeline.h`. This target is parse-only and stateless;
/// `json_ast_sql_execution_fuzzer` (programs/local/fuzzers) executes the generated SQL instead.

#include <Parsers/fuzzers/json_ast_sql_parser_fuzzer/JSONASTFuzzerPipeline.h>

#include <libfuzzer/libfuzzer_macro.h>

#include <json_ast.pb.h>

extern "C" int LLVMFuzzerInitialize(const int * argc, char *** argv);

extern "C" int LLVMFuzzerInitialize(const int * argc, char *** argv)
{
    DB::JSONASTFuzzer::initializePipeline("json_ast_sql_parser_fuzzer", argc, argv);
    return 0;
}

DEFINE_BINARY_PROTO_FUZZER(const json_ast_fuzzer::Node & root)
{
    DB::JSONASTFuzzer::PipelineInput input;
    if (!DB::JSONASTFuzzer::generateSQL(root, input))
        return;
    DB::JSONASTFuzzer::parseAndRoundTrip(input);
}
