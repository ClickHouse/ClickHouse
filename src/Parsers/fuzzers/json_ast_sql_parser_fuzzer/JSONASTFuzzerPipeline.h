#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>

#include <json_ast.pb.h>

#include <cstddef>
#include <ostream>
#include <string>
#include <string_view>

/// The stages shared by the structure-aware fuzz targets built on the JSON AST:
///
///     protobuf -> JSON AST text -> IAST::createFromJSON -> depth/size limits
///              -> IAST::formatWithSecretsOneLine -> (parseQuery -> format -> parseQuery)
///
/// `json_ast_sql_parser_fuzzer` runs every stage; `json_ast_sql_execution_fuzzer` stops after the
/// SQL is generated and executes it in an in-process `clickhouse local` instead.
///
/// Expected outcomes (rejected JSON, rejected ASTs, syntax errors) are counted in `PipelineStats`
/// and swallowed. Anything else that escapes a stage (a `LOGICAL_ERROR`, an exception code the stage
/// is not allowed to throw, a non-`DB::Exception` exception) is printed together with the JSON and
/// the SQL and the process aborts, so libFuzzer saves the input.
///
/// Environment variables (read by `initializePipeline`):
/// - `JSON_AST_FUZZER_DUMP=1` (or `stderr`) prints the JSON and every generated SQL to stderr for
///   each input; `JSON_AST_FUZZER_DUMP=<path>` appends the same text to a file. Use it when
///   reproducing a corpus or crash input; it is far too noisy for a fuzzing session.
/// - `JSON_AST_FUZZER_STRICT=1` additionally aborts when formatting an AST built from JSON throws an
///   otherwise tolerated validation exception, when re-parsing the formatted SQL fails, or when
///   format -> parse -> format is not stable.
/// - `JSON_AST_FUZZER_STATS=0` disables the stage statistics printed at exit.
///
/// Command line arguments (after `-ignore_remaining_args=1`, like the other parser fuzzers):
/// `-max_ast_depth=N`, `-max_ast_elements=N`, `-max_parser_depth=N`, `-max_parser_backtracks=N`,
/// `-max_json_length=N`, `-max_sql_length=N`.
namespace DB::JSONASTFuzzer
{

struct PipelineLimits
{
#if defined(SANITIZER) || !defined(NDEBUG)
    size_t max_parser_depth = 150;
#else
    size_t max_parser_depth = 300;
#endif
    size_t max_parser_backtracks = 1000000;
    size_t max_ast_depth = 200;
    size_t max_ast_elements = 10000;
    size_t max_json_length = 1 << 20;
    size_t max_sql_length = 256 * 1024;
};

/// How far the inputs got. Printed at exit and before an abort.
struct PipelineStats
{
    size_t inputs = 0;
    size_t json_rejected = 0;          /// protobuf -> JSON exceeded the depth or length limit
    size_t ast_rejected = 0;           /// `IAST::createFromJSON` or the AST limits rejected the JSON
    size_t ast_created = 0;
    size_t format_rejected = 0;        /// formatting the AST threw a tolerated validation exception
    size_t sql_too_long = 0;
    size_t sql_generated = 0;
    size_t sql_parse_rejected = 0;     /// the SQL parser rejected the generated SQL
    size_t sql_parsed = 0;
    size_t roundtrip_stable = 0;       /// format(parse(sql)) == sql
    size_t roundtrip_unstable = 0;
    size_t roundtrip_reparse_rejected = 0;
    size_t execution_skipped = 0;      /// statement kind not executed by the execution fuzzer
    size_t executed = 0;
};

struct PipelineInput
{
    std::string json;
    std::string sql;
    std::string reformatted_sql;
};

/// Parses the target's own arguments and environment variables, registers the statistics printer.
/// `name` is used in the messages.
void initializePipeline(std::string_view name, const int * argc, char *** argv);

PipelineLimits & pipelineLimits();
PipelineStats & pipelineStats();

/// protobuf -> JSON -> AST -> SQL. Returns the AST, with `input.json` and `input.sql` filled, or
/// nullptr when a stage rejected the input (the statistics are updated either way).
ASTPtr generateSQL(const json_ast_fuzzer::Node & root, PipelineInput & input);

/// Parses `input.sql` with `ParserQuery`, formats the result into `input.reformatted_sql`, compares
/// it with `input.sql` and parses it once more. Updates the statistics; aborts on unexpected exceptions.
void parseAndRoundTrip(PipelineInput & input);

/// Whether `JSON_AST_FUZZER_DUMP` is active, and the dump writer (no-op when it is not).
bool dumpEnabled();
void dumpSection(std::string_view title, const std::string & text);

/// Prints `reason`, the input and the statistics, then aborts.
[[noreturn]] void abortWithReport(std::string_view reason, const PipelineInput & input);

}
