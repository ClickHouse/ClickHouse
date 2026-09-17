/// Structure-aware libFuzzer target for the ClickHouse SQL parser.
///
/// The fuzz input is a binary protobuf message (`json_ast.proto`) that mirrors the native JSON
/// AST representation. libprotobuf-mutator mutates the message structurally, the target renders
/// it as JSON, deserializes it with `IAST::createFromJSON`, formats the resulting AST as SQL and
/// feeds that SQL to `ParserQuery`:
///
///     protobuf -> JSON AST text -> IAST::createFromJSON -> depth/size limits
///              -> IAST::formatWithSecretsOneLine -> parseQuery -> format -> parseQuery
///
/// Rejected JSON, rejected ASTs and syntax errors in the generated SQL are expected outcomes and
/// are counted, not reported. Anything else that escapes a stage (a `LOGICAL_ERROR`, an
/// exception code that no stage is allowed to throw, a non-`DB::Exception` exception) is printed
/// together with the JSON and the SQL and the process aborts, so libFuzzer saves the input.
/// Sanitizer reports and assertion failures are not intercepted at all.
///
/// Environment variables:
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

#include <Common/Exception.h>
#include <Core/Defines.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <libfuzzer/libfuzzer_macro.h>

#include <Parsers/fuzzers/json_ast_sql_parser_fuzzer/JSONASTProtoConverter.h>
#include <json_ast.pb.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>

namespace DB::ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_PARSE_UUID;
    extern const int CANNOT_RESTORE_FROM_FIELD_DUMP;
    extern const int DECIMAL_OVERFLOW;
    extern const int FIRST_AND_NEXT_TOGETHER;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INVALID_USAGE_OF_INPUT;
    extern const int LIMIT_BY_WITH_TIES_IS_NOT_SUPPORTED;
    extern const int NOT_IMPLEMENTED;
    extern const int OFFSET_FETCH_WITHOUT_ORDER_BY;
    extern const int ROW_AND_ROWS_TOGETHER;
    extern const int SYNTAX_ERROR;
    extern const int TOO_BIG_AST;
    extern const int TOO_DEEP_AST;
    extern const int TOO_DEEP_RECURSION;
    extern const int TOP_AND_LIMIT_TOGETHER;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int WITH_TIES_WITHOUT_ORDER_BY;
}

extern "C" int LLVMFuzzerInitialize(const int * argc, char *** argv);

namespace
{

using namespace DB;

struct Limits
{
#if defined(SANITIZER) || !defined(NDEBUG)
    size_t max_parser_depth = 150;
#else
    size_t max_parser_depth = 300;
#endif
    size_t max_parser_backtracks = DBMS_DEFAULT_MAX_PARSER_BACKTRACKS;
    size_t max_ast_depth = 200;
    size_t max_ast_elements = 10000;
    size_t max_json_length = 1 << 20;
    size_t max_sql_length = 256 * 1024;
};

Limits limits;

/// How far each input got. Printed at exit.
struct Stats
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
};

Stats stats;

bool strict_mode = false;
bool print_stats = true;
std::unique_ptr<std::ostream> dump_file;
std::ostream * dump_stream = nullptr;

enum class Stage
{
    JSON_TO_AST,
    FORMAT,
    PARSE,
    REPARSE,
};

std::string_view stageName(Stage stage)
{
    switch (stage)
    {
        case Stage::JSON_TO_AST: return "IAST::createFromJSON";
        case Stage::FORMAT: return "formatting the AST built from JSON";
        case Stage::PARSE: return "parsing the generated SQL";
        case Stage::REPARSE: return "re-parsing the formatted SQL";
    }
}

/// Exception codes that are legitimate input-validation outcomes of a stage. Everything else,
/// including `LOGICAL_ERROR`, is a finding.
bool isExpectedException(Stage stage, int code)
{
    switch (stage)
    {
        case Stage::JSON_TO_AST:
            /// `readJSON` implementations reject malformed documents with `BAD_ARGUMENTS`; the AST
            /// limits throw `TOO_DEEP_AST`/`TOO_BIG_AST`. `Literal` payloads go through
            /// `Field::restoreFromDump` and `parseFromString<UUID>`, which have their own codes.
            return code == ErrorCodes::BAD_ARGUMENTS
                || code == ErrorCodes::TOO_DEEP_AST
                || code == ErrorCodes::TOO_BIG_AST
                || code == ErrorCodes::CANNOT_RESTORE_FROM_FIELD_DUMP
                || code == ErrorCodes::CANNOT_PARSE_UUID
                || code == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED
                || code == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF
                || code == ErrorCodes::DECIMAL_OVERFLOW
                || code == ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
                || code == ErrorCodes::NOT_IMPLEMENTED;
        case Stage::FORMAT:
            /// Formatting code validates a few parser-impossible shapes itself.
            return code == ErrorCodes::BAD_ARGUMENTS
                || code == ErrorCodes::SYNTAX_ERROR
                || code == ErrorCodes::UNEXPECTED_AST_STRUCTURE
                || code == ErrorCodes::INVALID_USAGE_OF_INPUT
                || code == ErrorCodes::NOT_IMPLEMENTED
                || code == ErrorCodes::TOO_DEEP_RECURSION;
        case Stage::PARSE:
        case Stage::REPARSE:
            return code == ErrorCodes::SYNTAX_ERROR
                || code == ErrorCodes::BAD_ARGUMENTS
                || code == ErrorCodes::TOO_DEEP_RECURSION
                || code == ErrorCodes::TOO_DEEP_AST
                || code == ErrorCodes::TOO_BIG_AST
                || code == ErrorCodes::NOT_IMPLEMENTED
                || code == ErrorCodes::UNEXPECTED_AST_STRUCTURE
                || code == ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
                || code == ErrorCodes::ROW_AND_ROWS_TOGETHER
                || code == ErrorCodes::LIMIT_BY_WITH_TIES_IS_NOT_SUPPORTED
                || code == ErrorCodes::WITH_TIES_WITHOUT_ORDER_BY
                || code == ErrorCodes::TOP_AND_LIMIT_TOGETHER
                || code == ErrorCodes::OFFSET_FETCH_WITHOUT_ORDER_BY
                || code == ErrorCodes::FIRST_AND_NEXT_TOGETHER;
    }
}

struct Input
{
    std::string json;
    std::string sql;
    std::string reformatted_sql;
};

void dumpSection(std::ostream & out, std::string_view title, const std::string & text)
{
    out << "--- " << title << " ---\n" << text << std::endl;
}

void dump(const Input & input, std::ostream & out)
{
    dumpSection(out, "JSON AST", input.json);
    if (!input.sql.empty())
        dumpSection(out, "generated SQL", input.sql);
    if (!input.reformatted_sql.empty())
        dumpSection(out, "SQL after parse and format", input.reformatted_sql);
}

void printStats();

[[noreturn]] void abortWithReport(std::string_view reason, const Input & input)
{
    std::cerr << "\njson_ast_sql_parser_fuzzer: " << reason << '\n';
    dump(input, std::cerr);
    /// `abort` bypasses the `atexit` handler.
    printStats();
    abort();
}

/// Runs `action` and classifies whatever it throws: a tolerated exception makes the function
/// return false; anything else aborts with a report.
template <typename Action>
bool runStage(Stage stage, const Input & input, Action && action)
{
    try
    {
        action();
        return true;
    }
    catch (const Exception & e)
    {
        if (isExpectedException(stage, e.code()))
            return false;
        abortWithReport(
            "unexpected exception while " + std::string(stageName(stage)) + ": " + getExceptionMessage(e, /*with_stacktrace=*/ true),
            input);
    }
    catch (...)
    {
        abortWithReport(
            "unexpected non-DB exception while " + std::string(stageName(stage)) + ": " + getCurrentExceptionMessage(/*with_stacktrace=*/ true),
            input);
    }
}

ASTPtr parseSQL(const std::string & sql)
{
    /// `ParserQuery` rather than `ParserQueryWithOutput`: the JSON AST covers `INSERT`, `SET`, `USE`,
    /// `SYSTEM`, transaction control and the other statements that only `ParserQuery` accepts.
    ParserQuery parser(sql.data() + sql.size());
    ASTPtr ast = parseQuery(parser, sql.data(), sql.data() + sql.size(), "", /*max_query_size=*/ 0, limits.max_parser_depth, limits.max_parser_backtracks);
    ast->checkDepth(limits.max_ast_depth);
    ast->checkSize(limits.max_ast_elements);
    return ast;
}

void printStats()
{
    if (!print_stats || stats.inputs == 0)
        return;

    auto percent = [](size_t part, size_t whole) { return whole ? 100.0 * static_cast<double>(part) / static_cast<double>(whole) : 0.0; };
    std::cerr << "\njson_ast_sql_parser_fuzzer stage statistics:\n"
        << "  inputs:                         " << stats.inputs << '\n'
        << "  protobuf -> JSON rejected:      " << stats.json_rejected << '\n'
        << "  JSON -> AST rejected:           " << stats.ast_rejected << '\n'
        << "  AST created:                    " << stats.ast_created << " (" << percent(stats.ast_created, stats.inputs) << "%)\n"
        << "  AST formatting rejected:        " << stats.format_rejected << '\n'
        << "  SQL too long:                   " << stats.sql_too_long << '\n'
        << "  SQL generated:                  " << stats.sql_generated << " (" << percent(stats.sql_generated, stats.inputs) << "%)\n"
        << "  SQL parse rejected:             " << stats.sql_parse_rejected << '\n'
        << "  SQL parsed:                     " << stats.sql_parsed << " (" << percent(stats.sql_parsed, stats.inputs) << "%)\n"
        << "  format/parse round trip stable: " << stats.roundtrip_stable << " (" << percent(stats.roundtrip_stable, stats.sql_parsed) << "% of parsed)\n"
        << "  round trip unstable:            " << stats.roundtrip_unstable << '\n'
        << "  re-parse rejected:              " << stats.roundtrip_reparse_rejected << '\n';
    std::cerr.flush();
}

void parseSizeArgument(std::string_view arg, std::string_view name, size_t & out)
{
    if (!arg.starts_with(name) || arg.size() <= name.size() || arg[name.size()] != '=')
        return;
    std::string value(arg.substr(name.size() + 1));
    size_t pos = 0;
    unsigned long long parsed = std::stoull(value, &pos);
    if (pos != value.size())
    {
        std::cerr << "Invalid value for " << name << ": " << value << '\n';
        exit(1);
    }
    out = static_cast<size_t>(parsed);
}

}

extern "C" int LLVMFuzzerInitialize(const int * argc, char *** argv)
{
    bool ignore_remaining = false;
    for (int i = 1; i < *argc; ++i)
    {
        std::string_view arg((*argv)[i]);
        if (!ignore_remaining)
        {
            ignore_remaining = arg.starts_with("-ignore_remaining_args");
            continue;
        }
        parseSizeArgument(arg, "-max_parser_depth", limits.max_parser_depth);
        parseSizeArgument(arg, "-max_parser_backtracks", limits.max_parser_backtracks);
        parseSizeArgument(arg, "-max_ast_depth", limits.max_ast_depth);
        parseSizeArgument(arg, "-max_ast_elements", limits.max_ast_elements);
        parseSizeArgument(arg, "-max_json_length", limits.max_json_length);
        parseSizeArgument(arg, "-max_sql_length", limits.max_sql_length);
    }

    if (const char * value = getenv("JSON_AST_FUZZER_STRICT"))
        strict_mode = std::string_view(value) == "1";
    if (const char * value = getenv("JSON_AST_FUZZER_STATS"))
        print_stats = std::string_view(value) != "0";
    if (const char * value = getenv("JSON_AST_FUZZER_DUMP"); value && *value)
    {
        std::string_view target(value);
        if (target == "1" || target == "stderr")
            dump_stream = &std::cerr;
        else
        {
            dump_file = std::make_unique<std::ofstream>(std::string(target), std::ios::app);
            if (!*dump_file)
            {
                std::cerr << "Cannot open JSON_AST_FUZZER_DUMP file " << target << '\n';
                exit(1);
            }
            dump_stream = dump_file.get();
        }
    }

    atexit(printStats);
    return 0;
}

DEFINE_BINARY_PROTO_FUZZER(const json_ast_fuzzer::Node & root)
{
    ++stats.inputs;
    Input input;

    JSONASTFuzzer::ProtoToJSONLimits json_limits;
    /// Every AST level costs at most a `children` array plus the child object, and a structured
    /// `Field` value nests further; a small multiple of the AST depth limit keeps valid inputs.
    json_limits.max_depth = 4 * limits.max_ast_depth;
    json_limits.max_output_bytes = limits.max_json_length;
    if (!JSONASTFuzzer::protoToJSON(root, json_limits, input.json))
    {
        ++stats.json_rejected;
        return;
    }

    ASTPtr ast;
    bool created = runStage(Stage::JSON_TO_AST, input, [&]
    {
        ast = IAST::createFromJSON(input.json, limits.max_ast_depth, limits.max_ast_elements);
        /// Some `readJSON` implementations build extra nodes that bypass the deserialization
        /// counters; re-check the assembled tree like the `clickhouse_json` dialect does.
        ast->checkDepth(limits.max_ast_depth);
        ast->checkSize(limits.max_ast_elements);
    });
    if (dump_stream)
        dumpSection(*dump_stream, "JSON AST", input.json);
    if (!created)
    {
        ++stats.ast_rejected;
        return;
    }
    ++stats.ast_created;

    bool formatted = runStage(Stage::FORMAT, input, [&] { input.sql = ast->formatWithSecretsOneLine(); });
    if (!formatted)
    {
        ++stats.format_rejected;
        if (strict_mode)
            abortWithReport("strict mode: formatting the AST built from JSON threw a validation exception", input);
        return;
    }
    if (input.sql.size() > limits.max_sql_length)
    {
        ++stats.sql_too_long;
        return;
    }
    ++stats.sql_generated;
    if (dump_stream)
        dumpSection(*dump_stream, "generated SQL", input.sql);

    ASTPtr parsed;
    bool parsed_ok = runStage(Stage::PARSE, input, [&] { parsed = parseSQL(input.sql); });
    if (!parsed_ok)
    {
        ++stats.sql_parse_rejected;
        return;
    }
    ++stats.sql_parsed;

    /// Round trip: the SQL produced from the parsed AST should be the SQL we parsed, and it should
    /// parse again. Formatting an AST that the SQL parser itself produced must never throw.
    bool reformatted = runStage(Stage::REPARSE, input, [&] { input.reformatted_sql = parsed->formatWithSecretsOneLine(); });
    if (!reformatted)
        abortWithReport("formatting an AST produced by the SQL parser threw an exception", input);

    if (input.reformatted_sql == input.sql)
        ++stats.roundtrip_stable;
    else
    {
        ++stats.roundtrip_unstable;
        if (strict_mode)
            abortWithReport("strict mode: format -> parse -> format is not stable", input);
    }

    bool reparsed = runStage(Stage::REPARSE, input, [&] { parseSQL(input.reformatted_sql); });
    if (!reparsed)
    {
        ++stats.roundtrip_reparse_rejected;
        if (strict_mode)
            abortWithReport("strict mode: the formatted SQL does not parse", input);
    }

    if (dump_stream)
        dumpSection(*dump_stream, "SQL after parse and format", input.reformatted_sql);
}
