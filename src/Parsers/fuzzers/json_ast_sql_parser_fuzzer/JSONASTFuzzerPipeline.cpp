#include <Parsers/fuzzers/json_ast_sql_parser_fuzzer/JSONASTFuzzerPipeline.h>
#include <Parsers/fuzzers/json_ast_sql_parser_fuzzer/JSONASTProtoConverter.h>

#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>

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

namespace DB::JSONASTFuzzer
{

namespace
{

PipelineLimits limits;
PipelineStats stats;
std::string target_name = "json_ast_fuzzer";

bool strict_mode = false;
bool strict_reparse_mode = false; /// abort only when the formatted SQL does not parse back
bool strict_json_mode = false;    /// abort when the JSON writer's output is rejected or changes the SQL
std::string last_tolerated_exception; /// message of the last exception `runStage` classified as expected
std::unique_ptr<std::ofstream> json_findings_log; /// `JSON_AST_FUZZER_JSON_LOG`: collect JSON round-trip defects instead of aborting

bool print_stats = true;
std::unique_ptr<std::ostream> dump_file;
std::ostream * dump_stream = nullptr;

enum class Stage
{
    JSON_TO_AST,
    FORMAT,
    PARSE,
    REPARSE,
    AST_UTILITIES,
    JSON_WRITE,
    JSON_READ,
};

std::string_view stageName(Stage stage)
{
    switch (stage)
    {
        case Stage::JSON_TO_AST: return "IAST::createFromJSON";
        case Stage::FORMAT: return "formatting the AST built from JSON";
        case Stage::PARSE: return "parsing the generated SQL";
        case Stage::REPARSE: return "re-parsing the formatted SQL";
        case Stage::AST_UTILITIES: return "cloning and hashing the parsed AST";
        case Stage::JSON_WRITE: return "IAST::writeJSON of the parsed AST";
        case Stage::JSON_READ: return "IAST::createFromJSON of the writer's own output";
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
            /// limits throw `TOO_DEEP_AST`/`TOO_BIG_AST`. `Literal` payloads of the dump-encoded
            /// types (`UUID`, `IPv4`, `Decimal*`, `Int128`, ...) go through `Field::restoreFromDump`,
            /// whose text readers throw the `CANNOT_PARSE_*` family (quoted string, number, UUID, ...).
            return code == ErrorCodes::BAD_ARGUMENTS
                || code == ErrorCodes::TOO_DEEP_AST
                || code == ErrorCodes::TOO_BIG_AST
                || code == ErrorCodes::CANNOT_RESTORE_FROM_FIELD_DUMP
                || code == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF
                || code == ErrorCodes::DECIMAL_OVERFLOW
                || code == ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
                || code == ErrorCodes::NOT_IMPLEMENTED
                || std::string_view(ErrorCodes::getName(code)).starts_with("CANNOT_PARSE_");
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
            /// `ParserInsertQuery` and `ParserExplainQuery` validate the `input` table function while
            /// parsing (`tryFindInputFunction`), hence `INVALID_USAGE_OF_INPUT`.
            return code == ErrorCodes::SYNTAX_ERROR
                || code == ErrorCodes::BAD_ARGUMENTS
                || code == ErrorCodes::INVALID_USAGE_OF_INPUT
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
        case Stage::AST_UTILITIES:
            /// `clone` and `getTreeHash` of an AST the parser produced may not throw.
            return false;
        case Stage::JSON_WRITE:
            /// Nodes without a JSON writer (`IAST::writeJSON` default) and writers that refuse a shape.
            return code == ErrorCodes::NOT_IMPLEMENTED
                || code == ErrorCodes::BAD_ARGUMENTS;
        case Stage::JSON_READ:
            /// Reading the writer's own output must succeed; a tolerated code here is only a statistic
            /// (`json_roundtrip_rejected`), promoted to a finding by `JSON_AST_FUZZER_STRICT=json`.
            return code == ErrorCodes::BAD_ARGUMENTS
                || code == ErrorCodes::NOT_IMPLEMENTED
                || code == ErrorCodes::TOO_DEEP_AST
                || code == ErrorCodes::TOO_BIG_AST
                || code == ErrorCodes::CANNOT_RESTORE_FROM_FIELD_DUMP
                || std::string_view(ErrorCodes::getName(code)).starts_with("CANNOT_PARSE_");
    }
}

void writeSection(std::ostream & out, std::string_view title, const std::string & text)
{
    out << "--- " << title << " ---\n" << text << std::endl;
}

void dump(const PipelineInput & input, std::ostream & out)
{
    writeSection(out, "JSON AST", input.json);
    if (!input.sql.empty())
        writeSection(out, "generated SQL", input.sql);
    if (!input.reformatted_sql.empty())
        writeSection(out, "SQL after parse and format", input.reformatted_sql);
}

void printStats()
{
    if (!print_stats || stats.inputs == 0)
        return;

    auto percent = [](size_t part, size_t whole) { return whole ? 100.0 * static_cast<double>(part) / static_cast<double>(whole) : 0.0; };
    std::cerr << '\n' << target_name << " stage statistics:\n"
        << "  inputs:                         " << stats.inputs << '\n'
        << "  protobuf -> JSON rejected:      " << stats.json_rejected << '\n'
        << "  JSON -> AST rejected:           " << stats.ast_rejected << '\n'
        << "  AST created:                    " << stats.ast_created << " (" << percent(stats.ast_created, stats.inputs) << "%)\n"
        << "  AST formatting rejected:        " << stats.format_rejected << '\n'
        << "  SQL too long:                   " << stats.sql_too_long << '\n'
        << "  SQL generated:                  " << stats.sql_generated << " (" << percent(stats.sql_generated, stats.inputs) << "%)\n";
    if (stats.sql_parsed || stats.sql_parse_rejected)
        std::cerr
            << "  SQL parse rejected:             " << stats.sql_parse_rejected << '\n'
            << "  SQL parsed:                     " << stats.sql_parsed << " (" << percent(stats.sql_parsed, stats.inputs) << "%)\n"
            << "  format/parse round trip stable: " << stats.roundtrip_stable << " (" << percent(stats.roundtrip_stable, stats.sql_parsed) << "% of parsed)\n"
            << "  round trip unstable:            " << stats.roundtrip_unstable << '\n'
            << "  re-parse rejected:              " << stats.roundtrip_reparse_rejected << '\n'
            << "  clone unstable:                 " << stats.clone_unstable << '\n'
            << "  JSON round trip stable:         " << stats.json_roundtrip_ok << " (" << percent(stats.json_roundtrip_ok, stats.sql_parsed) << "% of parsed)\n"
            << "  JSON writer not supported:      " << stats.json_roundtrip_not_supported << '\n'
            << "  JSON reader rejected writer:    " << stats.json_roundtrip_rejected << '\n'
            << "  JSON round trip unstable:       " << stats.json_roundtrip_unstable << '\n';
    if (stats.executed || stats.execution_skipped)
        std::cerr
            << "  execution skipped (statement):  " << stats.execution_skipped << '\n'
            << "  executed:                       " << stats.executed << " (" << percent(stats.executed, stats.inputs) << "%)\n";
    std::cerr.flush();
}

/// Runs `action` and classifies whatever it throws: a tolerated exception makes the function
/// return false; anything else aborts with a report.
template <typename Action>
bool runStage(Stage stage, const PipelineInput & input, Action && action)
{
    try
    {
        action();
        return true;
    }
    catch (const Exception & e)
    {
        if (isExpectedException(stage, e.code()))
        {
            last_tolerated_exception = getExceptionMessage(e, /*with_stacktrace=*/ false);
            return false;
        }
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

void initializePipeline(std::string_view name, const int * argc, char *** argv)
{
    target_name = name;

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
    {
        strict_mode = std::string_view(value) == "1";
        strict_reparse_mode = strict_mode || std::string_view(value) == "reparse";
        strict_json_mode = strict_mode || std::string_view(value) == "json";
    }
    if (const char * value = getenv("JSON_AST_FUZZER_STATS"))
        print_stats = std::string_view(value) != "0";
    if (const char * value = getenv("JSON_AST_FUZZER_JSON_LOG"); value && *value)
    {
        json_findings_log = std::make_unique<std::ofstream>(value, std::ios::app);
        if (!*json_findings_log)
        {
            std::cerr << "Cannot open JSON_AST_FUZZER_JSON_LOG file " << value << '\n';
            exit(1);
        }
    }
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
}

PipelineLimits & pipelineLimits()
{
    return limits;
}

PipelineStats & pipelineStats()
{
    return stats;
}

bool dumpEnabled()
{
    return dump_stream != nullptr;
}

void dumpSection(std::string_view title, const std::string & text)
{
    if (dump_stream)
        writeSection(*dump_stream, title, text);
}

void abortWithReport(std::string_view reason, const PipelineInput & input)
{
    std::cerr << '\n' << target_name << ": " << reason << '\n';
    dump(input, std::cerr);
    /// `abort` bypasses the `atexit` handler.
    printStats();
    abort();
}

/// A JSON round-trip defect is either appended to the findings log (harvest mode), or aborts in strict json
/// mode, or is only counted.
static void reportJSONRoundTrip(const std::string & reason, const PipelineInput & input)
{
    if (json_findings_log)
    {
        *json_findings_log << "=== " << reason << '\n';
        dump(input, *json_findings_log);
        *json_findings_log << std::endl;
        return;
    }
    if (strict_json_mode)
        abortWithReport("strict mode: " + reason, input);
}

ASTPtr generateSQL(const json_ast_fuzzer::Node & root, PipelineInput & input)
{
    ++stats.inputs;

    ProtoToJSONLimits json_limits;
    /// Every AST level costs at most a `children` array plus the child object, and a structured
    /// `Field` value nests further; a small multiple of the AST depth limit keeps valid inputs.
    json_limits.max_depth = 4 * limits.max_ast_depth;
    json_limits.max_output_bytes = limits.max_json_length;
    if (!protoToJSON(root, json_limits, input.json))
    {
        ++stats.json_rejected;
        return nullptr;
    }
    dumpSection("JSON AST", input.json);

    ASTPtr ast;
    bool created = runStage(Stage::JSON_TO_AST, input, [&]
    {
        ast = IAST::createFromJSON(input.json, limits.max_ast_depth, limits.max_ast_elements);
        /// Some `readJSON` implementations build extra nodes that bypass the deserialization
        /// counters; re-check the assembled tree like the `clickhouse_json` dialect does.
        ast->checkDepth(limits.max_ast_depth);
        ast->checkSize(limits.max_ast_elements);
    });
    if (!created)
    {
        ++stats.ast_rejected;
        return nullptr;
    }
    ++stats.ast_created;

    bool formatted = runStage(Stage::FORMAT, input, [&] { input.sql = ast->formatWithSecretsOneLine(); });
    if (!formatted)
    {
        ++stats.format_rejected;
        if (strict_mode)
            abortWithReport("strict mode: formatting the AST built from JSON threw a validation exception", input);
        return nullptr;
    }
    if (input.sql.size() > limits.max_sql_length)
    {
        ++stats.sql_too_long;
        return nullptr;
    }
    ++stats.sql_generated;
    dumpSection("generated SQL", input.sql);
    return ast;
}

void parseAndRoundTrip(PipelineInput & input)
{
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
        if (strict_reparse_mode)
            abortWithReport("strict mode: the formatted SQL does not parse", input);
    }

    dumpSection("SQL after parse and format", input.reformatted_sql);

    /// The AST utilities every interpreter relies on: `clone` must give a tree that formats the same,
    /// `getTreeHash` (`updateTreeHashImpl` of every node) must not throw. Cheap, and otherwise never
    /// reached by the parser fuzzer (the coverage of `updateTreeHashImpl` was zero).
    std::string cloned_sql;
    runStage(Stage::AST_UTILITIES, input, [&]
    {
        cloned_sql = parsed->clone()->formatWithSecretsOneLine();
        parsed->getTreeHash(/*ignore_aliases=*/ false);
        parsed->getTreeHash(/*ignore_aliases=*/ true);
    });
    if (cloned_sql != input.reformatted_sql)
    {
        ++stats.clone_unstable;
        if (strict_mode)
            abortWithReport("strict mode: the clone of the parsed AST formats differently: " + cloned_sql, input);
    }

    /// The JSON writer side (`parseQueryToJSON`): the fuzzer builds ASTs from JSON, so the writers are
    /// only covered here. The reader must accept the writer's output and the SQL must survive.
    std::string written_json;
    bool written = runStage(Stage::JSON_WRITE, input, [&]
    {
        WriteBufferFromOwnString out;
        parsed->writeJSON(out);
        written_json = out.str();
    });
    if (!written)
    {
        ++stats.json_roundtrip_not_supported;
        return;
    }
    /// The JSON has no representation of the redundant parentheses the user wrote (`IAST::isParenthesized`),
    /// so both sides are compared without them.
    std::string json_sql;
    bool read = runStage(Stage::JSON_READ, input, [&]
    {
        ASTPtr from_json = IAST::createFromJSON(written_json, limits.max_ast_depth, limits.max_ast_elements);
        json_sql = from_json->formatIgnoringRedundantParentheses();
    });
    if (!read)
    {
        ++stats.json_roundtrip_rejected;
        reportJSONRoundTrip(
            "IAST::createFromJSON rejected the output of IAST::writeJSON: " + last_tolerated_exception + "\n--- JSON written ---\n"
                + written_json,
            input);
        return;
    }
    const std::string parsed_sql_without_parens = parsed->formatIgnoringRedundantParentheses();
    if (json_sql != parsed_sql_without_parens)
    {
        ++stats.json_roundtrip_unstable;
        reportJSONRoundTrip(
            "the JSON round trip changed the SQL from: " + parsed_sql_without_parens + "\nto: " + json_sql + "\n--- JSON written ---\n"
                + written_json,
            input);
        return;
    }
    ++stats.json_roundtrip_ok;
}

}
