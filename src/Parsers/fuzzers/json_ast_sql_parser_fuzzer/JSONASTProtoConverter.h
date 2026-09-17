#pragma once

#include <cstddef>
#include <string>
#include <string_view>

#include <json_ast.pb.h>

/// Conversion between the protobuf schema of `json_ast.proto` and the native ClickHouse
/// JSON AST representation understood by `IAST::createFromJSON`.
///
/// `protoToJSON` is the hot path of `json_ast_sql_parser_fuzzer`: it renders a mutated
/// protobuf message as JSON text. `jsonToProto` is its inverse and is used by
/// `json_ast_seed_converter` to build the binary seed corpus from the output of
/// `parseQueryToJSON`.
namespace DB::JSONASTFuzzer
{

struct ProtoToJSONLimits
{
    /// Maximum nesting of JSON objects and arrays. Both the renderer and the JSON parser
    /// recurse, so this bounds stack usage on hostile inputs produced by crossover.
    size_t max_depth = 512;
    /// Maximum size of the rendered JSON text in bytes.
    size_t max_output_bytes = 1 << 20;
};

/// Renders `node` as a JSON AST object into `out`. Returns false when a limit of `limits` is
/// exceeded; `out` is then incomplete and must be discarded. Never throws.
bool protoToJSON(const json_ast_fuzzer::Node & node, const ProtoToJSONLimits & limits, std::string & out);

/// Parses a JSON AST document (as produced by `parseQueryToJSON`) into `out`.
/// Throws `DB::Exception` with code `BAD_ARGUMENTS` when the document is not valid JSON, when it
/// uses a node type or a property key that is missing from `json_ast.proto`, or when a value has
/// a shape that the schema cannot express.
void jsonToProto(const std::string & json, json_ast_fuzzer::Node & out);

/// JSON text of the enumeration values (`(json_text)` option, or the identifier without its prefix for `Key`).
std::string_view nodeTypeText(json_ast_fuzzer::NodeType type);
std::string_view fieldTypeText(json_ast_fuzzer::FieldType type);
std::string_view keyText(json_ast_fuzzer::Key key);
std::string_view knownStringText(json_ast_fuzzer::KnownString value);

}
