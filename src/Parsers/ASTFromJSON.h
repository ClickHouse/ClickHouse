#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace Poco::JSON { class Object; }

namespace DB
{

/// Deserialize an AST tree from a JSON string produced by serializeASTToJSON.
ASTPtr deserializeASTFromJSON(const String & json);

/// Deserialize an AST node from a Poco::JSON::Object.
ASTPtr deserializeASTFromJSON(const Poco::JSON::Object & json);

/// Current thread-local depth limit configured by createFromJSON(json, max_depth, max_elements).
/// Returns 0 when no limit is active. Helpers that perform their own recursive parsing
/// (e.g. `Field::restoreFromDump` over `Array_/Tuple_/Map_` payloads) consult this value
/// to enforce the same depth bound on hostile input.
size_t getJSONDeserializationMaxDepth();

}
