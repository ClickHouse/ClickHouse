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

}
