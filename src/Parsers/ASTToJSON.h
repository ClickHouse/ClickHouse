#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace DB
{

class WriteBuffer;

/// Serialize an AST tree to a JSON string.
/// The resulting JSON is designed to be round-trippable: `IAST::createFromJSON(serializeASTToJSON(ast), max_depth, max_elements)`
/// should produce an equivalent AST.
String serializeASTToJSON(const IAST & ast);

/// Write the JSON representation of an AST node (and its subtree) to a WriteBuffer.
void serializeASTToJSON(const IAST & ast, WriteBuffer & out);

}
