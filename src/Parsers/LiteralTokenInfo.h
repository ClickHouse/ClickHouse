#pragma once

#include <absl/container/flat_hash_map.h>

namespace DB
{

class ASTLiteral;

/// Token position info for literals - stores raw character pointers into the query string.
/// Used for ConstantExpressionTemplate construction and LIKE/REGEXP syntax highlighting.
/// Stored externally to reduce ASTLiteral size by ~48 bytes per literal.
///
/// IMPORTANT: These are raw pointers into the original query string. They are only valid
/// during parsing while the query buffer exists. Do not store or access after parsing.
struct LiteralTokenInfo
{
    const char * begin = nullptr; /// Start of literal in query string
    const char * end = nullptr;   /// End of literal in query string

    LiteralTokenInfo() = default;

    LiteralTokenInfo(const char * begin_, const char * end_)
        : begin(begin_)
        , end(end_)
    {
    }
};

/// Map from ASTLiteral pointer to its token position in the query string.
/// Uses flat_hash_map for better performance with small maps that are created
/// and destroyed frequently during parsing.
///
/// NOTE: The map uses ASTLiteral pointers as keys. During parsing, the memory allocator
/// may reuse the same address for different ASTLiteral objects as intermediate nodes
/// are created and destroyed. The final AST contains only the surviving nodes, so we
/// use insert_or_assign to ensure the last literal at each address has its token info
/// recorded (which is the one that survives in the final AST).
///
/// This is a struct (not a type alias) so it can be forward-declared,
/// avoiding the heavy `absl/container/flat_hash_map.h` include in `IParser.h`.
struct LiteralTokenMap : absl::flat_hash_map<const ASTLiteral *, LiteralTokenInfo>
{
    using absl::flat_hash_map<const ASTLiteral *, LiteralTokenInfo>::flat_hash_map;
};

}
