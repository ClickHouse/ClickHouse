#pragma once

#include <unordered_map>

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
///
/// NOTE: The map uses ASTLiteral pointers as keys. During parsing, the memory allocator
/// may reuse the same address for different ASTLiteral objects as intermediate nodes
/// are created and destroyed. The final AST contains only the surviving nodes, so we
/// use insert_or_assign to ensure the last literal at each address has its token info
/// recorded (which is the one that survives in the final AST).
///
/// This is a struct (not a type alias) so it can be forward-declared, keeping the map out of
/// `IParser.h`. It is only populated when a caller asks for literal positions, so a plain
/// `std::unordered_map` is enough and keeps the parser free of abseil.
struct LiteralTokenMap : std::unordered_map<const ASTLiteral *, LiteralTokenInfo>
{
    using std::unordered_map<const ASTLiteral *, LiteralTokenInfo>::unordered_map;
};

}
