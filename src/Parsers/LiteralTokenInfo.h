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
/// `IParser.h`.
///
/// This was an `absl::flat_hash_map`, chosen because the map is created and destroyed often while
/// parsing. That is a real effect - for this construct/insert/lookup/destroy pattern the flat map
/// measures about twice as fast at one to three entries (13 ns against 29 ns, 55 ns against 121 ns)
/// and about three times as fast at 64. It is `std::unordered_map` now only because abseil is a
/// large dependency to carry for one map, and this one is built only when a caller asks for literal
/// positions: deducing a `ConstantExpressionTemplate` in the `Values` format, and highlighting in
/// the interactive client. If it ever shows up in a profile, the fix is a flat map that ClickHouse
/// already has - `HashMapWithStackMemory` would not allocate at all at these sizes - rather than
/// bringing abseil back.
struct LiteralTokenMap : std::unordered_map<const ASTLiteral *, LiteralTokenInfo>
{
    using std::unordered_map<const ASTLiteral *, LiteralTokenInfo>::unordered_map;
};

}
