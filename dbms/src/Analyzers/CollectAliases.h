#pragma once

#include <Parsers/IAST.h>
#include <unordered_map>


namespace DB
{

class WriteBuffer;


/** Build a map: alias -> AST node.
  *
  * Also fill information about what kind each alias has:
  * - expression alias;
  * - table alias;
  * - ARRAY JOIN alias.
  *
  * As extension to standard SQL, aliases could be specified and used in any part of query.
  * Example: SELECT a, (1 AS a) + 1 AS b FROM t GROUP BY a, b
  * Alias could be used in query before it is defined.
  *
  * Aliases could not be redefined. Example: 1 AS a, a + 1 AS a - is prohibited.
  *
  * Don't descend into subqueries (as aliases are local inside them).
  */
struct CollectAliases
{
    void process(const ASTPtr & ast);

    enum class Kind
    {
        Expression, /// Example: SELECT a AS b, f(x) AS y
        Table,      /// Example: SELECT t.* FROM (SELECT 1) AS t
        ArrayJoin   /// Example: SELECT t.x.a FROM t ARRAY JOIN arr AS x
    };

    struct AliasInfo
    {
        ASTPtr node;
        Kind kind;

        AliasInfo(const ASTPtr & node, Kind kind) : node(node), kind(kind) {}
    };

    using Aliases = std::unordered_map<String, AliasInfo>;
    Aliases aliases;

    /// Debug output
    void dump(WriteBuffer & out) const;
};

}
