#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;
class ColumnsDescription;
class ASTIdentifier;


/* The Visitor is used to replace ALIAS by EXPRESSION when we refer to ALIAS
 * column in index definition.
 *
 * For example, if we have following create statement:
 * CREATE TABLE t
 * (
 *     col UInt8,
 *     col_alias ALIAS  col + 1
 *     INDEX idx (col_alias) TYPE minmax
 * ) ENGINE = MergeTree ORDER BY col;
 * we need call the visitor to replace `col_alias` by `col` + 1 when get index
 * description from index definition AST.
*/
class ReplaceAliasByExpressionMatcher
{
public:
    struct Data
    {
        const ColumnsDescription & columns;
    };

    static void visit(ASTPtr & ast, Data &);
    static void visit(const ASTIdentifier &, ASTPtr & ast, Data &);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
};

}
