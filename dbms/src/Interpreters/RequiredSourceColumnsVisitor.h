#pragma once

#include <Interpreters/ColumnNamesContext.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class ASTIdentifier;
class ASTFunction;
class ASTSelectQuery;
struct ASTTablesInSelectQueryElement;
struct ASTArrayJoin;
struct ASTTableExpression;

class RequiredSourceColumnsMatcher
{
public:
    using Data = ColumnNamesContext;

    static constexpr const char * label = "RequiredSourceColumns";

    static bool needChildVisit(ASTPtr & node, const ASTPtr & child);
    static std::vector<ASTPtr *> visit(ASTPtr & ast, Data & data);

private:
    static void visit(const ASTIdentifier & node, const ASTPtr &, Data & data);
    static void visit(const ASTFunction & node, const ASTPtr &, Data & data);
    static void visit(ASTTablesInSelectQueryElement & node, const ASTPtr &, Data & data);
    static std::vector<ASTPtr *> visit(ASTTableExpression & node, const ASTPtr &, Data & data);
    static std::vector<ASTPtr *> visit(const ASTArrayJoin & node, const ASTPtr &, Data & data);
    static std::vector<ASTPtr *> visit(ASTSelectQuery & select, const ASTPtr &, Data & data);
};

/// Extracts all the information about columns and tables from ASTSelectQuery block into ColumnNamesContext object.
/// It doesn't use anything but AST. It visits nodes from bottom to top except ASTFunction content to get aliases in right manner.
/// @note There's some ambiguousness with nested columns names that can't be solved without schema.
using RequiredSourceColumnsVisitor = InDepthNodeVisitor<RequiredSourceColumnsMatcher, false>;

}
