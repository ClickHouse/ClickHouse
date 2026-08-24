#pragma once

#include <Interpreters/RequiredSourceColumnsData.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

namespace ErrorCodes
{
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
    using Visitor = ConstInDepthNodeVisitor<RequiredSourceColumnsMatcher, false>;
    using Data = RequiredSourceColumnsData;

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
    static void visit(const ASTPtr & ast, Data & data);

    static std::vector<String> extractNamesFromLambda(const ASTFunction & node);

private:
    static void visit(const ASTIdentifier & node, const ASTPtr &, Data & data);
    static void visit(const ASTFunction & node, const ASTPtr &, Data & data);
    static void visit(const ASTTablesInSelectQueryElement & node, const ASTPtr &, Data & data);
    static void visit(const ASTTableExpression & node, const ASTPtr &, Data & data);
    static void visit(const ASTArrayJoin & node, const ASTPtr &, Data & data);
    static void visit(const ASTSelectQuery & select, const ASTPtr &, Data & data);
};

/// Extracts all the information about columns and tables from ASTSelectQuery block into Data object.
/// It doesn't use anything but AST. It visits nodes from bottom to top except ASTFunction content to get aliases in right manner.
/// @note There's some ambiguousness with nested columns names that can't be solved without schema.
using RequiredSourceColumnsVisitor = RequiredSourceColumnsMatcher::Visitor;

}
