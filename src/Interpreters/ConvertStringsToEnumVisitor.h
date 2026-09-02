#pragma once

#include <vector>
#include <unordered_map>
#include <unordered_set>

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class ASTFunction;

struct FindUsedFunctionsMatcher
{
    using Visitor = ConstInDepthNodeVisitor<FindUsedFunctionsMatcher, true>;

    struct Data
    {
        const std::unordered_set<String> & names;
        std::unordered_set<String> & used_functions;
        std::vector<String> call_stack = {};
    };

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &);
    static void visit(const ASTPtr & ast, Data & data);
    static void visit(const ASTFunction & func, Data & data);
};

using FindUsedFunctionsVisitor = FindUsedFunctionsMatcher::Visitor;

struct ConvertStringsToEnumMatcher
{
    struct Data
    {
        std::unordered_set<String> & used_functions;
    };

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &);
    static void visit(ASTPtr & ast, Data & data);
    static void visit(ASTFunction & function_node, Data & data);
};

using ConvertStringsToEnumVisitor = InDepthNodeVisitor<ConvertStringsToEnumMatcher, true>;

}
