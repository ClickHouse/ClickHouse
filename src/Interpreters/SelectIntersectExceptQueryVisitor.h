#pragma once

#include <unordered_set>

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>


namespace DB
{

class ASTFunction;

class SelectIntersectExceptQueryMatcher
{
public:
    struct Data
    {
        Data() = default;

        void initialize(const ASTSelectIntersectExceptQuery * select_intersect_except)
        {
            reversed_list_of_selects = select_intersect_except->list_of_selects->clone()->children;
            reversed_list_of_operators = select_intersect_except->list_of_operators;

            std::reverse(reversed_list_of_selects.begin(), reversed_list_of_selects.end());
            std::reverse(reversed_list_of_operators.begin(), reversed_list_of_operators.end());
        }

        ASTs reversed_list_of_selects;
        ASTSelectIntersectExceptQuery::Operators reversed_list_of_operators;
    };

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }

    static void visit(ASTPtr & ast, Data &);
    static void visit(ASTSelectIntersectExceptQuery &, Data &);
    static void visit(ASTSelectWithUnionQuery &, Data &);
};

/// Visit children first.
using SelectIntersectExceptQueryVisitor
    = InDepthNodeVisitor<SelectIntersectExceptQueryMatcher, false>;
}
