#pragma once

#include <Parsers/ASTFunction.h>
#include <Parsers/queryToString.h>

#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/SyntaxAnalyzer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_JOIN_ON_EXPRESSION;
}


class CollectJoinOnKeysMatcher
{
public:
    using Visitor = ConstInDepthNodeVisitor<CollectJoinOnKeysMatcher, true>;

    struct Data
    {
        AnalyzedJoin & analyzed_join;
        bool has_some = false;
    };

    static void visit(const ASTPtr & ast, Data & data)
    {
        if (auto * func = ast->as<ASTFunction>())
            visit(*func, ast, data);
    }

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &)
    {
        if (auto * func = node->as<ASTFunction>())
            if (func->name == "equals")
                return false;
        return true;
    }

private:
    static void visit(const ASTFunction & func, const ASTPtr & ast, Data & data)
    {
        if (func.name == "and")
            return; /// go into children

        if (func.name == "equals")
        {
            ASTPtr left = func.arguments->children.at(0)->clone();
            ASTPtr right = func.arguments->children.at(1)->clone();
            addColumnsFromEqualsExpr(ast, left, right, data);
            return;
        }

        throwSyntaxException("Expected equals expression, got " + queryToString(ast) + ".");
    }

    static void getIdentifiers(const ASTPtr & ast, std::vector<const ASTIdentifier *> & out)
    {
        if (const auto * ident = ast->as<ASTIdentifier>())
        {
            if (IdentifierSemantic::getColumnName(*ident))
                out.push_back(ident);
            return;
        }

        for (const auto & child : ast->children)
            getIdentifiers(child, out);
    }

    static void addColumnsFromEqualsExpr(const ASTPtr & expr, ASTPtr left_ast, ASTPtr right_ast, Data & data)
    {
        std::vector<const ASTIdentifier *> left_identifiers;
        std::vector<const ASTIdentifier *> right_identifiers;

        getIdentifiers(left_ast, left_identifiers);
        getIdentifiers(right_ast, right_identifiers);

        size_t left_idents_table = checkSameTable(left_identifiers);
        size_t right_idents_table = checkSameTable(right_identifiers);

        if (left_idents_table && left_idents_table == right_idents_table)
        {
            auto left_name = queryToString(*left_identifiers[0]);
            auto right_name = queryToString(*right_identifiers[0]);

            throwSyntaxException("In expression " + queryToString(expr) + " columns " + left_name + " and " + right_name
                                 + " are from the same table but from different arguments of equal function.");
        }

        if (left_idents_table == 1 || right_idents_table == 2)
            data.analyzed_join.addOnKeys(left_ast, right_ast);
        else if (left_idents_table == 2 || right_idents_table == 1)
            data.analyzed_join.addOnKeys(right_ast, left_ast);
        else
        {
            /// Default variant when all identifiers may be from any table.
            data.analyzed_join.addOnKeys(left_ast, right_ast); /// FIXME
        }

        data.has_some = true;
    }

    static size_t checkSameTable(std::vector<const ASTIdentifier *> & identifiers)
    {
        size_t table_number = 0;
        const ASTIdentifier * detected = nullptr;

        for (const auto & identifier : identifiers)
        {
            /// It's set in TranslateQualifiedNamesVisitor
            size_t membership = IdentifierSemantic::getMembership(*identifier);
            if (membership && table_number == 0)
            {
                table_number = membership;
                detected = identifier;
            }

            if (membership && membership != table_number)
            {
                throw Exception("Invalid columns in JOIN ON section. Columns "
                            + detected->getAliasOrColumnName() + " and " + identifier->getAliasOrColumnName()
                            + " are from different tables.", ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
            }
        }

        identifiers.clear();
        if (detected)
            identifiers.push_back(detected);
        return table_number;
    }

    [[noreturn]] static void throwSyntaxException(const String & msg)
    {
        throw Exception("Invalid expression for JOIN ON. " + msg +
            " Supported syntax: JOIN ON Expr([table.]column, ...) = Expr([table.]column, ...) "
            "[AND Expr([table.]column, ...) = Expr([table.]column, ...) ...]",
            ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
    }
};

/// Parse JOIN ON expression and collect ASTs for joined columns.
using CollectJoinOnKeysVisitor = CollectJoinOnKeysMatcher::Visitor;

}
