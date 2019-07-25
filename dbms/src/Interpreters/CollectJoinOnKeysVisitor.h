#pragma once

#include <Parsers/ASTFunction.h>
#include <Parsers/queryToString.h>

#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/Aliases.h>
#include <Interpreters/SyntaxAnalyzer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int AMBIGUOUS_COLUMN_NAME;
    extern const int LOGICAL_ERROR;
}


class CollectJoinOnKeysMatcher
{
public:
    using Visitor = ConstInDepthNodeVisitor<CollectJoinOnKeysMatcher, true>;

    struct Data
    {
        AnalyzedJoin & analyzed_join;
        const NameSet & source_columns;
        const NameSet & joined_columns;
        const Aliases & aliases;
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
            addJoinKeys(ast, left, right, data);
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

    static void addJoinKeys(const ASTPtr & expr, ASTPtr left_ast, ASTPtr right_ast, Data & data)
    {
        std::vector<const ASTIdentifier *> left_identifiers;
        std::vector<const ASTIdentifier *> right_identifiers;

        getIdentifiers(left_ast, left_identifiers);
        getIdentifiers(right_ast, right_identifiers);

        size_t left_idents_table = getTableForIdentifiers(left_identifiers, data);
        size_t right_idents_table = getTableForIdentifiers(right_identifiers, data);

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
            throw Exception("Cannot detect left and right JOIN keys. JOIN ON section is ambiguous.",
                            ErrorCodes::AMBIGUOUS_COLUMN_NAME);

        data.has_some = true;
    }

    static const ASTIdentifier * unrollAliases(const ASTIdentifier * identifier, const Aliases & aliases)
    {
        if (identifier->compound())
            return identifier;

        UInt32 max_attempts = 100;
        for (auto it = aliases.find(identifier->name); it != aliases.end();)
        {
            const ASTIdentifier * parent = identifier;
            identifier = it->second->as<ASTIdentifier>();
            if (!identifier)
                break; /// not a column alias
            if (identifier == parent)
                break; /// alias to itself with the same name: 'a as a'
            if (identifier->compound())
                break; /// not an alias. Break to prevent cycle through short names: 'a as b, t1.b as a'

            it = aliases.find(identifier->name);
            if (!max_attempts--)
                throw Exception("Cannot unroll aliases for '" + identifier->name + "'", ErrorCodes::LOGICAL_ERROR);
        }

        return identifier;
    }

    /// @returns 1 if identifiers belongs to left table, 2 for right table and 0 if unknown. Throws on table mix.
    /// Place detected identifier into identifiers[0] if any.
    static size_t getTableForIdentifiers(std::vector<const ASTIdentifier *> & identifiers, const Data & data)
    {
        size_t table_number = 0;

        for (auto & ident : identifiers)
        {
            const ASTIdentifier * identifier = unrollAliases(ident, data.aliases);
            if (!identifier)
                continue;

            /// Column name could be cropped to a short form in TranslateQualifiedNamesVisitor.
            /// In this case it saves membership in IdentifierSemantic.
            size_t membership = IdentifierSemantic::getMembership(*identifier);

            if (!membership)
            {
                const String & name = identifier->name;
                bool in_left_table = data.source_columns.count(name);
                bool in_right_table = data.joined_columns.count(name);

                if (in_left_table && in_right_table)
                    throw Exception("Column '" + name + "' is ambiguous", ErrorCodes::AMBIGUOUS_COLUMN_NAME);

                if (in_left_table)
                    membership = 1;
                if (in_right_table)
                    membership = 2;
            }

            if (membership && table_number == 0)
            {
                table_number = membership;
                std::swap(ident, identifiers[0]); /// move first detected identifier to the first position
            }

            if (membership && membership != table_number)
            {
                throw Exception("Invalid columns in JOIN ON section. Columns "
                            + identifiers[0]->getAliasOrColumnName() + " and " + ident->getAliasOrColumnName()
                            + " are from different tables.", ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
            }
        }

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
