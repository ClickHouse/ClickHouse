#pragma once

#include <Core/Names.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/Aliases.h>


namespace DB
{

class ASTIdentifier;
class TableJoin;

namespace ASOF
{
    enum class Inequality;
}

enum class JoinIdentifierPos
{
    /// Position can't be established, identifier not resolved
    Unknown,
    /// Left side of JOIN
    Left,
    /// Right side of JOIN
    Right,
    /// Expression not valid, e.g. doesn't contain identifiers
    NotApplicable,
};

using JoinIdentifierPosPair = std::pair<JoinIdentifierPos, JoinIdentifierPos>;


class CollectJoinOnKeysMatcher
{
public:
    using Visitor = ConstInDepthNodeVisitor<CollectJoinOnKeysMatcher, true>;

    struct Data
    {
        TableJoin & analyzed_join;
        const TableWithColumnNamesAndTypes & left_table;
        const TableWithColumnNamesAndTypes & right_table;
        const Aliases & aliases;
        const bool is_asof{false};
        ASTPtr asof_left_key{};
        ASTPtr asof_right_key{};

        void addJoinKeys(const ASTPtr & left_ast, const ASTPtr & right_ast, JoinIdentifierPosPair table_pos);
        void addAsofJoinKeys(const ASTPtr & left_ast, const ASTPtr & right_ast, JoinIdentifierPosPair table_pos,
                             const ASOF::Inequality & asof_inequality);
        void asofToJoinKeys();
    };

    static void visit(const ASTPtr & ast, Data & data)
    {
        if (auto * func = ast->as<ASTFunction>())
        {
            visit(*func, ast, data);
        }
        else if (auto * ident = ast->as<ASTIdentifier>())
        {
            visit(*ident, ast, data);
        }
        else
        {
            /// visit children
        }
    }

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &)
    {
        if (auto * func = node->as<ASTFunction>())
            return func->name == "and";
        return true;
    }

private:
    static void visit(const ASTFunction & func, const ASTPtr & ast, Data & data);
    static void visit(const ASTIdentifier & ident, const ASTPtr & ast, Data & data);

    static void getIdentifiers(const ASTPtr & ast, std::vector<const ASTIdentifier *> & out);
    static JoinIdentifierPosPair getTableNumbers(const ASTPtr & left_ast, const ASTPtr & right_ast, Data & data);
    static const ASTIdentifier * unrollAliases(const ASTIdentifier * identifier, const Aliases & aliases);
    static JoinIdentifierPos getTableForIdentifiers(const ASTPtr & ast, bool throw_on_table_mix, const Data & data);
};

/// Parse JOIN ON expression and collect ASTs for joined columns.
using CollectJoinOnKeysVisitor = CollectJoinOnKeysMatcher::Visitor;

}
