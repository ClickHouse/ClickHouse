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
        bool has_some{false};

        void addJoinKeys(const ASTPtr & left_ast, const ASTPtr & right_ast, const std::pair<size_t, size_t> & table_no);
        void addAsofJoinKeys(const ASTPtr & left_ast, const ASTPtr & right_ast, const std::pair<size_t, size_t> & table_no,
                             const ASOF::Inequality & asof_inequality);
        void asofToJoinKeys();
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
    static void visit(const ASTFunction & func, const ASTPtr & ast, Data & data);

    static void getIdentifiers(const ASTPtr & ast, std::vector<const ASTIdentifier *> & out);
    static std::pair<size_t, size_t> getTableNumbers(const ASTPtr & expr, const ASTPtr & left_ast, const ASTPtr & right_ast, Data & data);
    static const ASTIdentifier * unrollAliases(const ASTIdentifier * identifier, const Aliases & aliases);
    static size_t getTableForIdentifiers(std::vector<const ASTIdentifier *> & identifiers, const Data & data);

    [[noreturn]] static void throwSyntaxException(const String & msg);
};

/// Parse JOIN ON expression and collect ASTs for joined columns.
using CollectJoinOnKeysVisitor = CollectJoinOnKeysMatcher::Visitor;

}
