#include <Analyzers/CollectAliases.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int MULTIPLE_EXPRESSIONS_FOR_ALIAS;
}


static void processImpl(const ASTPtr & ast, CollectAliases::Aliases & aliases, CollectAliases::Kind kind, size_t keep_kind_for_depth)
{
    String alias = ast->tryGetAlias();
    if (!alias.empty())
    {
        auto it_inserted = aliases.emplace(alias, CollectAliases::AliasInfo(ast, kind));

        if (!it_inserted.second && ast->getTreeHash() != it_inserted.first->second.node->getTreeHash())
        {
            std::stringstream message;
            message << "Different expressions with the same alias " << backQuoteIfNeed(alias) << ":\n";
            formatAST(*it_inserted.first->second.node, message, false, true);
            message << "\nand\n";
            formatAST(*ast, message, false, true);
            message << "\n";

            throw Exception(message.str(), ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS);
        }
    }

    for (auto & child : ast->children)
    {
        if (typeid_cast<const ASTSelectQuery *>(child.get()))
        {
            /// Don't go into subqueries.
        }
        else if (typeid_cast<const ASTTableExpression *>(child.get()))
        {
            processImpl(child, aliases, CollectAliases::Kind::Table, 1);
        }
        else if (typeid_cast<const ASTArrayJoin *>(child.get()))
        {
            /// ASTArrayJoin -> ASTExpressionList -> element of expression AS alias
            processImpl(child, aliases, CollectAliases::Kind::ArrayJoin, 3);
        }
        else if (keep_kind_for_depth > 0)
        {
            processImpl(child, aliases, kind, keep_kind_for_depth - 1);
        }
        else
        {
            processImpl(child, aliases, CollectAliases::Kind::Expression, 0);
        }
    }
}


void CollectAliases::process(const ASTPtr & ast)
{
    processImpl(ast, aliases, Kind::Expression, 0);
}


void CollectAliases::dump(WriteBuffer & out) const
{
    /// For need of tests, we need to dump result in some fixed order.
    std::vector<Aliases::const_iterator> vec;
    vec.reserve(aliases.size());
    for (auto it = aliases.begin(); it != aliases.end(); ++it)
        vec.emplace_back(it);

    std::sort(vec.begin(), vec.end(), [](const auto & a, const auto & b) { return a->first < b->first; });

    for (const auto & it : vec)
    {
        writeProbablyBackQuotedString(it->first, out);
        writeCString(" -> ", out);

        switch (it->second.kind)
        {
            case Kind::Expression:
                writeCString("(expression) ", out);
                break;
            case Kind::Table:
                writeCString("(table) ", out);
                break;
            case Kind::ArrayJoin:
                writeCString("(array join) ", out);
                break;
        }

        std::stringstream formatted_ast;
        formatAST(*it->second.node, formatted_ast, false, true);
        writeString(formatted_ast.str(), out);

        writeChar('\n', out);
    }
}


}
