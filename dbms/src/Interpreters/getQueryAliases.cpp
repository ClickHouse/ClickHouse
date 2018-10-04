#include <ostream>
#include <sstream>
#include <Interpreters/getQueryAliases.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTSubquery.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MULTIPLE_EXPRESSIONS_FOR_ALIAS;
}

/// ignore_levels - aliases in how many upper levels of the subtree should be ignored.
/// For example, with ignore_levels=1 ast can not be put in the dictionary, but its children can.
void getQueryAliases(ASTPtr & ast, Aliases & aliases, int ignore_levels)
{

    /// Bottom-up traversal. We do not go into subqueries.
    for (auto & child : ast->children)
    {
        int new_ignore_levels = std::max(0, ignore_levels - 1);

        /// The top-level aliases in the ARRAY JOIN section have a special meaning, we will not add them
        ///  (skip the expression list itself and its children).
        if (typeid_cast<ASTArrayJoin *>(ast.get()))
            new_ignore_levels = 3;

        /// Don't descent into table functions and subqueries.
        if (!typeid_cast<ASTTableExpression *>(child.get())
            && !typeid_cast<ASTSelectWithUnionQuery *>(child.get()))
            getQueryAliases(child, aliases, new_ignore_levels);
    }

    if (ignore_levels > 0)
        return;

    String alias = ast->tryGetAlias();
    if (!alias.empty())
    {
        if (aliases.count(alias) && ast->getTreeHash() != aliases[alias]->getTreeHash())
        {
            std::stringstream message;
            message << "Different expressions with the same alias " << backQuoteIfNeed(alias) << ":\n";
            formatAST(*ast, message, false, true);
            message << "\nand\n";
            formatAST(*aliases[alias], message, false, true);
            message << "\n";

            throw Exception(message.str(), ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS);
        }

        aliases[alias] = ast;
    }
    else if (auto subquery = typeid_cast<ASTSubquery *>(ast.get()))
    {
        /// Set unique aliases for all subqueries. This is needed, because content of subqueries could change after recursive analysis,
        ///  and auto-generated column names could become incorrect.

        if (subquery->alias.empty())
        {
            size_t subquery_index = 1;
            while (true)
            {
                alias = "_subquery" + toString(subquery_index);
                if (!aliases.count("_subquery" + toString(subquery_index)))
                    break;
                ++subquery_index;
            }

            subquery->setAlias(alias);
            subquery->prefer_alias_to_column_name = true;
            aliases[alias] = ast;
        }
    }
}

}
