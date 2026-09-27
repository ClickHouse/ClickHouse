#pragma once

#include <Analyzer/FunctionNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Resolve/ScopeAliases.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/Utils.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionFile.h>

namespace DB
{

class TableFunctionsWithClusterAlternativesVisitor : public InDepthQueryTreeVisitor<TableFunctionsWithClusterAlternativesVisitor, /*const_visitor=*/true>
{
public:
    /// `aliases` are the aliases of the scope of the visited query. An identifier on the right side of `IN` that names
    /// one of them is resolved to the aliased expression rather than to a table or a CTE.
    explicit TableFunctionsWithClusterAlternativesVisitor(const ScopeAliases * aliases_ = nullptr)
        : aliases(aliases_)
    {}

    void visitImpl(const QueryTreeNodePtr & node)
    {
        if (node->getNodeType() == QueryTreeNodeType::TABLE_FUNCTION)
            ++table_function_count;
        else if (node->getNodeType() == QueryTreeNodeType::TABLE)
            ++table_count;
        else if (node->getNodeType() == QueryTreeNodeType::QUERY && node->as<QueryNode>()->isSubquery())
            ++subquery_count;
        else if (node->getNodeType() == QueryTreeNodeType::JOIN)
            has_join = true;
        else if (const auto * function_node = node->as<FunctionNode>())
        {
            const auto & function_name = function_node->getFunctionName();
            const auto & arguments = function_node->getArguments().getNodes();
            if (isNameOfInFunction(function_name))
            {
                if (arguments.size() == 2 && mayBeSubquery(arguments[1]))
                    has_in_with_subquery = true;
            }
            else if (function_name == "exists" && arguments.size() == 1
                && (arguments[0]->getNodeType() == QueryTreeNodeType::QUERY || arguments[0]->getNodeType() == QueryTreeNodeType::UNION))
            {
                has_exists_with_subquery = true;
            }
            /// The body of a SQL user-defined function is expanded only while resolving, so it may hide an `IN`.
            else if (sqlUserDefinedFunctionMayHaveInWithSubquery(function_name))
            {
                calls_sql_user_defined_function_with_in = true;
            }
        }
    }

    bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child)
    {
        /// The aliases of the scope do not apply to identifiers inside a subquery, so visit it without them.
        if (aliases && (child->getNodeType() == QueryTreeNodeType::QUERY || child->getNodeType() == QueryTreeNodeType::UNION))
        {
            TableFunctionsWithClusterAlternativesVisitor subquery_visitor;
            subquery_visitor.visit(child);
            merge(subquery_visitor);
            return false;
        }
        return true;
    }

    /// Whether the query may have `IN` with a subquery (`x IN (SELECT ...)`, `x IN t`, `x IN cte`), which a cluster engine would
    /// execute on every replica. `EXISTS (subquery)` is rewritten to `IN` unless it is executed as a scalar subquery.
    bool mayHaveInWithSubquery(bool exists_is_rewritten_to_in) const
    {
        return has_in_with_subquery || calls_sql_user_defined_function_with_in || (exists_is_rewritten_to_in && has_exists_with_subquery);
    }

    bool shouldReplaceWithClusterAlternatives() const
    {
        return subquery_count <= 1 && !has_join && ((table_count + table_function_count) == 1 || (table_function_count == 0));
    }

private:
    /// Whether the right side of `IN` may be a set built from a subquery. The tree is not resolved yet, so `x IN t` and
    /// `x IN cte` are only identifiers here, as is `x IN s` with an alias `s` of a set of constants.
    bool mayBeSubquery(const QueryTreeNodePtr & set_node) const
    {
        switch (set_node->getNodeType())
        {
            case QueryTreeNodeType::QUERY:
            case QueryTreeNodeType::UNION:
            case QueryTreeNodeType::TABLE_FUNCTION:
                return true;
            case QueryTreeNodeType::IDENTIFIER:
            {
                const auto & identifier = set_node->as<const IdentifierNode &>().getIdentifier();
                if (!aliases || !identifier.isShort())
                    return true;
                auto it = aliases->alias_name_to_expression_node.find(identifier.getFullName());
                if (it == aliases->alias_name_to_expression_node.end())
                    return true;
                /// An alias of a constant or of a function (`[1, 3]`, `tuple(1, 3)`) is not a subquery. A function that
                /// hides one (a SQL user-defined function) is accounted for where the aliased expression is visited.
                const auto aliased_type = it->second->getNodeType();
                return aliased_type != QueryTreeNodeType::CONSTANT && aliased_type != QueryTreeNodeType::FUNCTION;
            }
            default:
                return false;
        }
    }

    /// Whether the body of the SQL user-defined function `function_name` may have `IN` with a subquery once it is expanded.
    static bool sqlUserDefinedFunctionMayHaveInWithSubquery(const String & function_name)
    {
        std::unordered_set<String> visited;
        return sqlUserDefinedFunctionMayHaveInWithSubquery(function_name, visited);
    }

    static bool sqlUserDefinedFunctionMayHaveInWithSubquery(const String & function_name, std::unordered_set<String> & visited)
    {
        if (!visited.insert(function_name).second)
            return false;
        auto function_ast = UserDefinedSQLFunctionFactory::instance().tryGet(function_name);
        if (!function_ast)
            return false;
        const auto * create_function_query = function_ast->as<ASTCreateSQLFunctionQuery>();
        if (!create_function_query)
            return false;
        return astMayHaveInWithSubquery(create_function_query->function_core, visited);
    }

    /// Any subquery in the body counts: it becomes a subquery of the query once the body is expanded. So does `IN` a
    /// parameter, an identifier of a table or a table function, because the argument of the call may be a subquery.
    static bool astMayHaveInWithSubquery(const ASTPtr & ast, std::unordered_set<String> & visited)
    {
        if (ast->as<ASTSubquery>())
            return true;
        if (const auto * function = ast->as<ASTFunction>())
        {
            if (isNameOfInFunction(function->name) && function->arguments && function->arguments->children.size() == 2)
            {
                const auto & set_ast = function->arguments->children[1];
                if (set_ast->as<ASTIdentifier>() || set_ast->as<ASTTableIdentifier>()
                    || (set_ast->as<ASTFunction>() && TableFunctionFactory::instance().isTableFunctionName(set_ast->as<ASTFunction>()->name)))
                    return true;
            }
            if (sqlUserDefinedFunctionMayHaveInWithSubquery(function->name, visited))
                return true;
        }
        for (const auto & child : ast->children)
            if (astMayHaveInWithSubquery(child, visited))
                return true;
        return false;
    }

    void merge(const TableFunctionsWithClusterAlternativesVisitor & other)
    {
        table_count += other.table_count;
        table_function_count += other.table_function_count;
        subquery_count += other.subquery_count;
        has_join |= other.has_join;
        has_in_with_subquery |= other.has_in_with_subquery;
        has_exists_with_subquery |= other.has_exists_with_subquery;
        calls_sql_user_defined_function_with_in |= other.calls_sql_user_defined_function_with_in;
    }

    const ScopeAliases * aliases = nullptr;

    size_t table_count = 0;
    size_t table_function_count = 0;
    // Number of subqueries that appear
    size_t subquery_count = 0;

    bool has_join = false;
    bool has_in_with_subquery = false;
    bool has_exists_with_subquery = false;
    bool calls_sql_user_defined_function_with_in = false;
};

}
