#pragma once

#include <Analyzer/FunctionNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Resolve/IdentifierResolveScope.h>
#include <Analyzer/Resolve/QueryExpressionsAliasVisitor.h>
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

#include <set>

namespace DB
{

class TableFunctionsWithClusterAlternativesVisitor : public InDepthQueryTreeVisitor<TableFunctionsWithClusterAlternativesVisitor, /*const_visitor=*/true>
{
public:
    /// `scope` is the scope of the visited query. An identifier on the right side of `IN` that names an alias of this
    /// scope, or of a parent scope if `aliases_visible_from_parent_scopes_` (`enable_global_with_statement`), is resolved
    /// to the aliased expression rather than to a table or a CTE.
    TableFunctionsWithClusterAlternativesVisitor(const IdentifierResolveScope & scope, bool aliases_visible_from_parent_scopes_)
        : aliases_visible_from_parent_scopes(aliases_visible_from_parent_scopes_)
    {
        alias_scopes.push_back(&scope.aliases);
        if (aliases_visible_from_parent_scopes)
            for (const auto * parent_scope = scope.parent_scope; parent_scope; parent_scope = parent_scope->parent_scope)
                alias_scopes.push_back(&parent_scope->aliases);
    }

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
            else
            {
                std::unordered_set<String> functions_in_expansion;
                visitSQLUserDefinedFunction(function_name, functions_in_expansion);
            }
        }
    }

    bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child)
    {
        /// A subquery is a scope of its own: its aliases come first, and the aliases of this scope are visible only with
        /// `enable_global_with_statement`.
        if (child->getNodeType() == QueryTreeNodeType::QUERY)
        {
            TableFunctionsWithClusterAlternativesVisitor subquery_visitor(*this, child->as<const QueryNode &>());
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
    TableFunctionsWithClusterAlternativesVisitor(const TableFunctionsWithClusterAlternativesVisitor & parent, const QueryNode & subquery)
        : aliases_visible_from_parent_scopes(parent.aliases_visible_from_parent_scopes)
        , subquery_aliases(std::make_shared<ScopeAliases>())
    {
        /// The same parts of the query as `QueryAnalyzer::resolveQuery` collects the aliases from.
        QueryExpressionsAliasVisitor alias_visitor(*subquery_aliases);
        for (QueryTreeNodePtr node : {subquery.getWithNode(), subquery.getProjectionNode(), subquery.getPrewhere(), subquery.getWhere(),
                 subquery.getGroupByNode(), subquery.getHaving(), subquery.getWindowNode(), subquery.getQualify(),
                 subquery.getOrderByNode(), subquery.getInterpolate()})
            if (node)
                alias_visitor.visit(node);

        alias_scopes.push_back(subquery_aliases.get());
        if (aliases_visible_from_parent_scopes)
            alias_scopes.insert(alias_scopes.end(), parent.alias_scopes.begin(), parent.alias_scopes.end());
    }

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
                return !identifier.isShort() || aliasMayBeSubquery(identifier.getFullName());
            }
            default:
                return false;
        }
    }

    /// Whether `name` may be a table, a CTE or an alias of a subquery. An alias of a constant or of a function (`[1, 3]`,
    /// `tuple(1, 3)`) is not a subquery: a function that hides one (a SQL user-defined function) is accounted for where
    /// the aliased expression is visited. An alias of an identifier (`WITH s1 AS s2`) is followed.
    bool aliasMayBeSubquery(String name) const
    {
        /// An alias found in a scope is resolved there, so the aliased identifier is looked up from that scope outwards.
        size_t first_scope = 0;
        std::set<std::pair<size_t, String>> visited;
        while (visited.emplace(first_scope, name).second)
        {
            const QueryTreeNodePtr * aliased_node = nullptr;
            for (size_t i = first_scope; i < alias_scopes.size() && !aliased_node; ++i)
            {
                auto it = alias_scopes[i]->alias_name_to_expression_node.find(name);
                if (it != alias_scopes[i]->alias_name_to_expression_node.end())
                {
                    aliased_node = &it->second;
                    first_scope = i;
                }
            }
            if (!aliased_node)
                return true;

            const auto * aliased_identifier = (*aliased_node)->as<IdentifierNode>();
            if (!aliased_identifier)
                return (*aliased_node)->getNodeType() != QueryTreeNodeType::CONSTANT && (*aliased_node)->getNodeType() != QueryTreeNodeType::FUNCTION;
            if (!aliased_identifier->getIdentifier().isShort())
                return true;
            name = aliased_identifier->getIdentifier().getFullName();
        }
        /// A cycle of aliases, which the analyzer rejects.
        return true;
    }

    /// Account for `IN` and `EXISTS` with a subquery in the body of the SQL user-defined function `function_name`, which
    /// appear once it is expanded.
    void visitSQLUserDefinedFunction(const String & function_name, std::unordered_set<String> & functions_in_expansion)
    {
        auto function_ast = UserDefinedSQLFunctionFactory::instance().tryGet(function_name);
        if (!function_ast)
            return;
        const auto * create_function_query = function_ast->as<ASTCreateSQLFunctionQuery>();
        if (!create_function_query)
            return;
        /// A recursive call is rejected when the body is expanded.
        if (!functions_in_expansion.insert(function_name).second)
            return;

        /// The arguments of the call are resolved before they are bound to the parameters, so a subquery argument is a
        /// scalar subquery, and a parameter on the right side of `IN` is never a set built from a subquery.
        const auto & lambda = create_function_query->function_core->children.at(0);
        std::unordered_set<String> parameter_names;
        for (const auto & parameter : lambda->children.at(0)->children.at(0)->children)
            parameter_names.insert(parameter->as<ASTIdentifier &>().name());

        visitSQLUserDefinedFunctionBody(lambda->children.at(1), parameter_names, functions_in_expansion);
        functions_in_expansion.erase(function_name);
    }

    /// A scalar subquery in the body is executed before the query is sent to the replicas, so only a subquery on the right
    /// side of `IN` or in `EXISTS` counts. So does `IN` a table, a CTE or a table function.
    void visitSQLUserDefinedFunctionBody(
        const ASTPtr & ast,
        const std::unordered_set<String> & parameter_names,
        std::unordered_set<String> & functions_in_expansion)
    {
        if (const auto * function = ast->as<ASTFunction>())
        {
            const auto * arguments = function->arguments.get();
            if (isNameOfInFunction(function->name) && arguments && arguments->children.size() == 2
                && astMayBeSubquery(arguments->children[1], parameter_names))
                calls_sql_user_defined_function_with_in = true;
            else if (function->name == "exists" && arguments && arguments->children.size() == 1 && arguments->children[0]->as<ASTSubquery>())
                has_exists_with_subquery = true;
            else
                visitSQLUserDefinedFunction(function->name, functions_in_expansion);
        }
        for (const auto & child : ast->children)
            visitSQLUserDefinedFunctionBody(child, parameter_names, functions_in_expansion);
    }

    /// The same as `mayBeSubquery`, for the right side of `IN` in the body of a SQL user-defined function.
    bool astMayBeSubquery(const ASTPtr & ast, const std::unordered_set<String> & parameter_names) const
    {
        if (ast->as<ASTSubquery>() || ast->as<ASTTableIdentifier>())
            return true;
        if (const auto * identifier = ast->as<ASTIdentifier>())
        {
            if (identifier->compound())
                return true;
            return !parameter_names.contains(identifier->name()) && aliasMayBeSubquery(identifier->name());
        }
        if (const auto * function = ast->as<ASTFunction>())
            return TableFunctionFactory::instance().isTableFunctionName(function->name);
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

    bool aliases_visible_from_parent_scopes = true;
    /// The aliases of the scope of the visited query, then of the parent scopes visible from it.
    std::vector<const ScopeAliases *> alias_scopes;
    std::shared_ptr<ScopeAliases> subquery_aliases;

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
