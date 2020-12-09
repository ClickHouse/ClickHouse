#include <Poco/String.h>
#include <Core/Names.h>
#include <Interpreters/QueryNormalizer.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_DEEP_AST;
    extern const int CYCLIC_ALIASES;
    extern const int UNKNOWN_QUERY_PARAMETER;
    extern const int BAD_ARGUMENTS;
}


class CheckASTDepth
{
public:
    explicit CheckASTDepth(QueryNormalizer::Data & data_)
        : data(data_)
    {
        if (data.level > data.settings.max_ast_depth)
            throw Exception("Normalized AST is too deep. Maximum: " + toString(data.settings.max_ast_depth), ErrorCodes::TOO_DEEP_AST);
        ++data.level;
    }

    ~CheckASTDepth()
    {
        --data.level;
    }

private:
    QueryNormalizer::Data & data;
};


class RestoreAliasOnExitScope
{
public:
    explicit RestoreAliasOnExitScope(String & alias_)
        : alias(alias_)
        , copy(alias_)
    {}

    ~RestoreAliasOnExitScope()
    {
        alias = copy;
    }

private:
    String & alias;
    const String copy;
};


void QueryNormalizer::visit(ASTIdentifier & node, ASTPtr & ast, Data & data)
{
    auto & current_asts = data.current_asts;
    String & current_alias = data.current_alias;

    if (!IdentifierSemantic::getColumnName(node))
        return;

    /// If it is an alias, but not a parent alias (for constructs like "SELECT column + 1 AS column").
    auto it_alias = data.aliases.find(node.name());
    if (it_alias != data.aliases.end() && current_alias != node.name())
    {
        if (!IdentifierSemantic::canBeAlias(node))
            return;

        /// We are alias for other column (node.name), but we are alias by
        /// ourselves to some other column
        const auto & alias_node = it_alias->second;

        String our_alias_or_name = alias_node->getAliasOrColumnName();
        std::optional<String> our_name = IdentifierSemantic::getColumnName(alias_node);

        String node_alias = ast->tryGetAlias();

        if (current_asts.count(alias_node.get()) /// We have loop of multiple aliases
            || (node.name() == our_alias_or_name && our_name && node_alias == *our_name)) /// Our alias points to node.name, direct loop
            throw Exception("Cyclic aliases", ErrorCodes::CYCLIC_ALIASES);

        /// Let's replace it with the corresponding tree node.
        if (!node_alias.empty() && node_alias != our_alias_or_name)
        {
            /// Avoid infinite recursion here
            auto opt_name = IdentifierSemantic::getColumnName(alias_node);
            bool is_cycle = opt_name && *opt_name == node.name();

            if (!is_cycle)
            {
                /// In a construct like "a AS b", where a is an alias, you must set alias b to the result of substituting alias a.
                ast = alias_node->clone();
                ast->setAlias(node_alias);
            }
        }
        else
            ast = alias_node;
    }
}

void QueryNormalizer::visit(ASTTablesInSelectQueryElement & node, const ASTPtr &, Data & data)
{
    /// normalize JOIN ON section
    if (node.table_join)
    {
        auto & join = node.table_join->as<ASTTableJoin &>();
        if (join.on_expression)
            visit(join.on_expression, data);
    }
}

static bool needVisitChild(const ASTPtr & child)
{
    return !(child->as<ASTSelectQuery>() || child->as<ASTTableExpression>());
}

/// special visitChildren() for ASTSelectQuery
void QueryNormalizer::visit(ASTSelectQuery & select, const ASTPtr &, Data & data)
{
    for (auto & child : select.children)
        if (needVisitChild(child))
            visit(child, data);

    /// If the WHERE clause or HAVING consists of a single alias, the reference must be replaced not only in children,
    /// but also in where_expression and having_expression.
    if (select.prewhere())
        visit(select.refPrewhere(), data);
    if (select.where())
        visit(select.refWhere(), data);
    if (select.having())
        visit(select.refHaving(), data);
}

/// Don't go into subqueries.
/// Don't go into select query. It processes children itself.
/// Do not go to the left argument of lambda expressions, so as not to replace the formal parameters
///  on aliases in expressions of the form 123 AS x, arrayMap(x -> 1, [2]).
void QueryNormalizer::visitChildren(const ASTPtr & node, Data & data)
{
    if (const auto * func_node = node->as<ASTFunction>())
    {
        if (func_node->tryGetQueryArgument())
        {
            if (func_node->name != "view")
                throw Exception("Query argument can only be used in the `view` TableFunction", ErrorCodes::BAD_ARGUMENTS);
            /// Don't go into query argument.
            return;
        }
        /// We skip the first argument. We also assume that the lambda function can not have parameters.
        size_t first_pos = 0;
        if (func_node->name == "lambda")
            first_pos = 1;

        if (func_node->arguments)
        {
            auto & func_children = func_node->arguments->children;

            for (size_t i = first_pos; i < func_children.size(); ++i)
            {
                auto & child = func_children[i];

                if (needVisitChild(child))
                    visit(child, data);
            }
        }
    }
    else if (!node->as<ASTSelectQuery>())
    {
        for (auto & child : node->children)
            if (needVisitChild(child))
                visit(child, data);
    }
}

void QueryNormalizer::visit(ASTPtr & ast, Data & data)
{
    CheckASTDepth scope1(data);
    RestoreAliasOnExitScope scope2(data.current_alias);

    auto & finished_asts = data.finished_asts;
    auto & current_asts = data.current_asts;

    if (finished_asts.count(ast))
    {
        ast = finished_asts[ast];
        return;
    }

    ASTPtr initial_ast = ast;
    current_asts.insert(initial_ast.get());

    {
        String my_alias = ast->tryGetAlias();
        if (!my_alias.empty())
            data.current_alias = my_alias;
    }

    if (auto * node_id = ast->as<ASTIdentifier>())
        visit(*node_id, ast, data);
    else if (auto * node_tables = ast->as<ASTTablesInSelectQueryElement>())
        visit(*node_tables, ast, data);
    else if (auto * node_select = ast->as<ASTSelectQuery>())
        visit(*node_select, ast, data);
    else if (auto * node_param = ast->as<ASTQueryParameter>())
        throw Exception("Query parameter " + backQuote(node_param->name) + " was not set", ErrorCodes::UNKNOWN_QUERY_PARAMETER);

    /// If we replace the root of the subtree, we will be called again for the new root, in case the alias is replaced by an alias.
    if (ast.get() != initial_ast.get())
        visit(ast, data);
    else
        visitChildren(ast, data);

    current_asts.erase(initial_ast.get());
    current_asts.erase(ast.get());
    finished_asts[initial_ast] = ast;

    /// @note can not place it in CheckASTDepth dtor cause of exception.
    if (data.level == 1)
    {
        try
        {
            ast->checkSize(data.settings.max_expanded_ast_elements);
        }
        catch (Exception & e)
        {
            e.addMessage("(after expansion of aliases)");
            throw;
        }
    }
}

}
