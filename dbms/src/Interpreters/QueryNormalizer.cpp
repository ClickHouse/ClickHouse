#include <Poco/String.h>
#include <Core/Names.h>
#include <Interpreters/QueryNormalizer.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/Context.h>
#include <Interpreters/AnalyzedJoin.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_DEEP_AST;
    extern const int CYCLIC_ALIASES;
}


class CheckASTDepth
{
public:
    CheckASTDepth(QueryNormalizer::Data & data_)
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
    RestoreAliasOnExitScope(String & alias_)
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


void QueryNormalizer::visit(ASTFunction & node, const ASTPtr &, Data & data)
{
    auto & aliases = data.aliases;
    String & func_name = node.name;
    ASTPtr & func_arguments = node.arguments;

    /// `IN t` can be specified, where t is a table, which is equivalent to `IN (SELECT * FROM t)`.
    if (functionIsInOrGlobalInOperator(func_name))
    {
        auto & ast = func_arguments->children.at(1);
        if (auto opt_name = getIdentifierName(ast))
            if (!aliases.count(*opt_name))
                setIdentifierSpecial(ast);
    }

    /// Special cases for count function.
    String func_name_lowercase = Poco::toLower(func_name);
    if (startsWith(func_name_lowercase, "count"))
    {
        /// Select implementation of countDistinct based on settings.
        /// Important that it is done as query rewrite. It means rewritten query
        ///  will be sent to remote servers during distributed query execution,
        ///  and on all remote servers, function implementation will be same.
        if (endsWith(func_name, "Distinct") && func_name_lowercase == "countdistinct")
            func_name = data.settings.count_distinct_implementation;
    }
}

void QueryNormalizer::visit(ASTIdentifier & node, ASTPtr & ast, Data & data)
{
    auto & current_asts = data.current_asts;
    String & current_alias = data.current_alias;

    if (!IdentifierSemantic::getColumnName(node))
        return;

    /// If it is an alias, but not a parent alias (for constructs like "SELECT column + 1 AS column").
    auto it_alias = data.aliases.find(node.name);
    if (IdentifierSemantic::canBeAlias(node) && it_alias != data.aliases.end() && current_alias != node.name)
    {
        auto & alias_node = it_alias->second;

        /// Let's replace it with the corresponding tree node.
        if (current_asts.count(alias_node.get()))
            throw Exception("Cyclic aliases", ErrorCodes::CYCLIC_ALIASES);

        String my_alias = ast->tryGetAlias();
        if (!my_alias.empty() && my_alias != alias_node->getAliasOrColumnName())
        {
            /// Avoid infinite recursion here
            auto opt_name = IdentifierSemantic::getColumnName(alias_node);
            bool is_cycle = opt_name && *opt_name == node.name;

            if (!is_cycle)
            {
                /// In a construct like "a AS b", where a is an alias, you must set alias b to the result of substituting alias a.
                ast = alias_node->clone();
                ast->setAlias(my_alias);
            }
        }
        else
            ast = alias_node;
    }
}

/// mark table identifiers as 'not columns'
void QueryNormalizer::visit(ASTTablesInSelectQueryElement & node, const ASTPtr &, Data &)
{
    if (node.table_expression)
    {
        auto & expr = static_cast<ASTTableExpression &>(*node.table_expression);
        setIdentifierSpecial(expr.database_and_table_name);
    }
}

/// special visitChildren() for ASTSelectQuery
void QueryNormalizer::visit(ASTSelectQuery & select, const ASTPtr & ast, Data & data)
{
    for (auto & child : ast->children)
    {
        if (typeid_cast<const ASTSelectQuery *>(child.get()) ||
            typeid_cast<const ASTTableExpression *>(child.get()))
            continue;

        visit(child, data);
    }

    /// If the WHERE clause or HAVING consists of a single alias, the reference must be replaced not only in children,
    /// but also in where_expression and having_expression.
    if (select.prewhere_expression)
        visit(select.prewhere_expression, data);
    if (select.where_expression)
        visit(select.where_expression, data);
    if (select.having_expression)
        visit(select.having_expression, data);
}

/// Don't go into subqueries.
/// Don't go into select query. It processes children itself.
/// Do not go to the left argument of lambda expressions, so as not to replace the formal parameters
///  on aliases in expressions of the form 123 AS x, arrayMap(x -> 1, [2]).
void QueryNormalizer::visitChildren(const ASTPtr & node, Data & data)
{
    ASTFunction * func_node = typeid_cast<ASTFunction *>(node.get());
    if (func_node && func_node->name == "lambda")
    {
        /// We skip the first argument. We also assume that the lambda function can not have parameters.
        for (size_t i = 1, size = func_node->arguments->children.size(); i < size; ++i)
        {
            auto & child = func_node->arguments->children[i];

            if (typeid_cast<const ASTSelectQuery *>(child.get()) ||
                typeid_cast<const ASTTableExpression *>(child.get()))
                continue;

            visit(child, data);
        }
    }
    else if (!typeid_cast<ASTSelectQuery *>(node.get()))
    {
        for (auto & child : node->children)
        {
            if (typeid_cast<const ASTSelectQuery *>(child.get()) ||
                typeid_cast<const ASTTableExpression *>(child.get()))
                continue;

            visit(child, data);
        }
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

    if (auto * node = typeid_cast<ASTFunction *>(ast.get()))
        visit(*node, ast, data);
    if (auto * node = typeid_cast<ASTIdentifier *>(ast.get()))
        visit(*node, ast, data);
    if (auto * node = typeid_cast<ASTTablesInSelectQueryElement *>(ast.get()))
        visit(*node, ast, data);
    if (auto * node = typeid_cast<ASTSelectQuery *>(ast.get()))
        visit(*node, ast, data);

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
