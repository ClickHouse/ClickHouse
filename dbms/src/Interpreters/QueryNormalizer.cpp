#include <Core/Names.h>
#include <Interpreters/QueryNormalizer.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>
#include <Poco/String.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_DEEP_AST;
    extern const int CYCLIC_ALIASES;
}


namespace
{

bool functionIsInOrGlobalInOperator(const String & name)
{
    return name == "in" || name == "notIn" || name == "globalIn" || name == "globalNotIn";
}

}

QueryNormalizer::QueryNormalizer(
    ASTPtr & query, const QueryNormalizer::Aliases & aliases, const Settings & settings, const Names & all_columns_name)
    : query(query), aliases(aliases), settings(settings), all_columns_name(all_columns_name)
{
}

void QueryNormalizer::perform()
{
    SetOfASTs tmp_set;
    MapOfASTs tmp_map;
    performImpl(query, tmp_map, tmp_set, "", 0);

    try
    {
        query->checkSize(settings.max_expanded_ast_elements);
    }
    catch (Exception & e)
    {
        e.addMessage("(after expansion of aliases)");
        throw;
    }
}

/// finished_asts - already processed vertices (and by what they replaced)
/// current_asts - vertices in the current call stack of this method
/// current_alias - the alias referencing to the ancestor of ast (the deepest ancestor with aliases)
void QueryNormalizer::performImpl(ASTPtr & ast, MapOfASTs & finished_asts, SetOfASTs & current_asts, std::string current_alias, size_t level)
{
    if (level > settings.max_ast_depth)
        throw Exception("Normalized AST is too deep. Maximum: " + settings.max_ast_depth.toString(), ErrorCodes::TOO_DEEP_AST);

    if (finished_asts.count(ast))
    {
        ast = finished_asts[ast];
        return;
    }

    ASTPtr initial_ast = ast;
    current_asts.insert(initial_ast.get());

    String my_alias = ast->tryGetAlias();
    if (!my_alias.empty())
        current_alias = my_alias;

    /// rewrite rules that act when you go from top to bottom.
    bool replaced = false;

    ASTIdentifier * identifier_node = nullptr;
    ASTFunction * func_node = nullptr;

    if ((func_node = typeid_cast<ASTFunction *>(ast.get())))
    {
        /// `IN t` can be specified, where t is a table, which is equivalent to `IN (SELECT * FROM t)`.
        if (functionIsInOrGlobalInOperator(func_node->name))
            if (ASTIdentifier * right = typeid_cast<ASTIdentifier *>(func_node->arguments->children.at(1).get()))
                if (!aliases.count(right->name))
                    right->setSpecial();

        /// Special cases for count function.
        String func_name_lowercase = Poco::toLower(func_node->name);
        if (startsWith(func_name_lowercase, "count"))
        {
            /// Select implementation of countDistinct based on settings.
            /// Important that it is done as query rewrite. It means rewritten query
            ///  will be sent to remote servers during distributed query execution,
            ///  and on all remote servers, function implementation will be same.
            if (endsWith(func_node->name, "Distinct") && func_name_lowercase == "countdistinct")
                func_node->name = settings.count_distinct_implementation;

            /// As special case, treat count(*) as count(), not as count(list of all columns).
            if (func_name_lowercase == "count" && func_node->arguments->children.size() == 1
                && typeid_cast<const ASTAsterisk *>(func_node->arguments->children[0].get()))
            {
                func_node->arguments->children.clear();
            }
        }
    }
    else if ((identifier_node = typeid_cast<ASTIdentifier *>(ast.get())))
    {
        if (identifier_node->general())
        {
            /// If it is an alias, but not a parent alias (for constructs like "SELECT column + 1 AS column").
            auto it_alias = aliases.find(identifier_node->name);
            if (it_alias != aliases.end() && current_alias != identifier_node->name)
            {
                /// Let's replace it with the corresponding tree node.
                if (current_asts.count(it_alias->second.get()))
                    throw Exception("Cyclic aliases", ErrorCodes::CYCLIC_ALIASES);

                if (!my_alias.empty() && my_alias != it_alias->second->getAliasOrColumnName())
                {
                    /// Avoid infinite recursion here
                    auto replace_to_identifier = typeid_cast<ASTIdentifier *>(it_alias->second.get());
                    bool is_cycle = replace_to_identifier && replace_to_identifier->general()
                        && replace_to_identifier->name == identifier_node->name;

                    if (!is_cycle)
                    {
                        /// In a construct like "a AS b", where a is an alias, you must set alias b to the result of substituting alias a.
                        ast = it_alias->second->clone();
                        ast->setAlias(my_alias);
                        replaced = true;
                    }
                }
                else
                {
                    ast = it_alias->second;
                    replaced = true;
                }
            }
        }
    }
    else if (ASTExpressionList * expr_list = typeid_cast<ASTExpressionList *>(ast.get()))
    {
        /// Replace * with a list of columns.
        ASTs & asts = expr_list->children;
        for (int i = static_cast<int>(asts.size()) - 1; i >= 0; --i)
        {
            if (typeid_cast<ASTAsterisk *>(asts[i].get()) && !all_columns_name.empty())
            {
                asts.erase(asts.begin() + i);

                for (size_t idx = 0; idx < all_columns_name.size(); idx++)
                    asts.insert(asts.begin() + idx + i, std::make_shared<ASTIdentifier>(all_columns_name[idx]));
            }
        }
    }
    else if (ASTTablesInSelectQueryElement * tables_elem = typeid_cast<ASTTablesInSelectQueryElement *>(ast.get()))
    {
        if (tables_elem->table_expression)
        {
            auto & database_and_table_name = static_cast<ASTTableExpression &>(*tables_elem->table_expression).database_and_table_name;
            if (database_and_table_name)
            {
                if (ASTIdentifier * right = typeid_cast<ASTIdentifier *>(database_and_table_name.get()))
                    right->setSpecial();
            }
        }
    }

    /// If we replace the root of the subtree, we will be called again for the new root, in case the alias is replaced by an alias.
    if (replaced)
    {
        performImpl(ast, finished_asts, current_asts, current_alias, level + 1);
        current_asts.erase(initial_ast.get());
        current_asts.erase(ast.get());
        finished_asts[initial_ast] = ast;
        return;
    }

    /// Recurring calls. Don't go into subqueries. Don't go into components of compound identifiers.
    /// We also do not go to the left argument of lambda expressions, so as not to replace the formal parameters
    ///  on aliases in expressions of the form 123 AS x, arrayMap(x -> 1, [2]).

    if (func_node && func_node->name == "lambda")
    {
        /// We skip the first argument. We also assume that the lambda function can not have parameters.
        for (size_t i = 1, size = func_node->arguments->children.size(); i < size; ++i)
        {
            auto & child = func_node->arguments->children[i];

            if (typeid_cast<const ASTSelectQuery *>(child.get()) || typeid_cast<const ASTTableExpression *>(child.get()))
                continue;

            performImpl(child, finished_asts, current_asts, current_alias, level + 1);
        }
    }
    else if (identifier_node)
    {
    }
    else
    {
        for (auto & child : ast->children)
        {
            if (typeid_cast<const ASTSelectQuery *>(child.get()) || typeid_cast<const ASTTableExpression *>(child.get()))
                continue;

            performImpl(child, finished_asts, current_asts, current_alias, level + 1);
        }
    }

    /// If the WHERE clause or HAVING consists of a single alias, the reference must be replaced not only in children, but also in where_expression and having_expression.
    if (ASTSelectQuery * select = typeid_cast<ASTSelectQuery *>(ast.get()))
    {
        if (select->prewhere_expression)
            performImpl(select->prewhere_expression, finished_asts, current_asts, current_alias, level + 1);
        if (select->where_expression)
            performImpl(select->where_expression, finished_asts, current_asts, current_alias, level + 1);
        if (select->having_expression)
            performImpl(select->having_expression, finished_asts, current_asts, current_alias, level + 1);
    }

    current_asts.erase(initial_ast.get());
    current_asts.erase(ast.get());
    finished_asts[initial_ast] = ast;
}

}
