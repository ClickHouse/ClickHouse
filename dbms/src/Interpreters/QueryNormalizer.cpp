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
#include <Parsers/ASTQualifiedAsterisk.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
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


void QueryNormalizer::visit(ASTPtr & ast, Data & data)
{
    CheckASTDepth scope1(data);
    RestoreAliasOnExitScope scope2(data.current_alias);

    auto & aliases = data.aliases;
    auto & tables_with_columns = data.tables_with_columns;
    auto & finished_asts = data.finished_asts;
    auto & current_asts = data.current_asts;
    String & current_alias = data.current_alias;

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
                func_node->name = data.settings.count_distinct_implementation;

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
        /// Replace *, alias.*, database.table.* with a list of columns.

        ASTs old_children;
        if (data.processAsterisks())
        {
            bool has_asterisk = false;
            for (const auto & child : expr_list->children)
            {
                if (typeid_cast<const ASTAsterisk *>(child.get()) ||
                    typeid_cast<const ASTQualifiedAsterisk *>(child.get()))
                {
                    has_asterisk = true;
                    break;
                }
            }

            if (has_asterisk)
            {
                old_children.swap(expr_list->children);
                expr_list->children.reserve(old_children.size());
            }
        }

        for (const auto & child : old_children)
        {
            if (typeid_cast<const ASTAsterisk *>(child.get()))
            {
                for (const auto & pr : tables_with_columns)
                    for (const auto & column_name : pr.second)
                        expr_list->children.emplace_back(std::make_shared<ASTIdentifier>(column_name));
            }
            else if (const auto * qualified_asterisk = typeid_cast<const ASTQualifiedAsterisk *>(child.get()))
            {
                const ASTIdentifier * identifier = typeid_cast<const ASTIdentifier *>(qualified_asterisk->children[0].get());
                size_t num_components = identifier->children.size();

                for (const auto & [table_name, table_columns] : tables_with_columns)
                {
                    if ((num_components == 2                    /// database.table.*
                            && !table_name.database.empty()     /// This is normal (not a temporary) table.
                            && static_cast<const ASTIdentifier &>(*identifier->children[0]).name == table_name.database
                            && static_cast<const ASTIdentifier &>(*identifier->children[1]).name == table_name.table)
                        || (num_components == 0                                                         /// t.*
                            && ((!table_name.table.empty() && identifier->name == table_name.table)         /// table.*
                                || (!table_name.alias.empty() && identifier->name == table_name.alias))))   /// alias.*
                    {
                        for (const auto & column_name : table_columns)
                            expr_list->children.emplace_back(std::make_shared<ASTIdentifier>(column_name));
                        break;
                    }
                }
            }
            else
                expr_list->children.emplace_back(child);
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
        visit(ast, data);
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

            visit(child, data);
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

            visit(child, data);
        }
    }

    /// If the WHERE clause or HAVING consists of a single alias, the reference must be replaced not only in children, but also in where_expression and having_expression.
    if (ASTSelectQuery * select = typeid_cast<ASTSelectQuery *>(ast.get()))
    {
        if (select->prewhere_expression)
            visit(select->prewhere_expression, data);
        if (select->where_expression)
            visit(select->where_expression, data);
        if (select->having_expression)
            visit(select->having_expression, data);
    }

    current_asts.erase(initial_ast.get());
    current_asts.erase(ast.get());
    finished_asts[initial_ast] = ast;

    /// @note can not place it in CheckASTDepth dror cause of throw.
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
