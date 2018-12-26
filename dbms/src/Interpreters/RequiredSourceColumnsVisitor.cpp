#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Common/typeid_cast.h>
#include <Core/Names.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

static std::vector<String> extractNamesFromLambda(const ASTFunction & node)
{
    if (node.arguments->children.size() != 2)
        throw Exception("lambda requires two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTFunction * lambda_args_tuple = typeid_cast<ASTFunction *>(node.arguments->children[0].get());

    if (!lambda_args_tuple || lambda_args_tuple->name != "tuple")
        throw Exception("First argument of lambda must be a tuple", ErrorCodes::TYPE_MISMATCH);

    std::vector<String> names;
    for (auto & child : lambda_args_tuple->arguments->children)
    {
        ASTIdentifier * identifier = typeid_cast<ASTIdentifier *>(child.get());
        if (!identifier)
            throw Exception("lambda argument declarations must be identifiers", ErrorCodes::TYPE_MISMATCH);

        names.push_back(identifier->name);
    }

    return names;
}

bool RequiredSourceColumnsMatcher::needChildVisit(ASTPtr & node, const ASTPtr & child)
{
    if (typeid_cast<ASTSelectQuery *>(child.get()))
        return false;

    /// Processed. Do not need children.
    if (typeid_cast<ASTIdentifier *>(node.get()) ||
        typeid_cast<ASTTableExpression *>(node.get()) ||
        typeid_cast<ASTArrayJoin *>(node.get()) ||
        typeid_cast<ASTSelectQuery *>(node.get()))
        return false;

    if (auto * f = typeid_cast<ASTFunction *>(node.get()))
    {
        /// "indexHint" is a special function for index analysis. Everything that is inside it is not calculated. @sa KeyCondition
        /// "lambda" visit children itself.
        if (f->name == "indexHint" || f->name == "lambda")
            return false;
    }

    return true;
}

std::vector<ASTPtr *> RequiredSourceColumnsMatcher::visit(ASTPtr & ast, Data & data)
{
    /// results are columns

    if (auto * t = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        data.addColumnAliasIfAny(*ast);
        visit(*t, ast, data);
        return {};
    }
    if (auto * t = typeid_cast<ASTFunction *>(ast.get()))
    {
        data.addColumnAliasIfAny(*ast);
        visit(*t, ast, data);
        return {};
    }

    /// results are tables

    if (auto * t = typeid_cast<ASTTablesInSelectQueryElement *>(ast.get()))
    {
        visit(*t, ast, data);
        return {};
    }

    if (auto * t = typeid_cast<ASTTableExpression *>(ast.get()))
    {
        //data.addTableAliasIfAny(*ast); alias is attached to child
        visit(*t, ast, data);
        return {};
    }
    if (auto * t = typeid_cast<ASTSelectQuery *>(ast.get()))
    {
        data.addTableAliasIfAny(*ast);
        return visit(*t, ast, data);
    }
    if (typeid_cast<ASTSubquery *>(ast.get()))
    {
        data.addTableAliasIfAny(*ast);
        return {};
    }

    /// other

    if (auto * t = typeid_cast<ASTArrayJoin *>(ast.get()))
    {
        data.has_array_join = true;
        return visit(*t, ast, data);
    }

    return {};
}

std::vector<ASTPtr *> RequiredSourceColumnsMatcher::visit(ASTSelectQuery & select, const ASTPtr &, Data & data)
{
    /// special case for top-level SELECT items: they are publics
    for (auto & node : select.select_expression_list->children)
    {
        if (auto * identifier = typeid_cast<ASTIdentifier *>(node.get()))
            data.addColumnIdentifier(*identifier, true);
        else
            data.addColumnAliasIfAny(*node, true);
    }

    std::vector<ASTPtr *> out;
    for (auto & node : select.children)
        if (node != select.select_expression_list)
            out.push_back(&node);

    /// revisit select_expression_list (with children) when all the aliases are set
    out.push_back(&select.select_expression_list);
    return out;
}

void RequiredSourceColumnsMatcher::visit(const ASTIdentifier & node, const ASTPtr &, Data & data)
{
    if (node.name.empty())
        throw Exception("Expected not empty name", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (!data.private_aliases.count(node.name))
        data.addColumnIdentifier(node);
}

void RequiredSourceColumnsMatcher::visit(const ASTFunction & node, const ASTPtr &, Data & data)
{
    /// Do not add formal parameters of the lambda expression
    if (node.name == "lambda")
    {
        Names local_aliases;
        for (const auto & name : extractNamesFromLambda(node))
            if (data.private_aliases.insert(name).second)
                local_aliases.push_back(name);

        /// visit child with masked local aliases
        visit(node.arguments->children[1], data);

        for (const auto & name : local_aliases)
            data.private_aliases.erase(name);
    }
}

void RequiredSourceColumnsMatcher::visit(ASTTablesInSelectQueryElement & node, const ASTPtr &, Data & data)
{
    ASTTableExpression * expr = nullptr;
    ASTTableJoin * join = nullptr;

    for (auto & child : node.children)
    {
        if (auto * e = typeid_cast<ASTTableExpression *>(child.get()))
            expr = e;
        if (auto * j = typeid_cast<ASTTableJoin *>(child.get()))
            join = j;
    }

    if (join)
        data.has_table_join = true;
    data.tables.emplace_back(ColumnNamesContext::JoinedTable{expr, join});
}

std::vector<ASTPtr *> RequiredSourceColumnsMatcher::visit(ASTTableExpression & node, const ASTPtr &, Data & data)
{
    /// ASTIdentifiers here are tables. Do not visit them as generic ones.
    if (node.database_and_table_name)
        data.addTableAliasIfAny(*node.database_and_table_name);

    std::vector<ASTPtr *> out;
    if (node.table_function)
    {
        data.addTableAliasIfAny(*node.table_function);
        out.push_back(&node.table_function);
    }

    if (node.subquery)
    {
        data.addTableAliasIfAny(*node.subquery);
        out.push_back(&node.subquery);
    }

    return out;
}

std::vector<ASTPtr *> RequiredSourceColumnsMatcher::visit(const ASTArrayJoin & node, const ASTPtr &, Data & data)
{
    ASTPtr expression_list = node.expression_list;
    if (!expression_list || expression_list->children.empty())
        throw Exception("Expected not empty expression_list", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    std::vector<ASTPtr *> out;

    /// Tech debt. Ignore ARRAY JOIN top-level identifiers and aliases. There's its own logic for them.
    for (auto & expr : expression_list->children)
    {
        data.addArrayJoinAliasIfAny(*expr);

        if (auto * identifier = typeid_cast<ASTIdentifier *>(expr.get()))
        {
            data.addArrayJoinIdentifier(*identifier);
            continue;
        }

        out.push_back(&expr);
    }

    return out;
}

}
