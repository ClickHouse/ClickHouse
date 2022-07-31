#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Common/typeid_cast.h>
#include <Core/Names.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTInterpolateElement.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

std::vector<String> RequiredSourceColumnsMatcher::extractNamesFromLambda(const ASTFunction & node)
{
    if (node.arguments->children.size() != 2)
        throw Exception("lambda requires two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * lambda_args_tuple = node.arguments->children.front()->as<ASTFunction>();

    if (!lambda_args_tuple || lambda_args_tuple->name != "tuple")
        throw Exception("First argument of lambda must be a tuple", ErrorCodes::TYPE_MISMATCH);

    std::vector<String> names;
    for (auto & child : lambda_args_tuple->arguments->children)
    {
        const auto * identifier = child->as<ASTIdentifier>();
        if (!identifier)
            throw Exception("lambda argument declarations must be identifiers", ErrorCodes::TYPE_MISMATCH);

        names.push_back(identifier->name());
    }

    return names;
}

bool RequiredSourceColumnsMatcher::needChildVisit(const ASTPtr & node, const ASTPtr & child)
{
    if (child->as<ASTSelectQuery>())
        return false;

    /// Processed. Do not need children.
    if (node->as<ASTTableExpression>() || node->as<ASTArrayJoin>() || node->as<ASTSelectQuery>() || node->as<ASTInterpolateElement>())
        return false;

    if (const auto * f = node->as<ASTFunction>())
    {
        /// "indexHint" is a special function for index analysis.
        /// Everything that is inside it is not calculated. See KeyCondition
        /// "lambda" visit children itself.
        if (f->name == "indexHint" || f->name == "lambda")
            return false;
    }

    return true;
}

void RequiredSourceColumnsMatcher::visit(const ASTPtr & ast, Data & data)
{
    /// results are columns

    if (auto * t = ast->as<ASTIdentifier>())
    {
        visit(*t, ast, data);
        return;
    }
    if (auto * t = ast->as<ASTFunction>())
    {
        data.addColumnAliasIfAny(*ast);
        visit(*t, ast, data);
        return;
    }

    /// results are tables

    if (auto * t = ast->as<ASTTablesInSelectQueryElement>())
    {
        visit(*t, ast, data);
        return;
    }

    if (auto * t = ast->as<ASTTableExpression>())
    {
        visit(*t, ast, data);
        return;
    }

    if (auto * t = ast->as<ASTSelectQuery>())
    {
        visit(*t, ast, data);
        return;
    }

    if (ast->as<ASTSubquery>())
    {
        return;
    }

    /// other

    if (auto * t = ast->as<ASTArrayJoin>())
    {
        data.has_array_join = true;
        visit(*t, ast, data);
        return;
    }
}

void RequiredSourceColumnsMatcher::visit(const ASTSelectQuery & select, const ASTPtr &, Data & data)
{
    NameSet select_columns;
    /// special case for top-level SELECT items: they are publics
    for (auto & node : select.select()->children)
    {
        select_columns.insert(node->getAliasOrColumnName());

        if (const auto * identifier = node->as<ASTIdentifier>())
            data.addColumnIdentifier(*identifier);
        else
            data.addColumnAliasIfAny(*node);
    }

    if (auto interpolate_list = select.interpolate())
    {
        auto find_columns = [&data, &select_columns](IAST * function)
        {
            auto f_impl = [&data, &select_columns](IAST * fn, auto fi)
            {
                if (auto * ident = fn->as<ASTIdentifier>())
                {
                    if (!select_columns.contains(ident->getColumnName()))
                        data.addColumnIdentifier(*ident);
                    return;
                }
                if (fn->as<ASTFunction>() || fn->as<ASTExpressionList>())
                    for (const auto & ch : fn->children)
                        fi(ch.get(), fi);
                return;
            };
            f_impl(function, f_impl);
        };

        for (const auto & interpolate : interpolate_list->children)
            find_columns(interpolate->as<ASTInterpolateElement>()->expr.get());
    }

    if (const auto & with = select.with())
    {
        for (auto & node : with->children)
        {
            if (const auto * identifier = node->as<ASTIdentifier>())
                data.addColumnIdentifier(*identifier);
            else
                data.addColumnAliasIfAny(*node);
        }
    }

    std::vector<ASTPtr *> out;
    for (const auto & node : select.children)
    {
        // We should not go into WITH statement because all needed aliases are already expanded to
        // the right place after normalization. And it might contain unused unknown columns.
        if (node != select.select() && node != select.with())
            Visitor(data).visit(node);
    }

    /// revisit select_expression_list (with children) when all the aliases are set
    Visitor(data).visit(select.select());
}

void RequiredSourceColumnsMatcher::visit(const ASTIdentifier & node, const ASTPtr &, Data & data)
{
    // FIXME(ilezhankin): shouldn't ever encounter
    if (node.name().empty())
        throw Exception("Expected not empty name", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (!data.private_aliases.contains(node.name()))
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
        RequiredSourceColumnsVisitor(data).visit(*++node.arguments->children.begin());

        for (const auto & name : local_aliases)
            data.private_aliases.erase(name);
    }
}

void RequiredSourceColumnsMatcher::visit(const ASTTablesInSelectQueryElement & node, const ASTPtr &, Data & data)
{
    for (const auto & child : node.children)
        if (child->as<ASTTableJoin>())
            data.has_table_join = true;
}

/// ASTIdentifiers here are tables. Do not visit them as generic ones.
void RequiredSourceColumnsMatcher::visit(const ASTTableExpression &, const ASTPtr &, Data &)
{
}

void RequiredSourceColumnsMatcher::visit(const ASTArrayJoin & node, const ASTPtr &, Data & data)
{
    ASTPtr expression_list = node.expression_list;
    if (!expression_list || expression_list->children.empty())
        throw Exception("Expected not empty expression_list", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    std::vector<ASTPtr *> out;

    /// Tech debt. Ignore ARRAY JOIN top-level identifiers and aliases. There's its own logic for them.
    for (auto & expr : expression_list->children)
    {
        data.addArrayJoinAliasIfAny(*expr);

        if (const auto * identifier = expr->as<ASTIdentifier>())
        {
            data.addArrayJoinIdentifier(*identifier);
            continue;
        }

        out.push_back(&expr);
    }

    for (ASTPtr * add_node : out)
        Visitor(data).visit(*add_node);
}

}
