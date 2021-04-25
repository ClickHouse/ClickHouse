#include <Storages/MergeTree/ProjectionCondition.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/misc.h>

namespace DB
{

ProjectionCondition::ProjectionCondition(const Names & key_column_names, const Names & required_column_names)
{
    for (const auto & name : key_column_names)
        key_columns[name] = 0;

    for (const auto & name : required_column_names)
        key_columns[name] = 1;
}

bool ProjectionCondition::check(const ASTPtr & node)
{
    if (node->as<ASTIdentifier>())
    {
        auto name = node->getColumnNameWithoutAlias();
        auto it = key_columns.find(name);
        if (key_columns.end() != it)
        {
            ++it->second;
            required_columns_in_predicate.insert(name);
            return true;
        }
        else
            return false;
    }

    if (node->as<ASTLiteral>())
        return true;

    if (auto * func = node->as<ASTFunction>())
    {
        // in function should be treated specially
        if (functionIsInOrGlobalInOperator(func->name))
        {
            if (func->arguments && func->arguments->children.size() == 2)
            {
                if (!check(func->arguments->children[0]))
                    return false;

                // If it's a dependent table or subquery, we can still use projection
                if (func->arguments->children[1]->as<ASTIdentifier>())
                    return true;

                if (check(func->arguments->children[1]))
                    return true;
            }
            return false;
        }

        // TODO Need to check other special functions such as joinGet/dictGet
        auto name = node->getColumnNameWithoutAlias();
        auto it = key_columns.find(name);
        if (key_columns.end() != it)
        {
            ++it->second;
            return true;
        }
    }

    for (auto & child : node->children)
    {
        if (!check(child))
            return false;
    }

    return true;
}

Names ProjectionCondition::getRequiredColumns() const
{
    Names ret;
    for (const auto & [key, value] : key_columns)
    {
        if (value > 0)
            ret.push_back(key);
    }
    return ret;
}

// Rewrite predicates for projection parts so exprs are treated as columns
void ProjectionCondition::rewrite(ASTPtr & node) const
{
    if (node->as<ASTFunction>() || node->as<ASTIdentifier>())
    {
        auto name = node->getColumnNameWithoutAlias();
        auto it = key_columns.find(name);
        if (key_columns.end() != it)
        {
            node = std::make_shared<ASTIdentifier>(name);
            return;
        }
    }

    for (auto & child : node->children)
        rewrite(child);
}

}
