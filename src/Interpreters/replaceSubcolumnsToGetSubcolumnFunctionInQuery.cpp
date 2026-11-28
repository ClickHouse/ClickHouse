#include <Interpreters/replaceSubcolumnsToGetSubcolumnFunctionInQuery.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/NestedUtils.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

namespace DB
{

namespace
{

/// A field `p.f` of a lambda parameter rewritten to getSubcolumn(): its place in the tree, the identifier and the replacement.
struct LambdaField
{
    ASTPtr * slot;
    ASTPtr identifier;
    ASTPtr function;
};

void replaceSubcolumns(ASTPtr & ast, const NamesAndTypesList & columns, NameSet & lambda_parameters, std::vector<LambdaField> * fields)
{
    if (auto * identifier = ast->as<ASTIdentifier>())
    {
        /// A lambda parameter shadows the table columns, and `p.f` is the subcolumn `f` of the parameter `p`.
        if (!identifier->name_parts.empty() && lambda_parameters.contains(identifier->name_parts.front()))
        {
            if (identifier->compound())
            {
                auto subcolumn_name = fmt::format("{}", fmt::join(identifier->name_parts.begin() + 1, identifier->name_parts.end(), "."));
                auto function = makeASTFunction(
                    "getSubcolumn", make_intrusive<ASTIdentifier>(identifier->name_parts.front()), make_intrusive<ASTLiteral>(subcolumn_name));
                function->setAlias(identifier->tryGetAlias());
                if (fields && identifier->tryGetAlias().empty())
                    fields->push_back({&ast, ast, function});
                ast = std::move(function);
            }
            return;
        }

        if (columns.contains(identifier->getColumnName()))
            return;

        auto [column_name, subcolumn_name] = Nested::splitName(identifier->getColumnName());
        auto column = columns.tryGetByName(column_name);
        if (!column || !column->type->hasSubcolumn(subcolumn_name))
            return;

        ast = makeASTFunction("getSubcolumn", make_intrusive<ASTIdentifier>(column_name), make_intrusive<ASTLiteral>(subcolumn_name));
    }
    else if (auto * node = ast->as<ASTFunction>())
    {
        if (node->name == "lambda")
        {
            Names added;
            for (auto & name : RequiredSourceColumnsMatcher::extractNamesFromLambda(*node))
                if (lambda_parameters.insert(name).second)
                    added.push_back(std::move(name));

            replaceSubcolumns(node->arguments->children[1], columns, lambda_parameters, fields);

            for (const auto & name : added)
                lambda_parameters.erase(name);
        }
        else if (node->arguments)
        {
            for (auto & child : node->arguments->children)
                replaceSubcolumns(child, columns, lambda_parameters, fields);
        }
    }
    else
    {
        for (auto & child : ast->children)
            replaceSubcolumns(child, columns, lambda_parameters, fields);
    }
}

void collectNames(const IAST & ast, NameSet & names)
{
    if (auto alias = ast.tryGetAlias(); !alias.empty())
        names.insert(std::move(alias));
    if (const auto * identifier = ast.as<ASTIdentifier>())
        names.insert(identifier->getColumnName());
    for (const auto & child : ast.children)
        collectNames(*child, names);
}

}

void replaceSubcolumnsToGetSubcolumnFunctionInQuery(ASTPtr & ast, const NamesAndTypesList & columns, bool name_lambda_parameter_fields)
{
    NameSet lambda_parameters;
    std::vector<LambdaField> fields;
    replaceSubcolumns(ast, columns, lambda_parameters, name_lambda_parameter_fields ? &fields : nullptr);
    if (fields.empty())
        return;

    /// The old analyzer resolves aliases in one scope for the whole expression, so a field whose name is used otherwise is left
    /// as written: the alias would replace a column read outside the lambda or conflict with another alias.
    NameSet names;
    collectNames(*ast, names);
    for (const auto & field : fields)
    {
        if (auto name = field.identifier->getColumnName(); !names.contains(name))
            field.function->setAlias(name);
        else
            *field.slot = field.identifier;
    }
}

}
