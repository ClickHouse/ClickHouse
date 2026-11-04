#include <Interpreters/replaceSubcolumnsToGetSubcolumnFunctionInQuery.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/NestedUtils.h>
#include <base/scope_guard.h>

namespace DB
{

namespace
{

/// A subcolumn of a column whose name has a dot, replaced without an alias of its own: the slot it was written to, the
/// identifier it replaced, the replacement and its name.
struct UnnamedReplacement
{
    ASTPtr * slot;
    ASTPtr identifier;
    boost::intrusive_ptr<ASTFunction> function;
    String name;
};

struct ReplaceState
{
    explicit ReplaceState(const NamesAndTypesList & columns_) : columns(columns_) {}

    const NamesAndTypesList & columns;
    /// The old analyzer has one alias scope for the whole expression, lambdas included.
    NameSet aliases;
    /// Aliases of the nodes around the visited one: an alias is not visible inside the expression it names.
    NameSet defining;
    NameSet lambda_parameters;
    /// Names of the identifiers left as written.
    NameSet kept;
    std::vector<UnnamedReplacement> unnamed;
};

void collectAliases(const IAST & ast, NameSet & aliases)
{
    if (auto alias = ast.tryGetAlias(); !alias.empty())
        aliases.insert(std::move(alias));
    for (const auto & child : ast.children)
        collectAliases(*child, aliases);
}

void replaceSubcolumns(ASTPtr & ast, ReplaceState & state, bool is_root)
{
    String own_alias = ast->tryGetAlias();
    bool defines = !own_alias.empty() && state.defining.insert(own_alias).second;
    SCOPE_EXIT({ if (defines) state.defining.erase(own_alias); });

    if (auto * identifier = ast->as<ASTIdentifier>())
    {
        String name = identifier->getColumnName();
        if (state.columns.contains(name))
            return;

        /// A column name can contain dots (`t.x`, a member of a flattened Nested), so every split is tried, unless the first
        /// part is a lambda parameter or an alias: `p.f.g` in `p -> p.f.g` reads the parameter, not the column `p.f`.
        auto splits = Nested::getAllColumnAndSubcolumnPairs(name);
        const auto & first_part = identifier->name_parts.front();
        bool first_part_is_alias = state.aliases.contains(first_part) && !state.defining.contains(first_part);
        if (splits.size() > 1 && (state.lambda_parameters.contains(first_part) || first_part_is_alias))
            splits.resize(1);

        for (size_t i = 0; i < splits.size(); ++i)
        {
            auto [column_name, subcolumn_name] = splits[i];
            auto column = state.columns.tryGetByName(String(column_name));
            if (!column || !column->type->hasSubcolumn(subcolumn_name))
                continue;

            auto function = makeASTFunction(
                "getSubcolumn", make_intrusive<ASTIdentifier>(String(column_name)), make_intrusive<ASTLiteral>(String(subcolumn_name)));
            ASTPtr original = ast;
            /// Beyond the first split the alias and the name of the identifier are kept, as the old analyzer reads it by them.
            if (i > 0 && !is_root)
            {
                if (auto alias = identifier->tryGetAlias(); !alias.empty())
                    function->setAlias(alias);
                else
                    state.unnamed.push_back({&ast, original, function, name});
            }
            ast = std::move(function);
            return;
        }
        state.kept.insert(name);
    }
    else if (auto * node = ast->as<ASTFunction>())
    {
        if (node->name == "lambda")
        {
            Names masked;
            for (auto & parameter : RequiredSourceColumnsMatcher::extractNamesFromLambda(*node))
                if (state.lambda_parameters.insert(parameter).second)
                    masked.push_back(std::move(parameter));

            replaceSubcolumns(node->arguments->children[1], state, false);

            for (const auto & parameter : masked)
                state.lambda_parameters.erase(parameter);
        }
        else if (node->arguments)
        {
            for (auto & child : node->arguments->children)
                replaceSubcolumns(child, state, false);
        }
    }
    else
    {
        for (auto & child : ast->children)
            replaceSubcolumns(child, state, false);
    }
}

}

void replaceSubcolumnsToGetSubcolumnFunctionInQuery(ASTPtr & ast, const NamesAndTypesList & columns)
{
    ReplaceState state(columns);
    collectAliases(*ast, state.aliases);
    replaceSubcolumns(ast, state, /*is_root=*/ true);

    /// A name the expression also uses otherwise is left as written: an alias would change what that name means, and no alias
    /// would change the name.
    for (const auto & [slot, identifier, function, name] : state.unnamed)
    {
        if (state.aliases.contains(name) || state.kept.contains(name))
        {
            *slot = identifier;
            continue;
        }
        function->setAlias(name);
        function->setPreferAliasToColumnName(true);
    }
}

}
