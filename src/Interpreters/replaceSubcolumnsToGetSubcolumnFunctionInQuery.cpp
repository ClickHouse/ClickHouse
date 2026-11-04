#include <Interpreters/replaceSubcolumnsToGetSubcolumnFunctionInQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/NestedUtils.h>

#include <unordered_map>
#include <unordered_set>

namespace DB
{

namespace
{

/// A subcolumn below the root replaced without an alias of its own, and the name it had.
struct UnnamedSubcolumn
{
    boost::intrusive_ptr<ASTFunction> function;
    String name;
};

/// A subcolumn below the root replaced with its alias, and the innermost lambda around it, or null.
struct AliasedSubcolumn
{
    boost::intrusive_ptr<ASTFunction> function;
    const IAST * lambda;
};

void replaceSubcolumns(
    ASTPtr & ast,
    const NamesAndTypesList & columns,
    bool is_root,
    const IAST * lambda,
    std::vector<UnnamedSubcolumn> & unnamed,
    std::vector<AliasedSubcolumn> & aliased)
{
    if (auto * identifier = ast->as<ASTIdentifier>())
    {
        if (columns.contains(identifier->getColumnName()))
            return;

        auto [column_name, subcolumn_name] = Nested::splitName(identifier->getColumnName());
        auto column = columns.tryGetByName(column_name);
        if (!column || !column->type->hasSubcolumn(subcolumn_name))
            return;

        auto function = makeASTFunction("getSubcolumn", make_intrusive<ASTIdentifier>(column_name), make_intrusive<ASTLiteral>(subcolumn_name));
        /// The replacement keeps what the expression calls the subcolumn: its alias, or its name, which `formatRow` reads.
        /// The root is named by the caller.
        if (!is_root)
        {
            if (auto alias = identifier->tryGetAlias(); !alias.empty())
            {
                function->setAlias(alias);
                aliased.push_back({function, lambda});
            }
            else
                unnamed.push_back({function, identifier->getColumnName()});
        }
        ast = std::move(function);
    }
    else if (auto * node = ast->as<ASTFunction>())
    {
        if (node->arguments)
        {
            for (auto & child : node->arguments->children)
                replaceSubcolumns(child, columns, false, node->name == "lambda" ? node : lambda, unnamed, aliased);
        }
    }
    else
    {
        for (auto & child : ast->children)
            replaceSubcolumns(child, columns, false, lambda, unnamed, aliased);
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

/// The aliases given to different expressions, compared as `QueryAliasesVisitor` compares them.
void collectConflictingAliases(const IAST & ast, std::unordered_map<String, IASTHash> & hashes, NameSet & conflicting)
{
    if (auto alias = ast.tryGetAlias(); !alias.empty())
    {
        auto hash = ast.getTreeHash(/*ignore_aliases=*/ true);
        if (auto [it, inserted] = hashes.emplace(alias, hash); !inserted && it->second != hash)
            conflicting.insert(alias);
    }
    for (const auto & child : ast.children)
        collectConflictingAliases(*child, hashes, conflicting);
}

bool hasIdentifierOutside(const IAST & ast, const std::unordered_set<const IAST *> & scopes, const String & name)
{
    if (scopes.contains(&ast))
        return false;
    if (const auto * identifier = ast.as<ASTIdentifier>(); identifier && identifier->getColumnName() == name)
        return true;
    for (const auto & child : ast.children)
        if (hasIdentifierOutside(*child, scopes, name))
            return true;
    return false;
}

}

void replaceSubcolumnsToGetSubcolumnFunctionInQuery(ASTPtr & ast, const NamesAndTypesList & columns)
{
    std::vector<UnnamedSubcolumn> unnamed;
    std::vector<AliasedSubcolumn> aliased;
    replaceSubcolumns(ast, columns, /*is_root=*/ true, /*lambda=*/ nullptr, unnamed, aliased);

    /// The old analyzer has one alias scope for the whole expression, lambdas included, so an alias that a different
    /// expression also has, an alias set only in lambdas and read outside all of them, or a name used elsewhere, is left off.
    if (!aliased.empty())
    {
        std::unordered_map<String, IASTHash> hashes;
        NameSet conflicting;
        collectConflictingAliases(*ast, hashes, conflicting);

        std::unordered_map<String, std::unordered_set<const IAST *>> scopes;
        for (const auto & [function, lambda] : aliased)
            scopes[function->tryGetAlias()].insert(lambda);

        NameSet dropped;
        for (const auto & [alias, lambdas] : scopes)
            if (conflicting.contains(alias) || (!lambdas.contains(nullptr) && hasIdentifierOutside(*ast, lambdas, alias)))
                dropped.insert(alias);

        for (const auto & [function, lambda] : aliased)
            if (dropped.contains(function->tryGetAlias()))
                function->setAlias("");
    }

    if (unnamed.empty())
        return;

    NameSet names;
    collectNames(*ast, names);
    for (const auto & [function, name] : unnamed)
    {
        if (names.contains(name))
            continue;
        function->setAlias(name);
        function->setPreferAliasToColumnName(true);
    }
}

}
