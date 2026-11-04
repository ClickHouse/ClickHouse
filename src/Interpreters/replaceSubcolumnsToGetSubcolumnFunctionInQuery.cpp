#include <Interpreters/replaceSubcolumnsToGetSubcolumnFunctionInQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/NestedUtils.h>

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

void replaceSubcolumns(ASTPtr & ast, const NamesAndTypesList & columns, bool is_root, std::vector<UnnamedSubcolumn> & unnamed)
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
                function->setAlias(alias);
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
                replaceSubcolumns(child, columns, false, unnamed);
        }
    }
    else
    {
        for (auto & child : ast->children)
            replaceSubcolumns(child, columns, false, unnamed);
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

void replaceSubcolumnsToGetSubcolumnFunctionInQuery(ASTPtr & ast, const NamesAndTypesList & columns)
{
    std::vector<UnnamedSubcolumn> unnamed;
    replaceSubcolumns(ast, columns, /*is_root=*/ true, unnamed);
    if (unnamed.empty())
        return;

    /// The old analyzer resolves aliases in one scope for the whole expression, so a name used elsewhere is left off.
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
