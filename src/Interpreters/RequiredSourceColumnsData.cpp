#include <Common/typeid_cast.h>
#include <algorithm>
#include <Interpreters/RequiredSourceColumnsData.h>
#include <Interpreters/IdentifierSemantic.h>
#include <DataTypes/NestedUtils.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

bool RequiredSourceColumnsData::addColumnAliasIfAny(const IAST & ast)
{
    String alias = ast.tryGetAlias();
    if (alias.empty())
        return false;

    if (required_names.contains(alias))
        masked_columns.insert(alias);

    complex_aliases.insert(alias);
    return true;
}

void RequiredSourceColumnsData::addColumnIdentifier(const ASTIdentifier & node)
{
    if (!IdentifierSemantic::getColumnName(node))
        return;

    /// There should be no complex cases after query normalization. Names to aliases: one-to-many.
    String alias = node.tryGetAlias();
    required_names[node.name()].addInclusion(alias);
}

bool RequiredSourceColumnsData::addArrayJoinAliasIfAny(const IAST & ast)
{
    String alias = ast.tryGetAlias();
    if (alias.empty())
        return false;

    array_join_columns.insert(alias);
    return true;
}

void RequiredSourceColumnsData::addArrayJoinIdentifier(const ASTIdentifier & node)
{
    array_join_columns.insert(node.name());
}

size_t RequiredSourceColumnsData::nameInclusion(const String & name) const
{
    auto it = required_names.find(name);
    if (it != required_names.end())
        return it->second.appears;
    return 0;
}

bool RequiredSourceColumnsData::isSubcolumnOfAliases(const String & name, size_t appears) const
{
    /// Each appearance of `name` must be a subcolumn of an alias visible from its scope.
    auto it = compound_identifiers.find(name);
    if (it == compound_identifiers.end() || it->second.size() != appears)
        return false;

    return std::ranges::all_of(it->second, [&](const CompoundIdentifier & identifier)
    {
        auto alias_it = alias_scopes.find(identifier.prefix);
        if (alias_it == alias_scopes.end() || masked_columns.contains(identifier.prefix))
            return false;
        return std::ranges::any_of(identifier.scopes, [&](size_t scope) { return alias_it->second.contains(scope); });
    });
}

NameSet RequiredSourceColumnsData::requiredColumns() const
{
    NameSet required;
    for (const auto & pr : required_names)
    {
        const auto & name = pr.first;
        String table_name = Nested::extractTableName(name);

        /// Tech debt. There's its own logic for ARRAY JOIN columns.
        if (array_join_columns.contains(name) || array_join_columns.contains(table_name))
            continue;

        if (mask_subcolumns_of_aliases && isSubcolumnOfAliases(name, pr.second.appears))
            continue;

        if (!complex_aliases.contains(name) || masked_columns.contains(name))
            required.insert(name);
    }
    return required;
}

std::ostream & operator << (std::ostream & os, const RequiredSourceColumnsData & cols)
{
    os << "required_names: ";
    for (const auto & pr : cols.required_names)
    {
        os << "'" << pr.first << "'";
        for (const auto & alias : pr.second.aliases)
            os << "/'" << alias << "'";
        os << ", ";
    }
    os << "complex_aliases: ";
    for (const auto & x : cols.complex_aliases)
        os << "'" << x << "', ";
    os << "masked_columns: ";
    for (const auto & x : cols.masked_columns)
        os << "'" << x << "', ";
    os << "array_join_columns: ";
    for (const auto & x : cols.array_join_columns)
        os << "'" << x << "', ";
    return os;
}

}
