#include <Interpreters/ColumnNamesContext.h>
#include <Interpreters/IdentifierSemantic.h>
#include <DataTypes/NestedUtils.h>

namespace DB
{

bool ColumnNamesContext::addTableAliasIfAny(const IAST & ast)
{
    String alias = ast.tryGetAlias();
    if (alias.empty())
        return false;

    table_aliases.insert(alias);
    return true;
}

bool ColumnNamesContext::addColumnAliasIfAny(const IAST & ast)
{
    String alias = ast.tryGetAlias();
    if (alias.empty())
        return false;

    if (required_names.count(alias))
        masked_columns.insert(alias);

    complex_aliases.insert(alias);
    return true;
}

void ColumnNamesContext::addColumnIdentifier(const ASTIdentifier & node)
{
    if (!IdentifierSemantic::getColumnName(node))
        return;

    /// There should be no complex cases after query normalization. Names to aliases: one-to-many.
    String alias = node.tryGetAlias();
    if (!alias.empty())
        required_names[node.name].insert(alias);

    if (!required_names.count(node.name))
        required_names[node.name] = {};
}

bool ColumnNamesContext::addArrayJoinAliasIfAny(const IAST & ast)
{
    String alias = ast.tryGetAlias();
    if (alias.empty())
        return false;

    array_join_columns.insert(alias);
    return true;
}

void ColumnNamesContext::addArrayJoinIdentifier(const ASTIdentifier & node)
{
    array_join_columns.insert(node.name);
}

NameSet ColumnNamesContext::requiredColumns() const
{
    NameSet required;
    for (const auto & pr : required_names)
    {
        const auto & name = pr.first;
        String table_name = Nested::extractTableName(name);

        /// Tech debt. There's its own logic for ARRAY JOIN columns.
        if (array_join_columns.count(name) || array_join_columns.count(table_name))
            continue;

        if (!complex_aliases.count(name) || masked_columns.count(name))
            required.insert(name);
    }
    return required;
}

std::ostream & operator << (std::ostream & os, const ColumnNamesContext & cols)
{
    os << "required_names: ";
    for (const auto & pr : cols.required_names)
    {
        os << "'" << pr.first << "'";
        for (auto & alias : pr.second)
            os << "/'" << alias << "'";
    }
    os << " source_tables: ";
    for (const auto & x : cols.tables)
    {
        auto alias = x.alias();
        auto name = x.name();
        if (alias && name)
            os << "'" << *alias << "'/'" << *name << "' ";
        else if (alias)
            os << "'" << *alias << "' ";
        else if (name)
            os << "'" << *name << "' ";
    }
    os << "table_aliases: ";
    for (const auto & x : cols.table_aliases)
        os << "'" << x << "' ";
    os << "complex_aliases: ";
    for (const auto & x : cols.complex_aliases)
        os << "'" << x << "' ";
    os << "masked_columns: ";
    for (const auto & x : cols.masked_columns)
        os << "'" << x << "' ";
    os << "array_join_columns: ";
    for (const auto & x : cols.array_join_columns)
        os << "'" << x << "' ";
    return os;
}

}
