#include <Interpreters/ColumnNamesContext.h>
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

bool ColumnNamesContext::addColumnAliasIfAny(const IAST & ast, bool is_public)
{
    String alias = ast.tryGetAlias();
    if (alias.empty())
        return false;

    if (required_names.count(alias))
        masked_columns.insert(alias);

    if (is_public)
        public_columns.insert(alias);
    column_aliases.insert(alias);
    return true;
}

void ColumnNamesContext::addColumnIdentifier(const ASTIdentifier & node, bool is_public)
{
    if (!node.general())
        return;

    required_names.insert(node.name);

    if (!addColumnAliasIfAny(node, is_public) && is_public)
        public_columns.insert(node.name);
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
    for (const auto & name : required_names)
    {
        String table_name = Nested::extractTableName(name);

        /// Tech debt. There's its own logic for ARRAY JOIN columns.
        if (array_join_columns.count(name) || array_join_columns.count(table_name))
            continue;

        if (!column_aliases.count(name) || masked_columns.count(name))
            required.insert(name);
    }
    return required;
}

std::ostream & operator << (std::ostream & os, const ColumnNamesContext & cols)
{
    os << "required_names: ";
    for (const auto & x : cols.required_names)
        os << "'" << x << "' ";
    os << "source_tables: ";
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
    os << "private_aliases: ";
    for (const auto & x : cols.private_aliases)
        os << "'" << x << "' ";
    os << "column_aliases: ";
    for (const auto & x : cols.column_aliases)
        os << "'" << x << "' ";
    os << "public_columns: ";
    for (const auto & x : cols.public_columns)
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
