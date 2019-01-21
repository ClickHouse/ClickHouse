#include <Parsers/ASTIdentifier.h>
#include <Common/typeid_cast.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>


namespace DB
{

struct IdentifierSemantic
{
    bool special = false;
    size_t num_qualifiers_to_strip = 0;
    std::unique_ptr<DatabaseAndTableWithAlias> db_and_table;
};


std::shared_ptr<ASTIdentifier> ASTIdentifier::createSpecial(const String & name, std::vector<String> && name_parts)
{
    auto ret = std::make_shared<ASTIdentifier>(name, std::move(name_parts));
    ret->semantic->special = true;
    return ret;
}

ASTIdentifier::ASTIdentifier(const String & name_, std::vector<String> && name_parts_)
    : name(name_)
    , name_parts(name_parts_)
    , semantic(std::make_shared<IdentifierSemantic>())
{
    range = StringRange(name.data(), name.data() + name.size());
}

void ASTIdentifier::formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    auto format_element = [&](const String & elem_name)
    {
        settings.ostr << (settings.hilite ? hilite_identifier : "");
        settings.writeIdentifier(elem_name);
        settings.ostr << (settings.hilite ? hilite_none : "");
    };

    /// A simple or compound identifier?

    if (name_parts.size() > 1)
    {
        for (size_t i = 0, size = name_parts.size(); i < size; ++i)
        {
            if (i != 0)
                settings.ostr << '.';

            format_element(name_parts[i]);
        }
    }
    else
    {
        format_element(name);
    }
}

void ASTIdentifier::appendColumnNameImpl(WriteBuffer & ostr) const
{
    writeString(name, ostr);
}

ASTPtr createTableIdentifier(const String & database_name, const String & table_name)
{
    if (database_name.empty())
        return ASTIdentifier::createSpecial(table_name);

    ASTPtr database_and_table = ASTIdentifier::createSpecial(database_name + "." + table_name, {database_name, table_name});
    return database_and_table;
}

bool isIdentifier(const IAST * const ast)
{
    if (ast)
        return typeid_cast<const ASTIdentifier *>(ast);
    return false;
}

std::optional<String> getIdentifierName(const IAST * const ast)
{
    if (ast)
        if (auto node = typeid_cast<const ASTIdentifier *>(ast))
            return node->name;
    return {};
}

bool getIdentifierName(const ASTPtr & ast, String & name)
{
    if (ast)
        if (auto node = typeid_cast<const ASTIdentifier *>(ast.get()))
        {
            name = node->name;
            return true;
        }
    return false;
}

std::optional<String> getColumnIdentifierName(const ASTIdentifier & node)
{
    if (!node.semantic->special)
        return node.name;
    return {};
}

std::optional<String> getColumnIdentifierName(const ASTPtr & ast)
{
    if (ast)
        if (auto id = typeid_cast<const ASTIdentifier *>(ast.get()))
            if (!id->semantic->special)
                return id->name;
    return {};
}

std::optional<String> getTableIdentifierName(const ASTIdentifier & node)
{
    if (node.semantic->special)
        return node.name;
    return {};
}

std::optional<String> getTableIdentifierName(const ASTPtr & ast)
{
    if (ast)
        if (auto id = typeid_cast<const ASTIdentifier *>(ast.get()))
            if (id->semantic->special)
                return id->name;
    return {};
}

void setIdentifierSpecial(ASTPtr & ast)
{
    if (ast)
        if (ASTIdentifier * id = typeid_cast<ASTIdentifier *>(ast.get()))
            id->semantic->special = true;
}

static void addIdentifierQualifier(ASTIdentifier & identifier, const DatabaseAndTableWithAlias & db_and_table)
{
    if (!db_and_table.alias.empty())
    {
        identifier.name_parts.emplace_back(db_and_table.alias);
    }
    else
    {
        if (!db_and_table.database.empty())
            identifier.name_parts.emplace_back(db_and_table.database);
        identifier.name_parts.emplace_back(db_and_table.table);
    }
}

static bool doesIdentifierBelongTo(const ASTIdentifier & identifier, const String & database, const String & table)
{
    size_t num_components = identifier.name_parts.size();
    if (num_components >= 3)
        return identifier.name_parts[0] == database &&
               identifier.name_parts[1] == table;
    return false;
}

static bool doesIdentifierBelongTo(const ASTIdentifier & identifier, const String & table)
{
    size_t num_components = identifier.name_parts.size();
    if (num_components >= 2)
        return identifier.name_parts[0] == table;
    return false;
}

size_t canReferColumnIdentifierToTable(const ASTIdentifier & identifier, const DatabaseAndTableWithAlias & db_and_table)
{
    /// database.table.column
    if (doesIdentifierBelongTo(identifier, db_and_table.database, db_and_table.table))
        return 2;

    /// table.column or alias.column.
    if (doesIdentifierBelongTo(identifier, db_and_table.table) ||
        doesIdentifierBelongTo(identifier, db_and_table.alias))
        return 1;

    return 0;
}

bool tryReferColumnIdentifierToTable(ASTIdentifier & identifier, const DatabaseAndTableWithAlias & db_and_table)
{
    size_t to_strip = canReferColumnIdentifierToTable(identifier, db_and_table);

    if (to_strip > identifier.semantic->num_qualifiers_to_strip)
    {
        identifier.semantic->num_qualifiers_to_strip = to_strip;
        identifier.semantic->db_and_table = std::make_unique<DatabaseAndTableWithAlias>(db_and_table);
        return true;
    }

    return false;
}

/// Checks that ast is ASTIdentifier and remove num_qualifiers_to_strip components from left.
/// Example: 'database.table.name' -> (num_qualifiers_to_strip = 2) -> 'name'.
void makeIdentifierShortName(ASTIdentifier & identifier)
{
    size_t & to_strip = identifier.semantic->num_qualifiers_to_strip;

    if (to_strip)
    {
        identifier.name_parts.erase(identifier.name_parts.begin(), identifier.name_parts.begin() + to_strip);
        DB::String new_name;
        for (const auto & part : identifier.name_parts)
        {
            if (!new_name.empty())
                new_name += '.';
            new_name += part;
        }
        identifier.name.swap(new_name);
    }

    to_strip = 0;
}

void makeIdentifierQualifiedName(ASTIdentifier & identifier)
{
    if (auto & db_and_table = identifier.semantic->db_and_table)
    {
        String prefix = db_and_table->getQualifiedNamePrefix();
        identifier.name.insert(identifier.name.begin(), prefix.begin(), prefix.end());

        addIdentifierQualifier(identifier, *db_and_table);
    }
}

}
