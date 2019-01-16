#include <Parsers/ASTIdentifier.h>
#include <Common/typeid_cast.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteHelpers.h>


namespace DB
{

void ASTIdentifier::formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    auto format_element = [&](const String & elem_name)
    {
        settings.ostr << (settings.hilite ? hilite_identifier : "");
        settings.writeIdentifier(elem_name);
        settings.ostr << (settings.hilite ? hilite_none : "");
    };

    /// A simple or compound identifier?

    if (children.size() > 1)
    {
        for (size_t i = 0, size = children.size(); i < size; ++i)
        {
            if (i != 0)
                settings.ostr << '.';

            format_element(static_cast<const ASTIdentifier &>(*children[i].get()).name);
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

    ASTPtr database = ASTIdentifier::createSpecial(database_name);
    ASTPtr table = ASTIdentifier::createSpecial(table_name);

    ASTPtr database_and_table = ASTIdentifier::createSpecial(database_name + "." + table_name);
    database_and_table->children = {database, table};
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
    if (!node.special)
        return node.name;
    return {};
}

std::optional<String> getColumnIdentifierName(const ASTPtr & ast)
{
    if (ast)
        if (auto id = typeid_cast<const ASTIdentifier *>(ast.get()))
            if (!id->special)
                return id->name;
    return {};
}

std::optional<String> getTableIdentifierName(const ASTIdentifier & node)
{
    if (node.special)
        return node.name;
    return {};
}

std::optional<String> getTableIdentifierName(const ASTPtr & ast)
{
    if (ast)
        if (auto id = typeid_cast<const ASTIdentifier *>(ast.get()))
            if (id->special)
                return id->name;
    return {};
}

void setIdentifierSpecial(ASTPtr & ast)
{
    if (ast)
        if (ASTIdentifier * id = typeid_cast<ASTIdentifier *>(ast.get()))
            id->setSpecial();
}

void addIdentifierQualifier(ASTIdentifier & identifier, const String & database, const String & table, const String & alias)
{
    if (!alias.empty())
    {
        identifier.children.emplace_back(std::make_shared<ASTIdentifier>(alias));
    }
    else
    {
        if (!database.empty())
            identifier.children.emplace_back(std::make_shared<ASTIdentifier>(database));
        identifier.children.emplace_back(std::make_shared<ASTIdentifier>(table));
    }
}

bool doesIdentifierBelongTo(const ASTIdentifier & identifier, const String & database, const String & table)
{
    size_t num_components = identifier.children.size();
    if (num_components >= 3)
        return  *getIdentifierName(identifier.children[0]) == database &&
                *getIdentifierName(identifier.children[1]) == table;
    return false;
}

bool doesIdentifierBelongTo(const ASTIdentifier & identifier, const String & table)
{
    size_t num_components = identifier.children.size();
    if (num_components >= 2)
        return *getIdentifierName(identifier.children[0]) == table;
    return false;
}

}
