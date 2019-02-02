#include <Common/typeid_cast.h>
#include <Parsers/ASTIdentifier.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/IdentifierSemantic.h>


namespace DB
{

std::shared_ptr<ASTIdentifier> ASTIdentifier::createSpecial(const String & name, std::vector<String> && name_parts)
{
    auto ret = std::make_shared<ASTIdentifier>(name, std::move(name_parts));
    ret->semantic->special = true;
    return ret;
}

ASTIdentifier::ASTIdentifier(const String & name_, std::vector<String> && name_parts_)
    : name(name_)
    , name_parts(name_parts_)
    , semantic(std::make_shared<IdentifierSemanticImpl>())
{
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

void setIdentifierSpecial(ASTPtr & ast)
{
    if (ast)
        if (ASTIdentifier * id = typeid_cast<ASTIdentifier *>(ast.get()))
            id->semantic->special = true;
}

}
