#include <Common/typeid_cast.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/queryToString.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/StorageID.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int SYNTAX_ERROR;
}


ASTPtr ASTIdentifier::clone() const
{
    auto ret = std::make_shared<ASTIdentifier>(*this);
    ret->semantic = std::make_shared<IdentifierSemanticImpl>(*ret->semantic);
    return ret;
}

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
    if (!name_parts.empty() && name_parts[0].empty())
        name_parts.erase(name_parts.begin());

    if (name.empty())
    {
        if (name_parts.size() == 2)
            name = name_parts[0] + '.' + name_parts[1];
        else if (name_parts.size() == 1)
            name = name_parts[0];
    }
}

ASTIdentifier::ASTIdentifier(std::vector<String> && name_parts_)
    : ASTIdentifier("", std::move(name_parts_))
{}

void ASTIdentifier::setShortName(const String & new_name)
{
    name = new_name;
    name_parts.clear();

    bool special = semantic->special;
    *semantic = IdentifierSemanticImpl();
    semantic->special = special;
}

void ASTIdentifier::restoreCompoundName()
{
    if (name_parts.empty())
        return;
    name = name_parts[0];
    for (size_t i = 1; i < name_parts.size(); ++i)
        name += '.' + name_parts[i];
}

void ASTIdentifier::formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    auto format_element = [&](const String & elem_name)
    {
        settings.ostr << (settings.hilite ? hilite_identifier : "");
        settings.writeIdentifier(elem_name);
        settings.ostr << (settings.hilite ? hilite_none : "");
    };

    /// It could be compound but short
    if (!isShort())
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

void ASTIdentifier::resetTable(const String & database_name, const String & table_name)
{
    auto ast = createTableIdentifier(database_name, table_name);
    auto & ident = ast->as<ASTIdentifier &>();
    name.swap(ident.name);
    name_parts.swap(ident.name_parts);
    uuid = ident.uuid;
}

ASTPtr createTableIdentifier(const String & database_name, const String & table_name)
{
    assert(database_name != "_temporary_and_external_tables");
    return createTableIdentifier(StorageID(database_name, table_name));
}

ASTPtr createTableIdentifier(const StorageID & table_id)
{
    std::shared_ptr<ASTIdentifier> res;
    if (table_id.database_name.empty())
        res = ASTIdentifier::createSpecial(table_id.table_name);
    else
        res = ASTIdentifier::createSpecial(table_id.database_name + "." + table_id.table_name, {table_id.database_name, table_id.table_name});
    res->uuid = table_id.uuid;
    return res;
}

String getIdentifierName(const IAST * ast)
{
    String res;
    if (tryGetIdentifierNameInto(ast, res))
        return res;
    throw Exception(ast ? queryToString(*ast) + " is not an identifier" : "AST node is nullptr", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
}

std::optional<String> tryGetIdentifierName(const IAST * ast)
{
    String res;
    if (tryGetIdentifierNameInto(ast, res))
        return res;
    return {};
}

bool tryGetIdentifierNameInto(const IAST * ast, String & name)
{
    if (ast)
    {
        if (const auto * node = ast->as<ASTIdentifier>())
        {
            name = node->name;
            return true;
        }
    }
    return false;
}

void setIdentifierSpecial(ASTPtr & ast)
{
    if (ast)
        if (auto * id = ast->as<ASTIdentifier>())
            id->semantic->special = true;
}

StorageID getTableIdentifier(const ASTPtr & ast)
{
    if (!ast)
        throw Exception("AST node is nullptr", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
    const auto & identifier = dynamic_cast<const ASTIdentifier &>(*ast);
    if (identifier.name_parts.size() > 2)
        throw Exception("Logical error: more than two components in table expression", ErrorCodes::SYNTAX_ERROR);

    if (identifier.name_parts.size() == 2)
        return { identifier.name_parts[0], identifier.name_parts[1], identifier.uuid };
    return { "", identifier.name, identifier.uuid };
}

}
