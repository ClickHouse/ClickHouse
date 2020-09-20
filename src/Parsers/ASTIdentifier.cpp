#include <Parsers/ASTIdentifier.h>

#include <Common/typeid_cast.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/StorageID.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteHelpers.h>
#include <Parsers/queryToString.h>


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

ASTIdentifier::ASTIdentifier(const String & short_name)
    : name(short_name), name_parts{name}, semantic(std::make_shared<IdentifierSemanticImpl>())
{
    assert(!name.empty());
}

ASTIdentifier::ASTIdentifier(std::vector<String> && name_parts_)
    : name_parts(name_parts_), semantic(std::make_shared<IdentifierSemanticImpl>())
{
    assert(!name_parts.empty());
}

void ASTIdentifier::setShortName(const String & new_name)
{
    name = new_name;
    name_parts.clear();

    bool special = semantic->special;
    *semantic = IdentifierSemanticImpl();
    semantic->special = special;
}

const String & ASTIdentifier::fullName() const
{
    assert(!name_parts.empty());

    if (name.empty())
    {
        name = name_parts[0];
        for (size_t i = 1; i < name_parts.size(); ++i)
            name += '.' + name_parts[i];
    }

    return name;
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

void ASTIdentifier::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(uuid);
    IAST::updateTreeHashImpl(hash_state);
}

ASTTableIdentifier::ASTTableIdentifier(const String & table) : ASTIdentifier(std::vector<String>{table})
{
    assert(!table.empty());
}

ASTTableIdentifier::ASTTableIdentifier(const StorageID & id) : ASTIdentifier({id.database_name, id.table_name})
{
    assert(!id.database_name.empty() && !id.table_name.empty());

    uuid = id.uuid;
}

ASTTableIdentifier::ASTTableIdentifier(const String & database, const String & table) : ASTIdentifier({database, table})
{
    assert(!database.empty() && !table.empty());
    assert(database != "_temporary_and_external_tables");
}

StorageID ASTTableIdentifier::getStorageId() const
{
    assert(name_parts.size() > 1);

    return {name_parts.size() == 2 ? name_parts[0] : "", name_parts.back(), uuid};
}

void ASTTableIdentifier::setDatabase(const String & database)
{
    assert(name_parts.size() > 1);

    if (name_parts.size() == 1) name_parts.insert(name_parts.begin(), database);
    else name_parts[0] = database;

    name = name_parts[0] + "." + name_parts[1];
}

void ASTTableIdentifier::setTable(const String & table)
{
    assert(name_parts.size() > 1);

    name_parts.back() = table;

    if (name_parts.size() == 1) name = name_parts.back();
    else name = name_parts[0] + "." + name_parts[1];
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
            name = node->fullName();
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

}
