#include <Parsers/ASTIdentifier.h>

#include <IO/WriteHelpers.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/StorageID.h>
#include <Parsers/queryToString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int SYNTAX_ERROR;
}

ASTIdentifier::ASTIdentifier(const String & short_name, ASTPtr && name_param)
    : full_name(short_name), name_parts{short_name}, semantic(std::make_shared<IdentifierSemanticImpl>())
{
    if (name_param == nullptr)
        assert(!full_name.empty());
    else
        children.push_back(std::move(name_param));
}

ASTIdentifier::ASTIdentifier(std::vector<String> && name_parts_, bool special, std::vector<ASTPtr> && name_params)
    : name_parts(name_parts_), semantic(std::make_shared<IdentifierSemanticImpl>())
{
    assert(!name_parts.empty());
    semantic->special = special;
    semantic->legacy_compound = true;
    if (!name_params.empty())
    {
        size_t params = 0;
        for (const auto & part [[maybe_unused]] : name_parts)
        {
            if (part.empty())
                ++params;
        }
        assert(params == name_params.size());
        children = std::move(name_params);
    }
    else
    {
        for (const auto & part [[maybe_unused]] : name_parts)
            assert(!part.empty());

        if (!special && name_parts.size() >= 2)
            semantic->table = name_parts.end()[-2];

        resetFullName();
    }
}

ASTPtr ASTIdentifier::getParam() const
{
    assert(full_name.empty() && children.size() == 1);
    return children.front()->clone();
}

ASTPtr ASTIdentifier::clone() const
{
    auto ret = std::make_shared<ASTIdentifier>(*this);
    ret->semantic = std::make_shared<IdentifierSemanticImpl>(*ret->semantic);
    return ret;
}

bool ASTIdentifier::supposedToBeCompound() const
{
    return semantic->legacy_compound;
}

void ASTIdentifier::setShortName(const String & new_name)
{
    assert(!new_name.empty());

    full_name = new_name;
    name_parts = {new_name};

    bool special = semantic->special;
    *semantic = IdentifierSemanticImpl();
    semantic->special = special;
}

const String & ASTIdentifier::name() const
{
    if (children.empty())
    {
        assert(!name_parts.empty());
        assert(!full_name.empty());
    }

    return full_name;
}

void ASTIdentifier::formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
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
        for (size_t i = 0, j = 0, size = name_parts.size(); i < size; ++i)
        {
            if (i != 0)
                settings.ostr << '.';

            if (name_parts[i].empty())
                children[j++]->formatImpl(settings, state, frame);
            else
                format_element(name_parts[i]);
        }
    }
    else
    {
        const auto & name = shortName();
        if (name.empty())
            children.front()->formatImpl(settings, state, frame);
        else
            format_element(name);
    }
}

void ASTIdentifier::appendColumnNameImpl(WriteBuffer & ostr) const
{
    writeString(name(), ostr);
}

void ASTIdentifier::restoreTable()
{
    if (!compound())
    {
        name_parts.insert(name_parts.begin(), semantic->table);
        resetFullName();
    }
}

void ASTIdentifier::resetTable(const String & database_name, const String & table_name)
{
    auto ast = createTableIdentifier(database_name, table_name);
    auto & ident = ast->as<ASTIdentifier &>();
    full_name.swap(ident.full_name);
    name_parts.swap(ident.name_parts);
    uuid = ident.uuid;
}

void ASTIdentifier::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(uuid);
    IAST::updateTreeHashImpl(hash_state);
}

void ASTIdentifier::resetFullName()
{
    full_name = name_parts[0];
    for (size_t i = 1; i < name_parts.size(); ++i)
        full_name += '.' + name_parts[i];
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
        res = std::make_shared<ASTIdentifier>(std::vector<String>{table_id.table_name}, true);
    else
        res = std::make_shared<ASTIdentifier>(std::vector<String>{table_id.database_name, table_id.table_name}, true);
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
            name = node->name();
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
    return { "", identifier.name_parts[0], identifier.uuid };
}

}
