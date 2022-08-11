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
}

ASTIdentifier::ASTIdentifier(const String & short_name, ASTPtr && name_param)
    : full_name(short_name), name_parts{short_name}, semantic(std::make_shared<IdentifierSemanticImpl>())
{
    if (!name_param)
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
        [[maybe_unused]] size_t params = 0;
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
    auto table = semantic->table;

    *semantic = IdentifierSemanticImpl();
    semantic->special = special;
    semantic->table = table;
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

    if (compound())
    {
        for (size_t i = 0, j = 0, size = name_parts.size(); i < size; ++i)
        {
            if (i != 0)
                settings.ostr << '.';

            /// Some AST rewriting code, like IdentifierSemantic::setColumnLongName,
            /// does not respect children of identifier.
            /// Here we also ignore children if they are empty.
            if (name_parts[i].empty() && j < children.size())
            {
                children[j]->formatImpl(settings, state, frame);
                ++j;
            }
            else
                format_element(name_parts[i]);
        }
    }
    else
    {
        const auto & name = shortName();
        if (name.empty() && !children.empty())
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

std::shared_ptr<ASTTableIdentifier> ASTIdentifier::createTable() const
{
    if (name_parts.size() == 1) return std::make_shared<ASTTableIdentifier>(name_parts[0]);
    if (name_parts.size() == 2) return std::make_shared<ASTTableIdentifier>(name_parts[0], name_parts[1]);
    return nullptr;
}

void ASTIdentifier::resetFullName()
{
    full_name = name_parts[0];
    for (size_t i = 1; i < name_parts.size(); ++i)
        full_name += '.' + name_parts[i];
}

ASTTableIdentifier::ASTTableIdentifier(const String & table_name, std::vector<ASTPtr> && name_params)
    : ASTIdentifier({table_name}, true, std::move(name_params))
{
}

ASTTableIdentifier::ASTTableIdentifier(const StorageID & table_id, std::vector<ASTPtr> && name_params)
    : ASTIdentifier(
        table_id.database_name.empty() ? std::vector<String>{table_id.table_name}
                                       : std::vector<String>{table_id.database_name, table_id.table_name},
        true, std::move(name_params))
{
    uuid = table_id.uuid;
}

ASTTableIdentifier::ASTTableIdentifier(const String & database_name, const String & table_name, std::vector<ASTPtr> && name_params)
    : ASTIdentifier({database_name, table_name}, true, std::move(name_params))
{
}

ASTPtr ASTTableIdentifier::clone() const
{
    auto ret = std::make_shared<ASTTableIdentifier>(*this);
    ret->semantic = std::make_shared<IdentifierSemanticImpl>(*ret->semantic);
    return ret;
}

StorageID ASTTableIdentifier::getTableId() const
{
    if (name_parts.size() == 2) return {name_parts[0], name_parts[1], uuid};
    else return {{}, name_parts[0], uuid};
}

String ASTTableIdentifier::getDatabaseName() const
{
    if (name_parts.size() == 2) return name_parts[0];
    else return {};
}

ASTPtr ASTTableIdentifier::getTable() const
{
    if (name_parts.size() == 2)
    {
        if (!name_parts[1].empty())
            return std::make_shared<ASTIdentifier>(name_parts[1]);

        if (name_parts[0].empty())
            return std::make_shared<ASTIdentifier>("", children[1]->clone());
        else
            return std::make_shared<ASTIdentifier>("", children[0]->clone());
    }
    else if (name_parts.size() == 1)
    {
        if (name_parts[0].empty())
            return std::make_shared<ASTIdentifier>("", children[0]->clone());
        else
            return std::make_shared<ASTIdentifier>(name_parts[0]);
    }
    else return {};
}

ASTPtr ASTTableIdentifier::getDatabase() const
{
    if (name_parts.size() == 2)
    {
        if (name_parts[0].empty())
            return std::make_shared<ASTIdentifier>("", children[0]->clone());
        else
            return std::make_shared<ASTIdentifier>(name_parts[0]);
    }
    else return {};
}

void ASTTableIdentifier::resetTable(const String & database_name, const String & table_name)
{
    auto identifier = std::make_shared<ASTTableIdentifier>(database_name, table_name);
    full_name.swap(identifier->full_name);
    name_parts.swap(identifier->name_parts);
    uuid = identifier->uuid;
}

void ASTTableIdentifier::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(uuid);
    ASTWithAlias::updateTreeHashImpl(hash_state);
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
        if (const auto * node = dynamic_cast<const ASTIdentifier *>(ast))
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

}
