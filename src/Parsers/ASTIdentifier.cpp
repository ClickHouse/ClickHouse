#include <Parsers/ASTIdentifier.h>

#include <Common/SipHash.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ExpressionElementParsers.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
}

ASTIdentifier::ASTIdentifier(const String & short_name, ASTPtr && name_param)
    : full_name(short_name), name_parts(std::vector<String>{short_name}), semantic(std::make_shared<IdentifierSemanticImpl>())
{
    if (!name_param)
        chassert(!full_name.empty());
    else
        children.push_back(std::move(name_param));
}

ASTIdentifier::ASTIdentifier(std::vector<String> && name_parts_, bool special, ASTs && name_params)
    : ASTIdentifier(IdentifierName(name_parts_), special, std::move(name_params))
{
}

ASTIdentifier::ASTIdentifier(IdentifierName name_parts_, bool special, ASTs && name_params)
    : name_parts(std::move(name_parts_)), semantic(std::make_shared<IdentifierSemanticImpl>())
{
    chassert(!name_parts.empty());
    semantic->special = special;
    semantic->legacy_compound = true;
    if (!name_params.empty())
    {
        [[maybe_unused]] size_t params = 0;
        for (const auto & part [[maybe_unused]] : name_parts)
        {
            if (part.spelling.empty())
                ++params;
        }
        chassert(params == name_params.size());
        children = std::move(name_params);
    }
    else
    {
        for (const auto & part [[maybe_unused]] : name_parts)
            chassert(!part.spelling.empty());

        if (!special && name_parts.size() >= 2)
            semantic->table = name_parts.end()[-2].spelling;

        resetFullName();
    }
}

bool ASTIdentifier::isParam() const
{
    return !children.empty();
}

ASTPtr ASTIdentifier::getParam() const
{
    chassert(full_name.empty() && children.size() == 1);
    return children.front()->clone();
}

ASTPtr ASTIdentifier::clone() const
{
    auto ret = make_intrusive<ASTIdentifier>(*this);
    ret->semantic = std::make_shared<IdentifierSemanticImpl>(*ret->semantic);
    ret->cloneChildren();
    return ret;
}

bool ASTIdentifier::supposedToBeCompound() const
{
    return semantic->legacy_compound;
}

void ASTIdentifier::setShortName(const String & new_name)
{
    chassert(!new_name.empty());

    full_name = new_name;
    name_parts = IdentifierName(std::vector<String>{new_name});

    bool special = semantic->special;
    auto table = semantic->table;

    *semantic = IdentifierSemanticImpl();
    semantic->special = special;
    semantic->table = table;
}

IdentifierPartQuote identifierPartQuoteFromAST(const ASTPtr & node)
{
    if (const auto * identifier = node ? node->as<ASTIdentifier>() : nullptr)
        if (!identifier->name_parts.empty())
            return identifier->name_parts[0].quote;
    return IdentifierPartQuote::Unquoted;
}

void ASTIdentifier::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// Part boundaries are semantic and survive the format/reparse round-trip, so mix them in.
    /// Quote styles do NOT survive formatting (presentation honors identifier_quoting_style) and
    /// must stay out of the hash; resolution reads them from the AST parts, not from the hash.
    if (name_parts.size() > 1)
    {
        for (const auto & part : name_parts)
            hash_state.update(part.spelling.size());
    }
    ASTWithAlias::updateTreeHashImpl(hash_state, ignore_aliases);
}

const String & ASTIdentifier::name() const
{
    if (children.empty())
    {
        chassert(!name_parts.empty());
        chassert(!full_name.empty());
    }

    return full_name;
}

void ASTIdentifier::formatImplWithoutAlias(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    auto format_element = [&](const String & elem_name)
    {
        if (auto special_delimiter_and_identifier = ParserCompoundIdentifier::splitSpecialDelimiterAndIdentifierIfAny(elem_name))
        {
            ostr << special_delimiter_and_identifier->first;
            settings.writeIdentifier(ostr, special_delimiter_and_identifier->second, /*ambiguous=*/false);
        }
        else
        {
            settings.writeIdentifier(ostr, elem_name, /*ambiguous=*/false);
        }
    };

    if (compound())
    {
        for (size_t i = 0, j = 0, size = name_parts.size(); i < size; ++i)
        {
            if (i != 0)
                ostr << '.';

            /// Some AST rewriting code, like IdentifierSemantic::setColumnLongName,
            /// does not respect children of identifier.
            /// Here we also ignore children if they are empty.
            if (name_parts[i].spelling.empty() && j < children.size())
            {
                children[j]->format(ostr, settings, state, frame);
                ++j;
            }
            else
                format_element(name_parts[i].spelling);
        }
    }
    else
    {
        const auto & name = shortName();
        if (name.empty() && !children.empty())
            children.front()->format(ostr, settings, state, frame);
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
        name_parts.parts.insert(name_parts.parts.begin(), IdentifierPart{semantic->table});
        resetFullName();
    }
}

boost::intrusive_ptr<ASTTableIdentifier> ASTIdentifier::createTable() const
{
    if (name_parts.size() == 1 || name_parts.size() == 2)
        return make_intrusive<ASTTableIdentifier>(name_parts);
    return nullptr;
}

void ASTIdentifier::resetFullName()
{
    full_name = name_parts[0].spelling;
    for (size_t i = 1; i < name_parts.size(); ++i)
        full_name += '.' + name_parts[i].spelling;
}

ASTTableIdentifier::ASTTableIdentifier(const String & table_name, ASTs && name_params)
    : ASTIdentifier({table_name}, true, std::move(name_params))
{
}

namespace
{

IdentifierName storageIDToIdentifierName(const StorageID & table_id)
{
    IdentifierName name;
    if (!table_id.database_name.empty())
        name.push_back(IdentifierPart{table_id.database_name, table_id.database_name_quote});
    name.push_back(IdentifierPart{table_id.table_name, table_id.table_name_quote});
    return name;
}

}

ASTTableIdentifier::ASTTableIdentifier(const StorageID & table_id, ASTs && name_params)
    : ASTIdentifier(storageIDToIdentifierName(table_id), true, std::move(name_params))
{
    uuid = table_id.uuid;
}

ASTTableIdentifier::ASTTableIdentifier(IdentifierName name_parts_, ASTs && name_params)
    : ASTIdentifier(std::move(name_parts_), true, std::move(name_params))
{
    chassert(name_parts.size() == 1 || name_parts.size() == 2);
}

ASTTableIdentifier::ASTTableIdentifier(const String & database_name, const String & table_name, ASTs && name_params)
    : ASTIdentifier({database_name, table_name}, true, std::move(name_params))
{
}

ASTPtr ASTTableIdentifier::clone() const
{
    auto ret = make_intrusive<ASTTableIdentifier>(*this);
    ret->semantic = std::make_shared<IdentifierSemanticImpl>(*ret->semantic);
    ret->cloneChildren();
    return ret;
}

StorageID ASTTableIdentifier::getTableId() const
{
    if (name_parts.size() == 2)
    {
        StorageID table_id{name_parts[0].spelling, name_parts[1].spelling, uuid};
        table_id.database_name_quote = name_parts[0].quote;
        table_id.table_name_quote = name_parts[1].quote;
        return table_id;
    }
    StorageID table_id{{}, name_parts[0].spelling, uuid};
    table_id.table_name_quote = name_parts[0].quote;
    return table_id;
}

String ASTTableIdentifier::getDatabaseName() const
{
    if (name_parts.size() == 2) return name_parts[0].spelling;
    return {};
}

ASTPtr ASTTableIdentifier::getTable() const
{
    if (name_parts.size() == 2)
    {
        if (!name_parts[1].spelling.empty())
            return make_intrusive<ASTIdentifier>(name_parts[1].spelling);

        if (name_parts[0].spelling.empty())
            return make_intrusive<ASTIdentifier>("", children[1]->clone());
        return make_intrusive<ASTIdentifier>("", children[0]->clone());
    }
    if (name_parts.size() == 1)
    {
        if (name_parts[0].spelling.empty())
            return make_intrusive<ASTIdentifier>("", children[0]->clone());
        return make_intrusive<ASTIdentifier>(name_parts[0].spelling);
    }
    return {};
}

ASTPtr ASTTableIdentifier::getDatabase() const
{
    if (name_parts.size() == 2)
    {
        if (name_parts[0].spelling.empty())
            return make_intrusive<ASTIdentifier>("", children[0]->clone());
        return make_intrusive<ASTIdentifier>(name_parts[0].spelling);
    }
    return {};
}

void ASTTableIdentifier::resetTable(const String & database_name, const String & table_name)
{
    auto identifier = make_intrusive<ASTTableIdentifier>(database_name, table_name);
    full_name.swap(identifier->full_name);
    std::swap(name_parts, identifier->name_parts);
    uuid = identifier->uuid;
}

void ASTTableIdentifier::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(uuid);
    ASTIdentifier::updateTreeHashImpl(hash_state, ignore_aliases);
}

String getIdentifierName(const IAST * ast)
{
    String res;
    if (tryGetIdentifierNameInto(ast, res))
        return res;
    if (ast)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "{} is not an identifier", ast->formatForErrorMessage());
    throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "AST node is nullptr");
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
