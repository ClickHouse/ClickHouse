#include <Parsers/ASTCreateTypeQuery.h>

#include <IO/Operators.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>

#include <string_view>


namespace DB
{

namespace
{

void hashString(SipHash & hash_state, const String & value)
{
    hash_state.update(value.size());
    hash_state.update(value);
}

}

String ASTCreateTypeQuery::getID(char delim) const
{
    String result = attach ? "AttachTypeQuery" : "CreateTypeQuery";
    const auto database_name = getDatabase();
    if (!database_name.empty())
        result += delim + database_name;
    result += delim + getTypeName();
    return result;
}

ASTPtr ASTCreateTypeQuery::clone() const
{
    auto result = make_intrusive<ASTCreateTypeQuery>(*this);
    result->children.clear();
    result->database = database ? database->clone() : nullptr;
    result->type_name = type_name ? type_name->clone() : nullptr;
    result->parameters = parameters ? parameters->clone() : nullptr;
    result->decreases = decreases ? decreases->clone() : nullptr;
    result->definition = definition ? definition->clone() : nullptr;
    result->comment = comment ? comment->clone() : nullptr;
    result->normalizeChildrenOrder();
    return result;
}

ASTPtr ASTCreateTypeQuery::getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const
{
    auto result = boost::static_pointer_cast<ASTCreateTypeQuery>(clone());
    result->cluster.clear();
    if (!result->database && !params.default_database.empty())
        result->setDatabase(params.default_database);
    return result;
}

String ASTCreateTypeQuery::getDatabase() const
{
    String result;
    tryGetIdentifierNameInto(database, result);
    return result;
}

String ASTCreateTypeQuery::getTypeName() const
{
    String result;
    tryGetIdentifierNameInto(type_name, result);
    return result;
}

void ASTCreateTypeQuery::setDatabase(const String & name)
{
    database = name.empty() ? nullptr : make_intrusive<ASTIdentifier>(name);
    normalizeChildrenOrder();
}

void ASTCreateTypeQuery::normalizeChildrenOrder()
{
    children.clear();
    if (database)
        children.push_back(database);
    if (type_name)
        children.push_back(type_name);
    if (parameters)
        children.push_back(parameters);
    if (decreases)
        children.push_back(decreases);
    if (definition)
        children.push_back(definition);
    if (comment)
        children.push_back(comment);
}

void ASTCreateTypeQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    hash_state.update(static_cast<UInt8>(attach));
    hash_state.update(static_cast<UInt8>(if_not_exists));
    hashString(hash_state, cluster);

    hash_state.update(static_cast<UInt8>(uuid.has_value()));
    if (uuid)
        hashString(hash_state, toString(*uuid));

    hash_state.update(static_cast<UInt8>(revision.has_value()));
    if (revision)
        hash_state.update(*revision);

    hash_state.update(static_cast<UInt8>(definition_hash.has_value()));
    if (definition_hash)
        hashString(hash_state, *definition_hash);
}

void ASTCreateTypeQuery::formatImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << (attach ? "ATTACH TYPE " : "CREATE TYPE ");
    if (if_not_exists)
        ostr << "IF NOT EXISTS ";

    if (database)
    {
        database->format(ostr, settings, state, frame);
        ostr << '.';
    }

    chassert(type_name);
    type_name->format(ostr, settings, state, frame);

    if (parameters)
    {
        ostr << '(';
        parameters->format(ostr, settings, state, frame);
        ostr << ')';
    }

    if (uuid)
        ostr << " UUID " << quoteString(toString(*uuid));
    if (revision)
        ostr << " REVISION " << *revision;

    formatOnCluster(ostr, settings);

    if (decreases)
    {
        ostr << " DECREASES ";
        decreases->format(ostr, settings, state, frame);
    }

    chassert(definition);
    ostr << " AS ";
    definition->format(ostr, settings, state, frame);

    if (definition_hash)
        ostr << " DEFINITION HASH " << quoteString(*definition_hash);

    if (comment)
    {
        ostr << " COMMENT ";
        comment->format(ostr, settings, state, frame);
    }
}

}
