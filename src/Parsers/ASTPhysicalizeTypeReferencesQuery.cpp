#include <Parsers/ASTPhysicalizeTypeReferencesQuery.h>

#include <IO/Operators.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>

#include <string_view>


namespace DB
{

ASTPtr ASTPhysicalizeTypeReferencesQuery::clone() const
{
    auto result = make_intrusive<ASTPhysicalizeTypeReferencesQuery>(*this);
    result->children.clear();
    for (auto member : output_option_members)
        (result.get()->*member).reset();
    result->object_kind = object_kind ? object_kind->clone() : nullptr;
    result->database = database ? database->clone() : nullptr;
    result->object_name = object_name ? object_name->clone() : nullptr;
    result->normalizeChildrenOrder();
    cloneOutputOptions(*result);
    return result;
}

ASTPtr ASTPhysicalizeTypeReferencesQuery::getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const
{
    auto result = boost::static_pointer_cast<ASTPhysicalizeTypeReferencesQuery>(clone());
    result->cluster.clear();
    if (result->scope != Scope::Database && !result->database && !params.default_database.empty())
        result->setDatabase(params.default_database);
    return result;
}

String ASTPhysicalizeTypeReferencesQuery::getDatabase() const
{
    String result;
    tryGetIdentifierNameInto(database, result);
    return result;
}

String ASTPhysicalizeTypeReferencesQuery::getObjectName() const
{
    String result;
    tryGetIdentifierNameInto(object_name, result);
    return result;
}

void ASTPhysicalizeTypeReferencesQuery::setDatabase(const String & name)
{
    database = name.empty() ? nullptr : make_intrusive<ASTIdentifier>(name);
    normalizeChildrenOrder();
}

void ASTPhysicalizeTypeReferencesQuery::normalizeChildrenOrder()
{
    children.clear();
    if (object_kind)
        children.push_back(object_kind);
    if (database)
        children.push_back(database);
    if (object_name)
        children.push_back(object_name);

    for (auto member : output_option_members)
        if (this->*member)
            children.push_back(this->*member);
}

void ASTPhysicalizeTypeReferencesQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    hash_state.update(static_cast<UInt8>(scope));
    hash_state.update(static_cast<UInt8>(drop_unused_types));
    hash_state.update(cluster.size());
    hash_state.update(cluster);
}

void ASTPhysicalizeTypeReferencesQuery::formatQueryImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "PHYSICALIZE TYPE REFERENCES ";

    if (scope == Scope::Database)
    {
        ostr << "DATABASE ";
        chassert(database);
        database->format(ostr, settings, state, frame);
    }
    else
    {
        ostr << (scope == Scope::DependentClosure ? "CLOSURE OF " : "OBJECT ");
        chassert(object_kind);
        if (equalsCaseInsensitive(getIdentifierName(object_kind), "MATERIALIZED VIEW"))
            ostr << "MATERIALIZED VIEW";
        else
            object_kind->format(ostr, settings, state, frame);
        ostr << ' ';
        if (database)
        {
            database->format(ostr, settings, state, frame);
            ostr << '.';
        }
        chassert(object_name);
        object_name->format(ostr, settings, state, frame);
    }

    formatOnCluster(ostr, settings);
    if (drop_unused_types)
        ostr << " DROP UNUSED TYPES";
    ostr << " DRY RUN";
}

ASTPtr ASTApplyPhysicalizeTypeReferencesQuery::clone() const
{
    return make_intrusive<ASTApplyPhysicalizeTypeReferencesQuery>(*this);
}

void ASTApplyPhysicalizeTypeReferencesQuery::updateTreeHashImpl(SipHash & hash_state, bool) const
{
    static constexpr std::string_view domain = "ASTApplyPhysicalizeTypeReferencesQuery:redacted-token-v1";
    hash_state.update(domain.size());
    hash_state.update(domain.data(), domain.size());
}

void ASTApplyPhysicalizeTypeReferencesQuery::formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << "PHYSICALIZE TYPE REFERENCES APPLY TOKEN " << quoteString(redacted_token);
}

}
