#include <Parsers/ASTDropTypeQuery.h>

#include <IO/Operators.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/SipHash.h>


namespace DB
{

ASTPtr ASTDropTypeQuery::clone() const
{
    auto result = make_intrusive<ASTDropTypeQuery>(*this);
    result->children.clear();
    result->database = database ? database->clone() : nullptr;
    result->type_name = type_name ? type_name->clone() : nullptr;
    result->normalizeChildrenOrder();
    return result;
}

String ASTDropTypeQuery::getID(char delim) const
{
    String result = "DropTypeQuery";
    const auto database_name = getDatabase();
    if (!database_name.empty())
        result += delim + database_name;
    result += delim + getTypeName();
    return result;
}

ASTPtr ASTDropTypeQuery::getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const
{
    auto result = boost::static_pointer_cast<ASTDropTypeQuery>(clone());
    result->cluster.clear();
    if (!result->database && !params.default_database.empty())
        result->setDatabase(params.default_database);
    return result;
}

String ASTDropTypeQuery::getDatabase() const
{
    String result;
    tryGetIdentifierNameInto(database, result);
    return result;
}

String ASTDropTypeQuery::getTypeName() const
{
    String result;
    tryGetIdentifierNameInto(type_name, result);
    return result;
}

void ASTDropTypeQuery::setDatabase(const String & name)
{
    database = name.empty() ? nullptr : make_intrusive<ASTIdentifier>(name);
    normalizeChildrenOrder();
}

void ASTDropTypeQuery::normalizeChildrenOrder()
{
    children.clear();
    if (database)
        children.push_back(database);
    if (type_name)
        children.push_back(type_name);
}

void ASTDropTypeQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    hash_state.update(static_cast<UInt8>(if_exists));
    hash_state.update(cluster.size());
    hash_state.update(cluster);
}

void ASTDropTypeQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "DROP TYPE ";
    if (if_exists)
        ostr << "IF EXISTS ";

    if (database)
    {
        database->format(ostr, settings, state, frame);
        ostr << '.';
    }

    chassert(type_name);
    type_name->format(ostr, settings, state, frame);
    formatOnCluster(ostr, settings);
    ostr << " RESTRICT";
}

}
