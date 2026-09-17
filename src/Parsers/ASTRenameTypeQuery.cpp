#include <Parsers/ASTRenameTypeQuery.h>

#include <IO/Operators.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/SipHash.h>


namespace DB
{

ASTPtr ASTRenameTypeQuery::clone() const
{
    auto result = make_intrusive<ASTRenameTypeQuery>(*this);
    result->children.clear();
    result->database = database ? database->clone() : nullptr;
    result->type_name = type_name ? type_name->clone() : nullptr;
    result->new_type_name = new_type_name ? new_type_name->clone() : nullptr;
    result->normalizeChildrenOrder();
    return result;
}

String ASTRenameTypeQuery::getID(char delim) const
{
    String result = "RenameTypeQuery";
    const auto database_name = getDatabase();
    if (!database_name.empty())
        result += delim + database_name;
    result += delim + getTypeName();
    result += delim + getNewTypeName();
    return result;
}

ASTPtr ASTRenameTypeQuery::getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const
{
    auto result = boost::static_pointer_cast<ASTRenameTypeQuery>(clone());
    result->cluster.clear();
    if (!result->database && !params.default_database.empty())
        result->setDatabase(params.default_database);
    return result;
}

String ASTRenameTypeQuery::getDatabase() const
{
    String result;
    tryGetIdentifierNameInto(database, result);
    return result;
}

String ASTRenameTypeQuery::getTypeName() const
{
    String result;
    tryGetIdentifierNameInto(type_name, result);
    return result;
}

String ASTRenameTypeQuery::getNewTypeName() const
{
    String result;
    tryGetIdentifierNameInto(new_type_name, result);
    return result;
}

void ASTRenameTypeQuery::setDatabase(const String & name)
{
    database = name.empty() ? nullptr : make_intrusive<ASTIdentifier>(name);
    normalizeChildrenOrder();
}

void ASTRenameTypeQuery::normalizeChildrenOrder()
{
    children.clear();
    if (database)
        children.push_back(database);
    if (type_name)
        children.push_back(type_name);
    if (new_type_name)
        children.push_back(new_type_name);
}

void ASTRenameTypeQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    hash_state.update(static_cast<UInt8>(if_exists));
    hash_state.update(cluster.size());
    hash_state.update(cluster);
}

void ASTRenameTypeQuery::formatImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "ALTER TYPE ";
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

    ostr << " RENAME TO ";
    chassert(new_type_name);
    new_type_name->format(ostr, settings, state, frame);
}

}
