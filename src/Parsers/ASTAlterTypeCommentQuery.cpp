#include <Parsers/ASTAlterTypeCommentQuery.h>

#include <IO/Operators.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Common/SipHash.h>


namespace DB
{

ASTPtr ASTAlterTypeCommentQuery::clone() const
{
    auto result = make_intrusive<ASTAlterTypeCommentQuery>(*this);
    result->children.clear();
    result->database = database ? database->clone() : nullptr;
    result->type_name = type_name ? type_name->clone() : nullptr;
    result->comment = comment ? comment->clone() : nullptr;
    result->normalizeChildrenOrder();
    return result;
}

String ASTAlterTypeCommentQuery::getID(char delim) const
{
    String result = "AlterTypeCommentQuery";
    const auto database_name = getDatabase();
    if (!database_name.empty())
        result += delim + database_name;
    result += delim + getTypeName();
    return result;
}

ASTPtr ASTAlterTypeCommentQuery::getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const
{
    auto result = boost::static_pointer_cast<ASTAlterTypeCommentQuery>(clone());
    result->cluster.clear();
    if (!result->database && !params.default_database.empty())
        result->setDatabase(params.default_database);
    return result;
}

String ASTAlterTypeCommentQuery::getDatabase() const
{
    String result;
    tryGetIdentifierNameInto(database, result);
    return result;
}

String ASTAlterTypeCommentQuery::getTypeName() const
{
    String result;
    tryGetIdentifierNameInto(type_name, result);
    return result;
}

String ASTAlterTypeCommentQuery::getComment() const
{
    if (!comment)
        return {};
    return comment->as<ASTLiteral &>().value.safeGet<String>();
}

void ASTAlterTypeCommentQuery::setDatabase(const String & name)
{
    database = name.empty() ? nullptr : make_intrusive<ASTIdentifier>(name);
    normalizeChildrenOrder();
}

void ASTAlterTypeCommentQuery::normalizeChildrenOrder()
{
    children.clear();
    if (database)
        children.push_back(database);
    if (type_name)
        children.push_back(type_name);
    if (comment)
        children.push_back(comment);
}

void ASTAlterTypeCommentQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    hash_state.update(static_cast<UInt8>(if_exists));
    hash_state.update(cluster.size());
    hash_state.update(cluster);
}

void ASTAlterTypeCommentQuery::formatImpl(
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

    chassert(comment);
    ostr << " COMMENT ";
    comment->format(ostr, settings, state, frame);
}

}
