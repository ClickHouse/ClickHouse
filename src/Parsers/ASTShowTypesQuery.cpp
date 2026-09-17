#include <Parsers/ASTShowTypesQuery.h>

#include <IO/Operators.h>
#include <Parsers/ASTIdentifier_fwd.h>


namespace DB
{

ASTPtr ASTShowTypesQuery::clone() const
{
    auto result = make_intrusive<ASTShowTypesQuery>(*this);
    result->children.clear();
    result->database = database ? database->clone() : nullptr;
    result->like_pattern = like_pattern ? like_pattern->clone() : nullptr;
    result->normalizeChildrenOrder();
    cloneOutputOptions(*result);
    return result;
}

String ASTShowTypesQuery::getDatabase() const
{
    String result;
    tryGetIdentifierNameInto(database, result);
    return result;
}

void ASTShowTypesQuery::normalizeChildrenOrder()
{
    children.clear();
    if (database)
        children.push_back(database);
    if (like_pattern)
        children.push_back(like_pattern);
}

void ASTShowTypesQuery::formatQueryImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "SHOW TYPES";
    if (database)
    {
        ostr << " FROM ";
        database->format(ostr, settings, state, frame);
    }
    if (like_pattern)
    {
        ostr << " LIKE ";
        like_pattern->format(ostr, settings, state, frame);
    }
}

}
