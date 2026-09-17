#include <Parsers/ASTDescribeTypeQuery.h>

#include <IO/Operators.h>
#include <Parsers/ASTIdentifier_fwd.h>


namespace DB
{

ASTPtr ASTDescribeTypeQuery::clone() const
{
    auto result = make_intrusive<ASTDescribeTypeQuery>(*this);
    result->children.clear();
    result->database = database ? database->clone() : nullptr;
    result->type_name = type_name ? type_name->clone() : nullptr;
    result->normalizeChildrenOrder();
    cloneOutputOptions(*result);
    return result;
}

String ASTDescribeTypeQuery::getID(char delim) const
{
    String result = "DescribeTypeQuery";
    const auto database_name = getDatabase();
    if (!database_name.empty())
        result += delim + database_name;
    result += delim + getTypeName();
    return result;
}

String ASTDescribeTypeQuery::getDatabase() const
{
    String result;
    tryGetIdentifierNameInto(database, result);
    return result;
}

String ASTDescribeTypeQuery::getTypeName() const
{
    String result;
    tryGetIdentifierNameInto(type_name, result);
    return result;
}

void ASTDescribeTypeQuery::normalizeChildrenOrder()
{
    children.clear();
    if (database)
        children.push_back(database);
    if (type_name)
        children.push_back(type_name);
}

void ASTDescribeTypeQuery::formatQueryImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "DESCRIBE TYPE ";
    if (database)
    {
        database->format(ostr, settings, state, frame);
        ostr << '.';
    }
    chassert(type_name);
    type_name->format(ostr, settings, state, frame);
}

}
