#include <Parsers/ASTShowCreateTypeQuery.h>

#include <IO/Operators.h>
#include <Parsers/ASTIdentifier_fwd.h>


namespace DB
{

ASTPtr ASTShowCreateTypeQuery::clone() const
{
    auto result = make_intrusive<ASTShowCreateTypeQuery>(*this);
    result->children.clear();
    result->database = database ? database->clone() : nullptr;
    result->type_name = type_name ? type_name->clone() : nullptr;
    result->normalizeChildrenOrder();
    cloneOutputOptions(*result);
    return result;
}

String ASTShowCreateTypeQuery::getID(char delim) const
{
    String result = "ShowCreateTypeQuery";
    const auto database_name = getDatabase();
    if (!database_name.empty())
        result += delim + database_name;
    result += delim + getTypeName();
    return result;
}

String ASTShowCreateTypeQuery::getDatabase() const
{
    String result;
    tryGetIdentifierNameInto(database, result);
    return result;
}

String ASTShowCreateTypeQuery::getTypeName() const
{
    String result;
    tryGetIdentifierNameInto(type_name, result);
    return result;
}

void ASTShowCreateTypeQuery::normalizeChildrenOrder()
{
    children.clear();
    if (database)
        children.push_back(database);
    if (type_name)
        children.push_back(type_name);
}

void ASTShowCreateTypeQuery::formatQueryImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "SHOW CREATE TYPE ";
    if (database)
    {
        database->format(ostr, settings, state, frame);
        ostr << '.';
    }
    chassert(type_name);
    type_name->format(ostr, settings, state, frame);
}

}
