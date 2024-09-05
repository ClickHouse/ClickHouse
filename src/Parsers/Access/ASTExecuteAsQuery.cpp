#include <Parsers/Access/ASTExecuteAsQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
String ASTExecuteAsQuery::getID(char) const
{
    return "ExecuteAsQuery";
}


ASTPtr ASTExecuteAsQuery::clone() const
{
    auto res = std::make_shared<ASTExecuteAsQuery>(*this);

    if (targetuser)
        res->targetuser = std::static_pointer_cast<ASTRolesOrUsersSet>(targetuser->clone());


    return res;
}


void ASTExecuteAsQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "");
    settings.ostr << "EXECUTE AS";
    settings.ostr << (settings.hilite ? hilite_none : "");

    settings.ostr << " ";
    targetuser->format(settings);
}
}
