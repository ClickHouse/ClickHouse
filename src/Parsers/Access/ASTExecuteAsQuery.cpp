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
    if (select)
        res->set(res->select, select->clone());

    return res;
}


void ASTExecuteAsQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "EXECUTE AS ";
    targetuser->format(ostr, settings);

    if (select)
    {
        ostr << settings.nl_or_ws;
        select->format(ostr, settings, state, frame);
    }
}
}
