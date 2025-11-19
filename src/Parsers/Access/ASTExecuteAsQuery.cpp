#include <Parsers/Access/ASTExecuteAsQuery.h>

#include <Parsers/Access/ASTUserNameWithHost.h>
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

    if (target_user)
        res->set(res->target_user, target_user->clone());
    if (subquery)
        res->set(res->subquery, subquery->clone());

    return res;
}


void ASTExecuteAsQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "EXECUTE AS ";

    target_user->format(ostr, settings);

    if (subquery)
    {
        ostr << settings.nl_or_ws;
        subquery->format(ostr, settings, state, frame);
    }
}

}
