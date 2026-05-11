#include <IO/Operators.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTNamedScalarDDLQuery.h>
#include <Common/quoteString.h>

namespace DB
{

ASTPtr ASTNamedScalarDDLQuery::clone() const
{
    chassert(named_scalar_name);
    auto res = make_intrusive<ASTNamedScalarDDLQuery>(*this);
    res->children.clear();

    res->named_scalar_name = named_scalar_name->clone();
    res->children.push_back(res->named_scalar_name);

    if (sql_security)
    {
        res->sql_security = sql_security->clone();
        res->children.push_back(res->sql_security);
    }

    if (expression)
    {
        res->expression = expression->clone();
        res->children.push_back(res->expression);
    }

    return res;
}

void ASTNamedScalarDDLQuery::formatImpl(
    WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    chassert(named_scalar_name);
    if (action == Action::Create)
    {
        ostr << "CREATE ";
        if (or_replace)
            ostr << "OR REPLACE ";
    }
    else
    {
        ostr << "DROP ";
    }

    if (action == Action::Create && cache_kind == CacheKind::Local)
        ostr << "LOCAL ";
    else if (action == Action::Create && cache_kind == CacheKind::Shared)
        ostr << "SHARED ";

    ostr << "NAMED SCALAR ";

    if (action == Action::Create && if_not_exists)
        ostr << "IF NOT EXISTS ";
    else if (action == Action::Drop && if_exists)
        ostr << "IF EXISTS ";

    named_scalar_name->format(ostr, settings, state, frame);

    if (action == Action::Create && uuid != UUIDHelpers::Nil)
        ostr << " UUID " << quoteString(toString(uuid));

    formatOnCluster(ostr, settings);

    if (action == Action::Create)
    {
        if (sql_security)
        {
            ostr << " ";
            sql_security->format(ostr, settings, state, frame);
        }

        if (refresh_period_seconds)
            ostr << " REFRESH EVERY " << *refresh_period_seconds << " SECOND";

        ostr << " AS ";
        expression->format(ostr, settings, state, frame);
    }
}

String ASTNamedScalarDDLQuery::getNamedScalarName() const
{
    String name;
    tryGetIdentifierNameInto(named_scalar_name, name);
    return name;
}

}
