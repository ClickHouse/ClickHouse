#include <Parsers/ASTCreateDriverFunctionQuery.h>

#include <Parsers/ASTIdentifier_fwd.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTCreateDriverFunctionQuery::clone() const
{
    auto res = std::make_shared<ASTCreateDriverFunctionQuery>(*this);
    res->children.clear();

    res->function_name = function_name->clone();
    res->children.push_back(res->function_name);

    res->function_params = function_params->clone();
    res->children.push_back(res->function_params);

    res->function_return_type = function_return_type->clone();
    res->children.push_back(res->function_return_type);

    res->engine_name = engine_name->clone();
    res->children.push_back(res->engine_name);

    res->function_body = function_body->clone();
    res->children.push_back(res->function_body);

    return res;
}

void ASTCreateDriverFunctionQuery::formatImpl(
    WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    ostr << (settings.hilite ? hilite_keyword : "") << "CREATE ";
    if (or_replace)
        ostr << "OR REPLACE ";

    ostr << "FUNCTION ";

    if (if_not_exists)
        ostr << "IF NOT EXISTS ";

    ostr << (settings.hilite ? hilite_none : "");

    ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(getFunctionName()) << (settings.hilite ? hilite_none : "");

    ostr << "(";
    function_params->format(ostr, settings, state, frame);
    ostr << ")";

    ostr << " RETURNS ";
    function_return_type->format(ostr, settings, state, frame);

    ostr << " ENGINE = ";
    ostr << (settings.hilite ? hilite_none : "");
    ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(getEngineName()) << (settings.hilite ? hilite_none : "");

    ostr << " AS ";
    function_body->format(ostr, settings, state, frame);
}

String ASTCreateDriverFunctionQuery::getFunctionName() const
{
    String name;
    tryGetIdentifierNameInto(function_name, name);
    return name;
}

String ASTCreateDriverFunctionQuery::getEngineName() const
{
    String name;
    tryGetIdentifierNameInto(engine_name, name);
    return name;
}
}
