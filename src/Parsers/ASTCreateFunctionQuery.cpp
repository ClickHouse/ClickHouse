#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

ASTPtr ASTCreateFunctionQuery::clone() const
{
    auto res = std::make_shared<ASTCreateFunctionQuery>(*this);
    res->children.clear();

    res->function_name = function_name->clone();
    res->children.push_back(res->function_name);

    return res;
}

void ASTCreateFunctionQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState & /*state*/, IAST::FormatStateStacked /*frame*/) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "CREATE ";

    if (or_replace)
        settings.ostr << "OR REPLACE ";

    settings.ostr << "FUNCTION ";

    if (if_not_exists)
        settings.ostr << "IF NOT EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");

    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(getFunctionName()) << (settings.hilite ? hilite_none : "");

    formatOnCluster(settings);
}

String ASTCreateFunctionQuery::getFunctionName() const
{
    String name;
    tryGetIdentifierNameInto(function_name, name);
    return name;
}

ASTPtr ASTCreateLambdaFunctionQuery::clone() const {
    auto res = ASTCreateFunctionQuery::clone();
    res->as<ASTCreateLambdaFunctionQuery>()->function_core = function_core->clone();
    res->children.push_back(res->as<ASTCreateLambdaFunctionQuery>()->function_core);
    return res;
}

void ASTCreateLambdaFunctionQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const {
    ASTCreateFunctionQuery::formatImpl(settings, state, frame);

    settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "");
    function_core->formatImpl(settings, state, frame);
}

ASTPtr ASTCreateInterpFunctionQuery::clone() const {
    auto res = ASTCreateFunctionQuery::clone();

    res->as<ASTCreateInterpFunctionQuery>()->function_args = function_args->clone();
    res->children.push_back(res->as<ASTCreateInterpFunctionQuery>()->function_args);
    res->as<ASTCreateInterpFunctionQuery>()->function_body = function_body->clone();
    res->children.push_back(res->as<ASTCreateInterpFunctionQuery>()->function_body);
    res->as<ASTCreateInterpFunctionQuery>()->interpreter_name = interpreter_name->clone();
    res->children.push_back(res->as<ASTCreateInterpFunctionQuery>()->interpreter_name);

    return res;
}

String ASTCreateInterpFunctionQuery::getInterpreterName() const
{
    String name;
    tryGetIdentifierNameInto(interpreter_name, name);
    return name;
}


void ASTCreateInterpFunctionQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const {
    ASTCreateFunctionQuery::formatImpl(settings, state, frame);
    settings.ostr << "(";
    function_args->formatImpl(settings, state, frame);
    settings.ostr << ") ";
    function_body->formatImpl(settings, state, frame);
    settings.ostr << (settings.hilite ? hilite_keyword : "") << " USING " << (settings.hilite ? hilite_none : "");
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(getInterpreterName()) << (settings.hilite ? hilite_none : "");
}

}
