#include <Parsers/ASTCreateTypeQuery.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Common/quoteString.h>

namespace DB
{

String ASTCreateTypeQuery::getID(char delim) const
{
    return "CreateTypeQuery" + (delim + name);
}

ASTPtr ASTCreateTypeQuery::clone() const
{
    auto res = std::make_shared<ASTCreateTypeQuery>();
    res->name = name;
    res->if_not_exists = if_not_exists;
    res->or_replace = or_replace;

    if (base_type)
    {
        res->base_type = base_type->clone();
        res->children.push_back(res->base_type);
    }
    if (type_parameters)
    {
        res->type_parameters = type_parameters->clone();
        res->children.push_back(res->type_parameters);
    }
    if (input_expression)
    {
        res->input_expression = input_expression->clone();
        res->children.push_back(res->input_expression);
    }
    if (output_expression)
    {
        res->output_expression = output_expression->clone();
        res->children.push_back(res->output_expression);
    }
    if (default_expression)
    {
        res->default_expression = default_expression->clone();
        res->children.push_back(res->default_expression);
    }
    return res;
}

void ASTCreateTypeQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << (settings.hilite ? hilite_keyword : "") << "CREATE TYPE " << (settings.hilite ? hilite_none : "");

    if (if_not_exists)
        ostr << (settings.hilite ? hilite_keyword : "") << "IF NOT EXISTS " << (settings.hilite ? hilite_none : "");
    else if (or_replace)
        ostr << (settings.hilite ? hilite_keyword : "") << "OR REPLACE " << (settings.hilite ? hilite_none : "");

    ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(name) << (settings.hilite ? hilite_none : "");

    if (type_parameters)
    {
        ostr << "(";
        type_parameters->format(ostr, settings, state, frame);
        ostr << ")";
    }

    ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "");

    if (base_type)
        base_type->format(ostr, settings, state, frame);

    if (input_expression)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << " INPUT " << (settings.hilite ? hilite_none : "");
        input_expression->format(ostr, settings, state, frame);
    }

    if (output_expression)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << " OUTPUT " << (settings.hilite ? hilite_none : "");
        output_expression->format(ostr, settings, state, frame);
    }

    if (default_expression)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << " DEFAULT " << (settings.hilite ? hilite_none : "");
        default_expression->format(ostr, settings, state, frame);
    }
}

}
