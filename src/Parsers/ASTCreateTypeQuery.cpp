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
    ostr << "CREATE TYPE ";

    if (if_not_exists)
        ostr << "IF NOT EXISTS ";
    else if (or_replace)
        ostr << "OR REPLACE ";

    ostr << backQuoteIfNeed(name);

    if (type_parameters)
    {
        ostr << "(";
        type_parameters->format(ostr, settings, state, frame);
        ostr << ")";
    }

    ostr << " AS ";

    if (base_type)
        base_type->format(ostr, settings, state, frame);

    if (input_expression)
    {
        ostr << " INPUT ";
        input_expression->format(ostr, settings, state, frame);
    }

    if (output_expression)
    {
        ostr << " OUTPUT ";
        output_expression->format(ostr, settings, state, frame);
    }

    if (default_expression)
    {
        ostr << " DEFAULT ";
        default_expression->format(ostr, settings, state, frame);
    }
}

}
