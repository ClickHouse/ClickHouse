#include <Parsers/ASTPredictQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

String ASTPredictQuery::getID(char delim) const
{
    return "Predict" + (delim + model_name->as<ASTIdentifier>()->name() + delim + table_name->as<ASTIdentifier>()->name());
}

ASTPtr ASTPredictQuery::clone() const
{
    auto res = make_intrusive<ASTPredictQuery>(*this);
    res->children.clear();

    res->model_name = model_name->clone();
    res->children.push_back(res->model_name);

    res->table_name = table_name->clone();
    res->children.push_back(res->table_name);

    return res;
}

void ASTPredictQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & format_settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    ostr << "PREDICT ";

    ostr << "(";

    ostr << "MODEL ";
    model_name->format(ostr, format_settings, state, frame);

    ostr << ", ";

    ostr << "TABLE ";
    table_name->format(ostr, format_settings, state, frame);

    ostr << ")";
}

}
