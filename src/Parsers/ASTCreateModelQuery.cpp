#include <Parsers/ASTCreateModelQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

String ASTCreateModelQuery::getID(char delim) const
{
    return "CreateModelQuery" + (delim + model_name->as<ASTIdentifier>()->name());
}

ASTPtr ASTCreateModelQuery::clone() const
{
    auto res = make_intrusive<ASTCreateModelQuery>(*this);
    res->children.clear();

    res->algorithm = algorithm->clone();
    res->children.push_back(res->algorithm);

    if (options)
    {
        res->options = options->clone();
        res->children.push_back(res->options);
    }

    res->target = target->clone();
    res->children.push_back(res->target);

    res->table_name = table_name->clone();
    res->children.push_back(res->table_name);

    return res;
}

void ASTCreateModelQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & format_settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    ostr << "CREATE MODEL ";
    model_name->format(ostr, format_settings, state, frame);

    ostr << " ALGORITHM ";
    algorithm->format(ostr, format_settings, state, frame);

    if (options)
    {
        ostr << " OPTIONS (";
        options->format(ostr, format_settings, state, frame);
        ostr << ")";
    }

    ostr << " TARGET ";
    target->format(ostr, format_settings, state, frame);

    ostr << " FROM TABLE ";
    table_name->format(ostr, format_settings, state, frame);
}

}
