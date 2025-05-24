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
    auto res = std::make_shared<ASTCreateModelQuery>(*this);
    res->children.clear();

    res->algorithm = algorithm->clone();
    res->children.push_back(res->algorithm);

    res->options = options->clone();
    res->children.push_back(res->options);

    res->target = target->clone();
    res->children.push_back(res->target);

    res->table_name = table_name->clone();
    res->children.push_back(res->table_name);

    return res;
}

void ASTCreateModelQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & format_settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    ostr << (format_settings.hilite ? hilite_keyword : "") << "CREATE MODEL " << (format_settings.hilite ? hilite_none : "");
    model_name->format(ostr, format_settings, state, frame);

    ostr << (format_settings.hilite ? hilite_keyword : "") << "ALGORITHM " << (format_settings.hilite ? hilite_none : "");
    algorithm->format(ostr, format_settings, state, frame);

    ostr << (format_settings.hilite ? hilite_keyword : "") << "OPTIONS" << (format_settings.hilite ? hilite_none : "");
    ostr << " (";
    options->format(ostr, format_settings, state, frame);
    ostr << " )";

    ostr << (format_settings.hilite ? hilite_keyword : "") << "TARGET " << (format_settings.hilite ? hilite_none : "");
    target->format(ostr, format_settings, state, frame);

    ostr << (format_settings.hilite ? hilite_keyword : "") << "FROM TABLE" << (format_settings.hilite ? hilite_none : "");
    table_name->format(ostr, format_settings, state, frame);
}

}
