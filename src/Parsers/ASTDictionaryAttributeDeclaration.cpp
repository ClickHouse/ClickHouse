#include <Parsers/ASTDictionaryAttributeDeclaration.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
ASTPtr ASTDictionaryAttributeDeclaration::clone() const
{
    const auto res = std::make_shared<ASTDictionaryAttributeDeclaration>(*this);
    res->children.clear();

    if (type)
    {
        res->type = type->clone();
        res->children.push_back(res->type);
    }

    if (default_value)
    {
        res->default_value = default_value;
        res->children.push_back(res->default_value);
    }

    if (expression)
    {
        res->expression = expression->clone();
        res->children.push_back(res->expression);
    }

    return res;
}

void ASTDictionaryAttributeDeclaration::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    settings.writeIdentifier(name, /*ambiguous=*/true);

    if (type)
    {
        settings.ostr << ' ';
        type->formatImpl(settings, state, frame);
    }

    if (default_value)
    {
        settings.ostr << ' ' << (settings.hilite ? hilite_keyword : "") << "DEFAULT" << (settings.hilite ? hilite_none : "") << ' ';
        default_value->formatImpl(settings, state, frame);
    }

    if (expression)
    {
        settings.ostr << ' ' << (settings.hilite ? hilite_keyword : "") << "EXPRESSION" << (settings.hilite ? hilite_none : "") << ' ';
        expression->formatImpl(settings, state, frame);
    }

    if (hierarchical)
        settings.ostr << ' ' << (settings.hilite ? hilite_keyword : "") << "HIERARCHICAL" << (settings.hilite ? hilite_none : "");

    if (bidirectional)
        settings.ostr << ' ' << (settings.hilite ? hilite_keyword : "") << "BIDIRECTIONAL" << (settings.hilite ? hilite_none : "");

    if (injective)
        settings.ostr << ' ' << (settings.hilite ? hilite_keyword : "") << "INJECTIVE" << (settings.hilite ? hilite_none : "");

    if (is_object_id)
        settings.ostr << ' ' << (settings.hilite ? hilite_keyword : "") << "IS_OBJECT_ID" << (settings.hilite ? hilite_none : "");
}

}
