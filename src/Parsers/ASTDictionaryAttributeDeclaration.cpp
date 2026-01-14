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

void ASTDictionaryAttributeDeclaration::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    settings.writeIdentifier(ostr, name, /*ambiguous=*/true);

    if (type)
    {
        ostr << ' ';
        type->format(ostr, settings, state, frame);
    }

    if (default_value)
    {
        ostr << ' ' << "DEFAULT" << ' ';
        default_value->format(ostr, settings, state, frame);
    }

    if (expression)
    {
        ostr << ' ' << "EXPRESSION" << ' ';
        expression->format(ostr, settings, state, frame);
    }

    if (hierarchical)
        ostr << ' ' << "HIERARCHICAL";

    if (bidirectional)
        ostr << ' ' << "BIDIRECTIONAL";

    if (injective)
        ostr << ' ' << "INJECTIVE";

    if (is_object_id)
        ostr << ' ' << "IS_OBJECT_ID";
}

}
