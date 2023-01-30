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

    settings.ostr << backQuote(name);

    if (type)
    {
        settings.ostr << ' ';
        type->formatImpl(settings, state, frame);
    }

    if (default_value)
    {
        settings.writeKeyword(" DEFAULT ");
        default_value->formatImpl(settings, state, frame);
    }

    if (expression)
    {
        settings.writeKeyword(" EXPRESSION ");
        expression->formatImpl(settings, state, frame);
    }

    if (hierarchical)
        settings.writeKeyword(" HIERARCHICAL");

    if (bidirectional)
        settings.writeKeyword(" BIDIRECTIONAL");

    if (injective)
        settings.writeKeyword(" INJECTIVE");

    if (is_object_id)
        settings.writeKeyword(" IS_OBJECT_ID");
}

}
