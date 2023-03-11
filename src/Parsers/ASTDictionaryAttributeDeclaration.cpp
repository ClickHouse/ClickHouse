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

void ASTDictionaryAttributeDeclaration::formatImpl(const FormattingBuffer & out) const
{
    out.setNeedsParens(false);

    out.ostr << backQuote(name);

    if (type)
    {
        out.ostr << ' ';
        type->formatImpl(out);
    }

    if (default_value)
    {
        out.writeKeyword(" DEFAULT ");
        default_value->formatImpl(out);
    }

    if (expression)
    {
        out.writeKeyword(" EXPRESSION ");
        expression->formatImpl(out);
    }

    if (hierarchical)
        out.writeKeyword(" HIERARCHICAL");

    if (bidirectional)
        out.writeKeyword(" BIDIRECTIONAL");

    if (injective)
        out.writeKeyword(" INJECTIVE");

    if (is_object_id)
        out.writeKeyword(" IS_OBJECT_ID");
}

}
