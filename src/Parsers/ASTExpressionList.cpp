#include <Parsers/ASTExpressionList.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTExpressionList::clone() const
{
    auto clone = std::make_shared<ASTExpressionList>(*this);
    clone->cloneChildren();
    return clone;
}

void ASTExpressionList::formatImpl(FormattingBuffer out) const
{
    if (out.getExpressionListPrependWhitespace())
        out.ostr << ' ';

    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
        {
            if (separator)
                out.ostr << separator;
            out.ostr << ' ';
        }

        if (out.getSurroundEachListElementWithParens())
            out.ostr << "(";

        (*it)->formatImpl(out.copy().setSurroundEachListElementWithParens(false));

        if (out.getSurroundEachListElementWithParens())
            out.ostr << ")";
    }
}

void ASTExpressionList::formatImplMultiline(FormattingBuffer out) const
{
    if (out.getExpressionListPrependWhitespace())
    {
        if (!(children.size() > 1 || out.getExpressionListAlwaysStartsOnNewLine()))
            out.ostr << ' ';
    }

    out.increaseIndent();

    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
        {
            if (separator)
                out.ostr << separator;
        }

        if (children.size() > 1 || out.getExpressionListAlwaysStartsOnNewLine())
            out.writeIndent();

        if (out.getSurroundEachListElementWithParens())
            out.ostr << "(";

        (*it)->formatImpl(out.copy()
                .setExpressionListAlwaysStartsOnNewLine(false)
                .setSurroundEachListElementWithParens(false));

        if (out.getSurroundEachListElementWithParens())
            out.ostr << ")";
    }
}

}
