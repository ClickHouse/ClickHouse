#include <Parsers/ASTExpressionList.h>


namespace DB
{

ASTPtr ASTExpressionList::clone() const
{
    auto clone = std::make_shared<ASTExpressionList>(*this);
    clone->cloneChildren();
    return clone;
}

void ASTExpressionList::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
            settings.ostr << ", ";

        (*it)->formatImpl(settings, state, frame);
    }
}

void ASTExpressionList::formatImplMultiline(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = "\n" + std::string(4 * (frame.indent + 1), ' ');

    ++frame.indent;
    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
            settings.ostr << ", ";

        if (children.size() > 1)
            settings.ostr << indent_str;

        (*it)->formatImpl(settings, state, frame);
    }
}

}
