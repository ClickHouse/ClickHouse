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

void ASTExpressionList::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (frame.expression_list_prepend_whitespace)
        settings.ostr << ' ';

    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
        {
            if (separator)
                settings.ostr << separator;
            settings.ostr << ' ';
        }

        if (frame.surround_each_list_element_with_parens)
            settings.ostr << "(";

        FormatStateStacked frame_nested = frame;
        frame_nested.surround_each_list_element_with_parens = false;
        (*it)->formatImpl(settings, state, frame_nested);

        if (frame.surround_each_list_element_with_parens)
            settings.ostr << ")";
    }
}

void ASTExpressionList::formatImplMultiline(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = "\n" + std::string(4 * (frame.indent + 1), ' ');

    if (frame.expression_list_prepend_whitespace)
    {
        if (!(children.size() > 1 || frame.expression_list_always_start_on_new_line))
            settings.ostr << ' ';
    }

    ++frame.indent;

    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
        {
            if (separator)
                settings.ostr << separator;
        }

        if (children.size() > 1 || frame.expression_list_always_start_on_new_line)
            settings.ostr << indent_str;

        FormatStateStacked frame_nested = frame;
        frame_nested.expression_list_always_start_on_new_line = false;
        frame_nested.surround_each_list_element_with_parens = false;

        if (frame.surround_each_list_element_with_parens)
            settings.ostr << "(";

        (*it)->formatImpl(settings, state, frame_nested);

        if (frame.surround_each_list_element_with_parens)
            settings.ostr << ")";
    }
}

}
