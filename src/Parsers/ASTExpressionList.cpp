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

void ASTExpressionList::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (frame.expression_list_prepend_whitespace)
        ostr << ' ';

    for (size_t i = 0, size = children.size(); i < size; ++i)
    {
        if (i)
        {
            if (separator)
                ostr << separator;
            ostr << ' ';
        }

        FormatStateStacked frame_nested = frame;
        frame_nested.surround_each_list_element_with_parens = false;
        frame_nested.list_element_index = i;

        if (frame.surround_each_list_element_with_parens)
            ostr << "(";

        children[i]->formatImpl(ostr, settings, state, frame_nested);

        if (frame.surround_each_list_element_with_parens)
            ostr << ")";
    }
}

void ASTExpressionList::formatImplMultiline(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ++frame.indent;
    std::string indent_str = "\n" + std::string(4 * frame.indent, ' ');

    if (frame.expression_list_prepend_whitespace)
    {
        if (!(children.size() > 1 || frame.expression_list_always_start_on_new_line))
            ostr << ' ';
    }

    for (size_t i = 0, size = children.size(); i < size; ++i)
    {
        if (i && separator)
            ostr << separator;

        if (size > 1 || frame.expression_list_always_start_on_new_line)
            ostr << indent_str;

        FormatStateStacked frame_nested = frame;
        frame_nested.expression_list_always_start_on_new_line = false;
        frame_nested.surround_each_list_element_with_parens = false;
        frame_nested.list_element_index = i;

        if (frame.surround_each_list_element_with_parens)
            ostr << "(";

        children[i]->formatImpl(ostr, settings, state, frame_nested);

        if (frame.surround_each_list_element_with_parens)
            ostr << ")";
    }
}

}
