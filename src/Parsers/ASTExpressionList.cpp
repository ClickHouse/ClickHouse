#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFormatting.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTExpressionList::clone() const
{
    auto clone = make_intrusive<ASTExpressionList>(*this);
    clone->cloneChildren();
    return clone;
}

void ASTExpressionList::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ExpressionList");
    if (getSeparator() != ',')
        w.writeString("separator", String(1, getSeparator()));
    w.writeChildren(children);
}

void ASTExpressionList::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    String sep = r.getString("separator");
    if (!sep.empty())
        setSeparator(sep[0]);
    children = r.readChildren();
}

void ASTExpressionList::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (frame.expression_list_prepend_whitespace)
        ostr << ' ';

    for (size_t i = 0, size = children.size(); i < size; ++i)
    {
        if (i)
        {
            if (char sep = getSeparator())
                ostr << sep;
            ostr << ' ';
        }

        FormatStateStacked frame_nested = frame;
        frame_nested.surround_each_list_element_with_parens = false;
        frame_nested.list_element_index = i;

        if (frame.surround_each_list_element_with_parens)
            ostr << "(";

        children[i]->format(ostr, settings, state, frame_nested);

        if (frame.surround_each_list_element_with_parens)
            ostr << ")";
    }
}

void ASTExpressionList::formatImplMultiline(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ++frame.indent;
    std::string indent_str = "\n" + std::string(4 * frame.indent, ' ');

    const auto format_element = [&](WriteBuffer & out, FormatState & out_state, size_t i)
    {
        FormatStateStacked frame_nested = frame;
        frame_nested.expression_list_always_start_on_new_line = false;
        frame_nested.surround_each_list_element_with_parens = false;
        frame_nested.list_element_index = i;

        if (frame.surround_each_list_element_with_parens)
            out << "(";

        children[i]->format(out, settings, out_state, frame_nested);

        if (frame.surround_each_list_element_with_parens)
            out << ")";
    };

    bool start_on_new_line = children.size() > 1 || frame.expression_list_always_start_on_new_line;
    if (frame.expression_list_prepend_whitespace && !start_on_new_line)
    {
        /// A single element which does not fit after the keyword starts on a new line, as several elements do.
        start_on_new_line = children.size() == 1 && shouldWrapAST(ostr, state, frame, [&]
        {
            return " " + formatASTWithoutWrapping([&](WriteBuffer & out, FormatState & flat_state) { format_element(out, flat_state, 0); });
        });
        if (!start_on_new_line)
            ostr << ' ';
    }

    for (size_t i = 0, size = children.size(); i < size; ++i)
    {
        if (char sep = getSeparator(); i && sep)
            ostr << sep;

        if (start_on_new_line)
            ostr << indent_str;

        format_element(ostr, state, i);
    }
}

}
