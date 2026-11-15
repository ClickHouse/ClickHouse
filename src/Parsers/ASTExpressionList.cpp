#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFormatting.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/Operators.h>
#include <optional>


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
    ASTFormattingGroup group(ostr, state);
    if (frame.expression_list_prepend_whitespace)
        writeASTFormattingBreak(ostr, state, 4 * (frame.indent + 1));

    for (size_t i = 0, size = children.size(); i < size; ++i)
    {
        if (i)
        {
            if (char sep = getSeparator())
                ostr << sep;
            writeASTFormattingBreak(ostr, state, 4 * (frame.indent + 1));
        }

        FormatStateStacked frame_nested = frame;
        if (group)
            ++frame_nested.indent;
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
    std::optional<ASTFormattingGroup> group;
    if (children.size() == 1 && !frame.expression_list_always_start_on_new_line)
        group.emplace(ostr, state);

    if (frame.expression_list_prepend_whitespace)
    {
        if (!(children.size() > 1 || frame.expression_list_always_start_on_new_line))
            writeASTFormattingBreak(ostr, state, 4 * frame.indent);
    }

    for (size_t i = 0, size = children.size(); i < size; ++i)
    {
        if (char sep = getSeparator(); i && sep)
            ostr << sep;

        if (size > 1 || frame.expression_list_always_start_on_new_line)
            ostr << indent_str;

        FormatStateStacked frame_nested = frame;
        frame_nested.expression_list_always_start_on_new_line = false;
        frame_nested.surround_each_list_element_with_parens = false;
        frame_nested.list_element_index = i;

        if (frame.surround_each_list_element_with_parens)
            ostr << "(";

        children[i]->format(ostr, settings, state, frame_nested);

        if (frame.surround_each_list_element_with_parens)
            ostr << ")";
    }
}

}
