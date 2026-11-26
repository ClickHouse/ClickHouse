#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFormatting.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Common/UTF8Helpers.h>


namespace DB
{

namespace
{

size_t displayWidth(std::string_view text, size_t prefix = 0)
{
    return UTF8::computeWidth(reinterpret_cast<const UInt8 *>(text.data()), text.size(), prefix);
}

/// Whether every line of `text` fits into `width`, if the first line starts at `column`.
/// Lines indented by more than `max_indent` spaces are not checked.
bool fitsWidth(std::string_view text, size_t column, size_t width, size_t max_indent = std::string_view::npos)
{
    while (true)
    {
        const size_t newline = text.find('\n');
        const std::string_view line = text.substr(0, newline);
        if (line.find_first_not_of(' ') <= max_indent && column + displayWidth(line, column) > width)
            return false;
        if (newline == std::string_view::npos)
            return true;
        text.remove_prefix(newline + 1);
        column = 0;
    }
}

/// Lines indented deeper than `indent` levels are not checked (see ASTFormatting.h).
bool fitsOnLine(const IAST::FormatState & state, std::string_view text, size_t indent)
{
    return fitsWidth(text, state.formatting->column(), state.formatting->width, 4 * indent);
}

}

size_t ASTFormatting::column()
{
    std::string_view added = out.stringView().substr(scanned);
    scanned += added.size();
    if (size_t newline = added.rfind('\n'); newline != std::string_view::npos)
    {
        added.remove_prefix(newline + 1);
        line_column = 0;
    }
    line_column += displayWidth(added, line_column);
    return line_column;
}

bool shouldWrapAST(
    const WriteBuffer & out,
    const IAST::FormatState & state,
    const IAST::FormatStateStacked & frame,
    const std::function<String()> & flat_text)
{
    if (!state.formatting || static_cast<const WriteBuffer *>(&state.formatting->out) != &out)
        return false;
    /// When the indentation alone takes the whole width, wrapping does not help
    /// and would only make the output grow quadratically with the nesting depth.
    if (4 * (frame.indent + 1) >= state.formatting->width)
        return false;
    return !fitsOnLine(state, flat_text(), frame.indent);
}

bool arePlainValuesAST(const ASTs & elements)
{
    return std::ranges::all_of(elements, [](const ASTPtr & element)
    {
        return (element->as<ASTLiteral>() || element->as<ASTIdentifier>()) && element->tryGetAlias().empty();
    });
}

String formatASTWithoutWrapping(const std::function<void(WriteBuffer &, IAST::FormatState &)> & format)
{
    WriteBufferFromOwnString out;
    IAST::FormatState state;
    format(out, state);
    return std::move(out.str());
}

void writeASTNewLine(WriteBuffer & out, size_t indent)
{
    out.write('\n');
    for (size_t i = 0; i < 4 * indent; ++i)
        out.write(' ');
}

void writeASTListSpace(WriteBuffer & out, const IAST::FormatState & state, size_t indent, bool wrap, std::string_view packed_next_item)
{
    if (!wrap || (!packed_next_item.empty() && fitsOnLine(state, " " + String(packed_next_item), indent)))
        out.write(' ');
    else
        writeASTNewLine(out, indent);
}

void formatASTList(
    const IAST & list,
    std::string_view open,
    char close,
    bool wrap,
    WriteBuffer & out,
    const IAST::FormatSettings & settings,
    IAST::FormatState & state,
    IAST::FormatStateStacked frame)
{
    const ASTs & elements = list.children;
    const bool pack = wrap && arePlainValuesAST(elements);
    if (wrap)
        ++frame.indent;
    out.write(open.data(), open.size());
    if (wrap)
        writeASTNewLine(out, frame.indent);
    for (size_t i = 0; i < elements.size(); ++i)
    {
        frame.list_element_index = i;
        if (i != 0)
        {
            out.write(',');
            writeASTListSpace(out, state, frame.indent, wrap, pack ? formatASTWithoutWrapping([&](WriteBuffer & flat, IAST::FormatState & flat_state)
            {
                elements[i]->format(flat, settings, flat_state, frame);
                if (i + 1 < elements.size())
                    flat.write(',');
            }) : "");
        }
        if (elements[i]->as<ASTSetQuery>())
            out.write("SETTINGS ", 9);
        elements[i]->format(out, settings, state, frame);
    }
    if (wrap)
        writeASTNewLine(out, frame.indent - 1);
    out.write(close);
}

void formatASTListInParentheses(
    const IAST & list,
    WriteBuffer & out,
    const IAST::FormatSettings & settings,
    IAST::FormatState & state,
    IAST::FormatStateStacked frame)
{
    const bool wrap = list.as<ASTExpressionList>() && !list.children.empty() && shouldWrapAST(out, state, frame, [&]
    {
        return formatASTWithoutWrapping([&](WriteBuffer & flat, IAST::FormatState & flat_state)
        {
            flat.write('(');
            list.format(flat, settings, flat_state, frame);
            flat.write(')');
        });
    });

    if (wrap)
        formatASTList(list, "(", ')', /*wrap=*/ true, out, settings, state, frame);
    else
    {
        out.write('(');
        list.format(out, settings, state, frame);
        out.write(')');
    }
}

String formatASTWithWrapping(const IAST & ast, const IAST::FormatSettings & settings, size_t wrap_width)
{
    WriteBufferFromOwnString out;
    ast.format(out, settings);
    if (!wrap_width || settings.one_line || fitsWidth(out.stringView(), 0, wrap_width))
        return std::move(out.str());

    WriteBufferFromOwnString wrapped;
    ASTFormatting formatting(wrapped, wrap_width);
    IAST::FormatState state;
    state.formatting = &formatting;
    ast.format(wrapped, settings, state, {});
    return std::move(wrapped.str());
}

}
