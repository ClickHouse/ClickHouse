#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFormatting.h>
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
bool fitsWidth(std::string_view text, size_t column, size_t width)
{
    for (size_t newline = 0; newline != std::string_view::npos; text.remove_prefix(newline + 1), column = 0)
    {
        newline = text.find('\n');
        if (column + displayWidth(text.substr(0, newline), column) > width)
            return false;
    }
    return true;
}

bool fitsOnLine(const IAST::FormatState & state, std::string_view text)
{
    return fitsWidth(text, state.formatting->column(), state.formatting->width);
}

}

void ASTFormatting::update()
{
    std::string_view added = out.stringView().substr(scanned);
    scanned += added.size();
    if (size_t newline = added.rfind('\n'); newline != std::string_view::npos)
    {
        added.remove_prefix(newline + 1);
        line_column = 0;
        line_indent = 0;
        line_indent_complete = false;
    }
    if (!line_indent_complete)
    {
        size_t spaces = std::min(added.find_first_not_of(' '), added.size());
        line_indent += spaces;
        line_indent_complete = spaces < added.size();
    }
    line_column += displayWidth(added, line_column);
}

size_t ASTFormatting::column()
{
    update();
    return line_column;
}

size_t ASTFormatting::lineIndent()
{
    update();
    return line_indent / 4;
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
    const size_t width = state.formatting->width;
    if (4 * (frame.indent + 1) >= width || 4 * (state.formatting->lineIndent() + 1) >= width)
        return false;
    return !fitsOnLine(state, flat_text());
}

UInt16 wrappedIndentAST(const IAST::FormatState & state, UInt16 min_indent)
{
    return static_cast<UInt16>(std::max<size_t>(state.formatting->lineIndent(), min_indent) + 1);
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
    if (!wrap || (!packed_next_item.empty() && fitsOnLine(state, " " + String(packed_next_item))))
        out.write(' ');
    else
        writeASTNewLine(out, indent);
}

void formatASTListInParentheses(
    const IAST & list,
    WriteBuffer & out,
    const IAST::FormatSettings & settings,
    IAST::FormatState & state,
    IAST::FormatStateStacked frame)
{
    const auto * expression_list = list.as<ASTExpressionList>();
    const bool wrap = expression_list && !list.children.empty() && shouldWrapAST(out, state, frame, [&]
    {
        return formatASTWithoutWrapping([&](WriteBuffer & flat, IAST::FormatState & flat_state)
        {
            flat.write('(');
            list.format(flat, settings, flat_state, frame);
            flat.write(')');
        });
    });

    out.write('(');
    if (wrap)
    {
        frame.indent = static_cast<UInt16>(wrappedIndentAST(state) - 1);
        frame.expression_list_always_start_on_new_line = true;
        expression_list->formatImplMultiline(out, settings, state, frame);
        writeASTNewLine(out, frame.indent);
    }
    else
        list.format(out, settings, state, frame);
    out.write(')');
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
