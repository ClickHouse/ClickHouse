#pragma once

#include <Parsers/IAST.h>

#include <functional>


namespace DB
{

class ASTExpressionList;
class WriteBufferFromOwnString;

/// Line wrapping for `clickhouse-format --wrap_width`.
///
/// A construct that can be wrapped (function arguments and parameters, an array, tuple or map, an AND/OR chain,
/// a window definition, the columns of INSERT) is first formatted in one line into a temporary buffer.
/// If this text does not fit on the current line, the construct starts its items on new lines
/// and formats them one level deeper than the construct itself.
/// The decision is made before the construct is written, so nested multiline parts (like subqueries)
/// get the right indentation by themselves. The lines of the text indented deeper than the construct are not checked:
/// they belong to nested subqueries, which wrapping cannot make shorter.
/// Text after the construct (like a closing parenthesis) is not taken into account, so the width is a soft limit.
///
/// Wrapping is active only inside `formatASTWithWrapping`, so other formatting (e.g. `formatQuery`) is not affected.
class ASTFormatting
{
public:
    ASTFormatting(const WriteBufferFromOwnString & out_, size_t width_) : out(out_), width(width_) {}

    const WriteBufferFromOwnString & out;
    const size_t width;

    /// Display width of the current line of `out`.
    /// Only the text written since the previous call is scanned, so the total cost is linear in the output size.
    size_t column();

private:
    size_t scanned = 0;
    size_t line_column = 0;
};

/// Whether a construct written at the current position of `out` has to be wrapped,
/// because its text without wrapping (returned by `flat_text`, called only if wrapping is possible) does not fit.
bool shouldWrapAST(
    const WriteBuffer & out,
    const IAST::FormatState & state,
    const IAST::FormatStateStacked & frame,
    const std::function<String()> & flat_text);

/// Whether all `elements` are plain values (literals and identifiers), which are packed several per line when wrapped.
bool arePlainValuesAST(const ASTs & elements);

/// Text written by `format` without wrapping.
String formatASTWithoutWrapping(const std::function<void(WriteBuffer &, IAST::FormatState &)> & format);

/// Starts a new line indented by `indent` levels.
void writeASTNewLine(WriteBuffer & out, size_t indent);

/// Writes the space after the separator of list items. If the list is wrapped, the next item starts a new line instead,
/// unless the list is packed (then `packed_next_item` is the next item with its separator) and it fits on the current line.
void writeASTListSpace(
    WriteBuffer & out, const IAST::FormatState & state, size_t indent, bool wrap, std::string_view packed_next_item = {});

/// Writes the elements of `list` between `open` and `close`. If `wrap`, the elements start on new lines
/// one level deeper than `frame`, and the closing bracket is on its own line.
void formatASTList(
    const IAST & list,
    std::string_view open,
    char close,
    bool wrap,
    WriteBuffer & out,
    const IAST::FormatSettings & settings,
    IAST::FormatState & state,
    IAST::FormatStateStacked frame);

/// Writes `(list)`. If `list` is an expression list that does not fit on the current line, it is wrapped (see `formatASTList`).
void formatASTListInParentheses(
    const IAST & list,
    WriteBuffer & out,
    const IAST::FormatSettings & settings,
    IAST::FormatState & state,
    IAST::FormatStateStacked frame);

/// Formats the query and wraps lines longer than `wrap_width` (0 disables wrapping).
String formatASTWithWrapping(const IAST & ast, const IAST::FormatSettings & settings, size_t wrap_width);

}
