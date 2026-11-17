#pragma once

#include <IO/WriteBuffer.h>
#include <Parsers/IAST.h>


namespace DB
{

/// Line wrapping for `clickhouse-format --wrap_width`.
///
/// While writing the usual output, formatting code marks groups and the places where a line may be broken.
/// If the output does not fit the width, the marks are used to lay the text out again.
/// When no layout is collected (the default), groups do nothing and breaks are written as their flat text.

class ASTFormattingGroup
{
public:
    enum class Mode : uint8_t
    {
        /// Lines that already exist in the group are kept, the group breaks only if its first line does not fit.
        PreserveLines,
        /// The group also breaks if any of its lines does not fit (used for function arguments).
        ConsistentLines,
        /// Like PreserveLines, but if the group starts on its own line, the items are aligned with the first one.
        List,
    };

    ASTFormattingGroup(
        WriteBuffer & out, IASTFormatState & state, const IAST::FormatStateStacked & frame, Mode mode = Mode::PreserveLines)
    {
        if (state.formatting)
            start(out, state, frame, mode);
    }

    ~ASTFormattingGroup() { finish(); }

    ASTFormattingGroup(const ASTFormattingGroup &) = delete;
    ASTFormattingGroup & operator=(const ASTFormattingGroup &) = delete;

    /// Can be changed while the group is open, e.g. when it becomes known that the node is a function call.
    void setMode(Mode mode);

    void finish() noexcept
    {
        if (formatting)
            close();
    }

private:
    void start(WriteBuffer & out, IASTFormatState & state, const IAST::FormatStateStacked & frame, Mode mode);
    void close() noexcept;

    ASTFormatting * formatting = nullptr;
    size_t index = 0;
};

enum class ASTFormattingBreak : uint8_t
{
    /// Breaks if the enclosing group does not fit.
    Consistent,
    /// Breaks if the next item of the enclosing list does not fit on the current line. Used to pack literals.
    Fill,
    /// Breaks if the text up to the next possible break does not fit on the current line.
    Optional,
};

namespace ASTFormattingDetail
{
void writeBreak(WriteBuffer & out, IASTFormatState & state, size_t indent, size_t nest, char flat, ASTFormattingBreak kind);
void writeIndent(WriteBuffer & out, IASTFormatState & state, size_t indent);
}

/// A possible line break that is written as `flat` (nothing if it is '\0') when the line is not broken.
/// After a break, the line is indented by `nest` levels relative to the line where the enclosing group starts.
inline void writeASTFormattingBreak(
    WriteBuffer & out,
    IASTFormatState & state,
    const IAST::FormatStateStacked & frame,
    size_t nest = 1,
    char flat = ' ',
    ASTFormattingBreak kind = ASTFormattingBreak::Consistent)
{
    if (state.formatting)
        ASTFormattingDetail::writeBreak(out, state, frame.indent, nest, flat, kind);
    else if (flat)
        out.write(flat);
}

/// Indentation of a line that the formatting code starts itself.
/// It is reported to the layout, so it can be shifted together with the group it belongs to.
inline void writeASTFormattingIndent(WriteBuffer & out, const IAST::FormatSettings & settings, IASTFormatState & state, size_t indent)
{
    if (settings.one_line)
        return;
    if (state.formatting)
        ASTFormattingDetail::writeIndent(out, state, indent);
    else
        for (size_t i = 0; i < 4 * indent; ++i)
            out.write(' ');
}

/// Formats the query. If `wrap_width` is not zero and some line is wider, long expressions are wrapped.
String formatASTWithWrapping(const IAST & ast, const IAST::FormatSettings & settings, size_t wrap_width);

}
