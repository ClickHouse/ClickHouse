#pragma once

#include <IO/WriteBuffer.h>
#include <Parsers/IAST.h>


namespace DB
{

class ASTFormattingGroup
{
public:
    enum class Mode : uint8_t
    {
        PreserveLines,
        ConsistentLines,
    };

    ASTFormattingGroup(
        WriteBuffer & out, IASTFormatState & state, const IAST::FormatStateStacked & frame, Mode mode = Mode::PreserveLines)
    {
        if (state.formatting)
            start(out, state, frame, mode);
    }

    ~ASTFormattingGroup()
    {
        finish();
    }
    ASTFormattingGroup(const ASTFormattingGroup &) = delete;
    ASTFormattingGroup & operator=(const ASTFormattingGroup &) = delete;

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

namespace ASTFormattingDetail
{
void writeBreak(WriteBuffer & out, IASTFormatState & state, size_t indent, size_t nest, char flat, bool fill);
void writeIndent(WriteBuffer & out, IASTFormatState & state, size_t indent);
}

/// Nesting is relative to the rendered indentation of the line where the containing group opened.
inline void writeASTFormattingBreak(
    WriteBuffer & out, IASTFormatState & state, const IAST::FormatStateStacked & frame, size_t nest = 1, char flat = ' ')
{
    if (state.formatting)
        ASTFormattingDetail::writeBreak(out, state, frame.indent, nest, flat, false);
    else if (flat)
        out.write(flat);
}

/// Fill breaks fit the next fragment independently, even when their containing group is broken.
inline void writeASTFormattingFillBreak(
    WriteBuffer & out, IASTFormatState & state, const IAST::FormatStateStacked & frame, size_t nest = 1, char flat = ' ')
{
    if (state.formatting)
        ASTFormattingDetail::writeBreak(out, state, frame.indent, nest, flat, true);
    else if (flat)
        out.write(flat);
}

inline void writeASTFormattingOptionalBreak(
    WriteBuffer & out, IASTFormatState & state, const IAST::FormatStateStacked & frame, size_t nest = 1, char flat = ' ')
{
    writeASTFormattingFillBreak(out, state, frame, nest, flat);
}

inline void writeASTFormattingIndent(WriteBuffer & out, IASTFormatState & state, size_t indent)
{
    if (state.formatting)
        ASTFormattingDetail::writeIndent(out, state, indent);
    else
        for (size_t i = 0; i < 4 * indent; ++i)
            out.write(' ');
}

inline void writeASTFormattingIndent(WriteBuffer & out, IASTFormatState & state, const IAST::FormatStateStacked & frame)
{
    writeASTFormattingIndent(out, state, frame.indent);
}

struct ASTFormattingIndent
{
    ASTFormattingIndent(IASTFormatState & state_, const IAST::FormatStateStacked & frame, bool enabled_)
        : state(state_), indent(frame.indent), enabled(enabled_)
    {
    }

    IASTFormatState & state;
    size_t indent;
    bool enabled;
};

inline void writeText(const ASTFormattingIndent & indent, WriteBuffer & out)
{
    if (indent.enabled)
        writeASTFormattingIndent(out, indent.state, indent.indent);
}

String formatASTWithLineLength(const IAST & ast, const IAST::FormatSettings & settings, size_t line_length);

}
