#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class ASTFormattingGroup
{
public:
    ASTFormattingGroup(WriteBuffer & out, IASTFormatState & state);
    ~ASTFormattingGroup();

    ASTFormattingGroup(const ASTFormattingGroup &) = delete;
    ASTFormattingGroup & operator=(const ASTFormattingGroup &) = delete;

    void finish() noexcept;
    explicit operator bool() const
    {
        return formatting != nullptr;
    }

private:
    ASTFormatting * formatting = nullptr;
    size_t index = 0;
};

void writeASTFormattingBreak(WriteBuffer & out, IASTFormatState & state, size_t indent, char flat = ' ');
void writeASTFormattingOptionalBreak(WriteBuffer & out, IASTFormatState & state, size_t indent, char flat = ' ');

String formatASTWithLineLength(const IAST & ast, const IAST::FormatSettings & settings, size_t line_length);

}
