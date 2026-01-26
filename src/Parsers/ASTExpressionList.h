#pragma once

#include <Parsers/IAST.h>
#include <Parsers/Lexer.h>


namespace DB
{

/** List of expressions, for example "a, b + c, f(d)"
  */
class ASTExpressionList : public IAST
{
public:
    explicit ASTExpressionList(char separator_ = ',') { setSeparator(separator_); }
    String getID(char) const override { return "ExpressionList"; }

    ASTPtr clone() const override;
    void formatImplMultiline(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const;

    char getSeparator() const { return static_cast<char>(FLAGS & SEPARATOR_MASK); }
    void setSeparator(char value) { FLAGS = (FLAGS & ~SEPARATOR_MASK) | static_cast<UInt32>(static_cast<unsigned char>(value)); }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

private:
    static constexpr UInt32 SEPARATOR_MASK = 0xFFu;  /// 8 bits for separator char
};

}
