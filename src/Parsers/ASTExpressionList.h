#pragma once

#include <Parsers/IAST.h>
#include <Parsers/Lexer.h>


namespace DB
{

/** List of expressions, for example "a, b + c, f(d)"
  */
class ASTExpressionList : public IAST
{
    struct ASTExpressionListFlags
    {
        using ParentFlags = void;
        static constexpr UInt32 RESERVED_BITS = 8;

        char separator;
        UInt8 unused[3];
    };

public:
    explicit ASTExpressionList(char separator_ = ',') { setSeparator(separator_); }
    String getID(char) const override { return "ExpressionList"; }

    ASTPtr clone() const override;
    void formatImplMultiline(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const;

    char getSeparator() const { return flags<ASTExpressionListFlags>().separator; }
    void setSeparator(char value) { flags<ASTExpressionListFlags>().separator = value; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
