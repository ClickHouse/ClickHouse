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
    explicit ASTExpressionList(char separator_ = ',') : separator(separator_) {}
    String getID(char) const override { return "ExpressionList"; }

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void formatImplMultiline(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const;

    char separator;
};

}
