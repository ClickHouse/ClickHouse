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
    void formatImpl(const FormattingBuffer & out) const override;
    void formatImplMultiline(const FormattingBuffer & out) const;

    char separator;
};

}
