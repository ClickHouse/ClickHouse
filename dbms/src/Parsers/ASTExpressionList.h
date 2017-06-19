#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/** List of expressions, for example "a, b + c, f(d)"
  */
class ASTExpressionList : public IAST
{
public:
    ASTExpressionList() = default;
    ASTExpressionList(const StringRange range_) : IAST(range_) {}

    String getID() const override { return "ExpressionList"; }

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void formatImplMultiline(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const;
};

}
