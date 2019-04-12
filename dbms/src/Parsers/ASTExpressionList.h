#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/** List of expressions, for example "a, b + c, f(d)"
  */
class ASTExpressionList : public IAST
{
public:
    /// Sometimes we want to print list without
    /// comma separator (in DDL dictionary queries for example)
    explicit ASTExpressionList(bool empty_separator_ = false)
    : empty_separator(empty_separator_)
    {
    }

    String getID(char) const override { return "ExpressionList"; }

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void formatImplMultiline(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const;

    bool empty_separator = false;
};

}
