#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

class ASTIntersectOrExcept : public ASTQueryWithOutput
{
public:
    String getID(char) const override { return is_except ? "Except" : "Intersect"; }
    ASTPtr clone() const override;
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    bool is_except;
};

}
