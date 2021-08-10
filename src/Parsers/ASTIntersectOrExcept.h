#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

class ASTIntersectOrExcept : public ASTQueryWithOutput
{
public:
    String getID(char) const override { return "IntersectExceptQuery"; }

    ASTPtr clone() const override;

    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    enum class Operator
    {
        INTERSECT,
        EXCEPT
    };

    using Operators = std::vector<Operator>;

    ASTPtr list_of_selects;
    Operators list_of_operators;
};

}
