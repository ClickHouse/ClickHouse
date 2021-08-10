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

    enum class Mode
    {
        INTERSECT,
        EXCEPT
    };

    using Modes = std::vector<Mode>;

    ASTPtr list_of_selects;
    Modes list_of_modes;
};

}
