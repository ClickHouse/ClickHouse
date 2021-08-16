#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

class ASTSelectIntersectExceptQuery : public ASTQueryWithOutput
{
public:
    String getID(char) const override { return "SelectIntersectExceptQuery"; }

    ASTPtr clone() const override;

    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    const char * getQueryKindString() const override { return "SelectIntersectExcept"; }

    enum class Operator
    {
        UNKNOWN,
        INTERSECT,
        EXCEPT
    };

    /// Final operator after applying visitor.
    Operator final_operator = Operator::UNKNOWN;
};

}
