#pragma once

#include <Parsers/ASTSelectQuery.h>


namespace DB
{

class ASTSelectIntersectExceptQuery : public ASTSelectQuery
{
public:
    String getID(char) const override { return "SelectIntersectExceptQuery"; }

    ASTPtr clone() const override;

    enum class Operator
    {
        UNKNOWN,
        INTERSECT,
        EXCEPT
    };

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    QueryKind getQueryKind() const override { return QueryKind::SelectIntersectExcept; }

    ASTs getListOfSelects() const;

    /// Final operator after applying visitor.
    Operator final_operator = Operator::UNKNOWN;
};

}
