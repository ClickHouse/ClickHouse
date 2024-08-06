#pragma once

#include <Parsers/ASTSelectQuery.h>
#include "Parsers/ExpressionListParsers.h"


namespace DB
{

class ASTSelectIntersectExceptQuery : public ASTSelectQuery
{
public:
    String getID(char) const override { return "SelectIntersectExceptQuery"; }

    ASTPtr clone() const override;

    enum class Operator : uint8_t
    {
        UNKNOWN,
        EXCEPT_ALL,
        EXCEPT_DISTINCT,
        INTERSECT_ALL,
        INTERSECT_DISTINCT,
    };

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    QueryKind getQueryKind() const override { return QueryKind::Select; }

    ASTs getListOfSelects() const;

    static const char * fromOperator(Operator op);

    /// Final operator after applying visitor.
    Operator final_operator = Operator::UNKNOWN;
};

}
