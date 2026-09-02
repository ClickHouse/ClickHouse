#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>

namespace Poco::JSON { class Object; }

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

    QueryKind getQueryKind() const override { return QueryKind::Select; }

    ASTs getListOfSelects() const;

    static const char * fromOperator(Operator op);

    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    /// Final operator after applying visitor.
    Operator final_operator = Operator::UNKNOWN;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
