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

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

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

    /// A trailing query-level `SETTINGS` clause of a set operation is left by the parser on its last
    /// operand. `SelectIntersectExceptQueryVisitor` may then nest that operand into `INTERSECT` / `EXCEPT`
    /// nodes, as the last operand of each: `A UNION ALL B INTERSECT ALL C SETTINGS ...` becomes a union
    /// of `A` and `B INTERSECT ALL C`. Returns the clause of `last_operand`, descending through such
    /// nodes, or nullptr if there is none.
    static ASTPtr getTrailingSettings(const ASTPtr & last_operand);

    static const char * fromOperator(Operator op);

    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    /// Final operator after applying visitor.
    Operator final_operator = Operator::UNKNOWN;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
