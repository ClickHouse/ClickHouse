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

    /// `getTrailingSettings` infers the owner of a clause from its position: the clause found at the right edge
    /// of the last operand is taken as the clause of the whole set operation. That is right only for the clause
    /// that ends the query as written. The rewrites of set operations (`SelectIntersectExceptQueryVisitor`,
    /// `NormalizeSelectWithUnionQueryVisitor`) also move operands that are followed by other operands to the
    /// right edge of new nodes: `A UNION DISTINCT B SETTINGS x = 1 UNION ALL C` becomes a union of
    /// `A UNION DISTINCT B` and `C`, and `A EXCEPT ALL B INTERSECT ALL C SETTINGS x = 1 UNION ALL D` a union of
    /// `A EXCEPT ALL (B INTERSECT ALL C)` and `D`. Such an operand is passed here: if its clause would be visible
    /// to `getTrailingSettings`, the operand - or, if the clause ends an `INTERSECT` / `EXCEPT` nested into it, the
    /// innermost such node, which the clause applied to as written - is wrapped into a single-element
    /// `ASTSelectWithUnionQuery`, which `getTrailingSettings` does not look into. The clause then keeps
    /// applying to that node alone.
    static void scopeNonTrailingSettings(ASTPtr & operand);

    static const char * fromOperator(Operator op);

    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    /// Final operator after applying visitor.
    Operator final_operator = Operator::UNKNOWN;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
