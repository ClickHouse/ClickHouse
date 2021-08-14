#pragma once

#include <Core/Names.h>
#include <Parsers/IAST.h>


namespace DB
{
/** PROJECTION SELECT query
  */
class ASTProjectionSelectQuery : public IAST
{
public:
    enum class Expression : uint8_t
    {
        WITH,
        SELECT,
        WHERE,
        GROUP_BY,
        ORDER_BY,
    };

    /** Get the text that identifies this element. */
    String getID(char) const override { return "ProjectionSelectQuery"; }

    ASTPtr clone() const override;

    bool distinct = false;

    ASTPtr & refSelect() { return getExpression(Expression::SELECT); }
    ASTPtr & refWhere() { return getExpression(Expression::WHERE); }

    const ASTPtr with() const { return getExpression(Expression::WITH); }
    const ASTPtr select() const { return getExpression(Expression::SELECT); }
    const ASTPtr where() const { return getExpression(Expression::WHERE); }
    const ASTPtr groupBy() const { return getExpression(Expression::GROUP_BY); }
    const ASTPtr orderBy() const { return getExpression(Expression::ORDER_BY); }

    /// Set/Reset/Remove expression.
    void setExpression(Expression expr, ASTPtr && ast);

    ASTPtr getExpression(Expression expr, bool clone = false) const
    {
        auto it = positions.find(expr);
        if (it != positions.end())
            return clone ? children[it->second]->clone() : children[it->second];
        return {};
    }

    void updateTreeHashImpl(SipHash & hash_state) const override;

    ASTPtr cloneToASTSelect() const;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

private:
    std::unordered_map<Expression, size_t> positions;

    ASTPtr & getExpression(Expression expr);
};

}
