#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/** GROUP BY element carrying the optional `WITH CLUSTER <distance>` modifier
  * (e.g. `event_time WITH CLUSTER 1800`). `children[0]` is the expression;
  * `children[1]`, when present, is the distance literal.
  */
class ASTGroupByElement : public IAST
{
public:
    bool with_cluster = false;

    void setClusterDistance(ASTPtr node)
    {
        if (node == nullptr)
            return;
        if (children.size() < 2)
            children.resize(2);
        children[1] = std::move(node);
    }

    ASTPtr getClusterDistance() const
    {
        if (children.size() > 1)
            return children[1];
        return {};
    }

    ASTPtr getExpression() const
    {
        return children[0];
    }

    String getID(char) const override { return "GroupByElement"; }

    ASTPtr clone() const override
    {
        auto res = make_intrusive<ASTGroupByElement>(*this);
        res->cloneChildren();
        return res;
    }

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
